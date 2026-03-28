import re
import json
import logging
import base64
import os
import sys
from io import BytesIO
from typing import List, Union, Any, Callable, Optional, TypedDict
from collections.abc import Generator
from uuid import uuid4

import boto3

from .GPT import GPT, GPTResponse
from pylcloud import _config_logger


class GPTAWS(GPT):
    """
    Helper that simplifies calls to AWS Bedrock LLM and embedding models.

    All generative calls (non-streaming, streaming, agentic) use the Bedrock
    Converse API, which provides a unified request/response format across all
    supported model families (Claude, Nova, etc.) and natively handles tool use.

    Embedding calls use invoke_model, as the Converse API does not support
    encoder-only models.

    Thinking behaviour
    ------------------
    All generative methods accept a ``thinking`` parameter:
    - ``None``: let the model decide (Nova default, Claude off)
    - ``True``: force thinking on
    - ``False``: force thinking off (Nova only, no-op for Claude)

    All generative methods return a ``ModelOutput`` dict with keys
    ``"thinking"`` (str | None) and ``"text"`` (str), parsed transparently
    regardless of model family. The ``"thinking"`` field is ``None`` when the
    model did not produce a reasoning trace.

    Thinking capability per model
    ------------------------------
    - nova-pro, nova-lite : think autonomously; can be forced on/off
    - claude-4-sonnet     : does not think by default; can be forced on
    - nova-micro          : no thinking support
    - claude-3-haiku      : no thinking support
    """

    def __init__(
        self,
        AWS_REGION_NAME: str = os.getenv("AWS_REGION_NAME", "eu-west-1"),
        AWS_ACCESS_KEY_ID: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID", None),
        AWS_ACCESS_KEY_SECRET: Optional[str] = os.getenv("AWS_ACCESS_KEY_SECRET", None),
        **kwargs,
    ):
        """
        All generative calls (non-streaming, streaming, agentic) use the Bedrock
        Converse API, which provides a unified request/response format across all
        supported model families (Claude, Nova, etc.) and natively handles tool use.

        Embedding calls use invoke_model, as the Converse API does not support
        encoder-only models.

        Thinking behaviour
        ------------------
        All generative methods accept a ``thinking`` parameter:
        - ``None``: let the model decide (Nova default, Claude off)
        - ``True``: force thinking on
        - ``False``: force thinking off (Nova only, no-op for Claude)

        All generative methods return a ``ModelOutput`` dict with keys
        ``"thinking"`` (str | None) and ``"text"`` (str), parsed transparently
        regardless of model family. The ``"thinking"`` field is ``None`` when the
        model did not produce a reasoning trace.

        Thinking capability per model
        ------------------------------
        - nova-pro, nova-lite : think autonomously; can be forced on/off
        - claude-4-sonnet     : does not think by default; can be forced on
        - nova-micro          : no thinking support
        - claude-3-haiku      : no thinking support
        """
        super().__init__()

        self.logger = _config_logger(logs_name="GPTAWS")

        self.AWS_REGION_NAME = AWS_REGION_NAME

        self.bedrock_client = boto3.client(
            service_name="bedrock",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_ACCESS_KEY_SECRET,
            region_name=AWS_REGION_NAME,
        )

        self.bedrock_runtime_client = boto3.client(
            service_name="bedrock-runtime",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_ACCESS_KEY_SECRET,
            region_name=AWS_REGION_NAME,
        )

        # Supported models, with inference profiles for eu.
        # "thinking" flags which models support the thinking parameter.
        self.generative_models = {
            "claude-4-sonnet": {
                "model_id": (
                    "eu.anthropic.claude-sonnet-4-20250514-v1:0"
                    if self.AWS_REGION_NAME.startswith("eu")
                    else "anthropic.claude-sonnet-4-20250514-v1:0"
                ),
                "thinking": True,
            },
            "claude-3-haiku": {
                "model_id": "anthropic.claude-3-haiku-20240307-v1:0",
                "thinking": False,
            },
            "nova-micro": {
                "model_id": (
                    "eu.amazon.nova-micro-v1:0"
                    if self.AWS_REGION_NAME.startswith("eu")
                    else "amazon.nova-micro-v1:0"
                ),
                "thinking": False,
            },
            "nova-lite": {
                "model_id": (
                    "eu.amazon.nova-lite-v1:0"
                    if self.AWS_REGION_NAME.startswith("eu")
                    else "amazon.nova-lite-v1:0"
                ),
                "thinking": True,
            },
            "nova-pro": {
                "model_id": (
                    "eu.amazon.nova-pro-v1:0"
                    if self.AWS_REGION_NAME.startswith("eu")
                    else "amazon.nova-pro-v1:0"
                ),
                "thinking": True,
            },
        }

        # Will default to latest dimension for invalid dimension intput
        self.embedding_models = {
            "titan-text-embeddings": {
                "model_id": "amazon.titan-embed-text-v2:0",
                "supported_dimensions": [1024, 512, 256],
            },
            "titan-multimodal-embeddings": {
                "model_id": "amazon.titan-embed-image-v1",
                "supported_dimensions": [1024, 384, 256],
            },
        }

        self.available_models = {**self.generative_models, **self.embedding_models}

        # See AWS pricing. Costs for eu-west-3 (Paris). Can be overwritten.
        self.costs = {
            "claude-4-sonnet": {
                "input_tokens": 0.003 * 1e-3,
                "output_tokens": 0.015 * 1e-3,
            },
            "claude-3-haiku": {
                "input_tokens": 0.00025 * 1e-3,
                "output_tokens": 0.00125 * 1e-3,
            },
            "nova-micro": {
                "input_tokens": 0.000052 * 1e-3,
                "output_tokens": 0.000208 * 1e-3,
            },
            "nova-lite": {
                "input_tokens": 0.000088 * 1e-3,
                "output_tokens": 0.000352 * 1e-3,
            },
            "nova-pro": {
                "input_tokens": 0.00118 * 1e-3,
                "output_tokens": 0.00472 * 1e-3,
            },
            "titan-text-embeddings": {
                "input_tokens": 0.00003 * 1e-3,
                "output_tokens": 0,
            },
            "titan-multimodal-embeddings": {
                "input_tokens": 0.001 * 1e-3,
                "output_tokens": 0,
            },
        }

    def return_embedding(
        self,
        model_name: str,
        prompt: str,
        dimensions: int = 256,
    ) -> dict[str, Union[list[float], dict[str, int]]]:
        """
        Generate a vector embedding for the given text using an encoder model.

        Parameters
        ----------
        model_name: str
            Embedding model to use. Must be one of:
            - 'titan-text-embeddings'
            - 'titan-multimodal-embeddings'
        prompt: str
            Input text to embed.
        dimensions: int
            Output vector length. Must match a supported dimension for the model.
            Falls back to smallest dimesion if unsupported.
            - 'titan-text-embeddings': 1024 | 512 | 256
            - 'titan-multimodal-embeddings': 1024 | 384 | 256

        Returns
        -------
        dict with keys:
            - "embedding": list[float]
            - "usage": {"input_tokens": int, "output_tokens": 0}

        Examples
        --------
        >>> result = gpt.return_embedding("titan-text-embeddings", "Hello world", dimensions=512)
        >>> print(result)
        ... {'embedding': [-0.12171094864606857, ..., -0.03663225471973419], 'usage': {'input_tokens': 5, 'output_tokens': 0}
        >>> print(len(result["embedding"]))
        ... 512
        """
        try:
            if model_name not in self.embedding_models:
                raise ValueError(
                    f"'{model_name}' is not an embedding model. "
                    f"Choose from: {list(self.embedding_models)}"
                )

            model_cfg = self.embedding_models[model_name]
            supported = model_cfg["supported_dimensions"]
            resolved_dim = dimensions if dimensions in supported else supported[-1]

            if dimensions not in supported:
                self.logger.warning(
                    f"Unsupported dimension {dimensions} for '{model_name}'. "
                    f"Falling back to {resolved_dim}."
                )

            payload = {
                "inputText": prompt,
                "dimensions": resolved_dim,
                "normalize": True,
            }

            response = self.bedrock_runtime_client.invoke_model(
                modelId=model_cfg["model_id"],
                accept="application/json",
                contentType="application/json",
                body=json.dumps(payload),
            )

            body = json.loads(response["body"].read())

            return {
                "embedding": body["embedding"],
                "usage": {
                    "input_tokens": body["inputTextTokenCount"],
                    "output_tokens": 0,  # Pricing depends on input only
                },
            }

        except Exception as e:
            self.logger.error(e)
            return {}

    def return_generation(
        self,
        model_name: str,
        user_prompt: str,
        system_prompt: str = "",
        messages: list[dict[str, Any]] = [],
        files: List[Union[str, BytesIO]] = [],
        max_tokens: int = 512,
        temperature: float = 0.9,
        top_p: float = 0.7,
        thinking_allowed: Optional[bool] = None,
        thinking_budget: int = 8000,
        thinking_effort: str = "low",
    ) -> Union[GPTResponse, dict]:
        """
        Single-turn (non-streaming) text generation.

        Parameters
        ----------
        model_name: str
            Generative model to use. Must be one of:
            - 'claude-4-sonnet'
            - 'claude-3-haiku'
            - 'nova-micro'
            - 'nova-lite'
            - 'nova-pro'
        user_prompt: str
            The user's message for this turn.
        system_prompt: str
            System-level instructions for the conversation.
        messages: list[dict]
            Prior conversation turns in Converse format:
            [{"role": "user"|"assistant", "content": [{"text": "..."}]}, ...]
            The current user_prompt is appended automatically.
        files: list[str | BytesIO]
            Images to attach to the user turn (paths or binary objects).
            Not supported by nova-micro.
        max_tokens: int
            Maximum tokens in the response.
        temperature: float
            Sampling temperature (0-1).
        top_p: float
            Output probalities p sampling limit (0-1).
        thinking_allowed: bool | None
            None: let the model decide (Nova default, Claude off).
            True: force thinking on.
            False: force thinking off (Nova only, no-op for Claude).
        thinking_budget: int
            Maximum thinking tokens for Claude extended thinking.
            Must be less than max_tokens. Defaults to 8000.
        thinking_effort: str
            Nova reasoning effort level: "low" | "medium" | "high".
            Defaults to "low".

        Returns
        -------
        dict with keys:
            - "thinking": str | None
            - "text": str
            - "usage": {"input_tokens": int, "output_tokens": int}

        Examples
        --------
        >>> result = gpt.return_generation("nova-lite", user_prompt="Who are you?")
        >>> print(result["text"])
        >>> print(result["thinking"])   # None if model did not think
        """
        try:
            full_messages = self._build_messages(
                model_name=model_name,
                user_prompt=user_prompt,
                messages=messages,
                files=files,
            )

            request = self._build_converse_request(
                model_name=model_name,
                messages=full_messages,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
                top_p=top_p,
                thinking_allowed=thinking_allowed,
                thinking_budget=thinking_budget,
                thinking_effort=thinking_effort,
            )

            response = self.bedrock_runtime_client.converse(**request)

            raw_content = response["output"]["message"]["content"]
            raw_text = next((b["text"] for b in raw_content if "text" in b), "")
            thinking, text = self.parse_model_output(raw_text, raw_content)
            usage = {
                "input_tokens": response["usage"]["inputTokens"],
                "output_tokens": response["usage"]["outputTokens"],
            }

            return {"thinking": thinking, "text": text, "usage": usage}

        except Exception as e:
            self.logger.error(e)
            return {}

    def yield_generation(
        self,
        model_name: str,
        user_prompt: str,
        system_prompt: str = "",
        messages: list[dict[str, Any]] = [],
        files: List[Union[str, BytesIO]] = [],
        max_tokens: int = 512,
        temperature: float = 0.9,
        top_p: float = 0.7,
        thinking_allowed: Optional[bool] = None,
        thinking_effort: str = "low",
    ) -> Generator[Union[GPTResponse, dict]]:
        """
        Streaming text generation.

        Parameters
        ----------
        model_name: str
            Generative model to use. Must be one of:
            - 'claude-4-sonnet'
            - 'claude-3-haiku'
            - 'nova-micro'
            - 'nova-lite'
            - 'nova-pro'
        user_prompt: str
            The user's message for this turn.
        system_prompt: str
            System-level instructions for the conversation.
        messages: list[dict]
            Prior conversation turns in Converse format:
            [{"role": "user"|"assistant", "content": [{"text": "..."}]}, ...]
            The current user_prompt is appended automatically.
        files: list[str | BytesIO]
            Images to attach to the user turn (paths or binary objects).
            Not supported by nova-micro.
        max_tokens: int
            Maximum tokens in the response.
        temperature: float
            Sampling temperature (0-1).
        top_p: float
            Output probalities p sampling limit (0-1).
        thinking_allowed: bool | None
            None: let the model decide (Nova default, Claude off).
            True: force thinking on (not supported for streaming on Claude; falls back to None).
            False: force thinking off (Nova only).
        thinking_effort: str
            Nova reasoning effort level: "low" | "medium" | "high".
            Defaults to "low".

        Yields
        ------
        str
            Each text token as it is generated (includes Nova thinking tags if present).
        dict
            Final item: {"thinking": str | None, "text": str, "usage": {"input_tokens": int, "output_tokens": int}}

        Notes
        -----
        Claude extended thinking is incompatible with streaming and will be
        ignored if ``thinking=True`` is passed for a Claude model. Nova thinking
        tags are streamed inline and parsed in the final summary dict only.

        Examples
        --------
        >>> for token in gpt.yield_generation("nova-micro", user_prompt="Who are you?"):
        ...     if isinstance(token, str):
        ...         print(token, end="", flush=True)
        ...     else:
        ...         print(token["thinking"], token["usage"])
        """
        try:
            # Claude extended thinking is incompatible with streaming
            if "claude" in model_name and thinking_allowed is True:
                self.logger.warning(
                    "Claude extended thinking is not supported with streaming. "
                    "Ignoring thinking=True."
                )
                thinking_allowed = None

            full_messages = self._build_messages(
                model_name=model_name,
                user_prompt=user_prompt,
                messages=messages,
                files=files,
            )

            request = self._build_converse_request(
                model_name=model_name,
                messages=full_messages,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
                top_p=top_p,
                thinking_allowed=thinking_allowed,
                thinking_effort=thinking_effort,
            )

            response = self.bedrock_runtime_client.converse_stream(**request)

            text = ""
            usage = {"input_tokens": 0, "output_tokens": 0}

            for event in response["stream"]:
                # Text delta
                if "contentBlockDelta" in event:
                    delta = event["contentBlockDelta"].get("delta", {})
                    token = delta.get("text", "")
                    if token:
                        text += token
                        yield token

                # Final usage metadata
                elif "metadata" in event:
                    meta_usage = event["metadata"].get("usage", {})
                    usage = {
                        "input_tokens": meta_usage.get("inputTokens", 0),
                        "output_tokens": meta_usage.get("outputTokens", 0),
                    }

            # Parse thinking from the fully accumulated text at the end
            thinking, text = self.parse_model_output(text)

            yield {"thinking": thinking, "text": text, "usage": usage}

        except Exception as e:
            self.logger.error(e)
            yield {}

    def return_agent(
        self,
        model_name: str,
        user_prompt: str,
        tools: list[dict[str, Any]],
        tool_handler: Callable,
        system_prompt: str = "",
        messages: list[dict[str, Any]] = [],
        max_tokens: int = 1024,
        temperature: float = 0.9,
        top_p: float = 0.7,
        thinking_allowed: Optional[bool] = None,
        thinking_budget: int = 4000,
        thinking_effort: str = "low",
        max_iterations: int = 10,
    ) -> tuple[Union[GPTResponse, dict], dict]:
        """
        Agentic generation loop via the Converse API with tool use.

        Runs a synchronous request -> tool execution -> request cycle until
        the model returns end_turn or the iteration limit is reached. Does not
        stream, as intermediate tool-call rounds are not meaningful to stream.

        Parameters
        ----------
        model_name: str
            Generative model to use. Must be one of:
            - 'claude-4-sonnet'
            - 'claude-3-haiku'
            - 'nova-micro'
            - 'nova-lite'
            - 'nova-pro'
        user_prompt: str
            The user's message that starts this agent run.
        tools: list[dict]
            Tool definitions in Converse toolSpec format:
            [{
                "toolSpec": {
                    "name": "tool_name",
                    "description": "What this tool does.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {"arg": {"type": "string"}},
                            "required": ["arg"]
                        }
                    }
                }
            }]
        tool_handler: Callable
            A function that dispatches tool calls.
            Signature: tool_handler(name: str, inputs: dict) -> str
            Must be synchronous. For async handlers, wrap with ``asyncio.run()``.
        system_prompt: str
            System-level instructions for the agent.
        messages: list[dict]
            Prior conversation turns in Converse format (for multi-turn agents).
        max_tokens: int
            Maximum tokens per individual inference call.
        temperature: float
            Sampling temperature (0-1).
        top_p: float
            Output probalities p sampling limit (0-1).
        thinking_allowed: bool | None
            None: let the model decide (Nova default, Claude off).
            True: force thinking on.
            False: force thinking off (Nova only, no-op for Claude).
        thinking_budget: int
            Maximum thinking tokens for Claude extended thinking.
            Must be less than max_tokens. Defaults to 8000.
        thinking_effort: str
            Nova reasoning effort level: "low" | "medium" | "high".
            Defaults to "low".
        max_iterations: int
            Safety limit on the number of tool-call rounds. Prevents runaway
            loops from buggy tools or confused models. Defaults to 10.

        Returns
        -------
        dict with keys:
            - "thinking": str | None, reasoning trace from the final answer turn
            - "text": str, the model's final text answer
            - "usage": {"input_tokens": int, "output_tokens": int}, cumulative across all inference calls in the loop
            - "iterations": int, number of tool-call rounds executed
            - "messages": list, full raw conversation history (never filtered)

        Examples
        --------
        >>> def my_handler(name, inputs):
        ...     if name == "search":
        ...         return search_vector_store(inputs["query"])
        ...     return "Unknown tool"

        >>> result = gpt.return_agent(
        ...     model_name="claude-4-sonnet",
        ...     user_prompt="Find documents about solar panels.",
        ...     tools=TOOLS_SPEC,
        ...     tool_handler=my_handler,
        ...     system_prompt="You are a helpful assistant.",
        ... )
        >>> print(result["text"])
        >>> print(result["thinking"])    # None if model did not think
        >>> print(result["usage"])       # total cost across all rounds
        >>> print(result["iterations"])  # how many tool calls were made
        """
        try:
            # Append user turn to history
            history = list(messages) + [
                {"role": "user", "content": [{"text": user_prompt}]}
            ]

            total_usage = {"input_tokens": 0, "output_tokens": 0}
            iterations = 0

            for _ in range(max_iterations):
                request = self._build_converse_request(
                    model_name=model_name,
                    messages=history,
                    system_prompt=system_prompt,
                    tools=tools,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    top_p=top_p,
                    thinking_allowed=thinking_allowed,
                    thinking_budget=thinking_budget,
                    thinking_effort=thinking_effort,
                )

                response = self.bedrock_runtime_client.converse(**request)

                # Accumulate token usage across all agent rounds
                total_usage["input_tokens"] += response["usage"]["inputTokens"]
                total_usage["output_tokens"] += response["usage"]["outputTokens"]

                stop_reason = response["stopReason"]
                raw_content = response["output"]["message"]["content"]

                # Bedrock requires consistent history including assistant tool-call turns
                history.append({"role": "assistant", "content": raw_content})

                # Model returned a final answer
                if stop_reason == "end_turn":
                    raw_text = next((b["text"] for b in raw_content if "text" in b), "")
                    thinking, text = self.parse_model_output(raw_text, raw_content)
                    return {"thinking": thinking, "text": text, "usage": total_usage}, {"iterations": iterations,"messages": history}

                # Tool call round
                elif stop_reason == "tool_use":
                    iterations += 1
                    tool_results = []

                    for block in raw_content:
                        if "toolUse" not in block:
                            continue

                        tool_use = block["toolUse"]
                        tool_name = tool_use["name"]
                        tool_inputs = tool_use["input"]
                        tool_use_id = tool_use["toolUseId"]

                        try:
                            output = tool_handler(tool_name, tool_inputs)
                            tool_results.append(
                                {
                                    "toolResult": {
                                        "toolUseId": tool_use_id,
                                        "content": [{"text": str(output)}],
                                        "status": "success",
                                    }
                                }
                            )
                        except Exception as tool_err:
                            self.logger.warning(
                                f"Tool '{tool_name}' raised an error: {tool_err}"
                            )
                            tool_results.append(
                                {
                                    "toolResult": {
                                        "toolUseId": tool_use_id,
                                        "content": [{"text": f"Error: {tool_err}"}],
                                        "status": "error",
                                    }
                                }
                            )

                    # Tool results count as a user turn to respect the
                    # user / assistant(tool_use) / user(tool_result) / ... format
                    history.append({"role": "user", "content": tool_results})

                # Token limit guardrail
                elif stop_reason == "max_tokens":
                    self.logger.warning(
                        "Agent hit max_tokens before finishing. "
                        "Consider increasing max_tokens."
                    )
                    raw_text = next((b["text"] for b in raw_content if "text" in b), "")
                    thinking, text = self.parse_model_output(raw_text, raw_content)
                    return {"thinking": thinking, "text": text, "usage": total_usage}, {"iterations": iterations,"messages": history}

                else:
                    self.logger.warning(f"Unexpected stop_reason: '{stop_reason}'.")
                    break

            # Reached iteration limit without end_turn
            self.logger.warning(
                f"Agent reached max_iterations ({max_iterations}) without finishing."
            )
            return {"thinking": None, "text": "", "usage": total_usage}, {"iterations": iterations,"messages": history}

        except Exception as e:
            self.logger.error(e)
            return {}, {}

    def compute_costs(self, model_name: str, usage: dict[str, int]) -> dict[str, float]:
        """
        Compute the dollar cost of an inference call given its token usage.

        Parameters
        ----------
        model_name: str
            Model key as used in this class.
        usage: dict
            {"input_tokens": int, "output_tokens": int}

        Returns
        -------
        dict with keys:
            - "input_cost" : float
            - "output_cost": float
            - "total_cost" : float
        """
        rates = self.costs.get(model_name, {})
        input_cost = usage.get("input_tokens", 0) * rates.get("input_tokens", 0)
        output_cost = usage.get("output_tokens", 0) * rates.get("output_tokens", 0)
        return {
            "input_cost": input_cost,
            "output_cost": output_cost,
            "total_cost": input_cost + output_cost,
        }

    def _build_converse_request(
        self,
        model_name: str,
        messages: list[dict[str, Any]],
        system_prompt: str = "",
        tools: list[dict[str, Any]] = [],
        max_tokens: int = 512,
        temperature: float = 0.9,
        top_p: float = 0.7,
        thinking_allowed: Optional[bool] = None,
        thinking_budget: int = 8000,
        thinking_effort: str = "low",
    ) -> dict[str, Any]:
        """
        Build a Converse (or converse_stream) request dict.

        Notes
        -----
        - thinking_allowed=True on Claude forces temperature=1 as required by the API.
        - thinking_allowed is silently ignored for models that do not support it
          (nova-micro, claude-3-haiku).
        """
        if model_name not in self.generative_models:
            raise ValueError(
                f"'{model_name}' is not a generative model. "
                f"Choose from: {list(self.generative_models)}"
            )

        request: dict[str, Any] = {
            "modelId": self.generative_models[model_name]["model_id"],
            "messages": messages,
            "inferenceConfig": {
                "maxTokens": max_tokens,
                "temperature": temperature,
                "topP": top_p,
            },
        }

        if system_prompt:
            request["system"] = [{"text": system_prompt}]

        if tools:
            request["toolConfig"] = {"tools": tools}

        # Thinking config: only applied when explicitly requested and supported
        model_supports_thinking = self.generative_models[model_name].get("thinking", False)

        if thinking_allowed is not None and not model_supports_thinking:
            self.logger.warning(
                f"Model '{model_name}' does not support thinking. Ignoring thinking parameter."
            )

        elif thinking_allowed is not None and model_supports_thinking:
            if "nova" in model_name:
                request["additionalModelRequestFields"] = {
                    "reasoningConfig": {
                        "type": "enabled" if thinking_allowed else "disabled",
                        **({"maxReasoningEffort": thinking_effort} if thinking_allowed else {}),
                    }
                }
            elif "claude" in model_name and thinking_allowed:
                # Claude requires temperature=1 when extended thinking is enabled.
                # Claude thinking=False: omitting the field is equivalent to disabled.
                request["inferenceConfig"]["temperature"] = 1
                request["additionalModelRequestFields"] = {
                    "reasoningConfig": {
                        "type": "enabled",
                        "maxReasoningEffort": thinking_effort,
                        "budgetTokens": min(thinking_budget, max_tokens - 1),
                    }
                }

        return request

    def _build_messages(
        self,
        model_name: str,
        user_prompt: str,
        messages: list[dict[str, Any]] = [],
        files: List[Union[str, BytesIO]] = [],
    ) -> list[dict[str, Any]]:
        """
        Append a new user turn (text + optional images) to an existing history.

        Returns the full messages list ready to pass to a Converse request.

        Notes
        -----
        - Images are not supported for ``nova-micro``.
        """
        content: list[dict[str, Any]] = []

        if files and model_name != "nova-micro":
            for b64_data in self._process_files(files):
                content.append(
                    {
                        "image": {
                            "format": "jpeg",
                            "source": {"bytes": base64.b64decode(b64_data)},
                        }
                    }
                )

        content.append({"text": user_prompt})

        return list(messages) + [{"role": "user", "content": content}]

    def _process_files(self, files: list[Union[str, BytesIO]] = []) -> list[str]:
        """
        Normalise a mixed list of file paths and BytesIO objects into a list
        of base64-encoded strings.
        """
        if isinstance(files, str):
            files = [files]
        if not isinstance(files, list):
            self.logger.warning(
                f"Invalid file input: expected list, got {type(files)}."
            )
            return []

        processed = []
        for f in files:
            if isinstance(f, str):
                try:
                    with open(f, "rb") as fh:
                        processed.append(BytesIO(fh.read()))
                except Exception as e:
                    self.logger.warning(f"Could not read file '{f}': {e}")
            elif isinstance(f, BytesIO):
                processed.append(f)
            else:
                self.logger.warning(
                    f"Invalid file type {type(f)}: expected str or BytesIO."
                )

        return [base64.b64encode(f.getvalue()).decode("utf-8") for f in processed]