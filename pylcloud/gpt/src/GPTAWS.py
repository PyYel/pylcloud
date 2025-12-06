import json
import logging
from contextlib import closing
import requests
from uuid import uuid4
import base64
import os, sys
from io import BytesIO
from typing import List, Union, Any
import boto3
# from typing import Generator
from collections.abc import Generator
import heapq

from gpt import GPT
from pylcloud import _config_logger

class GPTAWS(GPT):
    """
    An helper that simplifies calls to LLM API.
    """
    def __init__(self, **kwargs):
        """
        Initializes a self-contained connection to the AWS Bedrock API.

        This only supports generative AI. 

        Parameters
        ----------
        
        """
        super().__init__()
        
        self.logger = _config_logger(logs_name="GPTAWS")
        
        self.bedrock_client = boto3.client(service_name="bedrock",
                                            aws_access_key_id=kwargs.get("AWS_ACCESS_KEY_ID"),
                                            aws_secret_access_key=kwargs.get("AWS_ACCESS_KEY_SECRET"),
                                            region_name=kwargs.get("AWS_REGION_NAME"),)
                                            #aws_session_token=aws_session_token)

        self.bedrock_runtime_client = boto3.client(service_name="bedrock-runtime",
                                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                                    aws_secret_access_key=AWS_ACCESS_KEY_SECRET,
                                                    region_name=AWS_REGION_NAME,)
                                                    # aws_session_token=aws_session_token)

        self.available_models = {
            "claude-4-sonnet": {
                "model_id": "eu.anthropic.claude-sonnet-4-20250514-v1:0" if AWS_REGION_NAME.startswith("eu") else "anthropic.claude-sonnet-4-20250514-v1:0", 
                "anthropic_version": "bedrock-2023-05-31"
                },
            "claude-3-haiku": {
                "model_id": "anthropic.claude-3-haiku-20240307-v1:0", 
                "anthropic_version": "bedrock-2023-05-31" # TODO: check old models versions and inference profiles
                },
            "nova-micro": {
                "model_id": "eu.amazon.nova-micro-v1:0" if AWS_REGION_NAME.startswith("eu") else "amazon.nova-micro-v1:0", 
                },
            "nova-lite": {
                "model_id": "eu.amazon.nova-lite-v1:0" if AWS_REGION_NAME.startswith("eu") else "amazon.nova-lite-v1:0", 
                },
            "nova-pro": {
                "model_id": "eu.amazon.nova-pro-v1:0" if AWS_REGION_NAME.startswith("eu") else "amazon.nova-pro-v1:0",
                },
            "titan-text-embeddings": {
                "model_id": "amazon.titan-embed-text-v2:0"
                },
            "titan-multimodal-embeddings": {
                "model_id": "amazon.titan-embed-image-v1"
                }
            }

        self.costs = {
            "claude-4-sonnet": {
                "input_tokens": 0.003*1e-3,
                "output_tokens": 0.015*1e-3
                },
            "claude-3-haiku": {
                "input_tokens": 0.00025*1e-3,
                "output_tokens": 0.00125*1e-3
                },
            "nova-micro": {
                "input_tokens": 0.000052*1e-3,
                "output_tokens": 0.000208*1e-3
                },
            "nova-lite": {
                "input_tokens": 0.000088*1e-3,
                "output_tokens": 0.000352*1e-3
                },
            "nova-pro": {
                "input_tokens": 0.00118*1e-3,
                "output_tokens": 0.00472*1e-3
                },
            "titan-text-embeddings": {
                "input_tokens": 0.00003*1e-3,
                "output_tokens": 0
                },
            "titan-multimodal-embeddings": {
                "input_tokens": 0.001*1e-3,
                "output_tokens": 0
                }
            }

        return None


    def return_embedding(self, 
                        model_name: str, 
                        prompt: str, 
                        files: List[Union[str, BytesIO]] = [], 
                        dimensions: int = 512) -> dict[str, Union[list[float], dict[str, int]]]:
        """
        Function to interact with an AWS Bedrock model using boto3.

        Parameters
        ----------
        model_name: str
            The human readable name of the model in AWS Bedrock to invoke. See Notes below.
        prompt: str 
            The input text to send to the model.
        files: list[str|BytesIO], []
            The list of files to add to the payload. Can be local paths, binary files, or both. Supports only images.
            TODO: add other file types.
        dimensions: int
            Can be used to specify vector output length when calling embedding models.

        Returns
        -------
        response_body: dict
            Response from the AWS Bedrock model.

        Notes
        -----
        - Selected ``model_name`` must be one of:
            - 'titan-text-embeddings'
            - 'titan-multimodal-embeddings'
        - When calling an embedding model, use the ``dimensions`` parameters to specify output length. Value must be one of:
            - 'titan-text-embeddings': 1024, 512, 256
            - 'titan-multimodal-embeddings': 1024, 384, 256

        Examples
        --------
        >>> print(return_generation(model_name='nova-lite', user_prompt='who are you?'))
        >>> {'embedding': [-0.12171094864606857, ..., -0.03663225471973419], 'usage': {'input_tokens': 5}
        """

        try:
            
            model_id = self.available_models[model_name]["model_id"]

            payload = {
                "inputText": prompt,
                "dimensions": dimensions if dimensions in [1024, 512, 384, 256] else 1024,
                "normalize": True
            }
            
            response = self.bedrock_runtime_client.invoke_model(
                modelId=model_id,
                accept='application/json',  
                contentType='application/json',  
                body=json.dumps(payload)
            )
            
            response_body = json.loads(response['body'].read())
            if "titan" in model_name:
                embedding = response_body["embedding"]
                usage = {"input_tokens": response_body["inputTextTokenCount"]}
            else:
                self.logger.warning(f"Invalid model name '{model_name}'.")
                return {}

            return {"embedding": embedding, "usage": usage}
        
        except Exception as e:
            self.logger.error(e)
            return {}
        

    def return_generation(self, 
                        model_name: str, 
                        user_prompt: str, 
                        system_prompt: str = "", 
                        assistant_prompt: str = "",
                        messages: list[dict[str, Any]] = [],
                        files: List[Union[str, BytesIO]] = [], 
                        max_tokens: int = 512,
                        temperature: float = 0.9,
                        top_k: int = 32,
                        top_p: float = 0.7) -> Union[dict, dict[str, Union[str, int]]]:
        """
        Function to interact with an AWS Bedrock model using boto3.

        Parameters
        ----------
        model_name: str
            The human readable name of the model in AWS Bedrock to invoke. See Notes below.
        user_prompt: str 
            The user's input text to send to the model.
        system_prompt: str 
            The system text to send to the model.
        assistant_prompt: str 
            Text sent as a previous assistant response. Usefull for providing context.
        messages: list[dict[str, str]]
            The conversation history to pass alongside the prompts queries.
        files: list[str|BytesIO], []
            The list of files to add to the payload. Can be local paths, binary files, or both. Supports only images.
            TODO: add other file types.
        max_tokens: int
            The maximum token length of the response. Will stop and truncate the generation when reached. 
        temperature: float
            The generation output randomness. Higher temperature result in more various answers. Must be between 0 and 1.
        top_k: int
            The number of token to suggest as possible output. 
            The model will then select one answer among the ``top_k`` possibilities.
        top_p: float
            The sum of confidence score to reach when adding the value of all suggested output scores.
            Low ``top_p`` will be reached quickly when adding the most likely outputs. Large ``top_p`` will lengthen 
            the possible outputs with less likely tokens, resulting in more random generation.

        Returns
        -------
        response_body: dict
            Response from the AWS Bedrock model.

        Notes
        -----
        - Selected ``model_name`` must be one of:
            - 'claude-4-sonnet'
            - 'claude-3-haiku'
            - 'nova-micro'
            - 'nova-lite'
            - 'nova-pro'
        - When calling an embedding model, see ``return_embedding()`` instead.

        Examples
        --------
        >>> print(return_generation(model_name='nova-lite', user_prompt='who are you?'))
        >>> {'text': 'I am AWS Nova', 'usage': {'input_tokens': 5, 'output_tokens': 8}}
        """

        try:
            
            model_id = self.available_models[model_name]["model_id"]

            payload = self._create_payload(model_name=model_name,
                                            user_prompt=user_prompt,
                                            system_prompt=system_prompt,
                                            assistant_prompt=assistant_prompt,
                                            messages=messages,
                                            files=files,
                                            temperature=temperature,
                                            top_k=top_k,
                                            top_p=top_p,
                                            max_tokens=max_tokens)
            
            response = self.bedrock_runtime_client.invoke_model(
                modelId=model_id,
                accept='application/json',  
                contentType='application/json',  
                body=json.dumps(payload)
            )
            
            response_body = json.loads(response['body'].read())
            if "claude" in model_name:
                text = response_body["content"][0]["text"]
                usage = response_body["usage"]
            elif "nova" in model_name:
                text = response_body["output"]["message"]["content"][0]["text"]
                usage = response_body["usage"]
            else:
                self.logger.warning(f"Invalid model name '{model_name}'.")
                return {}

            return {"text": text, "usage": usage}
        
        except Exception as e:
            self.logger.error(e)
            return {}


    def yield_generation(self, 
                        model_name: str, 
                        user_prompt: str, 
                        system_prompt: str = "", 
                        assistant_prompt: str = "",
                        files: List[Union[str, BytesIO]] = [], 
                        max_tokens: int = 512,
                        temperature: float = 0.9,
                        top_k: int = 32,
                        top_p: float = 0.7) -> Union[dict, Generator[dict[str, Union[str, int]]]]:
        """
        Function to interact with an AWS Bedrock model using boto3.

        Parameters
        ----------
        model_name: str
            The human readable name of the model in AWS Bedrock to invoke. See Notes below.
        user_prompt: str 
            The user's input text to send to the model.
        system_prompt: str 
            The system text to send to the model.
        assistant_prompt: str 
            Text sent as a previous assistant response. Usefull for providing context.
        messages: list[dict[str, str]]
            The conversation history to pass alongside the prompts queries.
        files: list[str|BytesIO], []
            The list of files to add to the payload. Can be local paths, binary files, or both. Supports only images.
            TODO: add other file types.
        max_tokens: int
            The maximum token length of the response. Will stop and truncate the generation when reached. 
        temperature: float
            The generation output randomness. Higher temperature result in more various answers. Must be between 0 and 1.
        top_k: int
            The number of token to suggest as possible output. 
            The model will then select one answer among the ``top_k`` possibilities.
        top_p: float
            The sum of confidence score to reach when adding the value of all suggested output scores.
            Low ``top_p`` will be reached quickly when adding the most likely outputs. Large ``top_p`` will lengthen 
            the possible outputs with less likely tokens, resulting in more random generation.
 
        Yields
        -------
        item: str
            The current iterrated token, as string.
        final_item: dict[str, Any]
            The final response containing the whole generation result and its metadata. 

        Notes
        -----
        - Selected ``model_name`` must be one of:
            - 'claude-4-sonnet'
            - 'claude-3-haiku'
            - 'nova-micro'
            - 'nova-lite'
            - 'nova-pro'
        - When calling an embedding model, see ``return_embedding()`` instead.

        Examples
        --------
        >>> for token in yield_generation(model_name='nova-micro', user_prompt='Who are you?'): print(token)
        >>>     'I am'
        >>>     'AWS'
        >>>     'Nova'
        >>>     {'text': 'I am AWS Nova', 'usage': {'input_tokens': 5, 'output_tokens': 8}}
        """

        try:
                
            model_id = self.available_models[model_name]["model_id"]

            payload = self._create_payload(model_name=model_name,
                                            user_prompt=user_prompt,
                                            system_prompt=system_prompt,
                                            assistant_prompt=assistant_prompt,
                                            files=files,
                                            temperature=temperature,
                                            top_k=top_k,
                                            top_p=top_p,
                                            max_tokens=max_tokens)
            
            response = self.bedrock_runtime_client.invoke_model_with_response_stream(
                modelId=model_id,
                accept='application/json',  
                contentType='application/json',  
                body=json.dumps(payload)
            )

            text = ""
            buffer = []
            expected_seq = 0
            for event in response["body"]:
                chunk = json.loads(event["chunk"]["bytes"])

                # Anthropic
                if "type" in chunk:
                    if chunk["type"] == "content_block_delta":
                        seq = chunk.get("delta", {}).get("index", expected_seq)
                        heapq.heappush(buffer, (seq, chunk["delta"].get("text", "")))

                        # flush in-order chunks
                        while buffer and buffer[0][0] == expected_seq:
                            _, t = heapq.heappop(buffer)
                            text += t
                            yield t
                            expected_seq += 1

                    elif chunk["type"] == "message_stop":
                        usage = {
                            "input_tokens": chunk.get("amazon-bedrock-invocationMetrics", {}).get("inputTokenCount", 0),
                            "output_tokens": chunk.get("amazon-bedrock-invocationMetrics", {}).get("outputTokenCount", 0),
                        }
                        yield {"text": text, "usage": usage}

                # Nova
                else:
                    if "contentBlockDelta" in chunk:
                        seq = chunk["contentBlockDelta"].get("index", expected_seq)
                        heapq.heappush(buffer, (seq, chunk["contentBlockDelta"]["delta"].get("text", "")))

                        while buffer and buffer[0][0] == expected_seq:
                            _, t = heapq.heappop(buffer)
                            text += t
                            yield t
                            expected_seq += 1

                    elif "metadata" in chunk:
                        usage = {
                            "input_tokens": chunk["metadata"]["usage"].get("inputTokenCount", 0),
                            "output_tokens": chunk["metadata"]["usage"].get("outputTokenCount", 0),
                        }
                        yield {"text": text, "usage": usage}
                        
        except Exception as e:
            self.logger.error(e)
            return {}


    def list_models(self, display: bool = False):
        """
        List the available Amazon Bedrock foundation models.
        
        Parameters
        ----------
        display: bool, False
            Whereas to print in the terminal the model list, or not.

        Returns 
        -------
        models_list: list[str]
            The list of available bedrock foundation models.
        """

        response = self.bedrock_client.list_foundation_models()
        models_list = response["modelSummaries"]
        if display: print(models_list)

        return models_list
    

    def _create_payload(self, 
                        model_name: str, 
                        user_prompt: str, 
                        system_prompt: str = "", 
                        assistant_prompt: str = "",
                        messages: list[dict[str, Any]] = [],
                        files: list[Union[str, BytesIO]] = [], 
                        temperature: float = 0.9,
                        top_k: int = 32,
                        top_p: float = 0.8,
                        max_tokens: int = 512):
        """
        Formats a payload that respects the API models.
        """
        
        def _anthropic():
            content = [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/jpeg",
                        "data": file
                    }
                }
                for file in self._process_files(files=files) if model_name not in ["nova-micro"]
            ]
            content.append(
                {
                    "type": "text",
                    "text": user_prompt
                }
            )
            payload = {
                "anthropic_version": self.available_models[model_name]["anthropic_version"],
                "max_tokens": max_tokens,
                "stop_sequences": [],
                "temperature": temperature,
                "top_p": top_p,
                "top_k": top_k,
                "system": system_prompt,
                "messages": messages + [
                    {
                        "role": "user",
                        "content": content
                    },
                    {
                        "role": "assistant",
                        "content": [{"type": "text", "text": assistant_prompt}]
                    }
                ]
            }
            return payload

        def _nova():
            content = [
                {
                "image": {
                    "format": "png",
                    "source": {"bytes": file}
                    }
                }
                for file in self._process_files(files=files) if model_name not in ["nova-micro"]
            ]
            content.append({"text": user_prompt}) # type: ignore
            payload = {
                "system": [{"text": system_prompt}],
                "messages": messages + [
                    {
                        "role": "user",
                        "content": content
                    },
                    {
                        "role": "assistant",
                        "content": [{"text": assistant_prompt}]
                    }
                ],
                "inferenceConfig": {
                    "maxTokens": max_tokens,
                    "temperature": temperature,
                    "topP": top_p,
                    "topK": top_k,
                }
            }
            return payload
        

        if "claude" in model_name:
            return _anthropic()
        elif "nova" in model_name:
            return _nova()
        else:
            self.logger.warning(f"Could not initialize a payload for model '{model_name}'.")
            return {}


    def _process_files(self, files: list[Union[str, BytesIO]] = []) -> list[str]:
        """
        Processes a list of binary files, or file paths, or invalid files, and returns it as a pure list of valid binary files.
        """

        if isinstance(files, str):
            files = [files]
        if not isinstance(files, list):
            print(f"AWSBedrockModels >> Invalid file input, should be ``list``, got ``{type(files)}``.")
            return []

        processed_files = []
        for file in files:
            if isinstance(file, str):
                try:
                    with open(file, "rb") as f:
                        file_content = f.read()
                        processed_files.append(BytesIO(file_content))
                except Exception as e:
                    print(f"AWSBedrockModels >> Error reading file '{file}': {e}")
            elif isinstance(file, BytesIO):
                processed_files.append(file)
            else:
                print(f"AWSBedrockModels >> Invalid file type in payload, must be ``str`` or ``BytesIO``, got ``{type(file)}``.")

        return [base64.b64encode(file.getvalue()).decode('utf8') for file in processed_files]



    def _reset_session(self):
        """
        Creates a new session, by replacind the current session ID with a new UUID4.
        """
        self.session_id = str(uuid4())
        return None
    
