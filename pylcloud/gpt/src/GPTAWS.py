import websocket
import json
import logging
from contextlib import closing
import websocket
import requests
from uuid import uuid4
import base64
import os, sys
from io import BytesIO
from typing import List, Union, Any
import boto3
# from typing import Generator
from collections.abc import Generator


from gpt import GPT
from constants import AWS_ACCESS_KEY_SECRET, AWS_ACCESS_KEY_ID, AWS_REGION_NAME, LOGS_DIR


class GPTAWS(GPT):
    """
    An helper that simplifies calls to LLM API.
    """
    def __init__(self, **kwargs):
        """
        Initializes a self-contained connection to the AWS Bedrock API.

        This only supports generative AI. 
        """
        super().__init__(logs_name="GPTAWS", logs_dir=LOGS_DIR)
    
        
        self.bedrock_client = boto3.client(service_name="bedrock",
                                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                                            aws_secret_access_key=AWS_ACCESS_KEY_SECRET,
                                            region_name=AWS_REGION_NAME,)
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
                }
            }

        return None


    def return_query(self, 
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
            The human readable name of the model in AWS Bedrock to invoke. 
            See ``list_models()`` for the exact ID name. See the note below for the models names.
        user_prompt: str 
            The user's input text to send to the model.
        system_prompt: str 
            The system text to send to the model.
        assistant_prompt: str 
            Text sent as a previous assistant response. Usefull for providing context.
        files: list[str|BytesIO], []
            The list of files to add to the payload. Can be local paths, binary files, or both. Supports only images.
            TODO: add other file types.
        display: bool, False
            Whereas to print in the terminal the model response, or not.
 
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

        Examples
        --------
        >>> print(return_query(model_name='nova-lite', user_prompt='who are you?'))
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


    def yield_query(self, 
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
            The human readable name of the model in AWS Bedrock to invoke. 
            See ``list_models()`` for the exact ID name. See the note below for the models names.
        user_prompt: str 
            The user's input text to send to the model.
        system_prompt: str 
            The system text to send to the model.
        assistant_prompt: str 
            Text sent as a previous assistant response. Usefull for providing context.
        files: list[str|BytesIO], []
            The list of files to add to the payload. Can be local paths, binary files, or both. Supports only images.
            TODO: add other file types.
        display: bool, False
            Whereas to print in the terminal the model response, or not.
 
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

        Examples
        --------
        >>> for token in yield_query(model_name='nova-micro', user_prompt='Who are you?'): print(token)
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
            for event in response["body"]:
                chunk = json.loads(event["chunk"]["bytes"])
                if chunk["type"] == "content_block_delta":
                    text += chunk["delta"].get("text", "")
                    yield chunk["delta"].get("text", "")
                elif chunk["type"] == "message_stop":
                    usage = {
                        "input_tokens": chunk["amazon-bedrock-invocationMetrics"].get("inputTokenCount", ""),
                        "output_tokens": chunk["amazon-bedrock-invocationMetrics"].get("outputTokenCount", ""),
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
    
