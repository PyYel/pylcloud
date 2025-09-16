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
from typing import List, Union
import boto3
from typing import Generator


from gpt import GPT
from constants import AWS_ACCESS_KEY_SECRET, AWS_ACCESS_KEY_ID, AWS_REGION_NAME


class GPTAWS(GPT):
    """
    An helper that simplifies calls to LLM API.
    """
    def __init__(self, 
                 model_name: str,
                 session_id: str = str(uuid4()),
                 temperature: float = 0.1,
                 max_tokens: int = 500,
                 **kwargs):
        """
        Initializes a self-contained connection to the AWS Bedrock API.

        This only supports generative AI. 
        """
        super().__init__(model_name=model_name, session_id=session_id, temperature=temperature, max_tokens=max_tokens)
    
        
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

        return None


    def return_query(self, 
                     model_name: str, 
                     user_prompt: str, 
                     system_prompt: str = "", 
                     files: List[Union[str, BytesIO]] = [], 
                     max_tokens: int = 512,
                     temperature: int = 1,
                     top_k: int = 250,
                     top_p: float = 0.999,
                     display: bool = False):
        """
        Function to interact with an AWS Bedrock model using boto3.

        Parameters
        ----------
        model_id: str
            The ID of the model in AWS Bedrock to invoke. 
            See ``list_fundation_models()`` for the exact ID name.
        prompt_text: str 
            The input text to send to the model.
        display: bool, False
            Whereas to print in the terminal the model response, or not.

        Returns
        -------
        response_body: dict
            Response from the AWS Bedrock model.

        Notes
        -----
        - Selected ``model`` must be one of:
            - 'claude-3.5-sonnet'
            - 'claude-3.7-sonnet'
            - 'claude-3-huaiku'
            - 'claude-3-sonnet'

        """

        model_id = self.available_models[model_name]["model_id"]

        payload = self._create_payload(model_name=model_name,
                                       user_prompt=user_prompt,
                                       system_prompt=system_prompt,
                                       files=files,
                                       temperature=temperature,
                                       top_k=top_k,
                                       top_p=top_p,
                                       max_tokens=max_tokens)
        
        response = self.bedrock_runtime_client.invoke_model(
            modelId=model_id,
            accept='application/json',  
            contentType='application/json',  
            body=payload
        )
        
        response_body = json.loads(response['body'].read())
        text = response_body["content"][0]["text"]
        usage = response_body["usage"]

        if display: print(text)

        return {"text": text, "usage": usage}
        

    def yield_query(self, 
                     model_name: str, 
                     user_prompt: str, 
                     system_prompt: str = "", 
                     files: List[Union[str, BytesIO]] = [], 
                     max_tokens: int = 512,
                     temperature: int = 1,
                     top_k: int = 250,
                     top_p: float = 0.999,
                     display: bool = False):
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
            - 'claude-3.5-sonnet'
            - 'claude-3.7-sonnet'
            - 'claude-3-haiku'
            - 'claude-3-sonnet'
        """


        model_id = self.available_models[model_name]["model_id"]

        payload = self._create_payload(model_name=model_name,
                                       user_prompt=user_prompt,
                                       system_prompt=system_prompt,
                                       files=files,
                                       temperature=temperature,
                                       top_k=top_k,
                                       top_p=top_p,
                                       max_tokens=max_tokens)
        
        response = self.bedrock_runtime_client.invoke_model_with_response_stream(
            modelId=model_id,
            accept='application/json',  
            contentType='application/json',  
            body=payload
        )
        
        text = ""
        for event in response["body"]:
            chunk = json.loads(event["chunk"]["bytes"])
            if chunk["type"] == "content_block_delta":
                if display: print(chunk["delta"].get("text", ""), end="")
                text += chunk["delta"].get("text", "")
                yield chunk["delta"].get("text", "")
            elif chunk["type"] == "message_stop":
                usage = {
                    "input_tokens": chunk["amazon-bedrock-invocationMetrics"].get("inputTokenCount", ""),
                    "output_tokens": chunk["amazon-bedrock-invocationMetrics"].get("outputTokenCount", ""),
                }
                yield {"text": text, "usage": usage}
        

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
                        files: list[BytesIO] = [], 
                        temperature: int = 1,
                        top_k: int = 250,
                        top_p: float = 0.999,
                        max_tokens: int = 512):
        """
        Formats a payload that respects the API models.
        """

        def _create_claude_payload():
            
            content = [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/jpeg",
                        "data": file
                    }
                }
                for file in self._process_files(files=files)
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
                "messages": [
                    {
                        "role": "user",
                        "content": content
                    }
                ]
            }

            return payload 
        
        def _create_mistral_payload():

            payload = {}

            return payload 
        

        if model_name.startswith("claude"):
            return json.dumps(_create_claude_payload())
        elif model_name.startswith("mistral"):
            return json.dumps(_create_mistral_payload())


    def _process_files(self, files: List[Union[str, BytesIO]] = []) -> List[str]:
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
    
