
from abc import ABC, abstractmethod
from uuid import uuid4
from typing import List, Union
from collections.abc import Sequence
from io import BytesIO
import requests

from constants import INFERENCE_HOST
from gpt import GPT

class GPTServer(GPT):
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
        Initializes a self-contained connection to a local inference API.

        This only supports generative AI. 
        """
        super().__init__(model_name=model_name, session_id=session_id, temperature=temperature, max_tokens=max_tokens)

        return None
    

    def return_query(self,
                     model_name: str, 
                     user_prompt: str, 
                     system_prompt: str = "", 
                     files: Sequence[Union[str, BytesIO]] = [], 
                     max_tokens: int = 512,
                     temperature: int = 1,
                     top_k: int = 250,
                     top_p: float = 0.999,
                     display: bool = False):
        # TODO: add model name as payload setting for ultiple inference server        
        results = requests.post(url=f"{INFERENCE_HOST}/infer/generate", params={"message": user_prompt}).json()
        if results.get("success") == True:
            text = results.get("data")["response"][0]
            usage = {"input_tokens": 0, "output_tokens": 0} # not supported on local inference
            return {"text": text, "usage": usage}
        else:
            print(f"GPTLocal >> An error occured: {(results.get("message"))}")
            return results


    def yield_query(self):
        raise NotImplementedError
    
    def _reset_session(self, **kwargs):
        return None
