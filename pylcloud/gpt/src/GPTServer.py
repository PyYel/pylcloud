
from abc import ABC, abstractmethod
from uuid import uuid4
from typing import List, Union, Any
from collections.abc import Sequence
from io import BytesIO
import requests

from constants import INFERENCE_HOST
from gpt import GPT

class GPTServer(GPT):
    """
    An helper that simplifies calls to LLM API.
    """
    def __init__(self, **kwargs):
        """
        Initializes a self-contained connection to a local inference API.

        This only supports generative AI. 
        """
        super().__init__(logs_name="GPTServer", logs_dir=None)

        self.available_models = {
            "llama-3.2-1B": {
                "model_id": "llama-3.2-1B",
                },
            "llama-3.3-3B": {
                "model_id": "llama-3.2-3B",
                },
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
                     top_p: float = 0.7) -> dict[str, Union[str, dict[str, int]]]:
        # TODO: add model name as payload setting for ultiple inference server        
        results = requests.post(url=f"{INFERENCE_HOST}/infer/generate", params={"message": user_prompt}).json()
        if results.get("success") == True:
            text = results.get("data")["response"][0]
            usage = {"input_tokens": 0, "output_tokens": 0} # not supported on local inference
            return {"text": text, "usage": usage}
        else:
            print(f"GPTLocal >> An error occured: {(results.get('message'))}")
            return results


    def yield_query(self, 
                    model_name: str, 
                    user_prompt: str, 
                    system_prompt: str = "", 
                    assistant_prompt: str = "",
                    files: List[Union[str, BytesIO]] = [], 
                    max_tokens: int = 512,
                    temperature: float = 0.9,
                    top_k: int = 32,
                    top_p: float = 0.7):
        raise NotImplementedError
    
    def _reset_session(self, **kwargs):
        return None
