
from abc import ABC, abstractmethod
from uuid import uuid4


from gpt import GPT

class GPTAzure(GPT):
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
        Initializes a self-contained connection to the Azure OpenAI API.

        This only supports generative AI. 
        """
        super().__init__(model_name=model_name, session_id=session_id, temperature=temperature, max_tokens=max_tokens)

        return None
    

    @abstractmethod
    def return_query(self):
        raise NotImplementedError


    @abstractmethod
    def yield_query(self):
        raise NotImplementedError
