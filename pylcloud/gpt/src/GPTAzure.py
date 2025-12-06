from abc import ABC, abstractmethod
from io import BytesIO
from typing import Any, List, Union
from uuid import uuid4


from gpt import GPT


class GPTAzure(GPT):
    """
    An helper that simplifies calls to LLM API.
    """

    def __init__(self, **kwargs):
        """
        Initializes a self-contained connection to the Azure OpenAI API.

        This only supports generative AI.
        """
        super().__init__(logs_name="GPTAzure", logs_dir=None)

        self.available_models = {
            "chat-gpt-4o": {"model_id": "openai.gpt-4o"},
            "chat-gpt-4": {
                "model_id": "openai.gpt-4",
            },
            "chat-got-3.5-turbo": {
                "model_id": "openai.gpt-3.5-turbo",
            },
        }

        return None

    def return_query(
        self,
        model_name: str,
        user_prompt: str,
        system_prompt: str = "",
        assistant_prompt: str = "",
        messages: list[dict[str, Any]] = [],
        files: List[Union[str, BytesIO]] = [],
        max_tokens: int = 512,
        temperature: float = 0.9,
        top_k: int = 32,
        top_p: float = 0.7,
    ) -> dict[str, Union[str, dict[str, int]]]:
        raise NotImplementedError

    def yield_query(
        self,
        model_name: str,
        user_prompt: str,
        system_prompt: str = "",
        assistant_prompt: str = "",
        files: List[Union[str, BytesIO]] = [],
        max_tokens: int = 512,
        temperature: float = 0.9,
        top_k: int = 32,
        top_p: float = 0.7,
    ):
        raise NotImplementedError
