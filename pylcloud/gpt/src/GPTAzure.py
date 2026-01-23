from abc import ABC, abstractmethod
from io import BytesIO
from typing import Any, List, Union
from uuid import uuid4


from .GPT import GPT
from pylcloud import _config_logger


class GPTAzure(GPT):
    """
    An helper that simplifies calls to LLM API.
    """

    def __init__(self, **kwargs):
        """
        Initializes a self-contained connection to the Azure OpenAI API.

        This only supports generative AI.
        """
        super().__init__()

        self.logger = _config_logger(logs_name="GPTAzure")

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

    def return_generation(
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

    def yield_generation(
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

    def return_embedding(
        self,
        model_name: str,
        prompt: str,
        files: List[str | BytesIO] = [],
        dimensions: int = 512,
    ) -> dict | dict[str, str | int]:
        raise NotImplementedError
