import os, sys
from abc import ABC, abstractmethod
from uuid import uuid4
from collections import Counter
from typing import Optional, Union, List, Dict, Any, TypedDict, Callable
from io import BytesIO
import nltk
from nltk.data import find
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
import re

from pylcloud import _config_logger

class GPTResponse(TypedDict):
    """
    Standardized PyYel LLM inference output.

    Parameters
    ==========
    - thinking: str, optional
        The model thinking tokens, removed from the ``text`` output. None if the model did not think
    - text: str
        The model response intended for the user
    - usage: dict[str, int]
        The input and output tokens count
    """
    thinking: Optional[str] 
    text: str
    usage: dict[str, int]


class GPT(ABC):
    """
    Base class for generative AI inference.
    """

    def __init__(self, **kwargs):
        super().__init__()

        # Default logger fallback
        self.logger = _config_logger(logs_name="GPT")

        self._download_nltk_data()

        self.available_models = {}
        self.costs: dict[str, dict[str, float]] = {}

        return None

    def compute_costs(self, model_name: str, usage: dict[str, int]) -> dict[str, float]:
        """
        Takes a ``usage`` dictionnary with token numbers and returns dollar costs inplace.

        Parameters
        ----------
        model_name: str
            The name of the model to compute the token costs for.
        usage: dict[str, int]
            A formatted usage dictionnary of token numbers

        Returns
        -------
        usage: dict[str, float]
            A formatted usage dictionnary of token costs (US$ without VAT).

        Notes
        -----
        - Default prices are estimated in US dollars for Europe or Paris region, without VAT.
        To change these values, you may overwrite the cost attribute.

        Examples
        --------
        >>> # Overwrite with custom values
        >>> gpt_api.costs = {'claude-4-sonnet': {'input_tokens': 0.003*1e-3, 'output_tokens': 0.015*1e-3}}

        >>> # Result for these values
        >>> print(gpt_api.compute_costs(model_name='claude-4-sonnet, usage={'input_tokens': 534, 'output_tokens': 122})
        >>> {'input_tokens': 0.0016, 'output_tokens': 0.0018}
        """

        costs = self.costs[model_name]

        usage_compute = {
            "input_tokens": usage["input_tokens"] * costs["input_tokens"],
            "output_tokens": usage["output_tokens"] * costs["output_tokens"],
        }

        return usage_compute

    @abstractmethod
    def return_embedding(
        self,
        model_name: str,
        prompt: str,
        dimensions: int = 512,
    ) -> Union[dict, dict[str, Union[str, int]]]:
        raise NotImplementedError

    @abstractmethod
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
    ):
        raise NotImplementedError

    @abstractmethod
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
    ):
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    def generate_title(self, conversation: str):
        """
        Generates a title for a conversation.
        """

        words = word_tokenize(conversation.lower())

        # Remove stopwords and punctuation
        stop_words = set(stopwords.words("english"))
        filtered_words = [
            word for word in words if word.isalnum() and word not in stop_words
        ]

        # Get the most common words
        word_counts = Counter(filtered_words)
        common_words = [word for word, count in word_counts.most_common(4)]
        title = " ".join(common_words)

        return title.title()

    def parse_model_output(
        self,
        raw_text: str,
        raw_content: Optional[list[dict]] = None,
    ) -> tuple[Optional[str], str]:
        """
        Split raw model output into a thinking trace and a final answer.

        Handles both output formats transparently:
        - Nova models: inline ``<thinking>...</thinking>`` XML tags in the text block
        - Claude extended think: dedicated ``reasoningContent`` block in the content list

        Returns ``tuple[None, text]`` if no thinking beacons/payloads are detected.

        Parameters
        ----------
        raw_text: str
            The text content of the model response.
        raw_content: list[dict], optional
            The full raw content block list from the Converse API response.
            Required to detect Claude extended thinking blocks.

        Returns
        -------
        tuple:
            - "thinking": str | None
            - "text": str
        """

        # Claude extended thinking — dedicated reasoningContent block
        if raw_content:
            thinking_blocks = [
                b["reasoningContent"]["reasoningText"]["text"]
                for b in raw_content
                if "reasoningContent" in b
            ]
            if thinking_blocks:
                return "\n".join(thinking_blocks), raw_text.strip()

        # Nova — inline <thinking> tags in the text block
        match = re.search(r"<thinking>(.*?)</thinking>\s*", raw_text, flags=re.DOTALL)
        if match:
            return match.group(1).strip(), raw_text[match.end():].strip(),

        # No thinking detected
        return None, raw_text.strip()

    def _download_nltk_data(self):
        """
        Downloads the nltk corpus ressources.
        """
        try:
            find("tokenizers/punkt")
        except LookupError:
            nltk.download("punkt", quiet=True)

        try:
            find("corpora/stopwords")
        except LookupError:
            nltk.download("stopwords", quiet=True)

        # try:
        #     find("tokenizers/punkt_tab")
        # except LookupError:
        #     nltk.download("punkt_tab", quiet=True)

        return None
