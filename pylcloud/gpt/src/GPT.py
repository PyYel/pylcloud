
from abc import ABC, abstractmethod
from uuid import uuid4
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from collections import Counter
import nltk
from nltk.data import find

from constants import INFERENCE_TYPE, AWS_REGION_NAME

class GPT(ABC):
    """
    Base class for generative AI inference.
    """
    def __init__(self,
                 model_name: str = None,
                 session_id: str = str(uuid4()),
                 temperature: float = 0.1,
                 max_tokens: int = 500):
        super().__init__()

        self.model_name = model_name
        self.session_id = session_id,
        self.temperature = temperature,
        self.max_tokens = max_tokens

        if INFERENCE_TYPE == "server" or INFERENCE_TYPE == "built-in":
            self.available_models = {
                'llama-3.2-1B': {
                    "model_id": "llama-3.2-1B",
                    },
                'llama-3.3-3B': {
                    "model_id": "llama-3.2-3B",
                    },
                }
        elif INFERENCE_TYPE == "azure":
            self.available_models = {
                'chat-gpt-4o': {
                    "model_id": 'openai.gpt-4o'
                    },
                'chat-gpt-4': {
                    "model_id": 'openai.gpt-4',
                    },
                'chat-got-3.5-turbo': {
                    "model_id": 'openai.gpt-3.5-turbo',
                    },
                }
        elif INFERENCE_TYPE == "aws":
            self.available_models = {
                'claude-3.5-sonnet': {
                    "model_id": 'eu.anthropic.claude-3-7-sonnet-20250219-v1:0' if AWS_REGION_NAME.startswith("eu") else 'anthropic.claude-3-5-sonnet-20240620-v1:0',
                    "anthropic_version": "bedrock-2023-05-31" 
                    },
                'claude-3.7-sonnet': {
                    "model_id": 'eu.anthropic.claude-3-5-sonnet-20240620-v1:0' if AWS_REGION_NAME.startswith("eu") else 'anthropic.claude-3-5-sonnet-20240620-v1:0', 
                    "anthropic_version": "bedrock-2023-05-31"
                    },
                'clade-3-sonnet': {
                    "model_id": 'anthropic.claude-3-sonnet-20240229-v1:0', 
                    "anthropic_version": "bedrock-2023-05-31"
                    },
                'claude-3-haiku': {
                    "model_id": 'anthropic.claude-3-haiku-20240307-v1:0', 
                    "anthropic_version": "bedrock-2023-05-31" # TODO: check old models versions and inference profiles
                    }
                }
        else:
            self.available_models = {}
            print(f"GPT >> Invalid INFERENCE_TYPE environnement setting: '{INFERENCE_TYPE}'.")

        self._download_nltk_data()

        return None
    

    @abstractmethod
    def return_query(self):
        raise NotImplementedError


    @abstractmethod
    def yield_query(self):
        raise NotImplementedError
    
    @abstractmethod
    def _reset_session(self):
        return None


    def generate_title(self, conversation: str):
        """
        Generates a title for a conversation.
        """

        words = word_tokenize(conversation.lower())

        # Remove stopwords and punctuation
        stop_words = set(stopwords.words('english'))
        filtered_words = [word for word in words if word.isalnum() and word not in stop_words]

        # Get the most common words
        word_counts = Counter(filtered_words)
        common_words = [word for word, count in word_counts.most_common(4)]
        title = ' '.join(common_words)
        
        return title.title()


    def _download_nltk_data(self):
        """
        Downloads the nltk corpus ressources.
        """
        try:
            find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt', quiet=True)

        try:
            find('corpora/stopwords')
        except LookupError:
            nltk.download('stopwords', quiet=True)

        try:
            find('tokenizers/punkt_tab')
        except LookupError:
            nltk.download('punkt_tab', quiet=True)

        return None
