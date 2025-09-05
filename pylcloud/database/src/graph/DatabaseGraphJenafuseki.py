import os, sys
import hashlib
import logging
from typing import Optional
from datetime import datetime

from .DatabaseGraph import DatabaseGraph

class DatabaseGraphJenafuseki(DatabaseGraph):
    """
    Databases API helper.
    """
    def __init__(self, logs_name: str):
        """
        Initializes the helper and its logging.
        """
        super().__init__()

        self._config_logger(logs_name=logs_name)

        return None
