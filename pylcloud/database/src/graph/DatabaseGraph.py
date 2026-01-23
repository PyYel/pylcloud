import os, sys
import hashlib
import logging
from typing import Optional
from datetime import datetime
from abc import ABC, abstractmethod

from ..Database import Database


class DatabaseGraph(Database):
    """
    Databases API helper.
    """

    def __init__(self):
        """
        Initializes the helper and its logging.
        """
        super().__init__()

        return None
