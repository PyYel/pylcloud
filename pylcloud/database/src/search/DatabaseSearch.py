import os, sys
import hashlib
import logging
from typing import Optional
from datetime import datetime

from abc import ABC, abstractmethod

from database import Database


class DatabaseSearch(Database):
    """
    Databases API helper.
    """

    def __init__(self, logs_name: str):
        """
        Initializes the helper and its logging.
        """
        super().__init__(logs_name=logs_name)

        return None

    @abstractmethod
    def create_index(self, *args, **kwargs):
        """
        Creates a new index/collection.
        """
        raise NotImplementedError

    @abstractmethod
    def drop_index(self, *args, **kwargs):
        """
        Drops the matching indexes/collections on this cluster (database).
        """
        raise NotImplementedError

    @abstractmethod
    def list_indexes(self, *args, **kwargs):
        """
        Lists all the indexes/collections on this cluster (database).
        """
        raise NotImplementedError
