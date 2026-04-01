import os, sys
import hashlib
import logging
from typing import Optional, Union
from abc import ABC, abstractmethod

from ..Database import Database


class DatabaseSearch(Database):
    """
    Databases API helper.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the helper and its logging.
        """
        super().__init__()

        return None

    @abstractmethod
    def connect_database(
        self, 
        host: str = "127.0.0.1", 
        user: str = "user", 
        password: str = "password"
    ):
        raise NotImplementedError

    @abstractmethod
    def disconnect_database(self):
        """
        Closes the database linked to the connector ``conn``.
        """
        pass

    @abstractmethod
    def create_index(
        self,
        index_name: str,
        mappings: Optional[Union[dict, str]] = None,
        settings: Optional[Union[dict, str]] = None,
        shards: int = 1,
        replicas: int = 1,
    ):
        raise NotImplementedError
    
    @abstractmethod
    def drop_index(self, index_name: str):
        raise NotImplementedError

    @abstractmethod
    def similarity_search(
        self,
        index_name: str,
        vector_query: list[float],
        vector_field: str = "vector",
        must_pairs: list[dict[str, str]] = [],
        should_pairs: list[dict[str, str]] = [],
        k: int = 5,
        *args,
        **kwargs
    ):
        raise NotImplementedError


