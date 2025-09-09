import os, sys
import hashlib
import logging
from typing import Optional
from datetime import datetime
from abc import ABC, abstractmethod

from database import Database

class DatabaseDocument(Database):
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
    def create_collection(self, *args, **kwargs):
        """
        Creates a new index/collection.
        """
        raise NotImplementedError


    @abstractmethod
    def drop_collection(self, *args, **kwargs):
        """
        Drops the matching indexes/collections on this cluster (database).
        """
        raise NotImplementedError


    @abstractmethod
    def list_collections(self, *args, **kwargs):
        """
        Lists all the indexes/collections on this cluster (database).
        """
        raise NotImplementedError


    def _hash_content(self, content: str, prefixes: list[str]):
        """
        Hashes a document content into a unique id of format <prefixes>-<hashed_content>. This is usefull to automatically overwrite 
        a stored document when a document with the same timestamp and content is written into Elasticsearch. 

        Parameters
        ----------
        content: str
            The text content to hash.
        prefix: str
            A list of prefixes (such as metadata, timestamps...) to prefix the hashed content with.

        Returns
        -------
        hashed_id: str
            The unique hashed ID.

        Examples
        --------
        >>> print(_hash_content(content='Message from Caroline: Merry Christmas!', prefixes=['2024/12/25', '103010']))
        >>> '2024/12/25-103010-4432e1c6d1c4f0db2f157d501ae242a7'
        """
        return f"{'-'.join(prefixes)}-{hashlib.md5(content.encode()).hexdigest()}"


