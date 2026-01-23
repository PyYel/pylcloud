import os, sys
import hashlib
import logging
from typing import Optional, List
from datetime import datetime
import uuid
from abc import ABC, abstractmethod

from pylcloud import _config_logger


class Database(ABC):
    """
    Abstract class for databases.
    """

    def __init__(self, *args, **kwargs):
        """ """
        super().__init__()

        # Default logger fallback
        self.logger = _config_logger(
            logs_name="Database",
            logs_output="console",
        )

        return None

    @abstractmethod
    def connect_database(self, *args, **kwargs):
        """ """
        raise NotImplementedError

    @abstractmethod
    def delete_data(self, *args, **kwargs):
        """
        Deletes matching entries/records/documents from the DB.
        """
        raise NotImplementedError

    @abstractmethod
    def describe_database(self, *args, **kwargs):
        """
        High level database description.
        """
        raise NotImplementedError

    @abstractmethod
    def disconnect_database(self, *args, **kwargs):
        """ """
        raise NotImplementedError

    @abstractmethod
    def query_data(self, *args, **kwargs):
        """
        Retreives matching entries/records/documents from the DB.
        """
        raise NotImplementedError

    @abstractmethod
    def send_data(self, *args, **kwargs):
        """
        Injects data into the DB by creating new entry/record/document.
        """
        raise NotImplementedError

    @abstractmethod
    def update_data(self, *args, **kwargs):
        """
        Updates the values of existing records matching the query.
        """
        raise NotImplementedError

    def _hash_content(
        self, content: str, prefixes: List[str], algo: str = "md5"
    ) -> str:
        """
        Hashes a document content into a unique id of format <prefixes>-<hashed_content>.
        Useful to automatically overwrite a stored document when a document with the same
        timestamp and content is written into Elasticsearch or SQL.

        Parameters
        ----------
        content : str
            The text content to hash.
        prefixes : list[str]
            A list of prefixes (such as metadata, timestamps...) to prefix the hashed content with.
        algo : str, optional
            The hashing algorithm to use. Supported: "md5", "sha1", "sha256", "uuid5".
            Default is "md5".

        Returns
        -------
        hashed_id : str
            The unique hashed ID.

        Examples
        --------
        >>> print(_hash_content("Message from Caroline: Merry Christmas!", ["2024/12/25", "103010"], algo="md5"))
        '2024/12/25-103010-4432e1c6d1c4f0db2f157d501ae242a7'
        >>> print(_hash_content("Message from Caroline: Merry Christmas!", ["2024/12/25", "103010"], algo="sha256"))
        '2024/12/25-103010-84f6b29a7fa3e11e5f0b0f5d63c024c97b51a9c5f457d07d41b58738e2e0d7f4'
        >>> print(_hash_content("Message from Caroline: Merry Christmas!", ["2024/12/25", "103010"], algo="uuid5"))
        '2024/12/25-103010-4b28f4a0-6bcf-55cc-95b3-2e3d5a64f155'
        """
        base = "-".join(prefixes) + "-" + content

        if algo == "md5":
            digest = hashlib.md5(base.encode()).hexdigest()
        elif algo == "sha1":
            digest = hashlib.sha1(base.encode()).hexdigest()
        elif algo == "sha256":
            digest = hashlib.sha256(base.encode()).hexdigest()
        elif algo == "uuid5":
            digest = str(uuid.uuid5(uuid.NAMESPACE_DNS, base))
        else:
            raise ValueError(f"Unsupported algo: {algo}")

        return f"{'-'.join(prefixes)}-{digest}"
