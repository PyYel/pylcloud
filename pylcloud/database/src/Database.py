import os, sys
import hashlib

from abc import ABC, abstractmethod


class Database(ABC):
    """
    NoSQL databases API helper.
    """
    def __init__(self):
        """
        Initializes the helper.
        """
        super().__init__()

        return None


    @abstractmethod
    def connect_database(self, *args, **kwargs):
        """
        Connects to the database and creates a connector object. 
        """
        pass


    @abstractmethod
    def _commit(self):
        """
        Commits the transactions operated since the last commit.
        """
        raise NotImplementedError


    @abstractmethod
    def _rollback(self):
        """
        roolbacks the transactions operated since the last commit.
        """
        raise NotImplementedError


    @abstractmethod
    def create_table(self, *args, **kwargs):
        """
        Creates a new index/collection.
        """
        pass


    @abstractmethod
    def disconnect_database(self, *args, **kwargs):
        """
        Closes the database linked to the connector ``conn``.
        """
        pass


    @abstractmethod
    def drop_table(self, *args, **kwargs):
        """
        Drops the matching indexes/collections on this cluster (database).
        """
        pass

    

    @abstractmethod
    def list_tables(self, *args, **kwargs):
        """
        Lists all the indexes/collections on this cluster (database).
        """
        pass


    @abstractmethod    
    def query_data(self, *args, **kwargs):
        """
        Retreives matching entries/records/documents from the DB.
        """
        pass


    @abstractmethod    
    def send_data(self, *args, **kwargs):
        """
        Injects data into the DB by creating new entry/record/document.
        """
        pass


    @abstractmethod    
    def delete_data(self, *args, **kwargs):
        """
        Deletes matching entries/records/documents from the DB.
        """
        pass


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


