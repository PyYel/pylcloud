import os, sys
import hashlib
import logging
from typing import Optional
from datetime import datetime

from abc import ABC, abstractmethod


class Database(ABC):
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


    def _config_logger(self, 
                       logs_name: str, 
                       logs_dir: Optional[str] = None, 
                       logs_level: str = "INFO",
                       logs_output: list[str] = ["console", "file"]):
        """
        Will configure logging accordingly to the plateform the program is running on. This
        is the default behaviour. See ``custom_config()`` to override the parameters.
        """

        if logs_dir is None:
            logs_dir = os.path.join(os.getcwd(), "log", str(datetime.now().strftime("%Y-%m-%d")))
        os.makedirs(logs_dir, exist_ok=True)

        self.logger = logging.getLogger(logs_name)
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # If a logger already exists, this prevents duplication of the logger handlers
        if self.logger.hasHandlers():
            for handler in self.logger.handlers:
                handler.close()

        # Creates/recreates the handler(s)
        if not self.logger.hasHandlers():

            if "console" in logs_output:
                console_handler = logging.StreamHandler()
                console_handler.setLevel(logging._nameToLevel[logs_level])
                console_handler.setFormatter(formatter)
                self.logger.addHandler(console_handler)
                self.logger.info("Logging handler configured for console output.")

            if "file" in logs_output:
                file_handler = logging.FileHandler(os.path.join(logs_dir, f"{datetime.now().strftime('%H-%M-%S')}-app.log"))
                file_handler.setLevel(logging._nameToLevel[logs_level])
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
                self.logger.info("Logging handler configured for file output.")

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


