import os, sys
import hashlib
import logging
from typing import Optional
from datetime import datetime
from abc import ABC, abstractmethod

from database import Database

class DatabaseRelational(Database):
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
    def _get_connection(self):
        """
        Provides an active connection for the upcoming query, ensures alive connections.
        """
        raise NotImplementedError
    
    @abstractmethod
    def _clear_connection(self):
        """
        Closes all connection and clears the connection pool, acting as a disconnect from the DB.
        """
        raise NotImplementedError

    @abstractmethod
    def create_table(self, *args, **kwargs):
        """
        Creates a new index/collection.
        """
        raise NotImplementedError


    @abstractmethod
    def drop_table(self, *args, **kwargs):
        """
        Drops the matching indexes/collections on this cluster (database).
        """
        raise NotImplementedError
    

    @abstractmethod
    def execute_file(self, *args, **kwargs):
        """
        Runs a SQL file.
        """
        raise NotImplementedError