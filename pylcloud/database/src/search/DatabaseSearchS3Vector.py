import os, sys
import hashlib
import logging
from typing import Optional
from datetime import datetime
from abc import ABC, abstractmethod

from ..Database import Database


class DatabaseSearchS3Vectors(Database):
    """
    Databases API helper.
    """

    def __init__(self):
        """
        AWS proprietary S3 Vector helper. This only works with AWS S3 buckets, and is currently not supported by
        the broadest S3 protocole (like MinIO...).
        """
        super().__init__()

        self.logger = _config_logger(logs_name="DatabaseSearchElasticsearch")

        return None
    
    def connect_database(self, *args, **kwargs):
        return super().connect_database(*args, **kwargs)
    
    def disconnect_database(self, *args, **kwargs):
        return super().disconnect_database(*args, **kwargs)

    def create_index(self, *args, **kwargs):
        """
        An index is a S3 prefix.
        """
        raise NotImplementedError

    def drop_index(self, *args, **kwargs):
        """
        Drops the matching indexes/collections on this cluster (database).
        """
        raise NotImplementedError

    def list_indexes(self, *args, **kwargs):
        """
        Lists all the indexes/collections on this cluster (database).
        """
        raise NotImplementedError
