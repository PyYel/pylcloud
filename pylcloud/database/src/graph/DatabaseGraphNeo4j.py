import os, sys
import hashlib
import logging
from typing import Optional
from datetime import datetime

from .DatabaseGraph import DatabaseGraph
from pylcloud import _config_logger


class DatabaseGraphNeo4j(DatabaseGraph):
    """
    Databases API helper.
    """

    def __init__(self):
        """
        Initializes the helper and its logging.
        """
        super().__init__()

        self.logger = _config_logger(logs_name="DatabaseGraphNeo4j")

        return None

    def connect_database(self, *args, **kwargs):
        return super().connect_database(*args, **kwargs)

    def disconnect_database(self, *args, **kwargs):
        return super().disconnect_database(*args, **kwargs)

    def describe_database(self, *args, **kwargs):
        return super().describe_database(*args, **kwargs)

    def query_data(self, *args, **kwargs):
        return super().query_data(*args, **kwargs)

    def send_data(self, *args, **kwargs):
        return super().send_data(*args, **kwargs)

    def delete_data(self, *args, **kwargs):
        return super().delete_data(*args, **kwargs)

    def update_data(self, *args, **kwargs):
        return super().update_data(*args, **kwargs)
