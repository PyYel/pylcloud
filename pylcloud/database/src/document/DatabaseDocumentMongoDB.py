import os, sys
from typing import Union, Optional


from .DatabaseDocument import DatabaseDocument


class DatabaseDocumentMongoDB(DatabaseDocument):
    """
    MongoDB Python API helper.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:9200",
        username: str = "admin",
        password: str = "password",
    ):
        super().__init__(logs_name="DatabaseMongoDB")

        # TODO: Add connection certificate
        try:
            # self.es = Elasticsearch(base_url, basic_auth=(username, password), verify_certs=False)
            None
        except:
            print(f"NoSQLMongoDB >> An error occured when connecting to '{base_url}'.")

        # Note:
        # The authentication credentials above are required to connect to Elasticsearch.
        # When connecting to Kibana server, custom users credentials should be used. They can be created from withing the Kibana server
        # interface, through the elastic superuser account (which are actually the credentials used above: ELASTIC_USERNAME, ELASTIC_PASSWORD)

    def connect_database(self):
        pass

    def disconnect_database(self):
        """
        Closes the database linked to the connector ``conn``.
        """
        pass

    def describe_database(self, system_db: bool = False):
        raise NotADirectoryError

    def create_collection(self, *args, **kwargs):
        return super().create_collection(*args, **kwargs)

    def drop_collection(self, collection_name: str):
        pass

    def list_collections(self, system_db: bool = False):
        pass

    def send_data(self, index_name: str, documents: list[dict]):
        pass

    def query_data(
        self,
        index_name: str,
        must_pairs: list[dict[str, str]] = [],
        should_pairs: list[dict[str, str]] = [],
    ):
        pass

    def delete_data(self, index: str, pairs: dict[str, str] = {}):
        pass

    def update_data(self, *args, **kwargs):
        return super().update_data(*args, **kwargs)
