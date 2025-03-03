import os, sys

import json
import ssl

from database import Database


class DatabaseMongoDB(Database):
    """
    MongoDB Python API helper.
    """
    def __init__(self, base_url: str = "http://localhost:9200", username: str = "admin", password: str = "password"):
        super().__init__()

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



    def _connect_database(self):
        pass
    

    def _create_table(self, *args, **kwargs):
        """See ``create_index()``."""
        return self.create_index(*args, **kwargs)


    def _create_index(self):
        pass

    def _disconnect_database(self):
        """
        Closes the database linked to the connector ``conn``.
        """
        pass


    def _drop_database(self, database_name: str):
        """
        Drops all the indexes from a cluster.
        """
        print("NoSQLElasticsearch >> Can't drop an Elasticsearch. Will drop all the indexes from this cluster instead.")
        raise NotImplementedError


    def drop_collection(self, collection_name: str):
        pass

    def _drop_table(self, table_name: str):
        self.drop_collection(collection_name=table_name)

    def _delete_data(self, index: str, pairs: dict[str] = {}):
        pass

    def _list_databases(self, system_db: bool = False):
        pass
    

    def list_collections(self, system_db: bool = False):
        pass
    
    def _list_tables(self, database_name: str = None):
        self.list_collections()


    def _send_data(self, index_name: str, documents: list[dict]):
        pass


    def _query_data(self, 
                   index_name: str, 
                   must_pairs: list[dict[str]] = [], 
                   should_pairs: list[dict[str]] = []):
        pass
