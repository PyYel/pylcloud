import os, sys
import hashlib

from abc import ABC


class NoSQL(ABC):
    """
    NoSQL databases API helper.
    """
    def __init__(self):
        """
        Initializes the helper.
        """
        super().__init__()

        # TODO: Add connection certificate

        # Note:
        # The authentication credentials above are required to connect to Elasticsearch.
        # When connecting to Kibana server, custom users credentials should be used. They can be created from withing the Kibana server
        # interface, through the elastic superuser account (which are actually the credentials used above: ELASTIC_USERNAME, ELASTIC_PASSWORD)

        return None


    def _connect_database(self, 
                          base_url: str = "127.0.0.1",
                          username: str = "admin", 
                          password: str = "password"
                          ) -> None:
        """
        Connects to the Elasticsearch DB, and creates the ``es`` connector.
        """
        self.es = Elasticsearch(base_url, basic_auth=(username, password), verify_certs=False)


    def _hash_content(self, content: str, metadata_prefix: list[str] = [""]):
        """
        Hashes an elastic payload into a unique id of format <metadata_prefix>-<hashed_content>. This is usefull to automatically overwrite a stored document 
        when a document with the same timestamp and content is written into Elasticsearch.

        Parameters
        ----------
        content: str
            The text content to hash into a unique sequence.
        metadata_prefix: list[str], ['']
            A list of inputs joined with dashes '-' that prefixes the encoded hash, to help IDs management.

        Returns
        -------
        hashed_id: str
            The created ID.
        """
        return f"{'-'.join(metadata_prefix)}-{hashlib.md5(content.encode()).hexdigest()}"


    def delete_index(self, index: str):
        """Deletes an index and all its content from the Elasticsearch DB."""
        try:
            response = self.es.indices.delete(index=index)
            print(f"Elasticsearch >> Index '{index}' deleted successfully.")
        except Exception as e:
            print(f"Elasticsearch >> Failed to delete index '{index}': {e}")
        
        return True


    def list_indexes(self):
        """Returns a list of the non-builtin indexes names."""
        return [index['index'] for index in self.es.cat.indices(format='json') if not index['index'].startswith(".")]

