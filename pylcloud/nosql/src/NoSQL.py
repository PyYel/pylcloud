import os, sys
import hashlib

from abc import ABC, abstractmethod


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


    @abstractmethod
    def connect_database(self, 
                         host: str = "127.0.0.1",
                         user: str = "user",
                         password: str = "password",
                         database_name: str = "my_db",
                         port: str = "3306",
                         create_if_not_exists: bool = False):
        """
        Connects to the database and creates a connector object ``conn``. 
        """
        pass
    

    @abstractmethod
    def disconnect_database(self):
        """
        Closes the database linked to the connector ``conn``.
        """
        pass
    

    @abstractmethod
    def list_databases(self, system_db: bool = False):
        """
        List the databases (schemas) present on a SQL server.
        
        Parameters
        ----------
        system_db: bool
            Whereas returning the builtin databases if any, or not.
        """
        pass


    @abstractmethod
    def list_tables(self, database_name: str):
        """
        Lists all the tables present in a database (schemas).

        Parameters
        ----------
        database_name: str
            The name of the database (schema) to list tables from.

        Notes
        -----
        - To list the existing databases, see ``list_databases()``.
        """
        pass


    @abstractmethod
    def create_table(self, **kwargs):
        """
        
        """
        pass
    

    @abstractmethod
    def drop_database(self, database_name: str):
        """
        
        """
        pass


    @abstractmethod
    def drop_table(self, table_name: str):
        """
        
        """
        pass


    def _connect_database(self, 
                          base_url: str = "127.0.0.1",
                          username: str = "admin", 
                          password: str = "password"
                          ) -> None:
        """
        Connects to the Elasticsearch DB, and creates the ``es`` connector.
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
        >>> print(_hash_content(content='Message from Caroline: Merry Christmast!', prefixes=['2024/12/25', '103010']))
        >>> '2024/12/25-103010-4432e1c6d1c4f0db2f157d501ae242a7'
        """
        return f"{'-'.join(prefixes)}-{hashlib.md5(content.encode()).hexdigest()}"


