import os, sys
import mysql.connector
import json
import hashlib

from mysql.connector import Error
from abc import ABC, abstractmethod


class YesSQL(ABC):
    """
    A parent class that notably manages the global YesSQL database server.
    """
    def __init__(self, db_type: str) -> None:
        """
        Initializes the database connector helper. The database will still require to be created and/or connected to.
        """

        self.db_type = db_type

        return None


    @abstractmethod
    def _connect_database(self, *args, **kwargs):
        """
        Connects to the database and creates a connector object. 
        """
        pass


    @abstractmethod
    def _create_table(self, *args, **kwargs):
        """
        Creates a new index/collection.
        """
        pass


    @abstractmethod    
    def _delete_data(self, *args, **kwargs):
        """
        Deletes matching entries/records/documents from the DB.
        """
        pass


    @abstractmethod
    def _disconnect_database(self, *args, **kwargs):
        """
        Closes the database linked to the connector ``conn``.
        """
        pass


    @abstractmethod
    def _drop_database(self, *args, **kwargs):
        """
        Drops the whole cluster (database).
        """
        pass


    @abstractmethod
    def _drop_table(self, *args, **kwargs):
        """
        Drops the matching indexes/collections on this cluster (database).
        """
        pass


    @abstractmethod
    def _list_databases(self, *args, **kwargs):
        """
        Lists the cluster(s) and its metadata.
        """
        pass
    

    @abstractmethod
    def _list_tables(self, *args, **kwargs):
        """
        Lists all the indexes/collections on this cluster (database).
        """
        pass


    @abstractmethod    
    def _query_data(self, *args, **kwargs):
        """
        Retreives matching entries/records/documents from the DB.
        """
        pass


    @abstractmethod    
    def _send_data(self, *args, **kwargs):
        """
        Injects data into the DB by creating new entry/record/document.
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


    def _parse_database_json(self, json_file: str):
        """
        Reads an SQL-formatted JSON file and yields table definitions.
        
        Parameters
        ----------
        json_file: str
            The local path to the SQL instructions JSON.

        Yields
        ------
        table_name: str
            The name of the table.
        columns: str
            The name of the column.
        type: str
            The data type of the column.
        constraints: str
            Constraints applied to the column.

        Example
        -------
        >>> for name, column, dtype, constraints in _parse_database_json('path/to/db_setup.json')
        >>>     print(name, column, dtype, constraints)
        >>> "users", "username",

        >>> for name, column, dtype, constraints in _parse_database_json('path/to/db_setup.json')
        >>>     print(name, column, dtype, constraints)
        >>>     self.create_table(name=name, column=column, dtype=dtype, constraints=constraints)

        Notes
        -----
        The JSON file must follow this structure:

        {
            "database": "database_name",
            "tables": [
                {
                    "name": "table_name",
                    "columns": [
                        {
                            "name": "column_name",
                            "type": "data_type",
                            "constraints": "optional_constraints"
                        },
                        ...
                    ]
                },
                ...
            ]
        }
        Reads an SQL-formatted JSON file and yields detailed table definitions.
        
        """
        with open(json_file, 'r') as file:
            setup = json.load(file)
        
        print(f"YesSQL >> Reading database setup for '{setup['database']}'")
        
        for table in setup['tables']:

            table_name = table["name"]

            column_definitions = []
            for column in table["column"]:

                # If there is a dtype, this is a parent column
                if "type" in column: col_def = f"{column['name']} {column['type']} {column['constraints']}".strip()

                # Otherwise it is likely not, i.e., a foreign key constraints
                else: col_def = f"{column['name']} {column['constraints']}".strip()

                column_definitions.append(col_def)

            yield table_name, column_definitions


    def _check_table_exists(self, table_name: str):
        """
        Checks if a table already exists in the database schema, to avoid errors.
        """
        pass