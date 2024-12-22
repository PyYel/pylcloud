import os, sys
import mysql.connector
import json

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