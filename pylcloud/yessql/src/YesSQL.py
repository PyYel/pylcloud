import os, sys
import mysql.connector
import json

from mysql.connector import Error
from abc import ABC


class YesSQL(ABC):
    """
    A parent class that notably manages the global YesSQL database server.
    """
    def __init__(self, db_type: str = "mysql") -> None:
        """
        Initializes the database connector helper. The database will still require to be created and/or connected to.
        """

        self.db_type = db_type
        self.conn = None
        
        if db_type == "mysql":
            # TODO: wrap (reroute) connect method to corresponding db_type method 
            None
        else:
            print(f"YesSQL >> DB type '{db_type}' is not supported.")
            sys.exit(1)

        return None


    def _connect_database(self, 
                          host: str = "127.0.0.1",
                          user: str = "user",
                          password: str = "password",
                          database_name: str = "my_db",
                          port: str = "3306",
                          create_if_not_exists: bool = True):
        """
        Connects to the database and creates a connector object ``conn``. 
        """

        conn = None
        try:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database_name,
                port=port
            )
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute(f"USE {database_name};")
                print(f"YesSQL >> Connected to database schema '{database_name}'.")

        except mysql.connector.Error as e:
            if e.errno == 1045:
                print("YesSQL >> Login credential error, program interrupted.")
                return sys.exit(1)
            if e.errno == 2003:
                print(f"YesSQL >> Host server '{host}:{port}' is unrecheable, program interrupted.")
                return sys.exit(1)
            else:
                print(f"YesSQL >> Connection error: {e}")

            if create_if_not_exists:
                print(f"YesSQL >> Trying to create a database schema '{database_name}' instead.")
                try:
                    conn = mysql.connector.connect(
                        host=host,
                        user=user,
                        password=password,
                        port=port
                    )

                    cursor = conn.cursor()

                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name};")
                    cursor.execute(f"USE {database_name};")
                    conn.commit()
                    print(f"YesSQL >> Database '{database_name}' successfully created and connected to.")

                except mysql.connector.Error as e:
                    if conn is not None:
                        conn.rollback()
                        print(f"YesSQL >> YesSQL succesfully connected to database host, but an error occured when creating/using schema '{database_name}':", e)
                        return sys.exit(1)
                    else:
                        print(f"YesSQL >> Could not connect to database host, program interrupted.")
                        return sys.exit(1)

            else:
                print(f"YesSQL >> Could not connect to database host, program interrupted.")
                return sys.exit(1)

        return conn
    

    def connect_database(self, 
                         host: str = "127.0.0.1",
                         user: str = "user",
                         password: str = "password",
                         database_name: str = "my_db",
                         port: str = "3306",
                         create_if_not_exists: bool = True):
        """
        Connects to the database and creates a connector object ``conn``. 
        """
        return self._connect_database(host=host, user=user, password=password, database_name=database_name, port=port, create_if_not_exists=create_if_not_exists)


    def close_database(self):
        """
        Closes the database linked to the connector ``conn``.
        """

        if self.conn:
            self.conn.close()
            print(f"YesSQL >> Disconnected from database schema '{database_name}'.")


    def _init_database_from_json(self, json_file):
        """
        Creates all the tables of the connected database by reading a setup json file
        """
        with open(json_file, 'r') as file:
            setup = json.load(file)
        
        print(f"YesSQL >> Setting up the database '{setup['database']}' tables")
        cursor = self.conn.cursor()
        for table in setup['tables']:

            table_name = table["name"]
            columns = table["columns"]

            try:

                column_definitions = []
                for column in columns:
                    if "type" in column:
                        col_def = f"{column['name']} {column['type']} {column['constraints']}".strip()
                        column_definitions.append(col_def)
                    else:  # Foreign key constraints
                        col_def = f"{column['name']} {column['constraints']}".strip()
                        column_definitions.append(col_def)

                if not self._check_table_exists(table_name):
                    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print(f"YesSQL >> Table '{table['name']}' successfully created")
                
            except mysql.connector.Error as e:
                conn.rollback()
                print(f"YesSQL >> YesSQL error when creating table '{table['name']}':", e)


    def _check_table_exists(self, table_name):
        try:
            cursor = conn.cursor()
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            result = cursor.fetchone()
            if result:
                return True
            else:
                return False
        except mysql.connector.Error as e:
            print(f"YesSQL >> YesSQL error when creating table '{table_name}':", e)
            return False
        
