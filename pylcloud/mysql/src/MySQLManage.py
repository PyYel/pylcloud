

import sqlite3
import os
import json
import sys
import time
import mysql.connector

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))


class ManageSchemas():
    """
    Methods to generate and edit the MySQL databases (schemas).
    """
    def __init__(self, conn:mysql.connector.MySQLConnection) -> None:
        """
        The default connection is ``main.CONNECTION``. If ``conn`` is specified, it is used instead.

        Parameters
        ----------
        conn: a ``mysql.connector.MySQLConnection`` to a MySQL database. By default, the ``main.CONNECTION`` is used.
        """

        self.conn = conn


    def drop_database(self):
        """
        Deletes the whole database (schema). Must be connected to a user with admin rights, only DataHive can be affected.
        """

        try:
            cursor = self.conn.cursor()

            databases_list = self.list_databases()
            connected_database = self.conn.database
            if connected_database is None:
                print(f"ManageSchemas >> No databse connected, only a connected database (schema) may be dropped")
            elif (connected_database in databases_list) and (connected_database not in ["information_schema", "mysql", "performance_schema", "sys"]):
                cursor.execute(f"DROP DATABASE {connected_database};")
                print(f"ManageSchemas >> Successfully dropped '{connected_database}' database schema")
            elif (connected_database in ["information_schema", "mysql", "performance_schema", "sys"]):
                print(f"ManageSchemas >> Trying to drop a forbidden database schema:", connected_database)
            else:
                print(f"ManageSchemas >> Trying to drop an unknown database schema:", connected_database)

        except mysql.connector.Error as e:
            self.conn.rollback()
            print(f"ManageSchemas >> MySQL error when dropping database schema:", e)

        return None
    

    def setup_database(self):
        """
        Generates the main structure of the database. Most parameters come from .json files. 
        Note that if not existing yet, the database is created when connected to, i.e. when creating the conn object.
        """

        self._init_database_from_json(json_file=os.path.join(DATABASE_DIR_PATH, "json", "setup.json"))

        return None
    

    def list_databases(self, display=False):
        """
        Prints and returns the existing databases (schemas) visible to the user on the server
        """

        cursor = self.conn.cursor()

        cursor.execute("SHOW DATABASES;")
        databases_list = cursor.fetchall()
        databases_list = [db[0] for db in databases_list] # tuples singleton to list
        if display:
            print("Connection >> Visible databases (schemas):", databases_list)

        return databases_list


    def _init_database_from_json(self, json_file):
        """
        Creates all the tables of the connected database by reading a setup json file
        """
        with open(json_file, 'r') as file:
            setup = json.load(file)
        
        print(f"ManageSchemas >> Setting up the database '{setup['database']}' tables")
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
                    self.conn.commit()
                    print(f"ManageSchemas >> Table '{table['name']}' successfully created")
                
            except mysql.connector.Error as e:
                self.conn.rollback()
                print(f"ManageSchemas >> MySQL error when creating table '{table['name']}':", e)


    def _check_table_exists(self, table_name):
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            result = cursor.fetchone()
            if result:
                return True
            else:
                return False
        except mysql.connector.Error as e:
            print(f"ManageSchemas >> MySQL error when creating table '{table_name}':", e)
            return False



if __name__ == "__main__":

    from main import CONNECTION

    mng = ManageSchemas(conn=CONNECTION)

    # mng.drop_database()
    mng.setup_database()
