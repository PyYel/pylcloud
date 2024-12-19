import os, sys
import mysql.connector
import json

from mysql.connector import Error
from abc import ABC

from yessql import YesSQL

class YesSQLMySQL(YesSQL):
    """
    A parent class that notably manages the global MySQL database server.
    """
    def __init__(self, 
                 database_name="my_db",
                 host="127.0.0.1",
                 user="admin",
                 password="password",
                 port="3306"
                 ) -> None:
        """
        Initializes the database connector helper. The database will still require to be created and/or connected to.
        """
        
        self.connect_database
        self.database_name = database_name.lower()
        self.host = host
        self.user = user
        self.password = password
        self.port = port

        self.conn = None

        return None


    def close_database(self):
        """
        Closes the database linked to the connector ``conn``.
        """

        if self.conn:
            self.conn.close()
            print(f"MySQL >> Disconnected from database schema '{self.database_name}'.")


    def drop_database(self):
        """
        Deletes the whole database (schema). Must be connected to a user with admin rights, system schemas can't be dropped.
        """

        try:
            cursor = self.conn.cursor()

            databases_list = self.list_databases()
            connected_database = self.conn.database
            if connected_database is None:
                print(f"MySQL >> No databse connected, only a connected database (schema) may be dropped")
            elif (connected_database in databases_list) and (connected_database not in ["information_schema", "mysql", "performance_schema", "sys"]):
                cursor.execute(f"DROP DATABASE {connected_database};")
                print(f"MySQL >> Successfully dropped '{connected_database}' database schema.")
            elif (connected_database in ["information_schema", "mysql", "performance_schema", "sys"]):
                print(f"MySQL >> Trying to drop a forbidden database schema: '{connected_database}'.")
            else:
                print(f"MySQL >> Trying to drop an unknown database schema: '{connected_database}'.")

        except mysql.connector.Error as e:
            self.conn.rollback()
            print(f"MySQL >> MySQL error when dropping database schema:", e)

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
            print("MySQL >> Visible databases (schemas):", databases_list)

        return databases_list


    def delete_where(self, FROM: str, WHERE: str, VALUES: tuple[str]):
        """
        Removes data from a table under a condition

        Parameters
        ----------
        FROM: the table to remove data from
        WHERE: the condition on the columns
        VALUEs: the values these columns must match. Must be a tuple, even if only one value is given 
        
        Examples
        --------
        >>> delete_where(FROM="datapoints", WHERE="datapoint_path", VALUES=("DataHive/Data/dataset_example/001.pg",))
        """

        try:
            cursor = self.conn.cursor()

            format_strings = ','.join(['%s'] * len(VALUES))
            query = f"DELETE FROM {FROM} WHERE {WHERE}=({format_strings});"
            cursor.execute(query, VALUES)

            self.conn.commit()

        except mysql.connector.Error as e:
            print("RemoveData >> MySQL error:", e)

        return None
    

    def select(self, table_name:str, SELECT:str="*"):
        """
        Allows to input a simple SELECT SQL query.
        Returns a list (rows) of tuples (columns)

        Eg: SELECT {SELECT} FROM {table_name}
        >>> SELECT label_key, transcription FROM Audio_transcription
        >>> [(label_key1, text1), (label_key2, text2), ...]

        Parameters
        ----------
        - table_name: the name of the table to select rows from
        - SELECT: the name of the columns to select data from
        """

        try: 
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT {SELECT} FROM {table_name};")
            rows = cursor.fetchall()

            return rows

        except mysql.connector.Error as e:
            print("RequestData >> MySQL error:", e)


    def select_where(self, 
                     SELECT: str,
                     FROM: str,
                     WHERE: str,
                     VALUES: tuple[str]):
        """
        Selects columns from a table under one condition.

        >>> f"SELECT {SELECT} FROM {FROM} WHERE {WHERE}={VALUES}"; 

        Parameters
        ----------
        - SELECT: the names of the columns to select data from
        - FROM: the name of the table to select data from
        - WHERE: the name of the column to apply the condition on
        - VALUES: the condition, i.e. the value the cell element must be equal to
        """
        try:
            cursor = self.conn.cursor()

            format_strings = ','.join(['%s'] * len(VALUES))
            sql = f"SELECT {SELECT} FROM {FROM} WHERE {WHERE}=({format_strings});"
            cursor.execute(sql, VALUES)
            rows = cursor.fetchall()
            return rows
        
        except mysql.connector.Error as e:
            print("RequestData >> MySQL error:", e)
            self.conn.rollback()
            return None
        

    def select_like(self, 
                    SELECT: str,
                    FROM: str,
                    WHERE: tuple[str],
                    LIKE: tuple[str]):
            """
            Selects columns from a table under one condition.

            >>> f"SELECT {SELECT} FROM {FROM} WHERE {WHERE} LIKE {VALUES}";

            Args
            ----
            - SELECT: the names of the columns to select data from
            - FROM: the name of the table to select data from
            - WHERE: the name of the column to apply the condition on
            - VALUES: the condition, i.e. the value the cell element must be equal to
            """
            try:
                cursor = self.conn.cursor()

                sql = f"SELECT {SELECT} FROM {FROM} WHERE {WHERE} LIKE %s;"
                cursor.execute(sql, LIKE)
                rows = cursor.fetchall()
                return rows
            
            except mysql.connector.Error as e:
                print("RequestData >> MySQL error:", e)
                self.conn.rollback()
                return None
        


    def insert(self,
               table_name: str,
               **kwargs):
        """
        Inserts the input kwargs into the table ``table_name``. 
        """

        try:
            cursor = self.conn.cursor()

            fields = ",".join(list(kwargs.keys()))
            placeholders = ",".join(list([r'%s']*len(fields)))
            values = tuple(kwargs.values())

            cursor.execute(f"INSERT INTO users_projects {fields} VALUES ({placeholders});", 
                           (values))

            self.conn.commit()

        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        return None
    

    def _init_database_from_json(self, json_file):
        """
        Creates all the tables of the connected database by reading a setup json file
        """
        with open(json_file, 'r') as file:
            setup = json.load(file)
        
        print(f"MySQL >> Setting up the database '{setup['database']}' tables")
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
                    print(f"MySQL >> Table '{table['name']}' successfully created")
                
            except mysql.connector.Error as e:
                self.conn.rollback()
                print(f"MySQL >> MySQL error when creating table '{table['name']}':", e)


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
            print(f"MySQL >> MySQL error when creating table '{table_name}':", e)
            return False
        
