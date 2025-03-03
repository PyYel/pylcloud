import os, sys
import mysql.connector
import json

from mysql.connector import Error

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
        Initializes the database connector helper. 
        
        You must always be connected to a database, as a direct server connection is not allowed.
        To change of database (schema), you must 'reconnect' to the server (cf. Note below).

        Parameters
        ----------
        database_name: str, 'my_db'
            The name of the database (schema) to use.
        host: str, '127.0.0.1'
            The host server adress.
        user: str, 'admin
            The name of the DB user to connect with.
        password: str, 'passworrd'
            The DB user's password.
        port: str, '3306'
            The port the server is hosted on.

        Notes
        -----
        - A MySQL database is a server that can host multiple schemas on the same address.
        This design flattens server/database/schema concepts:
            - ``database_name`` refers to the schema.
            - Switching databases requires reconnecting to the server, even if credentials remain unchanged.
        """
        super().__init__(db_type="mysql")
        
        self.database_name = database_name.lower()
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        
        try:
            self.connect_database(database_name=self.database_name, create_if_not_exists=False)
        except:
            print(f"YesSQLMySQL >> Auto-connect to '{self.database_name}' failed. Use ``self.connect_database()`` to create a database.")
            self.conn = None 

        return None


    def connect_database(self, database_name: str = "my_db", create_if_not_exists: bool = True):
        """
        Connects to the database and creates a connector object ``conn``. 
        
        This will only allow you to create or switch of database schema. To change of host, or connect as an other user, 
        you should create a new ``YesSQML`` instance. 

        Parameters
        ----------
        database_name: str, 'my_db'
            The name of the database (schema) to use.
        create_if_not_exists: bool, True
            If the specified database does not exist, this will create an empty schema.
        """

        self.database_name = database_name

        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database_name,
                port=self.port
            )
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute(f"USE {database_name};")
                print(f"YesSQLMySQL >> Connected to database schema '{database_name}'.")

        except mysql.connector.Error as e:
            if e.errno == 1045:
                print("YesSQLMySQL >> Login credential error, program interrupted.")
                return sys.exit(1)
            if e.errno == 2003:
                print(f"YesSQLMySQL >> Host server '{self.host}:{self.port}' is unreachable, program interrupted.")
                return sys.exit(1)
            if e.errno == 1049:
                print(f"YesSQLMySQL >> Database schema '{database_name}' does not exist.")
            else:
                print(f"YesSQLMySQL >> Connection error: {e}")

            if create_if_not_exists:
                print(f"YesSQLMySQL >> Trying to create a database schema '{database_name}' instead.")
                try:
                    self._create_database(host=self.host, user=self.user, password=self.password, database_name=database_name, port=self.port)
                    print(f"YesSQLMySQL >> Connected to database schema '{database_name}'.")

                except mysql.connector.Error as e:
                    if self.conn is not None:
                        self.conn.rollback()
                        print(f"YesSQLMySQL >> YesSQLMySQL succesfully connected to database host, but an error occured when creating/using schema '{database_name}':", e)
                        return sys.exit(1)
                    else:
                        print(f"YesSQLMySQL >> Could not connect to database host, program interrupted.")
                        return sys.exit(1)

            else:
                print(f"YesSQLMySQL >> Could not connect to database host, program interrupted.")
                return sys.exit(1)

        return None
        

    def _create_database(self, host, user, password, database_name, port):
        """
        Creates a MySQL database if it doesn't exist.
        """
        try:
            self.conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                port=port
            )
            cursor = self.conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            cursor.close()
            print(f"YesSQLMySQL >> Database '{database_name}' created successfully.")
        except mysql.connector.Error as err:
            print(f"YesSQLMySQL >> Error creating database: {err}")
            sys.exit(1)

        return None
        

    def disconnect_database(self):
        """
        Closes the database linked to the connector ``conn``.
        """

        if self.conn:
            self.conn.close()
            print(f"YesSQLMySQL >> Disconnected from database schema '{self.database_name}'.")

        return None


    def drop_database(self, database_name: str):
        """
        Deletes the whole database (schema). Must be connected to a user with admin rights, system schemas can't be dropped.
        """

        if database_name in ['information_schema','mysql', 'performance_schema', 'sys']:
            print(f"YesMySQL >> Database schema '{database_name}' is a builtin system schema, and can't be dropped.")
            return None
        if database_name not in self.list_databases(system_db=False):
            print(f"YesMySQL >> Database schema '{database_name}' not found.")
            return None

        try:
            cursor = self.conn.cursor()

            cursor.execute(f"DROP DATABASE {database_name};")
            print(f"YesSQLMySQL >> Successfully dropped '{database_name}' database schema.")

        except mysql.connector.Error as e:
            self.conn.rollback()
            print(f"YesSQLMySQL >> MySQL error when dropping database schema:", e)

        return None
    

    def drop_table(self, table_name):
        return super().drop_table(table_name)
        

    def create_table(self, table_name: str, column_definitions: list[str]):
        """
        Creates a table in a MySQL database.

        Parameters
        ----------
        table_name: str
            The name of the table to create. If it already exists, you will need to delete it beforehand,
            as this won't overwrite any existing table.
        columns_definitions: list[str]
            The SQL synthax of a column definition, i.e, a list of string where each item is formatted as
            'column_name dtype constraint' (cf. example).

        Example
        -------
        >>> self.create_table('users', ['username VARCHAR(12) PRIMARY KEY', 'password VARCHAR(100) NOT NULL'])
        """
        cursor = self.conn.cursor()
        if not self._check_table_exists(table_name):
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
            cursor.execute(create_table_sql)
            self.conn.commit()
            print(f"YesSQLMySQL >> Table '{table_name}' successfully created in MySQL.")
    

    def list_databases(self, system_db: bool = False, display: bool = False):
        """
        Prints and returns the existing databases (schemas) visible to the user on the server.
        """

        cursor = self.conn.cursor()

        cursor.execute("SHOW DATABASES;")
        databases_list = cursor.fetchall()
        databases_list = [db[0] for db in databases_list] # tuples singleton to list

        if not system_db: 
            [databases_list.remove(schema) for schema in ['information_schema','mysql', 'performance_schema', 'sys']]

        if display:
            print("YesSQLMySQL >> Visible databases (schemas):", databases_list)
            print(f"YesSQLMySQL >> Currently connected to: '{self.database_name}'.")

        return databases_list
    
    
    def list_tables(self, display: bool = False):
        """
        Lists all tables in the currently connected MySQL database.

        Returns
        -------
        tables_list: list[str]
            A list of the table names present in the schema.
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute("SHOW TABLES;")
            tables_list = [row[0] for row in cursor.fetchall()]  # Extract table names from the query result
            if display: print(f"YesSQLMySQL >> Tables in '{self.database_name}': {tables_list}")
            return tables_list
        
        except mysql.connector.Error as e:
            print(f"YesSQLMySQL >> MySQL error when listing tables: {e}")
            return []


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
            print("YesSQLMySQL >> MySQL error when deleting data:", e)

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
            print("YesSQLMySQL >> MySQL error when selecting data:", e)


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
            print("YesSQLMySQL >> MySQL error when selecting data:", e)
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
                print("YesSQLMySQL >> MySQL error when selecting data:", e)
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
            print("YesSQLMySQL >> MySQL error when inserting data:", e)
            self.conn.rollback()

        return None
    

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
            print(f"YesSQLMySQL >> MySQL error when creating table '{table_name}':", e)
            return False
        
