import os, sys
import mysql.connector
import json
from typing import Union, Optional
from mysql.connector import Error

from .DatabaseRelational import DatabaseRelational


class DatabaseRelationalMySQL(DatabaseRelational):
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
        super().__init__(logs_name="DatabaseMySQL")
        
        self.database_name = database_name.lower()
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        
        try:
            self.connect_database(database_name=self.database_name, create_if_not_exists=False)
        except:
            print(f"DatabaseMySQL >> Auto-connect to '{self.database_name}' failed. Use ``self.connect_database()`` to create a database.")
            # self.conn = None 

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
                print(f"DatabaseMySQL >> Connected to database schema '{database_name}'.")

        except mysql.connector.Error as e:
            if e.errno == 1045:
                print("DatabaseMySQL >> Login credential error, program interrupted.")
                return sys.exit(1)
            if e.errno == 2003:
                print(f"DatabaseMySQL >> Host server '{self.host}:{self.port}' is unreachable, program interrupted.")
                return sys.exit(1)
            if e.errno == 1049:
                print(f"DatabaseMySQL >> Database schema '{database_name}' does not exist.")
            else:
                print(f"DatabaseMySQL >> Connection error: {e}")

            if create_if_not_exists:
                print(f"DatabaseMySQL >> Trying to create a database schema '{database_name}' instead.")
                try:
                    self.create_database(host=self.host, user=self.user, password=self.password, database_name=database_name, port=self.port)
                    print(f"DatabaseMySQL >> Connected to database schema '{database_name}'.")

                except mysql.connector.Error as e:
                    if self.conn is not None:
                        self.conn.rollback()
                        print(f"DatabaseMySQL >> DatabaseMySQL succesfully connected to database host, but an error occured when creating/using schema '{database_name}':", e)
                        return sys.exit(1)
                    else:
                        print(f"DatabaseMySQL >> Could not connect to database host, program interrupted.")
                        return sys.exit(1)

            else:
                print(f"DatabaseMySQL >> Could not connect to database host, program interrupted.")
                return sys.exit(1)

        return None
        

    def create_table(self, table_name: str, column_definitions: list[str]):
        """
        Creates a table in the PostgreSQL database.

        Parameters
        ----------
        table_name: str
            Name of the table to create
        column_definitions: list[str]
            List of column definitions (e.g., ["id SERIAL PRIMARY KEY", "name VARCHAR(100) NOT NULL"])

        Returns
        -------
        bool
            True if table was created or already exists and is accessible, False otherwise
        """
        def _check_table_exists(table_name):
            try:
                cursor = self.conn.cursor()
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                result = cursor.fetchone()
                if result:
                    return True
                else:
                    return False
            except mysql.connector.Error as e:
                print(f"DatabaseMySQL >> MySQL error when creating table '{table_name}':", e)
                return False
            
        cursor = self.conn.cursor()
        if not _check_table_exists(table_name):
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
            cursor.execute(create_table_sql)
            self.conn.commit()
            print(f"DatabaseMySQL >> Table '{table_name}' successfully created in MySQL.")
    
    
    def create_database(self, host, user, password, database_name, port):
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
            print(f"DatabaseMySQL >> Database '{database_name}' created successfully.")
        except mysql.connector.Error as e:
            print(f"DatabaseMySQL >> Error creating database: {e}")
            sys.exit(1)
        finally:
            cursor.close()

        return None
        

    def delete_data(self, FROM: str, WHERE: str, VALUES: tuple[str]):
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
            print("DatabaseMySQL >> MySQL error when deleting data:", e)
        finally:
            cursor.close()

        return None
    

    def disconnect_database(self):
        """
        Closes the database linked to the connector ``conn``.
        """

        if self.conn:
            self.conn.close()
            print(f"DatabaseMySQL >> Disconnected from database schema '{self.database_name}'.")

        return None


    def drop_table(self, table_name: str):
        """
        Drops a table from the database.
        """
        cursor = self.conn.cursor()
        try:
            drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
            cursor.execute(drop_table_query)
            self.conn.commit()
            print(f"DatabaseMySQL >> Successfully dropped table `{table_name}`.")
        except Error as e:
            print(f"DatabaseMySQL >> Failed to drop table `{table_name}`: {e}")
        finally:
            cursor.close()

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
            print(f"DatabaseMySQL >> Successfully dropped '{database_name}' database schema.")

        except mysql.connector.Error as e:
            self.conn.rollback()
            print(f"DatabaseMySQL >> MySQL error when dropping database schema:", e)

        return None       


    def list_databases(self, system_db: bool = False, display: bool = False):
        """
        Prints and returns the existing databases (schemas) visible to the user on the server.
        """

        cursor = self.conn.cursor()

        cursor.execute("SHOW DATABASES;")
        databases_list = cursor.fetchall()
        databases_list = [db[0] for db in databases_list] # tuples singleton to list # type: ignore

        if not system_db: 
            [databases_list.remove(schema) for schema in ['information_schema','mysql', 'performance_schema', 'sys']]

        if display:
            print("DatabaseMySQL >> Visible databases (schemas):", databases_list)
            print(f"DatabaseMySQL >> Currently connected to: '{self.database_name}'.")

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
            tables_list = [row[0] for row in cursor.fetchall()] # Extract table names from the query result # type: ignore
            if display: print(f"DatabaseMySQL >> Tables in '{self.database_name}': {', '.join(tables_list)}") # type: ignore
            return tables_list
        
        except mysql.connector.Error as e:
            print(f"DatabaseMySQL >> MySQL error when listing tables: {e}")
            return []


    def query_data(self, 
                   SELECT: str,
                   FROM: str,
                   WHERE: Optional[str] = None,
                   VALUES: Optional[tuple[str, Union[str, float, int]]] = None,
                   LIKE: Optional[tuple[str, Union[str, float, int]]] = None):
        
        """
        Selects columns from a table under one condition.

        Parameters
        ----------
        SELECT: str
            The names of the columns to select data from.
        FROM: 
            The name of the table to select data from.
        WHERE: str, None
            The name of the column to apply the condition on.
        VALUES: tuple[str], None
            The condition, i.e. the value the cell element must be equal to.
        Like: tuple[str], None
            A condition, i.e. the pattern the cell element must match.

        Returns
        -------
        rows: list[dict[...]]
            The queried records, mapped by columns: [{'col1': value1, 'col3': 'name1', ...}, ...] 

        Examples
        --------
        - Select
        >>> f"SELECT {SELECT} FROM {FROM}"
        >>> query_data(SELECT='full_name, department', FROM='employees')
        >>> SELECT full_name, department FROM employees

        - Select where (must match identically)
        >>> f"SELECT {SELECT} FROM {FROM} WHERE {WHERE}={VALUES}"; 
        >>> query_data(SELECT='full_name, department', FROM='employees', WHERE='department', VALUES=('Sales',))
        >>> SELECT * FROM employees WHERE department = 'Sales';
        
        - Select like (must match a pattern)
        >>> f"SELECT {SELECT} FROM {FROM} WHERE {WHERE} LIKE {VALUES}";
        >>> query_data(SELECT='full_name, department', FROM='employees', WHERE='department', LIKE=('john%',))
        >>> SELECT * FROM employees WHERE full_name LIKE 'john%';
        """

        # SELECT only
        if (SELECT is not None) and ((WHERE is None) and (VALUES is None) and (LIKE is None)):
            try: 
                cursor = self.conn.cursor()

                cursor.execute(f"SELECT {SELECT} FROM {FROM};")

                rows = cursor.fetchall()
                return rows
            except mysql.connector.Error as e:
                print("DatabaseMySQL >> MySQL error when selecting data:", e)
            finally:
                cursor.close()
        
        # WHERE VALUES
        if (SELECT is not None) and ((WHERE is not None) and (VALUES is not None) and (LIKE is None)):
            try:
                cursor = self.conn.cursor()

                format_strings = ','.join(['%s'] * len(VALUES))
                sql = f"SELECT {SELECT} FROM {FROM} WHERE {WHERE}=({format_strings});"
                cursor.execute(sql, VALUES)

                rows = cursor.fetchall()
                return rows
            except mysql.connector.Error as e:
                print("DatabaseMySQL >> MySQL error when selecting data:", e)
                self.conn.rollback()
            finally:
                cursor.close()
                return []

        # WHERE LIKE
        if (SELECT is not None) and ((WHERE is not None) and (VALUES is None) and (LIKE is not None)):
            try:
                cursor = self.conn.cursor()

                sql = f"SELECT {SELECT} FROM {FROM} WHERE {WHERE} LIKE %s;"
                cursor.execute(sql, LIKE)

                rows = cursor.fetchall()
                return rows
            except mysql.connector.Error as e:
                print("DatabaseMySQL >> MySQL error when selecting data:", e)
                self.conn.rollback()
            finally:
                cursor.close()
                return []

        return []


    def send_data(self, table_name: str, **kwargs):
        """
        Inserts the input kwargs into the table ``table_name``. 
        """

        try:
            cursor = self.conn.cursor()

            fields = ",".join(list(kwargs.keys()))
            placeholders = ",".join(list([r'%s']*len(fields)))
            values = tuple(kwargs.values())

            cursor.execute(f"INSERT INTO {table_name} {fields} VALUES ({placeholders});", 
                           (values))
            self.conn.commit()

        except mysql.connector.Error as e:
            print(f"DatabaseMySQL >> MySQL error when sending data into '{table_name}': {e}")
            self.conn.rollback()

        finally:
            cursor.close()

        return None
    

        
