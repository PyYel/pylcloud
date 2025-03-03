import sqlite3
import sys
import os

from .Database import Database


class DatabaseSQLite(Database):
    """
    A class that manages SQLite database operations.
    """
    def __init__(self, database_path: str = "database.db") -> None:
        """
        Initializes the SQLite database helper and connects to the database file.
        If the database file does not exist, it will be created.

        Parameters
        ----------
        database_path: str, '../database.db'
            The path to the SQLite database file. Default path is the current working directory.
        """
        super().__init__()
        
        self.database_path = database_path
        try:
            self.conn = self.connect_database(database_path=self.database_path)
        except Exception as e:
            print(f"DatabaseSQLite >> An error occured when connecting to '{os.path.basename(database_path)}': {e}")

        return None
    

    def _connect_database(self, *args, **kwargs):
        """See ``connect_database()``."""
        return self.connect_database(*args, **kwargs)
    
    def connect_database(self, database_path: str):
        """
        Connects to the SQLite database file and returns a connection object.
        If the file doesn't exist, it will be created.

        Parameters
        ----------
        database_path: str
            Path to the SQLite database file.

        Returns
        -------
        conn: sqlite3.Connection
            A connection object to the SQLite database.
        """
        try:
            self.conn = sqlite3.connect(database_path)
            print(f"DatabaseSQLite >> Connected to database at '{database_path}'.")
            return self.conn
        except sqlite3.Error as e:
            print(f"DatabaseSQLite >> SQLite error: {e}")
            sys.exit(1)

        return None
    

    def _create_table(self, *args, **kwargs):
        """See ``create_table()``."""
        return self.create_table(*args, **kwargs)

    def create_table(self, table_name: str, column_definitions: list[str]):
        """
        Creates a table in the SQLite database.

        Parameters
        ----------
        table_name: str
            The name of the table to create.
        column_definitions: list[str]
            The column definitions in SQL syntax.

        Example
        -------
        >>> self.create_table('users', ['username TEXT PRIMARY KEY', 'password TEXT NOT NULL'])
        """
        try:
            cursor = self.conn.cursor()
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
            cursor.execute(create_table_sql)
            self.conn.commit()
            print(f"DatabaseSQLite >> Table '{table_name}' created successfully.")
        except sqlite3.Error as e:
            print(f"DatabaseSQLite >> SQLite error when creating table '{table_name}': {e}")
    
        return None
    

    def _delete_data(self, *args, **kwargs):
        """See ``delete_data()``."""
        return self.delete_data(*args, **kwargs)
    
    def delete_data(self):
        raise NotImplementedError
    

    def _disconnect_database(self, *args, **kwargs):
        """See ``disconnect_database()``."""
        return self.disconnect_database(*args, **kwargs)
    
    def disconnect_database(self):
        """
        Closes the connection to the SQLite database.
        """
        if self.conn:
            self.conn.close()
            print(f"DatabaseSQLite >> Disconnected from database '{os.path.basename(self.database_path)}'.")

        return None
    

    def _drop_database(self, *args, **kwargs):
        """See ``drop_database()``."""
        return self.drop_database(*args, **kwargs)
    
    def drop_database(self, database_path: str = None):
        """
        Drops a database by deleting the .db local file. If no path is specified,
        will try to delete the DB currently in use instead.

        Parameters
        ----------
        database_path: str, None
            The local path of the DB to delete. If no path is specified, the DB currently in use will be erased.
        """
        if database_path is None:
            database_path = self.database_path
            self.disconnect_database()

        try:
            os.remove(database_path)
            print(f"DatabaseSQLite >> Successfully deleted '{os.path.basename(database_path)}'.")
        except Exception as e:
            print(f"DatabaseSQLite >> Could not delete '{os.path.basename(database_path)}': {e}")


    def _drop_table(self, *args, **kwargs):
        """See ``drop_table()``."""
        return self.drop_table(*args, **kwargs)

    def drop_table(self, table_name: str):
        """
        Drops a table if it exists in the SQLite database.

        Parameters
        ----------
        table_name: str
            The name of the table to drop.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.commit()
            print(f"DatabaseSQLite >> Table '{table_name}' dropped successfully.")
        except sqlite3.Error as e:
            print(f"DatabaseSQLite >> SQLite error when dropping table '{table_name}': {e}")

        return None
    

    def _list_databases(self, *args, **kwargs):
        """See ``list_databases()``."""
        return self.list_databases(*args, **kwargs)
    
    def list_databases(self):
        """
        SQLite can only be connected at one database at once, so this won't do anything but return the current database.
        """
        return [self.database_path]


    def _list_tables(self, *args, **kwargs):
        """See ``list_tables()``."""
        return self.list_tables(*args, **kwargs)

    def list_tables(self):
        """
        Lists all tables in the SQLite database.

        Returns
        -------
        tables: list[str]
            A list of table names.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"DatabaseSQLite >> Tables in database: {' | '.join(tables)}")
            return tables
        except sqlite3.Error as e:
            print(f"DatabaseSQLite >> SQLite error when listing tables: {e}")
            return []
    

    def _query_data(self, *args, **kwargs):
        """See ``query_data()``."""
        return self.query_data(*args, **kwargs)
    
    
    def query_data(self, table_name: str, columns: str = "*", condition: str = None, values: tuple = None):
        """
        Retrieves data from a table under a specific condition.

        Parameters
        ----------
        table_name: str
            The name of the table to select data from.
        condition: str
            The WHERE condition in SQL syntax.
        values: tuple
            The values to use in the condition.

        Returns
        -------
        rows: list[tuple]
            The rows retrieved from the table.
        """

        if (condition is None) and (values is None):
            try:
                cursor = self.conn.cursor()
                cursor.execute(f"SELECT {columns} FROM {table_name}")
                rows = cursor.fetchall()
                return rows
            except sqlite3.Error as e:
                print(f"DatabaseSQLite >> SQLite error when selecting data from table '{table_name}': {e}")
                return []
            except:
                # If there is no match
                return []
            
        elif (condition is not None) and (values is not None):
            try:
                cursor = self.conn.cursor()
                query = f"SELECT {columns} FROM {table_name} WHERE {condition}"
                cursor.execute(query, values)
                rows = cursor.fetchall()
                return rows
            except sqlite3.Error as e:
                print(f"DatabaseSQLite >> SQLite error when selecting data with condition: {e}")
                return []

        else:
            print(f"DatabaseSQLite >> Condition and values must both be filled.")
            return []


    def _send_data(self, *args, **kwargs):
        """See ``send_data()``."""
        return self.send_data(*args, **kwargs)

    def send_data(self, table_name: str, **kwargs):
        """
        Inserts data into a table.

        Parameters
        ----------
        table_name: str
            The name of the table to insert data into.
        kwargs: dict
            Column-value pairs to insert.

        Example
        -------
        >>> self.insert('users', username='john_doe', password='securepassword')
        """
        try:
            cursor = self.conn.cursor()
            columns = ', '.join(kwargs.keys())
            placeholders = ', '.join(['?'] * len(kwargs))
            values = tuple(kwargs.values())
            cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", values)
            self.conn.commit()
            print(f"DatabaseSQLite >> Data inserted into table '{table_name}'.")
        except sqlite3.Error as e:
            print(f"DatabaseSQLite >> SQLite error when inserting data into table '{table_name}': {e}")

        return None