import os, sys
import psycopg2
from psycopg2 import sql, OperationalError, errors
from typing import Union, Optional
import json

from .Database import Database


class DatabasePostgreSQL(Database):
    """
    A parent class that manages the global PostgreSQL database server.
    """
    def __init__(self,
                 database_name="my_db",
                 host="127.0.0.1",
                 user="admin",
                 password="password",
                 port="5432"
                 ) -> None:
        super().__init__()

        self.database_name = database_name.lower()
        self.host = host
        self.user = user
        self.password = password
        self.port = port

        try:
            self.connect_database(database_name=self.database_name, create_if_not_exists=False)
        except:
            print(f"DatabasePostgreSQL >> Auto-connect to '{self.database_name}' failed. Use ``self.connect_database()`` to create a database.")
    
        return None


    def connect_database(self, database_name: str = "my_db", create_if_not_exists: bool = True):
        """
        Connects to a PostgreSQL database.
        """
        self.database_name = database_name

        try:
            self.conn = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                dbname=self.database_name,
                port=self.port
            )
            print(f"DatabasePostgreSQL >> Connected to database schema '{database_name}'.")

        except OperationalError as e:
            if 'does not exist' in str(e):
                print(f"DatabasePostgreSQL >> Database schema '{database_name}' does not exist.")
            else:
                print(f"DatabasePostgreSQL >> Connection error: {e}")

            if create_if_not_exists:
                print(f"DatabasePostgreSQL >> Trying to create database schema '{database_name}' instead.")
                try:
                    self.create_database(database_name)
                    print(f"DatabasePostgreSQL >> Connected to database schema '{database_name}'.")
                except Exception as e:
                    print(f"DatabasePostgreSQL >> Could not connect to database host, program interrupted: {e}")
                    return sys.exit(1)
            else:
                print(f"DatabasePostgreSQL >> Could not connect to database host, program interrupted.")
                return sys.exit(1)

        return None


    def create_database(self, database_name):
        """
        Creates a PostgreSQL database if it doesn't exist.
        """
        try:
            conn = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                dbname='postgres',
                port=self.port
            )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
            cursor.close()
            conn.close()
            print(f"DatabasePostgreSQL >> Database '{database_name}' created successfully.")
            self.connect_database(database_name)
        except Exception as e:
            print(f"DatabasePostgreSQL >> Error creating database: {e}")
            sys.exit(1)

        return None


    def create_table(self, table_name: str, column_definitions: list[str]):
        """
        Creates a table in a PostgreSQL database.
        """
        def _check_table_exists(table_name):
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);", (table_name,))
                if cursor.fetchone() is not None:
                    return True
                else:
                    return False
            except Exception as e:
                print(f"DatabasePostgreSQL >> PostgreSQL error when checking table '{table_name}': {e}")
                return False

        cursor = self.conn.cursor()
        if not _check_table_exists(table_name):
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
            cursor.execute(create_table_sql)
            self.conn.commit()
            print(f"DatabasePostgreSQL >> Table '{table_name}' successfully created in PostgreSQL.")
        else:
            print(f"DatabasePostgreSQL >> Table '{table_name}' already exists.")
        cursor.close()

        return None


    def delete_data(self, FROM: str, WHERE: str, VALUES: tuple[str]):
        """
        Removes data from a table under a condition.
        """
        try:
            cursor = self.conn.cursor()
            format_strings = ','.join(['%s'] * len(VALUES))
            query = f"DELETE FROM {FROM} WHERE {WHERE}=({format_strings});"
            cursor.execute(query, VALUES)
            self.conn.commit()
        except Exception as e:
            print(f"DatabasePostgreSQL >> PostgreSQL error when deleting data: {e}")
        finally:
            cursor.close()

        return None


    def disconnect_database(self):
        """
        Closes the database connection.
        """
        if self.conn:
            self.conn.close()
            print(f"DatabasePostgreSQL >> Disconnected from database schema '{self.database_name}'.")

        return None


    def drop_table(self, table_name: str):
        """
        Drops a table from the database.
        """
        cursor = self.conn.cursor()
        try:
            drop_table_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
            cursor.execute(drop_table_query)
            self.conn.commit()
            print(f"DatabasePostgreSQL >> Successfully dropped table `{table_name}`.")
        except Exception as e:
            print(f"DatabasePostgreSQL >> Failed to drop table `{table_name}`: {e}")
        finally:
            cursor.close()

        return None


    def drop_database(self, database_name: str):
        """
        Drops the entire PostgreSQL database.
        """
        try:
            if self.conn:
                self.conn.close()

            conn = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                dbname='postgres',
                port=self.port
            )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database_name)))
            print(f"DatabasePostgreSQL >> Successfully dropped '{database_name}' database schema.")
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"DatabasePostgreSQL >> PostgreSQL error when dropping database schema: {e}")

        return None


    def list_databases(self, display: bool = False):
        """
        Lists all databases on the server.
        """
        try:
            conn = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                dbname='postgres',
                port=self.port
            )
            cursor = conn.cursor()
            cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
            databases_list = [db[0] for db in cursor.fetchall()]
            if display:
                print("DatabasePostgreSQL >> Available databases:", databases_list)
            cursor.close()
            conn.close()
            return databases_list
        except Exception as e:
            print(f"DatabasePostgreSQL >> PostgreSQL error when listing databases: {e}")
            return []


    def list_tables(self, display: bool = False):
        """
        Lists all tables in the current PostgreSQL schema.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables_list = [row[0] for row in cursor.fetchall()]
            if display:
                print(f"DatabasePostgreSQL >> Tables in '{self.database_name}': {', '.join(tables_list)}")
            cursor.close()
            return tables_list
        except Exception as e:
            print(f"DatabasePostgreSQL >> PostgreSQL error when listing tables: {e}")
            return []


    def query_data(self,
                   SELECT: str,
                   FROM: str,
                   WHERE: Optional[str] = None,
                   VALUES: Optional[tuple[str, Union[str, float, int]]] = None,
                   LIKE: Optional[tuple[str, Union[str, float, int]]] = None):
        """
        Selects data from a PostgreSQL table with optional filtering.
        """
        try:
            cursor = self.conn.cursor()

            # Basic SELECT
            if (SELECT is not None) and (WHERE is None) and (VALUES is None) and (LIKE is None):
                cursor.execute(f"SELECT {SELECT} FROM {FROM};")
                rows = cursor.fetchall()
                cursor.close()
                return rows

            # WHERE exact match
            if (SELECT is not None) and (WHERE is not None) and (VALUES is not None) and (LIKE is None):
                format_strings = ','.join(['%s'] * len(VALUES))
                sql_query = f"SELECT {SELECT} FROM {FROM} WHERE {WHERE}=({format_strings});"
                cursor.execute(sql_query, VALUES)
                rows = cursor.fetchall()
                cursor.close()
                return rows

            # WHERE LIKE pattern
            if (SELECT is not None) and (WHERE is not None) and (LIKE is not None):
                sql_query = f"SELECT {SELECT} FROM {FROM} WHERE {WHERE} LIKE %s;"
                cursor.execute(sql_query, LIKE)
                rows = cursor.fetchall()
                cursor.close()
                return rows

            cursor.close()
            return []
        except Exception as e:
            print(f"DatabasePostgreSQL >> PostgreSQL error when selecting data: {e}")
            return []


    def send_data(self, table_name: str, **kwargs):
        """
        Inserts data into a PostgreSQL table.
        """
        try:
            cursor = self.conn.cursor()
            fields = ",".join(list(kwargs.keys()))
            placeholders = ",".join(["%s"] * len(kwargs))
            values = tuple(kwargs.values())

            cursor.execute(f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders});", values)
            self.conn.commit()
        except Exception as e:
            print(f"DatabasePostgreSQL >> PostgreSQL error when inserting data into '{table_name}': {e}")
            self.conn.rollback()
        finally:
            cursor.close()

        return None


    def commit_transactions(self):
        """
        Commits the transactions operated since the last commit.
        """
        raise NotImplementedError


    def rollback_transactions(self):
        """
        roolbacks the transactions operated since the last commit.
        """
        raise NotImplementedError


    def execute_file(self, file_path: str):
        """
        Executes SQL commands from a .sql file or inserts data from a .json file.

        Parameters
        ----------
        file_path : str
            The path to the .sql or .json file.
        """

        import os

        if not os.path.isfile(file_path):
            print(f"DatabasePostgreSQL >> File '{file_path}' does not exist.")
            return None

        file_extension = os.path.splitext(file_path)[1]

        try:
            cursor = self.conn.cursor()

            if file_extension == '.sql':
                with open(file_path, 'r', encoding='utf-8') as sql_file:
                    sql_commands = sql_file.read()
                    cursor.execute(sql_commands)
                    self.conn.commit()
                    print(f"DatabasePostgreSQL >> SQL file '{file_path}' executed successfully.")

            elif file_extension == '.json':
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)

                if not isinstance(data, list):
                    print("DatabasePostgreSQL >> JSON file must contain a list of records (dictionaries).")
                    return None

                for record in data:
                    if not isinstance(record, dict) or 'table' not in record or 'data' not in record:
                        print("DatabasePostgreSQL >> JSON format invalid. Each record must contain 'table' and 'data' keys.")
                        return None

                    table_name = record['table']
                    fields = ', '.join(record['data'].keys())
                    placeholders = ', '.join(['%s'] * len(record['data']))
                    values = tuple(record['data'].values())

                    cursor.execute(f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders});", values)

                self.conn.commit()
                print(f"DatabasePostgreSQL >> JSON file '{file_path}' data inserted successfully.")

            else:
                print(f"DatabasePostgreSQL >> Unsupported file type '{file_extension}'. Only .sql and .json are supported.")

        except psycopg2.Error as e:
            print(f"DatabasePostgreSQL >> PostgreSQL error when executing file: {e}")
            self.conn.rollback()

        finally:
            cursor.close()

        return None
