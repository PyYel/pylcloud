import os, sys
import psycopg2
from psycopg2 import sql, OperationalError, errors
from psycopg2._psycopg import connection
from psycopg2.extras import DictCursor
from typing import Union, Optional, Any
import json
import boto3
import psycopg2._psycopg

from .Database import Database

class DatabasePostgreSQL(Database):
    """
    A parent class that manages PostgreSQL database connections,
    compatible with standard PostgreSQL, AWS Aurora PostgreSQL, and AWS RDS PostgreSQL.
    """
    def __init__(self,
                 schema_name: str = "public",
                 host: str = "127.0.0.1",
                 user: str = "postgres",
                 password: Optional[str] = None,
                 port: str = "5432",
                 ssl_mode: Optional[str] = None,
                 connection_timeout: int = 30,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 aws_region_name: Optional[str] = None,
                 ) -> None:
        """
        A high-level interface for PostgreSQL server database, compatible with standard PostgreSQL, 
        AWS Aurora PostgreSQL, and AWS RDS PostgreSQL.

        For an explanation of the 'database' and 'schema' denomination, see the Notes below.

        Parameters
        ----------
        schema_name: str
            The name of the schema (can be seen as the database name) to connect to. Having multiple databases on a same server is not
            supported, so the server management is limited to schema level. 
        host: str
            The host/address of the database server.
            - When connecting to a local server, the IP of host computer
            - When connecting to AWS, the address of the read/write endpoint
        user: str
            The user to assume when interacting with the DB
            - Must be an existing DB user
            - Can be an IAM user, provided an homonymous IAM user exists
        password: str
            The password for the database user (not used if IAM authentication is enabled)
            - When a password is given, will assume a direct connection to the DB using the given user and password
            - When password is None, will assume an IAM authentication, so a connection token will be generated for this IAM user
        port: str
            The port number for the database connection.
        ssl_mode: Optional[str]
            SSL mode for the connection (e.g., 'require', 'verify-ca', 'verify-full').
        connection_timeout: int
            Connection timeout in seconds.
        aws_access_key_id: Optional[str]
            AWS access key ID for IAM authentication.
        aws_secret_access_key: Optional[str]
            AWS secret access key for IAM authentication.
        aws_region_name: Optional[str]
            AWS region name for IAM authentication.

        Notes
        -----
        - The 'database' in common words often refers to a 'schema' in technical terms. Thus, a database can 
        rather be seen as a server, and a schema as a database.
            - A schema is a collection of tables
            - A database is a collection of schemas
        - Database management is a rather uncommon operation. For a more streamlined usage of this helper, management is limited
        to schema level.
        """
        super().__init__(logs_name="DatabasePostgreSQL")

        self.schema_name = schema_name.lower().replace("-", "_").replace(" ", "_")
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.ssl_mode = ssl_mode
        self.connection_timeout = connection_timeout

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region_name = aws_region_name

        self.conn: psycopg2._psycopg.connection = None # type: ignore

        try:
            self.connect_database(schema_name=self.schema_name, create_if_not_exists=False)
        except Exception as e:
            print(f"DatabasePostgreSQL >> Auto-connect to '{self.schema_name}' failed, make sure the schema and user exist: {e}")
        
        return None
    

    def _create_iam_user(self, user: str, schema_name: Optional[str] = None):
        """
        Create a database user with IAM authentication. 
        The user should match an existing IAM user with RDS permissions.

        Parameters
        ----------
        user: str
            The name of the IAM user to create.
        schema_name: str
            The schema to give user access to. If None, will apply to the current schema in use.
        """

        if schema_name is not None:
            self.schema_name = schema_name.lower().replace("-", "_").replace(" ", "_")

        try:
            with self.conn.cursor() as cursor:
                # Check if user exists
                cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = %s)",
                    (user,)
                )
                user_exists = cursor.fetchone()[0]

                if not user_exists:
                    cursor.execute(
                        sql.SQL("CREATE USER {} WITH LOGIN").format(sql.Identifier(user))
                    )
                    cursor.execute(
                        sql.SQL("GRANT rds_iam TO {}").format(sql.Identifier(user))
                    )
                    print(f"DatabasePostgreSQL >> User '{user}' created with IAM authentication")
                else:
                    print(f"DatabasePostgreSQL >> User '{user}' already exists")

                cursor.execute(
                    sql.SQL("GRANT ALL PRIVILEGES ON SCHEMA {} TO {}").format(
                        sql.Identifier(self.schema_name),
                        sql.Identifier(user)
                    )
                )
                cursor.execute(
                    sql.SQL("ALTER USER {} SET search_path TO {}").format(
                        sql.Identifier(user),
                        sql.Identifier(self.schema_name)
                    )
                )
                cursor.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                        sql.Identifier(user)
                    )
                )
                self.conn.commit()
                print(f"DatabasePostgreSQL >> Granted privileges on schema '{self.schema_name}' to user '{user}'. Consider reconnecting to the DB.")

        except Exception as e:
            print(f"DatabasePostgreSQL >> Error creating IAM user: {e}")

        return None


    def connect_database(self, schema_name: Optional[str] = None, create_if_not_exists: bool = False):
        """
        Connects to the PostgreSQL 'postgres' database and sets the specified schema as the search path.

        Parameters
        ----------
        schema_name: Optional[str] 
            The schema name to use within the postgres database. If None, uses the schema name from __init__.
        create_if_not_exists: bool
            Whether to create the specified schema if it doesn't exist.

        Note
        ----
        - Connection will instantiate a connector object: ``self.conn``.
        - This method always connects to the 'postgres' database and then sets the search path to the specified schema.
        """

        def _create_schema():
            """
            Creates a new schema in the PostgreSQL database if it doesn't exist.
            """
            try:
                with self.conn.cursor() as cursor:
                    # Create schema if it doesn't exist
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name};")
                    self.conn.commit()
                    print(f"DatabasePostgreSQL >> Schema '{self.schema_name}' created successfully.")

                    # Set the search path to the new schema
                    cursor.execute(f"SET search_path TO {self.schema_name}, public;")
                    self.conn.commit()
            except Exception as e:
                print(f"DatabasePostgreSQL >> Error creating schema: {e}")
                raise

            return None

        def _get_connection_params():
            """
            Prepares connection parameters based on the connection type.
            Always connects to 'postgres' database.
            """
            params = {
                'host': self.host,
                'user': self.user,
                'password': self.password,
                'dbname': 'postgres',  # Always connect to 'postgres' database
                'port': self.port,
                'connect_timeout': self.connection_timeout
            }

            if self.ssl_mode:
                params['sslmode'] = self.ssl_mode

            # No password assume an IAM connection
            if self.password is None:
                try:
                    client = boto3.client('rds', 
                                        aws_access_key_id=self.aws_access_key_id, 
                                        aws_secret_access_key=self.aws_secret_access_key, 
                                        region_name=self.aws_region_name)
                    token = client.generate_db_auth_token(
                        DBHostname=self.host,
                        Port=int(self.port), 
                        DBUsername=self.user, # IAM Username
                        Region=self.aws_region_name
                    )
                    params['password'] = token
                    # For Aurora Serverless, SSL is mandatory
                    # if 'sslmode' not in params:
                    params['sslmode'] = 'require'

                except Exception as e:
                    print(f"DatabasePostgreSQL >> Error generating AWS auth token: {e}")
                    return {}

            return params

        # Update schema name if provided
        if schema_name:
            self.schema_name = schema_name.lower().replace("-", "_").replace(" ", "_")

        try:
            # Connect and use schema (creates it if create_if_not_exists)
            connection_params = _get_connection_params()
            self.conn = psycopg2.connect(**connection_params)

            with self.conn.cursor() as cursor:
                cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %s)", 
                            (self.schema_name,))
                schema_exists = cursor.fetchone()[0]

                if not schema_exists:
                    if create_if_not_exists:
                        print(f"DatabasePostgreSQL >> Schema '{self.schema_name}' does not exist. Creating it.")
                        _create_schema()
                    else:
                        print(f"DatabasePostgreSQL >> Schema '{self.schema_name}' does not exist and create_if_not_exists=False.")
                        self.conn.close()
                        return None

                # Set the search path to the specified schema
                cursor.execute(f"SET search_path TO {self.schema_name}, public;")
                self.conn.commit()

            print(f"DatabasePostgreSQL >> Connected to schema '{self.schema_name}'.")

        except psycopg2.OperationalError as e:
            print(f"DatabasePostgreSQL >> Connection error: {e}")
            return None
        except Exception as e:
            print(f"DatabasePostgreSQL >> Unexpected error: {e}")
            return None

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

        try:
            with self.conn.cursor() as cursor:
                full_table_name = f"{self.schema_name}.{table_name}"

                create_table_sql = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({', '.join(column_definitions)})"
                cursor.execute(create_table_sql)

                cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)", 
                            (self.schema_name, table_name))
                table_exists = cursor.fetchone()[0]

                if table_exists:
                    print(f"DatabasePostgreSQL >> Table '{self.schema_name}.{table_name}' successfully created or already exists.")
                    self.conn.commit()
                    return True
                else:
                    print(f"DatabasePostgreSQL >> Failed to create table '{self.schema_name}.{table_name}'.")
                    return False

        except Exception as e:
            self._rollback()
            print(f"DatabasePostgreSQL >> Error creating table '{table_name}': {e}")
            return False
        
        
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
            print(f"DatabasePostgreSQL >> Disconnected from database schema '{self.schema_name}'.")

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


    def drop_database(self, schema_name: str):
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
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(schema_name)))
            self._commit()
            print(f"DatabasePostgreSQL >> Successfully dropped '{schema_name}' database schema.")
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


    def list_schemas(self, include_system_schemas: bool = False, display: bool = False):
        """
        Lists all schemas in the specified database or in the currently connected database.

        Parameters
        ----------
        include_system_schemas: bool, default=False
            Whether to include system schemas (pg_*, information_schema) in the results.
        display: bool, default=False
            Whether to print the list of schemas.

        Returns
        -------
        list
            A list of schema names.
        """
        try:
            cursor = self.conn.cursor()

            if include_system_schemas:
                query = "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;"
            else:
                query = """
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT LIKE 'pg_%' 
                AND schema_name != 'information_schema'
                ORDER BY schema_name;
                """

            cursor.execute(query)
            schemas_list = [schema[0] for schema in cursor.fetchall()]

            if display:
                print(f"DatabasePostgreSQL >> Schemas in database:", schemas_list)

            cursor.close()

            return schemas_list

        except Exception as e:
            print(f"DatabasePostgreSQL >> PostgreSQL error when listing schemas: {e}")
            return []
                

    def list_tables(self, display: bool = False):
        """
        Lists all tables in the specified PostgreSQL schema or the current schema.

        Parameters
        ----------
        display: bool, default=False
            Whether to print the list of tables.

        Returns
        -------
        list
            A list of table names.
        """
        try:

            cursor = self.conn.cursor()

            cursor.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata WHERE schema_name = %s
                );
            """, (self.schema_name,))
            schema_exists = cursor.fetchone()[0]

            if not schema_exists:
                print(f"DatabasePostgreSQL >> Schema '{self.schema_name}' does not exist.")
                return []

            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s
                ORDER BY table_name;
            """, (self.schema_name,))

            tables_list = [row[0] for row in cursor.fetchall()]

            if display:
                if tables_list:
                    print(f"DatabasePostgreSQL >> Tables in '{self.schema_name}': {', '.join(tables_list)}")
                else:
                    print(f"DatabasePostgreSQL >> No tables found in schema '{self.schema_name}'.")

            cursor.close()
            return tables_list

        except Exception as e:
            print(f"DatabasePostgreSQL >> PostgreSQL error when listing tables: {e}")
            return []
            

    def query_data(self,
                SELECT: str,
                FROM: str,
                WHERE: Optional[Union[str, list[str], tuple[str]]] = None,
                VALUES: Optional[Union[Any, list[Any], tuple[Any]]] = None,
                LIKE: Optional[Union[str, list[str], tuple[Any]]] = None):
        """
        Selects data from a PostgreSQL table with optional filtering.

        Parameters
        ----------
        SELECT: str
            The columns to select in SQL syntax.
        FROM: str
            The name of the table to select data from.
        WHERE: Optional[str or list]
            The column name(s) for the WHERE condition.
        VALUES: Optional[Any or list]
            The value(s) to use for exact matching in the WHERE condition.
        LIKE: Optional[str or list]
            The pattern(s) to use for LIKE matching in the WHERE condition.

        Returns
        -------
        rows: list[dict]
            The rows retrieved from the table as a list of dictionaries.
        """
        try:
            cursor = self.conn.cursor(cursor_factory=DictCursor)

            # Basic SELECT
            if WHERE is None:

                cursor.execute(f"SELECT {SELECT} FROM {FROM};")
                rows = [dict(row) for row in cursor.fetchall()]

                cursor.close()
                return rows

            # WHERE exact match
            if VALUES is not None:

                where_cols = [WHERE] if isinstance(WHERE, str) else WHERE
                values = [VALUES] if not isinstance(VALUES, (list, tuple)) else VALUES
                if len(where_cols) != len(values):
                    print("DatabasePostgreSQL >> Number of WHERE columns must match number of LIKE patterns.")
                    cursor.close()
                    return []

                where_clause = " AND ".join([f"{col} = %s" for col in where_cols])
                sql_query = f"SELECT {SELECT} FROM {FROM} WHERE {where_clause};"
                cursor.execute(sql_query, values)
                rows = [dict(row) for row in cursor.fetchall()]

                cursor.close()
                return rows

            # WHERE LIKE pattern
            if LIKE is not None:

                where_cols = [WHERE] if isinstance(WHERE, str) else WHERE
                like_patterns = [LIKE] if not isinstance(LIKE, (list, tuple)) else LIKE
                if len(where_cols) != len(like_patterns):
                    print("DatabasePostgreSQL >> Number of WHERE columns must match number of LIKE patterns.")
                    cursor.close()
                    return []

                where_clause = " AND ".join([f"{col} LIKE %s" for col in where_cols])
                sql_query = f"SELECT {SELECT} FROM {FROM} WHERE {where_clause};"
                cursor.execute(sql_query, like_patterns)
                rows = [dict(row) for row in cursor.fetchall()]

                cursor.close()
                return rows

            cursor.close()
            return []

        except psycopg2.Error as e:
            print(f"DatabasePostgreSQL >> PostgreSQL error when selecting data: {e}")
            return []

        except Exception as e:
            print(f"DatabasePostgreSQL >> Unexpected error when selecting data: {e}")
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
            self._commit()
        except AttributeError:
            print(f"DatabasePostgreSQL >> Connection to the DB is not defined. Try reconnecting to the DB.")
        except Exception as e:
            print(f"DatabasePostgreSQL >> PostgreSQL error when inserting data into '{table_name}': {e}")
        finally:
            self._rollback()

        return None


    def _commit(self):
        """
        Handles the commits of the transactions operated since last commit.
        """
        try:
            self.conn.commit()
        except AttributeError:
            print("DatabasePostgreSQL >> Connector is not defined. Try reconnecting to the DB.")
        except:
            print("DatabasePostgreSQL >> Failed to commit transaction.")

        return None
    

    def _rollback(self):
        """
        Handles the roolbacks of the transactions operated since last commit.
        """
        try:
            self.conn.rollback()
        except AttributeError:
            print("DatabasePostgreSQL >> Connector is not defined. Try reconnecting to the DB.")
        except:
            print("DatabasePostgreSQL >> Failed to rollback transaction.")

        return None


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
            self._rollback()

        finally:
            cursor.close()

        return None
