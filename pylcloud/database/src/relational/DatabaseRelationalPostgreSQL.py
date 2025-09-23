import os, sys
import psycopg2
from psycopg2 import sql, OperationalError, errors, pool
from psycopg2._psycopg import connection
from psycopg2.extras import DictCursor, register_uuid
from psycopg2.extensions import register_type, UNICODE
from typing import Union, Optional, Any
import json
import boto3
import psycopg2._psycopg

from .DatabaseRelational import DatabaseRelational
import os, sys
import psycopg2
from psycopg2 import sql, OperationalError, errors, pool
from psycopg2._psycopg import connection
from psycopg2.extras import DictCursor, register_uuid
from psycopg2.extensions import register_type, UNICODE
from typing import Union, Optional, Any
import json
import boto3
import psycopg2._psycopg

from .DatabaseRelational import DatabaseRelational


class DatabaseRelationalPostgreSQL(DatabaseRelational):
    """
    A class to manage PostgreSQL databases (RDS, Aurora, local) with optional IAM authentication.
    """

    def __init__(self,
                 host: str = "127.0.0.1",
                 database: str = "app_database",
                 schema: str = "app_schema",
                 user: str = "app_user",
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
        schema: str
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
        - Database management is a rather uncommon operation. For a more streamlined usage of this helper, once connected, \
        management is limited to schema level.
        """
        super().__init__(logs_name="DatabaseRelationalPostgreSQL")

        self.host = host
        self.database = database
        self.schema = schema.lower().replace("-", "_").replace(" ", "_")
        self.user = user
        self.password = password
        self.port = port
        self.ssl_mode = ssl_mode
        self.connection_timeout = connection_timeout
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region_name = aws_region_name

        conn: Optional[psycopg2._psycopg.connection] = None
        self.pool: Optional[pool.SimpleConnectionPool] = None

        register_uuid()
        register_type(UNICODE) # Register UUID type

        return None


    def _get_connection(self, 
                        database: Optional[str] = None,
                        schema: Optional[str] = None,
                        user: Optional[str] = None,
                        password: Optional[str] = None,
                        port: Optional[str] = None,
                        minconn: int = 1, 
                        maxconn: int = 5
                        ) -> Optional[psycopg2._psycopg.connection]:
        """
        Gets a live connection ``conn`` from the pool.
        If pool is missing or broken, recreates it.
        """

        def _get_connection_params():
            """Return psycopg2 connection parameters, handling IAM if no password given."""
            params = {
                "host": self.host,
                "user": self.user if user is None else user,
                "dbname": self.database if database is None else database,
                "port": self.port if port is None else port,
                "connect_timeout": self.connection_timeout,
            }
            self.logger.debug(params)
            if self.ssl_mode:
                params["sslmode"] = self.ssl_mode

            if not self.password:  # IAM token
                self.logger.info(f"Using IAM auth for user '{self.user}'")
                client = boto3.client(
                    "rds",
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.aws_region_name,
                )
                token = client.generate_db_auth_token(
                    DBHostname=self.host,
                    Port=int(self.port),
                    DBUsername=self.user,
                    Region=self.aws_region_name,
                )
                params["password"] = token
                if "sslmode" not in params:
                    params["sslmode"] = "require"
            else:
                params["password"] = self.password
            return params
        
        # Creates connections pool
        if self.pool is None:
            try:
                self.logger.info("Initializing new connection pool...")
                self.pool = pool.SimpleConnectionPool(
                    minconn, maxconn, **_get_connection_params()
                )
                # Test one connection & set schema
                conn = self.pool.getconn()
                if conn is not None:
                    try:
                        with conn.cursor() as cur:
                            cur.execute(f"SET search_path TO {self.schema}, public;")
                    finally:
                        self.pool.putconn(conn)
                else:
                    self.logger.critical(f"Failed to initialize connection pool: {e}")
                    return None

            except Exception as e:
                self.logger.critical(f"Failed to initialize connection pool: {e}")
                self.pool = None
                return None

        # Creates connection from pool
        try:
            conn: psycopg2._psycopg.connection = self.pool.getconn()
            with conn.cursor() as cur:
                # Tries connection
                cur.execute("SELECT 1")
            conn.commit()
            return conn
        
        except Exception as e:
            self.logger.error(f"Bad connection from pool ({e}), try reconnecting.")
            if self.pool:
                self.pool.closeall()
                self.pool = None
            return None


    def _clear_connection(self):
        """
        Close all pool connections.
        """
        if self.pool:
            self.pool.closeall()
            self.logger.info(f"Disconnected from database schema '{self.schema}'.")
            self.pool = None
        
        return None
        

    def describe(self, 
                 include_system_schemas: bool = False, 
                 display: bool = False):
        """
        High-level description schemas and tables of the current PostgreSQL DB.

        Parameters
        ----------
        include_system_schemas : bool, default=False
            Whether to include system schemas (pg_*, information_schema).
        display : bool, default=False
            Whether to print the results.

        Returns
        -------
        dict
            A nested dictionary: { database: { schema: [tables...] } }
        """
        structure = {}

        conn = self._get_connection()
        try:
            with conn.cursor() as db_cur:
                # --- Schemas ---
                if include_system_schemas:
                    schema_query = "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;"
                else:
                    schema_query = """
                        SELECT schema_name 
                        FROM information_schema.schemata 
                        WHERE schema_name NOT LIKE 'pg_%' 
                        AND schema_name != 'information_schema'
                        ORDER BY schema_name;
                    """
                db_cur.execute(schema_query)
                schemas = [s[0] for s in db_cur.fetchall()]

                for schema in schemas:
                    # --- Tables ---
                    db_cur.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = %s
                        ORDER BY table_name;
                    """, (schema,))
                    tables = [t[0] for t in db_cur.fetchall()]
                    structure[schema] = tables

            db_cur.close()

            if display:
                print("====== Describing database ======")
                print(f"Database: {self.database}")
                for schema, tables in structure.items():
                    print(f"  🗂️  Schema: {schema}")
                    if tables:
                        for tbl in tables:
                            print(f"     📄 {tbl}")
                    else:
                        print("     (no tables)")
                print("====== Ending  description ======")

            return structure

        except Exception as e:
            self.logger.error(f"PostgreSQL error in describe(): {e}")
            return {}

        finally:
            self.pool.putconn(conn)



    def create_table(self, table_name: str, column_definitions: list[str]) -> bool:
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
        conn = self._get_connection()
            
        try:
            with conn.cursor() as cursor:
                full_table_name = f"{self.schema}.{table_name}"
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {full_table_name} ({', '.join(column_definitions)})"
                )
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = %s)",
                    (self.schema, table_name),
                )
                if cursor.fetchone()[0]:
                    self.logger.info(f"Table '{full_table_name}' is ready.")
                    conn.commit()
                    return True
                else:
                    self.logger.warning(f"Failed to create table '{full_table_name}'.")
                    conn.rollback()
                    return False
                
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error creating table '{table_name}': {e}")
            return False
        
        finally:
            self.pool.putconn(conn)


    def drop_table(self, table_name: str) -> bool:
        """
        Drops a table from the current schema.
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            conn.commit()
            self.logger.info(f"Successfully dropped table `{table_name}`.")
            return True
        
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to drop table `{table_name}`: {e}")
            return False
        
        finally:
            self.pool.putconn(conn)


    def drop_schema(self, schema: str) -> bool:
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
            conn.commit()
            self.logger.info(f"Successfully dropped schema '{schema}'.")
            return True
        except Exception as e:
            conn.rollback()
            self.logger.error(f"PostgreSQL error when dropping schema: {e}")
            return False
        finally:
            self.pool.putconn(conn)


    def delete_data(self,
                    FROM: str,
                    WHERE: Union[str, list[str], tuple[str]],
                    VALUES: Optional[Union[str, int, list, tuple]] = None,
                    LIKE: Optional[Union[str, list[str], tuple[str]]] = None):

        if not WHERE:
            self.logger.warning("Deleting the whole data without WHERE clause is not supported.")
            return False

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                where_cols = [WHERE] if isinstance(WHERE, str) else list(WHERE)

                if VALUES is not None:
                    values = [VALUES] if not isinstance(VALUES, (list, tuple)) else list(VALUES)
                    if len(where_cols) != len(values):
                        self.logger.warning("Number of WHERE columns must match number of VALUES.")
                        return False
                    where_clause = " AND ".join([f"{col} = %s" for col in where_cols])
                    query = f"DELETE FROM {FROM} WHERE {where_clause};"
                    cursor.execute(query, tuple(values))
                    self.logger.debug(f"{query} -- {values}")

                elif LIKE is not None:
                    patterns = [LIKE] if not isinstance(LIKE, (list, tuple)) else list(LIKE)
                    if len(where_cols) != len(patterns):
                        self.logger.warning("Number of WHERE columns must match number of LIKE patterns.")
                        return False
                    where_clause = " AND ".join([f"{col} LIKE %s" for col in where_cols])
                    query = f"DELETE FROM {FROM} WHERE {where_clause};"
                    cursor.execute(query, tuple(patterns))
                    self.logger.debug(f"{query} -- {patterns}")

            conn.commit()
            return True
        
        except Exception as e:
            conn.rollback()
            self.logger.error(f"PostgreSQL error when deleting data: {e}")
            return False
        
        finally:
            self.pool.putconn(conn)


    def query_data(self,
                SELECT: str,
                FROM: str,
                JOIN: Optional[Union[str, list[str], tuple[str]]] = None,
                WHERE: Optional[Union[str, list[str], tuple[str]]] = None,
                VALUES: Optional[Union[Any, list[Any], tuple[Any]]] = None,
                LIKE: Optional[Union[str, list[str], tuple[Any]]] = None):
        """
        Selects data from a PostgreSQL table with optional JOIN and filtering.
        Returns a list of dictionaries.
        """
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=DictCursor) as cursor:

                sql_parts = [f"SELECT {SELECT}", f"FROM {FROM}"]

                if JOIN is not None:
                    joins = [f"JOIN {JOIN}"] if isinstance(JOIN, str) else [f"JOIN {j}" for j in JOIN]
                    sql_parts.extend(joins)

                where_clause = ""
                params = []

                # Exact match
                if VALUES is not None and WHERE is not None:
                    where_cols = [WHERE] if isinstance(WHERE, str) else list(WHERE)
                    values = [VALUES] if not isinstance(VALUES, (list, tuple)) else list(VALUES)
                    if len(where_cols) != len(values):
                        self.logger.warning("Number of WHERE columns must match number of VALUES.")
                        return []
                    where_clause = " AND ".join([f"{col} = %s" for col in where_cols])
                    params = values

                # LIKE match
                elif LIKE is not None and WHERE is not None:
                    where_cols = [WHERE] if isinstance(WHERE, str) else list(WHERE)
                    patterns = [LIKE] if not isinstance(LIKE, (list, tuple)) else list(LIKE)
                    if len(where_cols) != len(patterns):
                        self.logger.warning("Number of WHERE columns must match number of LIKE patterns.")
                        return []
                    where_clause = " AND ".join([f"{col} LIKE %s" for col in where_cols])
                    params = patterns

                if where_clause:
                    sql_parts.append(f"WHERE {where_clause}")

                sql_query = " ".join(sql_parts) + ";"
                self.logger.debug(sql_query)
                cursor.execute(sql_query, params)
                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            self.logger.error(f"Error querying data: {e}")
            return []

        finally:
            self.pool.putconn(conn)


    def send_data(self, table_name: str, **kwargs):
        """
        Inserts data into a PostgreSQL table using column-value pairs from kwargs.
        """
        if not kwargs:
            self.logger.warning("No data provided for insertion.")
            return

        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                fields = ",".join(kwargs.keys())
                placeholders = ",".join(["%s"] * len(kwargs))
                values = tuple(kwargs.values())

                cursor.execute(f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders});", values)
                self.logger.debug(f"INSERT INTO {table_name} ({fields}) VALUES ({values});")
                conn.commit()

        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"PostgreSQL error when inserting data into '{table_name}': {e}")

        finally:
            self.pool.putconn(conn)



    def update_data(self,
                    table_name: str,
                    WHERE: Optional[Union[str, list[str], tuple[str]]] = None,
                    VALUES: Optional[Union[Any, list[Any], tuple[Any]]] = None,
                    LIKE: Optional[Union[str, list[str], tuple[str]]] = None,
                    **kwargs):
        """
        Updates data in a PostgreSQL table based on WHERE clause and kwargs for columns to update.
        """
        if not kwargs:
            self.logger.warning("No columns provided for update.")
            return

        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:

                # SET clause
                set_clause = ", ".join([f"{col} = %s" for col in kwargs])
                set_values = tuple(kwargs.values())

                # WHERE clause
                where_clause = ""
                where_values = ()

                if WHERE is not None:
                    where_cols = [WHERE] if isinstance(WHERE, str) else list(WHERE)

                    if VALUES is not None:
                        values = [VALUES] if not isinstance(VALUES, (list, tuple)) else list(VALUES)
                        if len(where_cols) != len(values):
                            self.logger.warning("Number of WHERE columns must match number of VALUES.")
                            return
                        where_clause = " AND ".join([f"{col} = %s" for col in where_cols])
                        where_values = tuple(values)

                    elif LIKE is not None:
                        patterns = [LIKE] if not isinstance(LIKE, (list, tuple)) else list(LIKE)
                        if len(where_cols) != len(patterns):
                            self.logger.warning("Number of WHERE columns must match number of LIKE patterns.")
                            return
                        where_clause = " AND ".join([f"{col} LIKE %s" for col in where_cols])
                        where_values = tuple(patterns)

                query = f"UPDATE {table_name} SET {set_clause}"
                if where_clause:
                    query += f" WHERE {where_clause}"
                query += ";"

                cursor.execute(query, set_values + where_values)
                self.logger.debug(f"{query} -- {set_values + where_values}")
                conn.commit()

        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"PostgreSQL error when updating data in '{table_name}': {e}")

        finally:
            self.pool.putconn(conn)



    def raw_sql(self, SQL: str, VALUES: tuple[str] = ()):
        """
        Executes a raw SQL query with optional parameters.

        Parameters
        ----------
        SQL : str
            The SQL query string.
        VALUES : tuple, optional
            Values to safely parameterize into the SQL query.

        Returns
        -------
        list[dict]
            List of rows as dictionaries, or [] on failure.
        """
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                self.logger.warning(f"Running raw SQL query: {SQL}")
                cursor.execute(SQL, VALUES)
                rows = cursor.fetchall()
                conn.commit()
                return rows

        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.critical(f"SQL raw query failed:\n {e}")
            return []

        finally:
            self.pool.putconn(conn)



    def execute_file(self, file_path: str):
        """
        Executes SQL commands from a .sql file or inserts data from a .json file.

        Parameters
        ----------
        file_path : str
            Path to a .sql or .json file.
        """
        if not os.path.isfile(file_path):
            self.logger.warning(f"File '{file_path}' does not exist.")
            return None

        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:

                file_extension = os.path.splitext(file_path)[1].lower()

                if file_extension == ".sql":
                    with open(file_path, "r", encoding="utf-8") as sql_file:
                        sql_commands = sql_file.read()
                        cursor.execute(sql_commands)
                    conn.commit()
                    self.logger.info(f"SQL file '{file_path}' executed successfully.")

                elif file_extension == ".json":
                    with open(file_path, "r", encoding="utf-8") as json_file:
                        data = json.load(json_file)

                    if not isinstance(data, list):
                        self.logger.warning("JSON file must contain a list of records (dicts).")
                        return None

                    for record in data:
                        if not isinstance(record, dict) or "table" not in record or "data" not in record:
                            self.logger.warning("JSON format invalid: each record must have 'table' and 'data'.")
                            return None

                        table_name = record["table"]
                        fields = ", ".join(record["data"].keys())
                        placeholders = ", ".join(["%s"] * len(record["data"]))
                        values = tuple(record["data"].values())

                        cursor.execute(
                            f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders});",
                            values
                        )

                    conn.commit()
                    self.logger.info(f"JSON file '{file_path}' data inserted successfully.")

                else:
                    self.logger.warning(f"Unsupported file type '{file_extension}'. Only .sql and .json are supported.")

        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Error executing file '{file_path}': {e}")

        finally:
            self.pool.putconn(conn)


    def _init_db(
        self,
        database: str = "app_database",
        schema: str = "app_schema",
        user: str = "app_user",
        master_user: str = "postgres",
        master_password: Optional[str] = "password"
    ):
        """
        Initialize the target database, schema, and user.
        Uses IAM if credentials are provided, else password auth.
        """
        schema = schema.lower().replace("-", "_").replace(" ", "_")
        iam_mode = all([self.aws_access_key_id, self.aws_secret_access_key, self.aws_region_name])

        self.logger.info(
            f"Initializing DB '{database}', schema '{schema}', user '{user}' "
            f"using {'IAM' if iam_mode else 'password'} auth"
        )

        def _run(conn, query, params=None):
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params)
            except Exception as e:
                self.logger.error(f"SQL query failed: {e}", exc_info=True)

        # 1. Get a fresh connection to 'postgres' as master
        conn = self._get_connection(database="postgres", user=master_user, password=master_password)
        if conn is None:
            self.logger.critical("Failed to init DB: cannot connect as master user.")
            return None

        try:
            # Must enable autocommit for CREATE DATABASE
            conn.autocommit = True

            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (database,))
                if not cur.fetchone():
                    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
                    self.logger.info(f"Database '{database}' created successfully.")
                else:
                    self.logger.warning(f"Database '{database}' already exists.")
        except Exception as e:
            self.logger.error(f"Error creating database '{database}': {e}", exc_info=True)
        finally:
            self.pool.putconn(conn)  # release back to pool

        # 2. Connect to target database normally
        conn = self._get_connection(database=database, user=master_user, password=master_password)
        if conn is None:
            self.logger.critical("Failed to init DB: cannot connect to target database.")
            return None

        # 3. Create app user, schema, and privileges
        _run(conn, sql.SQL(
            "DO $$ BEGIN CREATE ROLE {} LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
        ).format(sql.Identifier(user)))
        if iam_mode:
            _run(conn, sql.SQL("GRANT rds_iam TO {};").format(sql.Identifier(user)))

        _run(conn, "REVOKE CREATE ON SCHEMA public FROM PUBLIC;")
        _run(conn, sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC;").format(sql.Identifier(database)))
        _run(conn, sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema)))
        _run(conn, sql.SQL("ALTER SCHEMA {} OWNER TO {};").format(sql.Identifier(schema), sql.Identifier(user)))
        _run(conn, sql.SQL("GRANT CONNECT ON DATABASE {} TO {};").format(sql.Identifier(database), sql.Identifier(user)))
        _run(conn, sql.SQL("GRANT USAGE ON SCHEMA {} TO {};").format(sql.Identifier(schema), sql.Identifier(user)))
        _run(conn, sql.SQL(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {} TO {};"
        ).format(sql.Identifier(schema), sql.Identifier(user)))
        _run(conn, sql.SQL(
            "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {} TO {};"
        ).format(sql.Identifier(schema), sql.Identifier(user)))

        _run(conn, sql.SQL("""
            ALTER DEFAULT PRIVILEGES IN SCHEMA {}
            GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {};
        """).format(sql.Identifier(schema), sql.Identifier(user)))
        _run(conn, sql.SQL("""
            ALTER DEFAULT PRIVILEGES IN SCHEMA {}
            GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {};
        """).format(sql.Identifier(schema), sql.Identifier(user)))

        _run(conn, sql.SQL("SET search_path TO {}, public;").format(sql.Identifier(schema)))

        # Release connection back to pool
        self.pool.putconn(conn)

        self.logger.info(f"DB setup complete: Database={database}, Schema={schema}, User={user} (IAM={iam_mode})")
        return None
