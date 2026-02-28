import os
import json
from typing import Union, Optional, Any

import psycopg
from psycopg import sql, OperationalError
from psycopg.rows import dict_row

import boto3

from .DatabaseRelational import DatabaseRelational
from pylcloud import _config_logger


class DatabaseRelationalPostgreSQL(DatabaseRelational):
    """
    A class to manage PostgreSQL databases (RDS, Aurora, local) with optional IAM authentication.
    Fully compatible with psycopg v3. All queries are parameterised / SQL-safe.
    """

    def __init__(
        self,
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
        host: str
            The host/address of the database server.
            - When connecting to a local server, the IP of host computer
            - When connecting to AWS, the address of the read/write endpoint
        database: str
            The name of the database to connect to within the host server.
        schema: str
            The name of the database schema to use (will set ``search_path`` to this default value).
        user: str
            The user to assume when interacting with the DB.
            - Must be an existing DB user
            - Can be an IAM user, provided an homonymous IAM user exists
        password: str, optional
            The password for the database user. When ``None``, IAM token authentication is used.
            - When a password is given, will assume a direct connection to the DB using the given user and password
            - When password is None, will assume an IAM authentication, so a connection token will be generated for this IAM user
        port: str
            The port number for the database connection.
        ssl_mode: str, optional
            SSL mode for the connection (e.g., ``'require'``, ``'verify-ca'``, ``'verify-full'``).
        connection_timeout: int
            Connection timeout in seconds.
        aws_access_key_id: str, optional
            AWS access key ID for IAM authentication.
        aws_secret_access_key: str, optional
            AWS secret access key for IAM authentication.
        aws_region_name: str, optional
            AWS region name for IAM authentication.

        Notes
        -----
        - The 'database' in common words often refers to a 'schema' in technical terms. Thus, a
          database can rather be seen as a server, and a schema as a database.
            - A schema is a collection of tables.
            - A database is a collection of schemas.
        - Database management is a rather uncommon operation. Once connected, management is limited
          to schema level.
        """
        super().__init__()

        self.logger = _config_logger(logs_name="DatabaseRelationalPostgreSQL")

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
        self.conn: Optional[psycopg.Connection] = None

    def connect_database(
        self, database: str, user: str, password: Optional[str] = None
    ) -> None:
        """Open a psycopg v3 connection to *database* as *user*."""

        def _get_connection_params(
            database: str, connect_user: str, password: Optional[str]
        ) -> dict:
            """Build psycopg v3 connection keyword arguments. Generates an IAM token when ``password`` is ``None``."""
            params: dict = {
                "host": self.host,
                "user": connect_user,
                "dbname": database,
                "port": self.port,
                "connect_timeout": self.connection_timeout,
            }

            if self.ssl_mode:
                params["sslmode"] = self.ssl_mode

            if not password:
                self.logger.info(f"Using IAM auth for user '{connect_user}'")
                client = boto3.client(
                    "rds",
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.aws_region_name,
                )
                token = client.generate_db_auth_token(
                    DBHostname=self.host,
                    Port=int(self.port),
                    DBUsername=connect_user,
                    Region=self.aws_region_name,
                )
                params["password"] = token
                params.setdefault("sslmode", "require")
            else:
                params["password"] = password

            return params

        # Close any existing connection first
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            pass
        finally:
            self.conn = None

        try:
            conn_params = _get_connection_params(database, user, password)
            self.conn = psycopg.connect(**conn_params)
            # psycopg v3: autocommit is a property set after connect
            self.conn.autocommit = True

            # Safe search_path using sql.Identifier
            with self.conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SET search_path TO {schema}, public;").format(
                        schema=sql.Identifier(self.schema)
                    )
                )

            self.logger.info(f"Connected to database='{database}'.")
            self.logger.info(f"search_path set to schema='{self.schema}'.")

        except Exception as e:
            self.logger.critical(f"Database connection failed: {e}")

    def disconnect_database(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info(
                f"Disconnected from database='{self.database}', schema='{self.schema}'."
            )

    def _init_db(
        self,
        database: str = "app_database",
        schema: str = "app_schema",
        user: str = "app_user",
        master_user: str = "postgres",
        master_password: Optional[str] = "password",
    ) -> None:
        """
        Initialise the target database, schema, and user.
        If ``master_password`` is ``None``, IAM token authentication is used.
        """

        def _run(query: sql.Composable, params: Optional[tuple] = None) -> None:
            """
            Execute a fire-and-forget administrative SQL statement.
            ``query`` must be a ``psycopg.sql.Composable`` object (never a raw f-string).
            """
            query_str = query.as_string(self.conn) if self.conn else repr(query)
            self.logger.debug(f"Running SQL: {query_str} | Params: {params}")

            if self.conn is None:
                self.logger.error("Cannot run SQL query — connection is closed.")
                return

            try:
                with self.conn.cursor() as cur:
                    cur.execute(query, params)
                self.logger.debug(f"SQL query succeeded.")
            except Exception as e:
                self.logger.error(f"SQL query failed: {query_str} | Error: {e}", exc_info=True)
                
        schema = schema.lower().replace("-", "_").replace(" ", "_")
        iam_mode = all(
            [self.aws_access_key_id, self.aws_region_name, self.aws_secret_access_key]
        )
        self.logger.info(
            f"Initialising DB='{database}', schema='{schema}', user='{user}' "
            f"using {'IAM' if iam_mode else 'password'} auth."
        )

        # 1. Connect as master to the postgres maintenance DB
        self.connect_database("postgres", master_user, master_password)

        # 2. Create target database if it does not exist
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE DATABASE {db};").format(db=sql.Identifier(database))
                )
        except Exception as e:
            if "already exists" in str(e):
                self.logger.warning(
                    f"Database '{database}' already exists and will not be recreated."
                )
            else:
                self.logger.error(str(e))

        # 3. Re-connect to the target database
        self.connect_database(database, master_user, master_password)

        # 4. Create application user (idempotent)
        _run(
            sql.SQL(
                "DO $$ BEGIN CREATE ROLE {user} LOGIN;"
                " EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
            ).format(user=sql.Identifier(user))
        )
        if iam_mode:
            _run(
                sql.SQL("GRANT rds_iam TO {user};").format(user=sql.Identifier(user))
            )

        # 5. Tighten public schema permissions
        _run(sql.SQL("REVOKE CREATE ON SCHEMA public FROM PUBLIC;"))
        _run(
            sql.SQL("REVOKE ALL ON DATABASE {db} FROM PUBLIC;").format(
                db=sql.Identifier(database)
            )
        )

        # 6. Create application schema
        _run(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};").format(
                schema=sql.Identifier(schema)
            )
        )
        _run(
            sql.SQL("ALTER SCHEMA {schema} OWNER TO {user};").format(
                schema=sql.Identifier(schema), user=sql.Identifier(user)
            )
        )

        # 7. Grant privileges on database and schema
        _run(
            sql.SQL("GRANT CONNECT ON DATABASE {db} TO {user};").format(
                db=sql.Identifier(database), user=sql.Identifier(user)
            )
        )
        _run(
            sql.SQL("GRANT USAGE ON SCHEMA {schema} TO {user};").format(
                schema=sql.Identifier(schema), user=sql.Identifier(user)
            )
        )
        _run(
            sql.SQL(
                "GRANT SELECT, INSERT, UPDATE, DELETE"
                " ON ALL TABLES IN SCHEMA {schema} TO {user};"
            ).format(schema=sql.Identifier(schema), user=sql.Identifier(user))
        )
        _run(
            sql.SQL(
                "GRANT USAGE, SELECT, UPDATE"
                " ON ALL SEQUENCES IN SCHEMA {schema} TO {user};"
            ).format(schema=sql.Identifier(schema), user=sql.Identifier(user))
        )

        # 8. Default privileges for future objects
        _run(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema}"
                " GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {user};"
            ).format(schema=sql.Identifier(schema), user=sql.Identifier(user))
        )
        _run(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema}"
                " GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {user};"
            ).format(schema=sql.Identifier(schema), user=sql.Identifier(user))
        )

        # 9. Set search_path for this session
        _run(
            sql.SQL("SET search_path TO {schema}, public;").format(
                schema=sql.Identifier(schema)
            )
        )
        self.logger.info(
            f"DB setup complete: database='{database}', schema='{schema}', user='{user}' (IAM={iam_mode})."
        )

    def create_table(self, table_name: str, column_definitions: list[str]) -> None:
        """
        Create a table in the current schema.

        Parameters
        ----------
        table_name: str
            Name of the table to create.
        column_definitions: list[str]
            Column definitions, e.g. ``["id SERIAL PRIMARY KEY", "name VARCHAR(100) NOT NULL"]``.
        """
        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        try:
            with self.conn.cursor() as cur:
                cols_sql = sql.SQL(", ").join(sql.SQL(col) for col in column_definitions)
                create_sql = sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {schema}.{table} ({cols});"
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier(table_name),
                    cols=cols_sql,
                )
                cur.execute(create_sql)

                # Verify creation
                cur.execute(
                    "SELECT EXISTS ("
                    "  SELECT 1 FROM information_schema.tables"
                    "  WHERE table_schema = %s AND table_name = %s"
                    ");",
                    (self.schema, table_name),
                )
                table_exists = cur.fetchone()[0]

            if table_exists:
                self.logger.info(
                    f"Table '{self.schema}.{table_name}' created or already exists."
                )
                self._commit()
            else:
                self.logger.warning(f"Failed to create table '{self.schema}.{table_name}'.")
                self._rollback()

        except Exception as e:
            self._rollback()
            self.logger.error(f"Error creating table '{table_name}': {e}")

    def drop_table(self, table_name: str) -> None:
        """Drop a table (CASCADE) from the current schema."""
        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {schema}.{table} CASCADE;").format(
                        schema=sql.Identifier(self.schema),
                        table=sql.Identifier(table_name),
                    )
                )
            self._commit()
            self.logger.info(f"Dropped table '{self.schema}.{table_name}'.")
        except Exception as e:
            self._rollback()
            self.logger.error(f"Failed to drop table '{table_name}': {e}")

    def drop_schema(self, schema: str) -> None:
        """Drop a schema (CASCADE) from the current database."""
        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {schema} CASCADE;").format(
                        schema=sql.Identifier(schema)
                    )
                )
            self._commit()
            self.logger.info(f"Dropped schema '{schema}'.")
        except Exception as e:
            self._rollback()
            self.logger.error(f"Error dropping schema '{schema}': {e}")

    def describe_database(self) -> list:
        """Return the non-system schemas of the connected database."""
        return self.list_schemas(include_system_schemas=False)

    def list_databases(self, display: bool = False) -> list:
        """List all non-template databases on the server."""
        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;"
                )
                databases = [row[0] for row in cur.fetchall()]

            if display:
                print("Available databases:", databases)
            self.logger.debug(f"Available databases: {databases}")
            return databases

        except Exception as e:
            self.logger.error(f"Error listing databases: {e}")
            return []

    def list_schemas(
        self, include_system_schemas: bool = False, display: bool = False
    ) -> list:
        """
        List schemas in the connected database.

        Parameters
        ----------
        include_system_schemas: bool
            When ``True``, includes ``pg_*`` and ``information_schema``.
        display: bool
            Print the result to stdout when ``True``.
        """
        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        try:
            with self.conn.cursor() as cur:
                if include_system_schemas:
                    cur.execute(
                        "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;"
                    )
                else:
                    cur.execute(
                        "SELECT schema_name FROM information_schema.schemata"
                        " WHERE schema_name NOT LIKE 'pg_%%'"
                        "   AND schema_name <> 'information_schema'"
                        " ORDER BY schema_name;"
                    )
                schemas = [row[0] for row in cur.fetchall()]

            if display:
                print("Schemas in database:", schemas)
            self.logger.debug(f"Schemas: {schemas}")
            return schemas

        except Exception as e:
            self.logger.error(f"Error listing schemas: {e}")
            return []

    def list_tables(self, display: bool = False) -> list[str]:
        """List all tables in the current schema.
        Parameters
        ----------
        display: bool
            Print the result to stdout when ``True``.

        Returns
        -------
        tables: list[str]
            A list of table names.
        """

        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        try:
            with self.conn.cursor() as cur:
                # Check schema existence
                cur.execute(
                    "SELECT EXISTS("
                    "  SELECT 1 FROM information_schema.schemata WHERE schema_name = %s"
                    ");",
                    (self.schema,),
                )
                if not cur.fetchone()[0]:
                    self.logger.info(f"Schema '{self.schema}' does not exist.")
                    return []

                cur.execute(
                    "SELECT table_name FROM information_schema.tables"
                    " WHERE table_schema = %s ORDER BY table_name;",
                    (self.schema,),
                )
                tables = [row[0] for row in cur.fetchall()]

            if display:
                if tables:
                    print(f"Tables in '{self.schema}':", ", ".join(tables))
                else:
                    print(f"No tables found in schema '{self.schema}'.")
            self.logger.debug(f"Tables in '{self.schema}': {tables}")
            return tables

        except Exception as e:
            self.logger.error(f"Error listing tables: {e}")
            return []

    def query_data(
        self,
        SELECT: str,
        FROM: str,
        JOIN: Optional[Union[str, list[str]]] = None,
        WHERE: Optional[Union[str, list[str]]] = None,
        VALUES: Optional[Union[Any, list[Any]]] = None,
        LIKE: Optional[Union[str, list[str]]] = None,
    ) -> list[dict]:
        """
        Select rows from a PostgreSQL table with optional JOIN and WHERE filtering.

        Parameters
        ----------
        SELECT : str
            Columns to select, e.g. ``"*"`` or ``"id, name"``.
        FROM : str
            Table name (may include schema prefix).
        JOIN : str or list[str], optional
            Full JOIN clause(s), e.g. ``"JOIN other_table o ON t.id = o.t_id"``.
        WHERE : str or list[str], optional
            Column name(s) for the WHERE condition.
        VALUES : Any or list[Any], optional
            Exact-match value(s) corresponding to WHERE column(s).
        LIKE : str or list[str], optional
            LIKE pattern(s) corresponding to WHERE column(s).

        Returns
        -------
        list[dict]
            Rows as a list of dictionaries (column name → value).
        """
        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                # --- Build query parts ---
                parts: list[sql.Composable] = [
                    sql.SQL("SELECT ") + sql.SQL(SELECT),
                    sql.SQL("FROM ") + sql.SQL(FROM),
                ]

                if JOIN is not None:
                    join_clauses = [JOIN] if isinstance(JOIN, str) else list(JOIN)
                    for clause in join_clauses:
                        parts.append(sql.SQL("JOIN ") + sql.SQL(clause))

                params: list = []
                where_composable: Optional[sql.Composable] = None

                if WHERE is not None:
                    where_cols = [WHERE] if isinstance(WHERE, str) else list(WHERE)

                    if VALUES is not None:
                        values = (
                            [VALUES] if not isinstance(VALUES, (list, tuple)) else list(VALUES)
                        )
                        if len(where_cols) != len(values):
                            self.logger.warning("WHERE columns and VALUES count mismatch.")
                            return []
                        where_composable = sql.SQL(" AND ").join(
                            sql.SQL("{col} = %s").format(col=self._col_to_identifier(col))
                            for col in where_cols
                        )
                        params = values

                    elif LIKE is not None:
                        patterns = (
                            [LIKE] if not isinstance(LIKE, (list, tuple)) else list(LIKE)
                        )
                        if len(where_cols) != len(patterns):
                            self.logger.warning("WHERE columns and LIKE patterns count mismatch.")
                            return []
                        where_composable = sql.SQL(" AND ").join(
                            sql.SQL("{col} LIKE %s").format(col=self._col_to_identifier(col))
                            for col in where_cols
                        )
                        params = patterns

                if where_composable is not None:
                    parts.append(sql.SQL("WHERE ") + where_composable)

                final_query = sql.SQL(" ").join(parts) + sql.SQL(";")
                self.logger.debug(final_query.as_string(self.conn))

                cur.execute(final_query, params)
                rows = [dict(row) for row in cur.fetchall()]

            return rows

        except psycopg.Error as e:
            self.logger.error(f"PostgreSQL error during SELECT: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error during SELECT: {e}")
            return []

    def send_data(self, table_name: str, **kwargs: Any) -> None:
        """
        Insert a single row into *table_name*.

        Column names are passed as keyword-argument keys.

        Parameters
        ----------
        table_name: str
            Target table name (may include schema prefix, e.g. ``"myschema.users"``).
        **kwargs
            Column-value pairs, e.g. ``user_id=42, user_name="jdoe"``.

        Examples
        --------
        >>> send_data(table_name="users", user_id=42, user_name="jdoe")
        """
        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        if not kwargs:
            self.logger.warning("send_data called with no column values — nothing to insert.")
            return

        try:
            with self.conn.cursor() as cur:
                columns = sql.SQL(", ").join(sql.Identifier(k) for k in kwargs)
                placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in kwargs)
                # Resolve schema.table safely
                parts = table_name.split(".", 1)
                if len(parts) == 2:
                    table_ident = sql.SQL("{s}.{t}").format(
                        s=sql.Identifier(parts[0]), t=sql.Identifier(parts[1])
                    )
                else:
                    table_ident = sql.Identifier(table_name)

                insert_sql = sql.SQL("INSERT INTO {table} ({cols}) VALUES ({vals});").format(
                    table=table_ident, cols=columns, vals=placeholders
                )
                cur.execute(insert_sql, list(kwargs.values()))

            self._commit()
            self.logger.debug(f"INSERT into '{table_name}': {list(kwargs.keys())}")

        except Exception as e:
            self._rollback()
            self.logger.error(f"Error inserting data into '{table_name}': {e}")

    def update_data(
        self,
        table_name: str,
        WHERE: Optional[Union[str, list[str]]] = None,
        VALUES: Optional[Union[Any, list[Any]]] = None,
        LIKE: Optional[Union[str, list[str]]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Update rows in *table_name*.

        Parameters
        ----------
        table_name: str
            Target table name.
        WHERE: str or list[str], optional
            Column name(s) for the WHERE clause.
        VALUES: Any or list[Any], optional
            Exact-match value(s) for the WHERE clause.
        LIKE: str or list[str], optional
            LIKE pattern(s) for the WHERE clause.
        **kwargs
            Column-value pairs to set, e.g. ``status="active"``.
        """
        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        if not kwargs:
            self.logger.warning("update_data called with no SET values — nothing to update.")
            return

        try:
            with self.conn.cursor() as cur:
                # --- SET clause ---
                set_clause = sql.SQL(", ").join(
                    sql.SQL("{col} = %s").format(col=sql.Identifier(k)) for k in kwargs
                )
                set_values = list(kwargs.values())

                # --- WHERE clause ---
                where_composable: Optional[sql.Composable] = None
                where_values: list = []

                if WHERE is not None:
                    where_cols = [WHERE] if isinstance(WHERE, str) else list(WHERE)

                    if VALUES is not None:
                        values = (
                            [VALUES] if not isinstance(VALUES, (list, tuple)) else list(VALUES)
                        )
                        if len(where_cols) != len(values):
                            self.logger.warning("WHERE columns and VALUES count mismatch.")
                            return
                        where_composable = sql.SQL(" AND ").join(
                            sql.SQL("{col} = %s").format(col=self._col_to_identifier(col))
                            for col in where_cols
                        )
                        where_values = values

                    elif LIKE is not None:
                        patterns = (
                            [LIKE] if not isinstance(LIKE, (list, tuple)) else list(LIKE)
                        )
                        if len(where_cols) != len(patterns):
                            self.logger.warning("WHERE columns and LIKE patterns count mismatch.")
                            return
                        where_composable = sql.SQL(" AND ").join(
                            sql.SQL("{col} LIKE %s").format(col=self._col_to_identifier(col))
                            for col in where_cols
                        )
                        where_values = patterns

                # --- Resolve table identifier ---
                parts = table_name.split(".", 1)
                if len(parts) == 2:
                    table_ident = sql.SQL("{s}.{t}").format(
                        s=sql.Identifier(parts[0]), t=sql.Identifier(parts[1])
                    )
                else:
                    table_ident = sql.Identifier(table_name)

                # --- Assemble query ---
                update_sql = sql.SQL("UPDATE {table} SET {set}").format(
                    table=table_ident, set=set_clause
                )
                if where_composable is not None:
                    update_sql = update_sql + sql.SQL(" WHERE ") + where_composable
                update_sql = update_sql + sql.SQL(";")

                all_values = set_values + where_values
                self.logger.debug(update_sql.as_string(self.conn))
                cur.execute(update_sql, all_values)

            self._commit()

        except Exception as e:
            self._rollback()
            self.logger.error(f"Error updating data in '{table_name}': {e}")

    def delete_data(
        self,
        FROM: str,
        WHERE: Union[str, list[str]],
        VALUES: Optional[Union[Any, list[Any]]] = None,
        LIKE: Optional[Union[str, list[str]]] = None,
    ) -> None:
        """
        Delete rows from ``FROM`` matching the given ``WHERE`` condition.

        Parameters
        ----------
        FROM: str
            Table name to delete from.
        WHERE: str or list[str]
            Column name(s) for the WHERE clause.
        VALUES: Any or list[Any], optional
            Exact-match value(s).
        LIKE: str or list[str], optional
            LIKE pattern(s).

        Notes
        -----
        Cascading is handled by ``ON DELETE CASCADE`` constraints in the schema.
        Unrestricted (blanket) deletes without a WHERE clause are intentionally unsupported.
        """
        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        if not WHERE:
            self.logger.warning(
                "DELETE without a WHERE clause is not supported. "
                "Use a wildcard LIKE pattern if you intend to clear the table."
            )
            return

        try:
            where_cols = [WHERE] if isinstance(WHERE, str) else list(WHERE)

            where_composable: Optional[sql.Composable] = None
            params: list = []

            if VALUES is not None:
                values = [VALUES] if not isinstance(VALUES, (list, tuple)) else list(VALUES)
                if len(where_cols) != len(values):
                    self.logger.warning("WHERE columns and VALUES count mismatch.")
                    return
                where_composable = sql.SQL(" AND ").join(
                    sql.SQL("{col} = %s").format(col=self._col_to_identifier(col))
                    for col in where_cols
                )
                params = values

            elif LIKE is not None:
                patterns = [LIKE] if not isinstance(LIKE, (list, tuple)) else list(LIKE)
                if len(where_cols) != len(patterns):
                    self.logger.warning("WHERE columns and LIKE patterns count mismatch.")
                    return
                where_composable = sql.SQL(" AND ").join(
                    sql.SQL("{col} LIKE %s").format(col=self._col_to_identifier(col))
                    for col in where_cols
                )
                params = patterns

            else:
                self.logger.warning("Either VALUES or LIKE must be provided for DELETE.")
                return

            # Resolve table identifier
            parts = FROM.split(".", 1)
            if len(parts) == 2:
                table_ident = sql.SQL("{s}.{t}").format(
                    s=sql.Identifier(parts[0]), t=sql.Identifier(parts[1])
                )
            else:
                table_ident = sql.Identifier(FROM)

            delete_sql = (
                sql.SQL("DELETE FROM {table} WHERE ").format(table=table_ident)
                + where_composable
                + sql.SQL(";")
            )

            with self.conn.cursor() as cur:
                self.logger.debug(delete_sql.as_string(self.conn))
                cur.execute(delete_sql, params)

            self._commit()

        except Exception as e:
            self._rollback()
            self.logger.error(f"Error deleting data from '{FROM}': {e}")

    def raw_sql(
        self, SQL: str, VALUES: Optional[tuple] = None
    ) -> list[dict]:
        """
        Execute an arbitrary SQL statement and return all rows as dicts.

        .. warning::
            Avoid constructing *SQL* from user input. Prefer the typed CRUD helpers.

        Parameters
        ----------
        SQL: str
            Raw SQL query string (use ``%s`` placeholders for values).
        VALUES: tuple, optional
            Parameterised values bound to the placeholders.

        Returns
        -------
        list[dict]
        """
        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        try:
            self.logger.warning(f"Running raw SQL: {SQL}")
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(SQL, VALUES or ())
                rows = [dict(row) for row in cur.fetchall()]
            return rows

        except Exception as e:
            self.logger.critical(f"Raw SQL failed: {e}")
            return []

    def execute_file(self, file_path: str) -> None:
        """
        Execute SQL from a ``.sql`` file or insert records from a ``.json`` file.

        JSON format
        -----------
        A list of objects, each with ``"table"`` and ``"data"`` keys::

            [{"table": "users", "data": {"id": 1, "name": "Alice"}}]

        Parameters
        ----------
        file_path: str
            Path to a ``.sql`` or ``.json`` file.
        """
        if self.conn is None:
            self.connect_database(self.database, self.user, self.password)

        if not os.path.isfile(file_path):
            self.logger.warning(f"File '{file_path}' does not exist.")
            return

        _, ext = os.path.splitext(file_path)

        try:
            with self.conn.cursor() as cur:
                if ext == ".sql":
                    with open(file_path, "r", encoding="utf-8") as fh:
                        cur.execute(fh.read())
                    self._commit()
                    self.logger.info(f"SQL file '{file_path}' executed successfully.")

                elif ext == ".json":
                    with open(file_path, "r", encoding="utf-8") as fh:
                        data = json.load(fh)

                    if not isinstance(data, list):
                        self.logger.warning("JSON file must contain a list of records.")
                        return

                    for record in data:
                        if not isinstance(record, dict) or "table" not in record or "data" not in record:
                            self.logger.warning(
                                "Invalid JSON record — each item must have 'table' and 'data' keys."
                            )
                            return

                        tbl = record["table"]
                        row_data: dict = record["data"]

                        parts = tbl.split(".", 1)
                        if len(parts) == 2:
                            table_ident = sql.SQL("{s}.{t}").format(
                                s=sql.Identifier(parts[0]), t=sql.Identifier(parts[1])
                            )
                        else:
                            table_ident = sql.Identifier(tbl)

                        columns = sql.SQL(", ").join(sql.Identifier(k) for k in row_data)
                        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in row_data)
                        insert_sql = sql.SQL(
                            "INSERT INTO {table} ({cols}) VALUES ({vals});"
                        ).format(table=table_ident, cols=columns, vals=placeholders)

                        cur.execute(insert_sql, list(row_data.values()))

                    self._commit()
                    self.logger.info(f"JSON file '{file_path}' inserted successfully.")

                else:
                    self.logger.warning(
                        f"Unsupported file type '{ext}'. Only .sql and .json are supported."
                    )

        except psycopg.Error as e:
            self._rollback()
            self.logger.error(f"PostgreSQL error executing file '{file_path}': {e}")
        except Exception as e:
            self._rollback()
            self.logger.error(f"Unexpected error executing file '{file_path}': {e}")

    def _commit(self) -> None:
        """Commit the current transaction."""
        if self.conn is None:
            self.logger.error("Connector is not defined. Try reconnecting to the DB.")
            return
        try:
            self.conn.commit()
        except Exception:
            self.logger.error("Failed to commit transaction.")

    def _rollback(self) -> None:
        """Rollback the current transaction."""
        if self.conn is None:
            self.logger.error("Connector is not defined. Try reconnecting to the DB.")
            return
        try:
            self.conn.rollback()
        except Exception:
            self.logger.error("Failed to rollback transaction.")

    def _col_to_identifier(self, col: str) -> sql.Composable:
        """Convert 'table.column' or 'column' to a safe sql.Composable identifier."""
        parts = col.split(".", 1)
        if len(parts) == 2:
            return sql.SQL("{t}.{c}").format(
                t=sql.Identifier(parts[0]),
                c=sql.Identifier(parts[1])
            )
        return sql.Identifier(col)