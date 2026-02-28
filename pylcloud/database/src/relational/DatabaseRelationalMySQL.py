import os, sys
from typing import Union, Optional, Any
import json
from mysql import connector

from .DatabaseRelational import DatabaseRelational
from pylcloud import _config_logger


class DatabaseRelationalMySQL(DatabaseRelational):
    """
    A class to manage MySQL databases (RDS, Aurora, local) with optional IAM authentication.
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
        A high-level interface for MySQL server database, compatible with standard MySQL, 
        AWS Aurora MySQL, and AWS RDS MySQL.

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
        super().__init__()

        self.logger = _config_logger(logs_name="DatabaseRelationalMySQL")

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
        self.conn: connector.connection = None  # type: ignore

        return None

    def connect_database(self, *args, **kwargs):
        return super().connect_database(*args, **kwargs)

    def disconnect_database(self, *args, **kwargs):
        return super().disconnect_database(*args, **kwargs)

    def describe_database(self, *args, **kwargs):
        return super().describe_database(*args, **kwargs)

    def create_table(self, *args, **kwargs):
        return super().create_table(*args, **kwargs)

    def drop_table(self, *args, **kwargs):
        return super().drop_table(*args, **kwargs)

    def list_tables(self, *args, **kwargs):
        return super().list_tables(*args, **kwargs)

    def query_data(self, *args, **kwargs):
        return super().query_data(*args, **kwargs)

    def send_data(self, *args, **kwargs):
        return super().send_data(*args, **kwargs)

    def delete_data(self, *args, **kwargs):
        return super().delete_data(*args, **kwargs)

    def update_data(self, *args, **kwargs):
        return super().update_data(*args, **kwargs)

    def _commit(self):
        return super()._commit()

    def _rollback(self):
        return super()._rollback()
