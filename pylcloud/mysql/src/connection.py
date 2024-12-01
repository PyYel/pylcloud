import sqlite3
import os
import sqlite3
import mysql.connector
from mysql.connector import Error
import sys

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))


class Connection():
    """
    Connects and disconnects the database.
    The <conn> attribute is required to operate other operations and transactions.
    """

    def __init__(self, 
                database_name="datahive",
                host="127.0.0.1",
                user="admin",
                password="password",
                port="3306"
                ):
        
        self.database_name = database_name.lower()
        self.host = host
        self.user = user
        self.password = password
        self.port = port


    def connect_database(self):
        """
        Connects the database and returns the connector <conn>.
        """

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
                cursor.execute(f"USE {self.database_name};")
                print(f"Connection >> Connected to database '{self.database_name}'")

        except mysql.connector.Error as e:
            print(f"Connection >> MySQL connection error: {e}")
            if e.errno == 1045:
                print("Connection >> MySQL login credential error, program interrupted")
                return sys.exit(1)
            print(f"Connection >> Trying to create a database '{self.database_name}' instead")

            try:
                self.conn = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    port=self.port
                )

                cursor = self.conn.cursor()

                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name};")
                cursor.execute(f"USE {self.database_name};")
                self.conn.commit()
                print(f"Connection >> Database '{self.database_name}' successfully created and connected to")

            except mysql.connector.Error as e:
                self.conn.rollback()
                print(f"Connection >> MySQL error when creating/using database '{self.database_name}':", e)


        return self.conn
    

    def close_database(self):
        """
        Closes the database linked to the connector <conn>.
        """
        if self.conn:
            self.conn.close()
            print(f"Connection >> Disconnected from database '{self.database_name}'")


if __name__ == "__main__":

    db = Connection(user="client",
                    password="password",
                    database_name="DataHive",
                    port="330")
    conn = db.connect_database()
        
    db.close_database()
