
import mysql
import mysql.connector
import os
import pandas as pd
import sys

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))



class RequestData():
    """
    Queries/fetches data from the database.
    """

    def __init__(self, conn:mysql.connector.MySQLConnection) -> None:
        self.conn = conn

    def sql_query(self, sql_query:str):

        cursor = self.conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()

        if __name__ == "__main__":
            for select_row_idx, row in enumerate(rows):
                print(f"RequestData >> Row n°{select_row_idx}: {row}")

        return rows


    def select(self, table_name:str, SELECT:str="*"):
        """
        Allows to input a simple SELECT SQL query.
        Returns a list (rows) of tuples (columns)

        Eg: SELECT {SELECT} FROM {table_name}
        >>> SELECT label_key, transcription FROM Audio_transcription
        >>> [(label_key1, text1), (label_key2, text2), ...]

        Args
        ----
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

        Args
        ----
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
        

if __name__ == "__main__":

    from main import CONNECTION

    rqs = RequestData(conn=CONNECTION)


