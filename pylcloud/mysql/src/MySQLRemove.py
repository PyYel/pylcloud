
import mysql
import mysql.connector
import os
import sys

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))



class RemoveData():
    """
    Queries/fetches data from the database.
    """

    def __init__(self, conn:mysql.connector.MySQLConnection) -> None:
        """
        The default connection is ``main.CONNECTION``. If ``conn`` is specified, it is used instead.

        Parameters
        ----------
        conn: a ``mysql.connector.MySQLConnection`` to a MySQL database. By default, the ``main.CONNECTION`` is used.
        """

        self.conn = conn


    def delete_dataset(self, dataset_name:str):
        """
        Erases a dataset record from the Datasets table, as well as all the data records referencing an involved
        foreign key. The affected tables are deleted in the following order:
        - Batches_Labels
        - Labels
        - Batches_Datapoints
        - Datapoints
        - Datasets


        Parameters
        ----------
        - dataset_name: the name of the dataset to delete. Every record referencing the Datasets primary key
        ``'DataHive/Data/{dataset_name}/'`` will be subsequently deleted, as stated above.

        Returns
        -------
        - affected_datapoints: the list of datapoint_path keys that should be deleted from the cloud
        - affected_labels: the list of label_path keys that should be deleted from the cloud

        Note
        ----
        The Batches (resp. Projetcs) table will delibirately be not affected by this, as a single batch (resp. project) 
        may reference multiple datasets. It might thus result into empty batches, which should be cleaned separatly.
        """

        try:
            cursor = self.conn.cursor()

            # Retreiving affected label_path keys
            cursor.execute(f"SELECT label_path FROM Labels WHERE dataset_name=%s;", (dataset_name,))
            affected_labels = [value[0] for value in cursor.fetchall()] # List of singleton to list
            # Retreiving affected datapoint_path keys
            cursor.execute(f"SELECT datapoint_path FROM Datapoints WHERE dataset_name=%s;", (dataset_name,))
            affected_datapoints = [value[0] for value in cursor.fetchall()] # List of singleton to list
            
            if affected_labels:
                # Deleting from Batches_Labels
                format_strings = ','.join(['%s'] * len(affected_labels))
                query = f"DELETE FROM Batches_Labels WHERE label_path IN ({format_strings});"
                cursor.execute(query, affected_labels)
                # Deleting from Labels
                format_strings = ','.join(['%s'] * len(affected_labels))
                query = f"DELETE FROM Labels WHERE label_path IN ({format_strings});"
                cursor.execute(query, affected_labels)

            if affected_datapoints:
                # Deleting from Batches_Datapoints
                format_strings = ','.join(['%s'] * len(affected_datapoints))
                query = f"DELETE FROM Batches_Datapoints WHERE datapoint_path IN ({format_strings});"
                cursor.execute(query, affected_datapoints)
                # Deleting from Datapoints
                format_strings = ','.join(['%s'] * len(affected_datapoints))
                query = f"DELETE FROM Datapoints WHERE datapoint_path IN ({format_strings});"
                cursor.execute(query, affected_datapoints)

            # Deleting from Datasets now that all foreign keys have been deleted
            query = f"DELETE FROM Datasets WHERE dataset_name=%s;"
            cursor.execute(query, (dataset_name,))

            self.conn.commit()
            print(f"RemoveData >> Dataset '{dataset_name}' and its references successfully removed")
            
        except mysql.connector.Error as e:
            print("RemoveData >> MySQL error:", e)

        return affected_datapoints, affected_labels
    

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
            print("RemoveData >> MySQL error:", e)

        return None


if __name__ == "__main__":

    from main import CONNECTION

    remove = RemoveData(conn=CONNECTION)
