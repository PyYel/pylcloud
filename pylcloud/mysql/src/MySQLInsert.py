
import mysql.connector
import mysql
import os
import sys
import pandas as pd
import numpy as np
from tqdm import tqdm
import warnings
from datetime import datetime

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))


class InsertData():
    """
    Methods to insert records into the database.
    """
    def __init__(self, conn:mysql.connector.MySQLConnection) -> None:
        """
        The default connection is ``main.CONNECTION``. If ``conn`` is specified, it is used instead.

        Parameters
        ----------
        conn: a ``mysql.connector.MySQLConnection`` to a MySQL database.
        """
        
        self.conn = conn

    def insert_users(self,
                     user_name:str,
                     user_entity:str,
                     user_metadata_path:str,
                     user_creation_date:str=None
                     ):
        """
        Insert a user record to the Users tables.

        Args:

        """

        if not user_creation_date:
            user_creation_date = datetime.now().strftime("%Y/%m/%d:%Hh%Mm%Ss")

        try:
            cursor = self.conn.cursor()

            cursor.execute(f"INSERT INTO users (user_name, user_entity, user_creation_date, user_metadata_path) VALUES (%s, %s, %s, %s);", 
                           (user_name, user_entity, user_creation_date, user_metadata_path))

            self.conn.commit()

        except mysql.connector.IntegrityError as e:
            print(f"InsertData >> User name '{user_name}' already taken")
            self.conn.rollback()

        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        return None


    def insert_projects(self,
                       project_name:str,
                       project_description:str,
                       project_entity:str,
                       project_creation_date:str=None
                       ):
        """
        Insert a project record to the Projects table.

        Args:

        """
        if not project_creation_date:
            project_creation_date = datetime.now().strftime("%Y/%m/%d:%Hh%Mm%Ss")
        project_metadata_path = f"DataHive/Projects/{project_name}.json"

        try:
            cursor = self.conn.cursor()
            
            cursor.execute(f"INSERT INTO Projects (project_name, project_description, project_entity, project_creation_date, project_metadata_path) VALUES (%s, %s, %s, %s, %s);", 
                           (project_name, project_description, project_entity, project_creation_date, project_metadata_path))

            self.conn.commit()

        except mysql.connector.IntegrityError as e:
            print(f"InsertData >> Project name '{project_name}' already taken")
            self.conn.rollback()

        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        return None
    

    def insert_users_projects(self,
                              user_name:str,
                              project_name:str,
                              ):
        """
        Affects a user to a project.

        Parameters
        ----------
        - user_name: the name of the user to assign
        - project_name: the name of the project to assign the user to

        """
        try:
            cursor = self.conn.cursor()

            cursor.execute(f"INSERT INTO users_projects (user_name, project_name) VALUES (%s, %s);", 
                           (user_name, project_name))

            self.conn.commit()

        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        return None


    def insert_batches(self, 
                       batch_name: str,
                       batch_creation_date: str = None,
                       batch_size: int = None,
                       batch_length: int = None):
        """
        Insert a batch record to the batches table

        Args:

        """

        if not batch_creation_date:
            batch_creation_date = datetime.now().strftime("%Y/%m/%d:%Hh%Mm%Ss")

        try:
            cursor = self.conn.cursor()

            cursor.execute(f"INSERT INTO batches (batch_name, batch_creation_date,  batch_size, batch_length) VALUES (%s, %s, %s, %s);", 
                           (batch_name, batch_creation_date, batch_size, batch_length))

            self.conn.commit()

        except mysql.connector.IntegrityError as e:
            print(f"InsertData >> Batch name '{batch_name}' already taken")
            self.conn.rollback()

        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        return None


    def insert_batches_projects(self,
                                batch_name:str,
                                project_name:str,
                                ):
        """
        Affects a batch to a project.

        Args:

        """
        try:
            cursor = self.conn.cursor()

            cursor.execute(f"INSERT INTO batches_projects (batch_name, project_name) VALUES (%s, %s);", 
                           (batch_name, project_name))

            self.conn.commit()

        except mysql.connector.Error as e:
            # print("InsertData >> MySQL error:", e)
            if e.errno == 1452:
                print(f"InsertData >> Foreign keys do not exist in parent tables")
            else:
                print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        return None


    def insert_batches_datapoints(self,
                                  batch_name:str,
                                  datapoint_path:str
                                  ) -> None:
        """
        Inserts a record into the Batches_Datapoints table.

        Parameters
        ----------

        """

        try:
            cursor = self.conn.cursor()

            cursor.execute(f"INSERT INTO Batches_Datapoints (batch_name, datapoint_path) VALUES (%s, %s)",
                           (batch_name, datapoint_path))
            self.conn.commit()
            
        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        return None
    

    def insert_batches_labels(self,
                              batch_name:str,
                              label_path:str
                              ) -> None:
        """
        Inserts a record into the Batches_Labels table.

        Parameters
        ----------

        """

        try:
            cursor = self.conn.cursor()


            cursor.execute(f"INSERT INTO Batches_Labels (batch_name, label_path) VALUES (%s, %s)",
                           (batch_name, label_path))
            self.conn.commit()
            
        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        return None


    def insert_batches_users(self, user_name: str, project_name: str, batch_name: str):
        """
        Inserts a record into the Batches_users table. 

        Parameters
        ----------

        """

        try:
            cursor = self.conn.cursor()

            cursor.execute(f"INSERT INTO Batches_Users (user_name, project_name, batch_name) VALUES (%s, %s, %s)",
                           (user_name, project_name, batch_name))
            self.conn.commit()
            
        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        return None


    def insert_batches_datasets(self, batch_name: str, dataset_name: str):
        """
        Inserts a record into the Batches_Datasets table. 

        Parameters
        ----------

        """

        try:
            cursor = self.conn.cursor()

            cursor.execute(f"INSERT INTO Batches_Datasets (batch_name, dataset_name) VALUES (%s, %s)",
                           (batch_name, dataset_name))
            self.conn.commit()
            
        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        return None


    def insert_labels(self, 
                      label_path:str,
                      label_task: str,
                      label_extension:str,
                      datapoint_path:str,
                      dataset_name:str,
                      label_size: int
                     ):
        """
        Inserts a record into the Labels table. This won't create a label file on the 
        cloud storage, so it should only be used when uploading new datasets and labelsets.

        TODO: insert multiple records with one commit to speed up the process (or parallelize ?)

        Parameters
        ----------

        """

        try:
            cursor = self.conn.cursor()

            cursor.execute(f"INSERT INTO Labels (label_path, label_task, label_extension, label_size, datapoint_path, dataset_name) VALUES (%s, %s, %s, %s, %s, %s)",
                           (label_path, label_task, label_extension, label_size, datapoint_path, dataset_name))
            self.conn.commit()

        except mysql.connector.Error as e:
            if e.errno == 1062:
                # This means a file already exists, which isn't an issue because what matters is the json content
                None
            else:
                print("InsertData >> MySQL error:", e)
                self.conn.rollback()

        return None


    def insert_datapoints(self, records: dict[str] | list[dict[str]] | pd.DataFrame) -> None:
        """
        Inserts a record into the Datapoints table. This won't upload a datapoint file on the 
        cloud storage, so it should only be used when uploading new datasets and labelsets.

        Args:
        - records: a dictionnary, list of dictionnaries or pandas dataframe whose keys/columns are:
            - datapoint_path
            - datapoint_folder
            - datapoint_extension
            - dataset_name
        """

        cursor = self.conn.cursor()
        def std_insert(datapoint_path:str, 
                       datapoint_extension:str,
                       datapoint_size: int,
                       dataset_name:str
                       ) -> bool:
            # Inserts a record (row) into the Datapoints table without commiting
            try:
                cursor.execute(f"INSERT INTO Datapoints (datapoint_path, datapoint_size, datapoint_extension, dataset_name) VALUES (%s, %s, %s, %s);",
                               (datapoint_path, datapoint_size, datapoint_extension, dataset_name))
                return True
            except mysql.connector.Error as e:
                if e.errno == 1062:
                    # Primary key already exists, i.e. a datapoint_path is overwritten or a previous datapoint wasn't 
                    # correctly removed. Either way, the cloud datapoint is replaced and this record stays the same
                    return True
                else:
                    return False
            except:
                return False

        if isinstance(records, dict):
            success = [std_insert(**records)]
            rows = 1

        elif isinstance(records, list):
            rows = len(records)
            success = [False] * rows
            for record_idx, record in enumerate(records):
                success[record_idx] = std_insert(**record)

        elif isinstance(records, pd.DataFrame):
            rows = len(records)
            success = [False] * rows
            for record_idx, record in records.iterrows():
                success[record_idx] = std_insert(**record.to_dict())

        else:
            raise ValueError(f"InsertData >> Datapoints records must be parsed from ``dict[str]|list[dict[str]]|pd.DataFrame``, got {type(records)} instead")

        if all(success):
            self.conn.commit()
            print(f"InsertData >> Successfully inserted {rows} rows into the Datapoints table")
        else:
            self.conn.rollback()
            print(f"InsertData >> Failed to insert all {rows} rows into the Datapoints table: {np.count_nonzero(success)} fails. Transaction aborted.")

        return None


    def insert_datasets(self,
                        dataset_name:str,
                        dataset_description:str,
                        dataset_size: int
                       ) -> None:
        """
        Inserts a record into the Datasets table. This won't upload a data folder on the 
        cloud storage, so it should only be used when uploading new datasets and labelsets.
        """

        try:
            cursor = self.conn.cursor()
            
            cursor.execute(f"INSERT INTO Datasets (dataset_name, dataset_description, dataset_size) VALUES (%s, %s, %s);", 
                           (dataset_name, dataset_description, dataset_size))
            self.conn.commit()
            return True

        except mysql.connector.Error as e:
            if e.errno == 1062:
                print(f"InsertData >> Dataset '{dataset_name}' already exists")
            else:
                print("InsertData >> MySQL error:", e)
            self.conn.rollback()
            return False
    


class InserDataUtils(InsertData):
    """
    A QOL subclass of `InserData`
    """

    def __init__(self, conn: mysql.connector.MySQLConnection) -> None:
        super().__init__(conn)



    def insert_datapoint_folder(self, folder_path, table_name, dataset_name:str, extensions=['png', 'jpg']):
        """
        Reads all the files from a <folder_path> and inserts into <table_name> all the files of type among <extensions>.

        Returns the list of the automatically incremented PRIMARY KEY allocated to every datapoint added. 
        """

        file_list = sorted([file for file in os.listdir(folder_path) 
                            if (any(file.endswith(ext) for ext in extensions))
                            and not file.startswith("GT_")])
        
        auto_incremented_primary_keys = [0] * len(file_list)

        for file_idx, file in enumerate(file_list):
            file_data = self._read_path(os.path.join(folder_path, file))
            auto_incremented_primary_keys[file_idx] = self.insert_datapoint(*file_data, dataset_name=dataset_name, table_name=table_name)
        
        return auto_incremented_primary_keys


    def upload_dataset(self,
                       dataset_type:str, 
                       extensions:list[str], 
                       dataset_name:str, 
                       dataset_description:str=None,
                       path:str=None):
        """
        Uploads a dataset, and adds its datapoints to the corresponding DatapointsTable.
        Creates as well a dataset of OriginalDataset type.

        Args:
            dataset_name: The name of the dataset. Must be unique, and should be close if not identical to the uploaded folder, if relevant.
            datapoint_table_name: The name of the DatapointsTable to push the data into, i.e. the datatype (cf. MainTable for the current existing list)
            label_table_name: The name of the LabelsTable to push and pull the labels from/into, i.e. the task type (cf. MainTable for the current existing list)
            extensions: the list of extensions (i.e. filetypes) to insert into the database (eg: ['png', 'jpg'], ['mp3'], ...)
        """
        try:
            cursor = self.conn.cursor()

            # References this new SourceDataset into the SourceDatasets table
            sql = f"INSERT INTO SourceDatasets (dataset_name, dataset_type, dataset_description) VALUES (%s, %s, %s)"
            values = (dataset_name, dataset_type, dataset_description)
            cursor.execute(sql, values)

            # Add the datapoints to the <datapoint_table_name> DatapointsTable
            self.insert_datapoint_folder(folder_path=path, table_name=dataset_type, extensions=extensions, dataset_name=dataset_name)

            self.conn.commit()
            print(f"InsertData >> Dataset {dataset_name} successfully uploaded.")

        except mysql.connector.IntegrityError as e:
            error = f"InsertData >> Can't upload dataset: Dataset name {dataset_name} already taken."
            print(error)
            self.conn.rollback()
            return error

        except mysql.connector.ProgrammingError as e:
            # When the database is not online, the cursor can't be created.
            # Would raise an error for every instance of the class when disconnecting the database.
            # print(e)
            pass

        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        return None


    def upload_labelset(self,
                        table_name:str,
                        dataset_name:str,
                        columns:list[str],
                        labelset_frame:pd.DataFrame=None,
                        labelset_path:str=None
                        ):
        """
        Uploads labels from a pandas Dataframe whose columns are the same as the table to push
        the labels into.

        Parameters
        ----------
        labelset_type: the name of the table to reference the labels into, i.e. the type of task that is annotated
        labelset_frame: the pandas DataFrame to write into the label table
        labelset_path: the path to the CSV file recording the labels to write into the label table
        """
        try:

            if labelset_path:
                df = pd.read_csv(labelset_path)
            elif labelset_frame:
                df = labelset_frame
            else:
                raise ValueError("InsertData >> Invalid labelset DataFrame or CSV")

            # df = df[columns]
            # df["origin"] = f"Source:{dataset_name}"
            # df.to_sql(name=table_name, con=self.conn, if_exists='append', index=False)

            cursor = self.conn.cursor()

            for index, row in df.iterrows():
                values = tuple(row[column] for column in columns)
                origin = f"Source:{dataset_name}"
                sql = f"INSERT INTO {table_name} ({', '.join(columns)}, origin) VALUES ({', '.join(['%s']*len(columns))}, %s)"
                cursor.execute(sql, values + (origin,))
            
            self.conn.commit()
            
            print(f"InsertData >> Labels table {table_name} columns {columns} successfully populated.")

        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        except pd.errors.DatabaseError as e:
            print("InsertData >> Pandas database error:", e)

        except Exception as e:
            print("InsertData >> Upload labelset unexpected error:", e)

        return None

    def create_subdataset(self, 
                        subdataset_name:str, 
                        subdataset_description:str,
                        source_datasets:list[str],
                        datapoint_types:str,
                        merging_table:str=None,
                        merging_conditions:str=None,
                        merging_value:str="TRUE"):
        """
        Create a SubDataset table that retrieves a selection of one or many existing datapoints. This subdataset
        will then be able to be affected to one or many projects. The SourceDatasets may be concatenated 
        or joined under the <merging_conditions> conditions.

        Args:

        """
        try:
            cursor = self.conn.cursor()

            # The new subdataset is referenced into the main SubDatasets table
            sql = f"""INSERT INTO SubDatasets (subdataset_name, subdataset_type, subdataset_description) 
                    VALUES (%s, %s, %s)"""
            values = (subdataset_name, datapoint_types, subdataset_description)
            cursor.execute(sql, values)

            # If the name is available, the subdataset table will be created
            self.create.create_subdataset(subdataset_name=subdataset_name, datapoint_type=datapoint_types)

            rows = []
            for source_dataset in source_datasets:
                # Retreiving the data (as keys)
                if merging_table and merging_conditions and merging_value:
                    # If the SourceDataset keys must meet a condition (eg: the datapoints must feature a certain label)
                    query = f"""SELECT t1.datapoint_key FROM {source_dataset} t1 
                            INNER JOIN {merging_table} t2 ON t1.datapoint_key = t2.datapoint_key
                            WHERE t1.dataset_name=(?) AND t2.{merging_conditions} = (?);"""
                    cursor.execute(query, (source_dataset, merging_value))
                    rows += cursor.fetchall()
                else:
                    # Else the whole SourceDataset keys are retreived
                    rows += self.request.select_where_one(SELECT="datapoint_key",
                                                          FROM=datapoint_types,
                                                          WHERE="dataset_name",
                                                          VALUES=(source_dataset,))

                datapoints_keys = [0] * len(rows)
                for idx, row in enumerate(rows):
                    datapoints_keys[idx] = row[0]
                
                # print(rows)
                # print(datapoints_keys)

                # The datapoints are pushed into the new subdataset
                for datapoint_key in datapoints_keys:
                    sql = f"INSERT INTO {subdataset_name} (datapoint_key, source_dataset) VALUES (%s, %s)"
                    values = (datapoint_key, source_dataset)
                    cursor.execute(sql, values)

            # If the name is available, the creation is confirmed in SQL database
            self.conn.commit()
            print(f"InsertData >> SubDataset {subdataset_name} successfully created.")

        except mysql.connector.IntegrityError as e:
            error = f"InsertData >> Can't create subdataset: Subdataset name {subdataset_name} already taken."
            print(error)
            self.conn.rollback()
            return error

        except mysql.connector.ProgrammingError as e:
            # When the database is not online, the cursor can't be created.
            # Would raise an error for every instance of the class when disconnecting the database.
            print("InsertData >> MySQL error:", e)
            pass

        except mysql.connector.Error as e:
            print("InsertData >> MySQL error:", e)
            self.conn.rollback()

        except Exception as e:
            print(e) 

        return None
    

    def _read_path(self, filepath) -> tuple:
        """
        Parses a file path and returns its path, name, type (extension), size. 
        """
        filename = os.path.splitext(os.path.basename(filepath))[0]
        filetype = os.path.splitext(filepath)[1][1:]
        filesize = os.path.getsize(filepath)

        return (filepath, filename, filetype, filesize)
    
    def _image_txt_labels(self,
                        folder_path:str,
                        source_dataset:str,
                        class_txt:dict,
                        format="yolo"
                        ):
        """
        Retreives the image detection labels from a folder where the data is stored in.
        The labels are saved as a csv in the same folder, so it can then easily be added
        to the database using the `upload_labelset` method. 

        Parameters
        ----------
        - format: the conversion to apply to the labels
            - "yolo": reads the YOLO format (x_center, y_center, width, height) into standard 
            - "coco": reads the COCO format (x_min, y_min, width, height) into standard 
            - "coco_max": reads the inversed COCO format (x_max, _max, width, height) into standard 
        """

        file_list = sorted([os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith(".txt")])
        datapoints_keys = self.request.select_where_one(SELECT="datapoint_key",
                                                        FROM="Image_datapoints",
                                                        WHERE="dataset_name",
                                                        VALUES=(source_dataset,))
        datapoints_keys = [item for row in datapoints_keys for item in row] # list of tuples to list

        if len(file_list) != len(datapoints_keys):
            warnings.warn(f"Lists of datapoints {len(datapoints_keys)} and labels {len(file_list)} don't match in length, they are unlikely to be from the same source.")

        labels = pd.DataFrame(data=[[0, 0, 0, 0, 0, 0, 0, 0, "0"]], columns=["datapoint_key", "confidence", "contributions", "class", "x_min", "y_min", "x_max", "y_max", "class_txt"])
        for file, key in list(zip(file_list, datapoints_keys)):
            targets = self._parse_txt(txt_file=file, format=format)
            targets["confidence"] = 1
            targets["contributions"] = 1
            targets["datapoint_key"] = key
            targets["class_txt"] = [class_txt[row] for row in targets["class"]]
            labels = pd.concat([labels, targets], ignore_index=True)

        labels = labels.iloc[1:, :] # Removes the first intializing row of 0s
        labels.to_csv(os.path.join(folder_path, "labels.csv"), index=False)

        return labels
    
    def _image_mask_labels(self,
                           folder_path:str,
                           source_dataset:str,
                           class_txt:str=None,
                           extensions:tuple=("jpg", "png")
                           ):
        """
        Retreives the image segmentation labels from a folder where the data is stored in.
        The labels are saved as a csv in the same folder, so it can then easily be added
        to the database using the `upload_labelset` method. 
        """

        file_list = sorted([os.path.join(folder_path, file) for file in os.listdir(folder_path) 
                            if file.startswith("GT_") and any(file.endswith(ext) for ext in extensions)])

        datapoints_keys = self.request.select_where_one(SELECT="datapoint_key",
                                                        FROM="Image_datapoints",
                                                        WHERE="dataset_name",
                                                        VALUES=(source_dataset,))
        datapoints_keys = [item for row in datapoints_keys for item in row] # list of tuples to list

        if len(file_list) != len(datapoints_keys):
            warnings.warn(f"Lists of datapoints {len(datapoints_keys)} and labels {len(file_list)} don't match in length, they are unlikely to be from the same source.")
        
        print("Parsing labels into pandas Dataframe:")
        labels = pd.DataFrame(data=[[0, 0, 0, "0", "0"]], columns=["datapoint_key", "confidence", "contributions", "label_path", "class_txt"])
        for file, key in tqdm(list(zip(file_list, datapoints_keys))):
            targets = pd.DataFrame(data=[[key, 1, 1, file, class_txt]], columns=["datapoint_key", "confidence", "contributions", "label_path", "class_txt"])
            labels = pd.concat([labels, targets], ignore_index=True)

        labels = labels.iloc[1:, :] # Removes the first intializing row of 0s
        labels.to_csv(os.path.join(folder_path, "labels.csv"), index=False)

        return labels

    def _parse_txt(self, txt_file:str, format="yolo", delimiter=" ", bbox_threshold=1e-2):
        """
        Hidden method.
        Returns the labels and bbox positions of a txt file in YOLO format (xywh) into stadard (xyxy) format.

        Parameters
        ----------
        - format: the conversion to apply to the labels
            - "yolo": reads the YOLO format (x_center, y_center, width, height) into standard 
            - "coco": reads the COCO format (x_min, y_min, width, height) into standard 
            - "coco_max": reads the inversed COCO format (x_max, _max, width, height) into standard 
        """

        targets = pd.read_csv(f"{txt_file}", delimiter=delimiter, header=None, names=["class", "x_min", "y_min", "x_max", "y_max"])
        targets.dropna(inplace=True, how='any') # Removes empty/incomplete/NaN rows

        drop_list = []
        for row_idx, row in enumerate(targets.values):
            if format=="yolo":
                data = self._yolo_to_standard(row)
            if format=="coco":
                data = self._coco_min_to_standard(row)
            if format=="coco_max":
                data = self._coco_max_to_standard(row)
                
            if data: # If data is not None
                targets.iloc[row_idx, :] = data
            else: # If the bbox are too small, they are removed
                print("Data had to be sanitized:", row)
                drop_list.append(row_idx)
        targets.drop(index=drop_list, inplace=True)

        if targets.empty:
            print(f"File {txt_file} was sanitized to the point is became empty.")

        return targets


    def _yolo_to_standard(self, row:np.ndarray):
        """
        Reads yolo coordinates and returns it to a standard format.
        """
        label, x_center, y_center, width, height = row
        if width <= 1e-2 or height <= 1e-2:
            return None
        else:
            return [label, max(0, x_center-width/2), max(0, y_center-height/2), min(1, x_center+width/2), min(1, y_center+height/2)]

    def _coco_max_to_standard(self, row:np.ndarray):
        """
        Reads coco coordinates and returns it to a standard format.
        """
        label, x_max, y_max, width, height = row
        if width <= 1e-2 or height <= 1e-2:
            return None
        else:
            return [label, max(0, x_max-width), max(0, y_max-height), x_max, y_max]

    def _coco_min_to_standard(self, row:np.ndarray):
        """
        Reads coco coordinates and returns it to a standard format.
        """
        label, x_bottom, y_bottom, width, height = row
        if width <= 1e-2 or height <= 1e-2:
            return None
        else:
            return [label, x_bottom, y_bottom, min(1, x_bottom+width), min(1, y_bottom+height)]



if __name__ == "__main__":

    from main import CONNECTION


    ins = InsertData(conn=CONNECTION)
    ins.insert_users(user_name="nblidi", user_password="130601", user_entity="R&I")
    ins.insert_projects(project_name="bigproject", project_description="a real big project", project_entity="R&I")
    ins.insert_users_projects(user_name="nblidi", project_name="bigproject")

    ins.insert_batches(batch_name="batch1", batch_description="desc")
    ins.insert_batches_projects(batch_name="batch1", project_name="bigproject")

