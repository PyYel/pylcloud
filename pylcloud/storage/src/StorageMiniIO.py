from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from typing import List
import os
import shutil
import uvicorn
import re
import boto3
from typing import Optional, Union, List, Dict

from .Storage import Storage


class StorageMiniIO(Storage):

    def __init__(self, 
                 bucket_name: str = "storage",
                 endpoint="http://localhost:9000", 
                 access_key="admin", 
                 secret_key="password", 
                 region_name="eu-west-1"):
        super().__init__(bucket_name=bucket_name)

        self.s3_client = boto3.client(service_name='s3',
                                endpoint_url=endpoint,
                                aws_access_key_id=access_key,
                                aws_secret_access_key=secret_key,
                                region_name=region_name)

    def create_bucket(self):
        """
        Creates the chosen bucket if it does not exist.
        """
        try:
            self.s3_client.create_bucket(Bucket=self.bucket_name)
            print(f"Bucket '{self.bucket_name}' created.")
        except Exception as e:
            print(f"Create bucket failed: {e}")
    

    def upload_files(self, paths: Union[str, List[str]], keys: Optional[Union[str, List[str]]] = None):
        """
        Uploads files to the remote storage.
        """
        
        if object_name is None:
            object_name = os.path.basename(file_path)
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, object_name)
            print(f"Uploaded {file_path} to {self.bucket_name}/{object_name}")
        except Exception as e:
            print(f"Upload failed: {e}")


    def download_files(self, keys: Union[str, List[str]], paths: Optional[Union[str, List[str]]]):
        """
        Downloads files from the remote storage.
        """
        try:
            self.s3_client.download_file(self.bucket_name, object_name, file_path)
            print(f"Downloaded {self.bucket_name}/{object_name} to {file_path}")
        except Exception as e:
            print(f"Download failed: {e}")


    def delete_files(self, keys: Union[str, List[str]]):
        """
        Deletes files from the remote storage.
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=object_name)
            print(f"Deleted {object_name} from {self.bucket_name}")
        except Exception as e:
            print(f"Delete failed: {e}")


    def list_files(self, key: Optional[str]):
        """
        Lists files from remote storage, starting down to the root (or from the key) up to the leaves.
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            return [item['Key'] for item in response.get('Contents', [])]
        except Exception as e:
            print(f"List failed: {e}")
            return []


