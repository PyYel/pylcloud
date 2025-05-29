from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from typing import List, Optional, Union
import os
import boto3
from concurrent.futures import ThreadPoolExecutor
from .Storage import Storage


class StorageMinIO(Storage):
    """
    MinIO helper.
    """
    def __init__(self,
                 bucket_name: str = "storage",
                 endpoint="http://localhost:9000",
                 access_key="admin",
                 secret_key="password",
                 region_name="eu-west-1"):
        """
        MinIO helper
        """
        super().__init__(bucket_name=bucket_name)

        self.s3_client = boto3.client(
            service_name='s3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name
        )

        return None


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

        def _upload_file(file_path: str, object_key: str):
            try:
                self.s3_client.upload_file(file_path, self.bucket_name, object_key)
                print(f"Uploaded {file_path} to {self.bucket_name}/{object_key}")
            except Exception as e:
                print(f"Upload failed for {file_path}: {e}")

        if isinstance(paths, str):
            paths = [paths]
        if keys is None:
            keys = [os.path.basename(p) for p in paths]
        elif isinstance(keys, str):
            keys = [keys]

        with ThreadPoolExecutor() as executor:
            for path, key in zip(paths, keys):
                executor.submit(_upload_file, path, key)


    def download_files(self, keys: Union[str, List[str]], paths: Optional[Union[str, List[str]]]):
        """
        Downloads files from the remote storage.
        """

        def _download_file( object_key: str, file_path: str):
            try:
                self.s3_client.download_file(self.bucket_name, object_key, file_path)
                print(f"Downloaded {self.bucket_name}/{object_key} to {file_path}")
            except Exception as e:
                print(f"Download failed for {object_key}: {e}")

        if isinstance(keys, str):
            keys = [keys]
        if paths is None:
            paths = [os.path.basename(k) for k in keys]
        elif isinstance(paths, str):
            paths = [paths]

        with ThreadPoolExecutor() as executor:
            for key, path in zip(keys, paths):
                executor.submit(_download_file, key, path)


    def delete_files(self, keys: Union[str, List[str]]):
        """
        Deletes files from the remote storage.
        """

        def _delete_file(object_key: str):
            try:
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=object_key)
                print(f"Deleted {object_key} from {self.bucket_name}")
            except Exception as e:
                print(f"Delete failed for {object_key}: {e}")

        if isinstance(keys, str):
            keys = [keys]

        with ThreadPoolExecutor() as executor:
            for key in keys:
                executor.submit(_delete_file, key)


    def list_files(self, key: Optional[str] = None):
        """
        Lists files from remote storage, starting down to the root (or from the key) up to the leaves.
        """
        try:
            if key:
                response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=key)
            else:
                response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            return [item['Key'] for item in response.get('Contents', [])]
        except Exception as e:
            print(f"List failed: {e}")
            return []
