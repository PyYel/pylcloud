from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from typing import List, Union, Optional
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import mimetypes

from .Storage import Storage
from aws import AWS


class StorageS3(Storage, AWS):
    """
    Standard S3 class with all purpose methods.
    """
    def __init__(self, 
                 bucket_name: str,
                 aws_access_key_id: str,
                 aws_secret_access_key: str, 
                 aws_region_name: str = 'eu-west-1',
                 tmp_dir: Optional[str] = None) -> None:
        """
        Initiates a connection to a given S3 bucket.

        Parameters
        ----------
        aws_access_key_id: str
            The user's private key ID to use to connect to the AWS services.
        aws_secret_access_key: str
            The user's private key to use to connect to the AWS services.
        aws_region_name: str, 'us-east-1'
            The AWS ressources region name.
        bucket_name: str
            The name of the bucket to fetch and send data to.
        tmp_dir: str, '~/tmp'
            The ``/tmp`` folder is used as a default folder to read and write data locally. It
            can be considered as a default input/output folder. If None, ``/tmp`` folder will
            be created in the current working directory.

        Notes
        -----
        - An instance of this class can connect to only one bucket.
        Create other instances to connect to multiple buckets at once.
        - The ``/temp`` folder should be a folder that can be completely erased.
        - The `key` kwarg usually refers to a cloud path.
        - The `path` kwarg usually refers to a local path.

        Exemple
        -------
        >>> bucket_api = AWSS3(KEY_ID, KEY, 'eu-west-1', 'my_bucket_name', 'C:/Users/Me/myapp/output_folder')
        """
        super().__init__(bucket_name=bucket_name, tmp_dir=tmp_dir)

        self.s3_client = self._create_client(aws_service_name="s3",
                                            aws_access_key_id=aws_access_key_id,
                                            aws_secret_access_key=aws_secret_access_key,
                                            aws_region_name=aws_region_name)

        # AWSS3 neeeds a working folder to download/upload files from a unique entry point
        if not os.path.exists(self.tmp_dir):
            os.mkdir(self.tmp_dir)
            print(f"AWSS3 >> Temporary folder created: '{self.tmp_dir}")


    def ensure_key(self, key: str):
        """
        Checks if the cloud keys exist and returns a tuple of the results

        Parameters
        ----------
        key: str
            The key to check on the cloud 

        Returns
        -------
        tuple: (str, bool)
            The tested key and whereas it exists (True) or not (False) 
        """

        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=key)
        if 'Contents' in response:
            return key, True
        else: 
            return key, False
    

    def ensure_keys(self, keys: list[str]):
        """
        Checks if the cloud keys exist and returns a dictionnary of the results, 
        where keys are the cloud keys, and the values are booleans.

        Parameters
        ----------
        keys: list[str]
            The keys to check on the cloud.

        Returns
        -------
        results: dict[str, bool]
            A dictionnary (dict{key: bool}) of the boolean response for each key.
        """

        print(f"AWSS3 >> Verifying the existence of {len(keys)} keys on the cloud.")
        results = {}
        failures = 0
        with tqdm(total=len(keys)) as pbar:
            with ThreadPoolExecutor(max_workers=2*os.cpu_count()) as executor: # type: ignore
                futures = []
                for key in keys:
                    futures.append(executor.submit(self.ensure_key, key=key))
                for future in as_completed(futures):
                    try:
                        result_key, result = future.result()
                        results[result_key] = result
                    except Exception as e:
                        failures += 1
                    pbar.update(1)
        if failures:
            print(f"AWSS3 >> Failed to verify the existence of {failures}/{len(keys)} keys.")

        return results




    def download_files(self, keys: list[str], paths: list[str] | None = None, display: bool = False): # type: ignore
        """
        Downloads a list of files into the path folder.

        Parameters
        ----------
        keys: list[str] 
            The list of cloud paths of each file
        path: list[str]
            The local path to save the dowload the data to. If None is specified, the data is
            saved into the default temp folder
        """

        def download_file(key: str, path: str):
            """
            Downloads a file ``key`` to the local ``path``.

            Parameters
            ----------
            key: str
                The cloud path of the file.
            path: str
                The local path to save the download the data to.
            """

            self.s3_client.download_file(Bucket=self.bucket_name, 
                                        Key=key, 
                                        Filename=path)
            
            return None

        keys, paths = self._check_args(keys=keys, paths=paths)

        if not paths:
            path = self.tmp_dir
            paths = [os.path.join(path, f"{idx}{os.path.splitext(key)[-1]}") for idx, key in enumerate(keys)]
        else:
            if not all([os.path.exists(os.path.dirname(path)) for path in paths]):
                print("AWSS3 >> Download path do not exist. Download aborted.")
                return False

        failed_files = []
        print(f"AWSS3 >> Downloading {len(keys)} files.")
        with tqdm(total=len(keys)) as pbar:
            with ThreadPoolExecutor(max_workers=2*os.cpu_count()) as executor: # type: ignore
                futures = []
                for key, path in zip(keys, paths):
                    futures.append(executor.submit(download_file, key=key, path=path)) 
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        failed_files.append(future)
                    pbar.update(1)
        if failed_files:
            print("AWSS3 >> An error was raised (only one is shown if many): \n", e) # type: ignore
            print(f"AWSS3 >> Error downloading {len(failed_files)}/{len(keys)} files. See error above.")

        return failed_files


    def download_directory(self, key: str, path: Optional[str] = None):
        """
        Downloads all the files from a folder into a single output folder where
        each file is renamed fileidx_filename.fileextension.

        Parameters
        ----------
        key: str
            The cloud path of the folder on the S3 storage.
        path: str
            The local path to save the dowload the data to.

        Returns
        -------
        failed_files: list[str]
            The list of files that were not correctly downloaded.

        Example
        -------
        >>> folder = my_folder/my_data_subfolder
        >>> download_directory(key=folder)
        >>> os.listdir(path)
        ["img1.jpg", "cat.png", "sample.wav"]
        """

        if not path:
            path = self.tmp_dir
        else:
            if not os.path.exists(path): os.mkdir(path)
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_total = 0
        for result in paginator.paginate(Bucket=self.bucket_name, Prefix=key):
            page_total += 1

        file_idx = 0
        failed_files = []
        print(f"AWSS3 >> Downloading batch of content from '{key}'")#: {page}/{page_total}")
        with tqdm(total=page_total) as pbar:
            for result in paginator.paginate(Bucket=self.bucket_name, Prefix=key):
                if result.get('Contents') is not None:
                    with ThreadPoolExecutor(max_workers=2*os.cpu_count()) as executor: # type: ignore
                        futures = []
                        for obj in result['Contents']:
                            key = obj['Key']
                            dirpath = os.path.join(os.path.join(path), f"{os.path.basename(os.path.dirname(key))}")
                            if not os.path.exists(dirpath): os.mkdir(dirpath)
                            filename = os.path.basename(key)
                            futures.append(executor.submit(self.s3_client.download_file, 
                                                           Bucket=self.bucket_name, 
                                                           Key=key,
                                                           Filename=os.path.join(dirpath, filename)))
                            file_idx += 1

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        failed_files.append(future)
                    pbar.update(1)

        if failed_files:
            print("AWSS3 >> An error was raised (only one is shown if many): \n", e) # type: ignore
            print(f"AWSS3 >> Error downloading {len(failed_files)} files. See error above.")

        return failed_files


    def list_keys(self, prefix: str = ""):
        """
        Returns a list of all the files present in a bucket whose key starts with ``key``.

        Parameters
        ----------
        prefix: str
            The key path to match. Hence if ``''``, then the whole bucket content will be listed.

        Returns
        -------
        file_list: list[str]
            The list of keys that match the ``prefix``.
        """

        if not self.ensure_key(key=prefix):
            raise ValueError(f"AWSS3 >> File listing prefix '{prefix}' does not exist")

        paginator = self.s3_client.get_paginator('list_objects_v2')
        file_list = []
        print(f"AWSS3 >> Listing files in {self.bucket_name}/{prefix}")
        for result in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            if result.get('Contents') is not None:
                for obj in result['Contents']:
                    file_list.append(obj['Key'])

        return file_list

             
    def delete_key(self, key: str):
        """
        Deletes a file on the cloud from its key

        Parameters
        ----------
        key: str
            The path of the file to delete on the cloud
        """

        if self.ensure_key(key=key):
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)

        return None


    def delete_keys(self, keys: list[str]):
        """
        Deletes from the bucket the files specified in keys, 
        and returns the list of the successfully deleted files

        Parameters
        ----------
        keys: list[str] 
            The list of keys (cloud path) to delete.
        """

        if keys:
            batched_keys = []
            for batch_start in range(0, len(keys), 1000):
                batched_keys.append(keys[batch_start:batch_start+1000])

            deleted_counter = 0
            print(f"AWSS3 >> Deleting {len(keys)} keys.")
            for batch in tqdm(batched_keys):
                response = self.s3_client.delete_objects(Bucket=self.bucket_name, 
                                                    Delete={'Objects': [{'Key': key} for key in batch]})
                deleted_counter += len(response.get('Deleted', []))

            if deleted_counter == len(keys):
                print(f"AWSS3 >> All {deleted_counter} keys were successfully deleted.")
            else:
                print(f"AWSS3 >> Only {deleted_counter} out of {len(keys)} keys were successfully deleted.")

            return None
        
        else:
            print(f"AWSS3 >> No keys were deleted as the input was empty.")

        return None        


    def edit_key(self, original_key: str, new_key: str, replace: bool = True):
        """
        Moves, renames, or copy-pastes a key, by editing its cloud path.

        Parameters
        ----------
        original_key : str
            The key to edit.
        new_key: str
            The new key name or path.
        replace: str, True
            If ``False`` the ``original_key`` is kept, hence resulting in a copy-paste action.
            Otherwise, is similar to either a cut-paste or rename acion. 

        Notes
        -----
        - Either way, the key is copied to a ``new_key`` cloud path. If ``replace`` is set to ``True``, the old
        version of the key is deleted after.
        """
        
        if not(self.ensure_key(key=original_key)):
            print(f"AWSS3 >> Original key {original_key} does not exist.")
            
        elif self.ensure_key(key=new_key):
            print(f"AWSS3 >> New key {new_key} already exist.")
            
        else:
            self.s3_client.copy(CopySource={'Bucket' : self.bucket_name,'Key': original_key}, 
                                Bucket=self.bucket_name, 
                                Key=new_key)
            if replace: self.s3_client.delete_object(Bucket=self.bucket_name, Key=original_key)

        return None

   
    def edit_keys(self, original_keys: list[str], new_keys: list[str]):
        """
        Cut from list of the keys_original file to the list of keys_cut
        and displays the list of the successfully cut files.

        # TODO: parallelize the process
        """

        for original_key, new_key in zip(original_keys, new_keys):
            self.cut_key(original_key, new_key)


    def upload_file(self, key: str, path: str):
        """
        Uploads a local file (path) into its cloud location (key).
        """
        
        if not os.path.exists(path):
            raise ValueError("AWSS3 >> Local file path does not exist. Check the path argument.")

        self.s3_client.upload_file(Bucket=self.bucket_name, 
                                   Key=key,
                                   Filename=path)
        
        return None


    def upload_files(self, keys: list[str], paths: list[str]):
        """
        Uploads a list of local files (paths) into their cloud locations (keys).
        """

        if len(keys) != len(paths):
            raise ValueError(f"AWSS3 >> Number of keys ({len(keys)}) doesn't match the number of paths ({len(paths)}). Upload aborted.")

        failed_files = []
        print(f"AWSS3 >> Uploading {len(keys)} files on the cloud")
        with tqdm(total=len(paths)) as pbar:
            with ThreadPoolExecutor(max_workers=2*os.cpu_count()) as executor:
                futures = []
                for key, file in list(zip(keys, paths)):
                    futures.append(executor.submit(self.s3_client.upload_file, 
                                                    Bucket=self.bucket_name, 
                                                    Key=key,
                                                    Filename=file))

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"AWSS3 >> Error uploading file: {e}")
                        failed_files.append(future)
                    pbar.update(1)

        return None

     
    def upload_directory(self, key: str, path: str):
        """
        Uploads local directory to key from bucket
        and displays the successfully uploaded file
        """

        if not key.endswith("/"):
            key += "/"
        if not os.path.exists(path):
            raise ValueError(f"AWSS3 >> Upload folder '{path}' does not exist")
        file_list = os.listdir(path)

        failed_files = []
        confirm = input(f"AWSS3 >> Uploading files from folder '{path}' into '{key}'. Confirm [Y/n] ? ")
        if confirm in ["Y", "y"]:
            with tqdm(total=len(file_list)) as pbar:
                with ThreadPoolExecutor(max_workers=2*os.cpu_count()) as executor:
                    futures = []
                    for file in file_list:
                        futures.append(executor.submit(self.s3_client.upload_file, 
                                                        Bucket=self.bucket_name, 
                                                        Key=key+os.path.split(file)[1],
                                                        Filename=os.path.join(path, file)))

                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            print(f"AWSS3 >> Error uploading file: {e}")
                            failed_files.append(future)
                        pbar.update(1)
        else:
            print("AWSS3 >> Upload aborted")

        return failed_files
    

class AWSS3(AWS):
    """
    Standard S3 class with all purpose methods.
    """
    def __init__(self, 
                 bucket_name: str,
                 aws_access_key_id: str,
                 aws_secret_access_key: str, 
                 aws_session_token: str = None,
                 aws_region_name: str = 'us-west-1',
                 temp_folder_path: str = TMP_DIR) -> None:
        """
        Initiates a connection to a given S3 bucket.

        Parameters
        ----------
        aws_access_key_id: str
            The user's private key ID to use to connect to the AWS services.
        aws_secret_access_key: str
            The user's private key to use to connect to the AWS services.
        aws_region_name: str, 'us-east-1'
            The AWS ressources region name.
        bucket_name: str
            The name of the bucket to fetch and send data to.
        temp_folder_path: str, '~/temp'
            The ``/temp`` folder is used as a default folder to read and write data locally. It
            can be considered as a default input/output folder. If None, ``/temp`` folder will
            be created in the current working directory.

        Notes
        -----
        - An instance of this class can connect to only one bucket.
        Create other instances to connect to multiple buckets at once.
        - The ``/temp`` folder should be a folder that can be completely erased.
        - The `key` kwarg usually refers to a cloud path.
        - The `path` kwarg usually refers to a local path.

        Exemple
        -------
        >>> bucket_api = AWSS3(KEY_ID, KEY, 'eu-west-1', 'my_bucket_name', 'C:/Users/Me/myapp/output_folder')
        """
        super().__init__()

        self.bucket_name = bucket_name
        self.temp_folder_path = temp_folder_path

        self.s3_client: boto3.s3.inject = self._create_client(aws_service_name="s3",
                                                              aws_access_key_id=aws_access_key_id,
                                                              aws_secret_access_key=aws_secret_access_key,
                                                              aws_session_token=aws_session_token,
                                                              aws_region_name=aws_region_name)

        # AWSS3 neeeds a working folder to download/upload files from a unique entry point
        if self.temp_folder_path is None:
            self.temp_folder_path = os.path.join(os.getcwd(), 'temp')
            if not os.path.exists(self.temp_folder_path):
                os.mkdir(self.temp_folder_path)
                print("AWSS3 >> Temporary folder created:", self.temp_folder_path)

        self._init_mimetypes()

        return None
    

    def _init_mimetypes(self):

        # Initialize mimetypes
        mimetypes.init()

        # Add specific mappings that might be missing
        mimetypes.add_type('text/html', '.html')
        mimetypes.add_type('text/css', '.css')
        mimetypes.add_type('application/javascript', '.js')
        mimetypes.add_type('application/json', '.json')
        mimetypes.add_type('image/svg+xml', '.svg')
        mimetypes.add_type('application/rdf+xml', '.owl')
        mimetypes.add_type('text/turtle', '.ttl')

        return None

    def ensure_key(self, key: str):
        """
        Checks if the cloud keys exist and returns a tuple of the results

        Parameters
        ----------
        key: str
            The key to check on the cloud 

        Returns
        -------
        tuple: (str, bool)
            The tested key and whereas it exists (True) or not (False) 
        """

        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=key)
        if 'Contents' in response:
            return key, True
        else: 
            return key, False
    

    def ensure_keys(self, keys: list[str]):
        """
        Checks if the cloud keys exist and returns a dictionnary of the results, 
        where keys are the cloud keys, and the values are booleans.

        Parameters
        ----------
        keys: list[str]
            The keys to check on the cloud.

        Returns
        -------
        results: dict[str, bool]
            A dictionnary (dict{key: bool}) of the boolean response for each key.
        """

        print(f"AWSS3 >> Verifying the existence of {len(keys)} keys on the cloud.")
        results = {}
        failures = 0
        with tqdm(total=len(keys)) as pbar:
            with ThreadPoolExecutor(max_workers=2*os.cpu_count()) as executor:
                futures = []
                for key in keys:
                    futures.append(executor.submit(self.ensure_key, key=key))
                for future in as_completed(futures):
                    try:
                        result_key, result = future.result()
                        results[result_key] = result
                    except Exception as e:
                        failures += 1
                    pbar.update(1)
        if failures:
            print(f"AWSS3 >> Failed to verify the existence of {failures}/{len(keys)} keys.")

        return results


    def download_file(self, key: str, path: str):
        """
        Downloads a file ``key`` to the local ``path``.

        Parameters
        ----------
        key: str
            The cloud path of the file.
        path: str
            The local path to save the download the data to.
        """

        self.s3_client.download_file(Bucket=self.bucket_name, 
                                     Key=key, 
                                     Filename=path)
        
        return None


    def download_files(self, keys: list[str], paths: list[str] = None):
        """
        Downloads a list of files into the path folder.

        Parameters
        ----------
        keys: list[str] 
            The list of cloud paths of each file
        path: list[str]
            The local path to save the dowload the data to. If None is specified, the data is
            saved into the default temp folder
        """

        if isinstance(keys, str):
            keys = [keys]
        if not isinstance(keys, list):
            print(f"AWSS3 >> Argument keys is of type {type(keys)} instead of list[str]. Dowload aborted.")
            return False

        if not paths:
            path = self.temp_folder_path
            paths = [os.path.join(path, f"{idx}{os.path.splitext(key)[-1]}") for idx, key in enumerate(keys)]
        else:
            if not all([os.path.exists(os.path.dirname(path)) for path in paths]):
                print("AWSS3 >> Download path do not exist. Download aborted.")
                return False

        failed_files = []
        print(f"AWSS3 >> Downloading {len(keys)} files.")
        with tqdm(total=len(keys)) as pbar:
            with ThreadPoolExecutor(max_workers=2*os.cpu_count()) as executor:
                futures = []
                for key, path in zip(keys, paths):
                    futures.append(executor.submit(self.s3_client.download_file, 
                                                    Bucket=self.bucket_name, 
                                                    Key=key,
                                                    Filename=path))
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        failed_files.append(future)
                    pbar.update(1)
        if failed_files:
            print("AWSS3 >> An error was raised (only one is shown if many): \n", e)
            print(f"AWSS3 >> Error downloading {len(failed_files)}/{len(keys)} files. See error above.")

        return failed_files


    def download_directory(self, key: str, path: str = None):
        """
        Downloads all the files from a folder into a single output folder where
        each file is renamed fileidx_filename.fileextension.

        Parameters
        ----------
        key: str
            The cloud path of the folder on the S3 storage.
        path: str
            The local path to save the dowload the data to.

        Returns
        -------
        failed_files: list[str]
            The list of files that were not correctly downloaded.

        Example
        -------
        >>> folder = my_folder/my_data_subfolder
        >>> download_directory(key=folder)
        >>> os.listdir(path)
        ["img1.jpg", "cat.png", "sample.wav"]
        """

        if not path:
            path = self.temp_folder_path
        else:
            if not os.path.exists(path): os.mkdir(path)
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_total = 0
        for result in paginator.paginate(Bucket=self.bucket_name, Prefix=key):
            page_total += 1

        file_idx = 0
        failed_files = []
        print(f"AWSS3 >> Downloading batch of content from '{key}'")#: {page}/{page_total}")
        with tqdm(total=page_total) as pbar:
            for result in paginator.paginate(Bucket=self.bucket_name, Prefix=key):
                if result.get('Contents') is not None:
                    with ThreadPoolExecutor(max_workers=2*os.cpu_count()) as executor:
                        futures = []
                        for obj in result['Contents']:
                            key = obj['Key']
                            dirpath = os.path.join(os.path.join(path), f"{os.path.basename(os.path.dirname(key))}")
                            if not os.path.exists(dirpath): os.mkdir(dirpath)
                            filename = os.path.basename(key)
                            futures.append(executor.submit(self.s3_client.download_file, 
                                                           Bucket=self.bucket_name, 
                                                           Key=key,
                                                           Filename=os.path.join(dirpath, filename)))
                            file_idx += 1

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        failed_files.append(future)
                    pbar.update(1)

        if failed_files:
            print("AWSS3 >> An error was raised (only one is shown if many): \n", e)
            print(f"AWSS3 >> Error downloading {len(failed_files)} files. See error above.")

        return failed_files


    def list_keys(self, prefix: str = ""):
        """
        Returns a list of all the files present in a bucket whose key starts with ``key``.

        Parameters
        ----------
        prefix: str
            The key path to match. Hence if ``''``, then the whole bucket content will be listed.

        Returns
        -------
        file_list: list[str]
            The list of keys that match the ``prefix``.
        """

        if not self.ensure_key(key=prefix):
            raise ValueError(f"AWSS3 >> File listing prefix '{prefix}' does not exist")

        paginator = self.s3_client.get_paginator('list_objects_v2')
        file_list = []
        print(f"AWSS3 >> Listing files in {self.bucket_name}/{prefix}")
        for result in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            if result.get('Contents') is not None:
                for obj in result['Contents']:
                    file_list.append(obj['Key'])

        return file_list

             
    def delete_key(self, key: str):
        """
        Deletes a file on the cloud from its key

        Parameters
        ----------
        key: str
            The path of the file to delete on the cloud
        """

        if self.ensure_key(key=key):
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)

        return None


    def delete_keys(self, keys: list[str]):
        """
        Deletes from the bucket the files specified in keys, 
        and returns the list of the successfully deleted files

        Parameters
        ----------
        keys: list[str] 
            The list of keys (cloud path) to delete.
        """

        if keys:
            batched_keys = []
            for batch_start in range(0, len(keys), 1000):
                batched_keys.append(keys[batch_start:batch_start+1000])

            deleted_counter = 0
            print(f"AWSS3 >> Deleting {len(keys)} keys.")
            for batch in tqdm(batched_keys):
                response = self.s3_client.delete_objects(Bucket=self.bucket_name, 
                                                    Delete={'Objects': [{'Key': key} for key in batch]})
                deleted_counter += len(response.get('Deleted', []))

            if deleted_counter == len(keys):
                print(f"AWSS3 >> All {deleted_counter} keys were successfully deleted.")
            else:
                print(f"AWSS3 >> Only {deleted_counter} out of {len(keys)} keys were successfully deleted.")

            return None
        
        else:
            print(f"AWSS3 >> No keys were deleted as the input was empty.")

        return None        


    def edit_key(self, original_key: str, new_key: str, replace: bool = True):
        """
        Moves, renames, or copy-pastes a key, by editing its cloud path.

        Parameters
        ----------
        original_key : str
            The key to edit.
        new_key: str
            The new key name or path.
        replace: str, True
            If ``False`` the ``original_key`` is kept, hence resulting in a copy-paste action.
            Otherwise, is similar to either a cut-paste or rename acion. 

        Notes
        -----
        - Either way, the key is copied to a ``new_key`` cloud path. If ``replace`` is set to ``True``, the old
        version of the key is deleted after.
        """
        
        if not(self.ensure_key(key=original_key)):
            print(f"AWSS3 >> Original key {original_key} does not exist.")
            
        elif self.ensure_key(key=new_key):
            print(f"AWSS3 >> New key {new_key} already exist.")
            
        else:
            self.s3_client.copy(CopySource={'Bucket' : self.bucket_name,'Key': original_key}, 
                                Bucket=self.bucket_name, 
                                Key=new_key)
            if replace: self.s3_client.delete_object(Bucket=self.bucket_name, Key=original_key)

        return None

   
    def edit_keys(self, original_keys: list[str], new_keys: list[str]):
        """
        Cut from list of the keys_original file to the list of keys_cut
        and displays the list of the successfully cut files.

        # TODO: parallelize the process
        """

        for original_key, new_key in zip(original_keys, new_keys):
            self.cut_key(original_key, new_key)


    def upload_file(self, key: str, path: str):
        """
        Uploads a local file (path) into its cloud location (key).
        """
        
        if not os.path.exists(path):
            raise ValueError("AWSS3 >> Local file path does not exist. Check the path argument.")

        self.s3_client.upload_file(Bucket=self.bucket_name, 
                                   Key=key,
                                   Filename=path)
        
        return None


    def upload_files(self, keys: list, paths: list, content_types: list = None) -> None:
        """
        Uploads multiple files to S3 with specified content types.

        Args:
            keys (list): List of S3 keys (destination paths)
            paths (list): List of local file paths to upload
            content_types (list): List of content types for each file

        Returns:
            None
        """

        if content_types is None:
            content_types = ['application/octet-stream']*len(paths)

        if len(keys) != len(paths) or len(keys) != len(content_types):
            raise ValueError("The lists of keys, paths, and content_types must have the same length")

        for i, (key, path, content_type) in enumerate(zip(keys, paths, content_types)):
            try:
                # Upload with ExtraArgs to specify content type
                self.s3_client.upload_file(
                    Filename=path,
                    Bucket=self.bucket_name,
                    Key=key,
                    ExtraArgs={'ContentType': content_type}
                )

                # Optional: Print progress for large uploads
                if (i + 1) % 10 == 0 or i == len(keys) - 1:
                    print(f"AWSS3 >> Uploaded {i + 1}/{len(keys)} files")

            except Exception as e:
                print(f"AWSS3 >> Error uploading {path} to {key}: {str(e)}")
    

    def upload_directory(self, local_dir: str, s3_prefix: str = "") -> None:
        """
        Uploads an entire local directory to AWS S3, preserving the directory structure
        and setting appropriate content types based on file extensions.

        Args:
            local_dir (str): Path to the local directory to upload
            s3_prefix (str, optional): Prefix to add to all S3 keys (like a folder path in S3). Defaults to "".

        Returns:
            None
        """

        # Ensure local_dir has a trailing slash for consistency
        local_dir = os.path.normpath(local_dir)

        # Collect all files to upload
        all_files = []
        all_keys = []
        all_content_types = []


        # Walk through the directory
        for root, _, files in os.walk(local_dir):
            for file in files:
                # Get the full local path
                local_path = os.path.join(root, file)

                # Calculate the S3 key by preserving directory structure
                # Remove the base directory and convert to S3 path format
                rel_path = os.path.relpath(local_path, local_dir)
                s3_key = os.path.join(s3_prefix, rel_path).replace("\\", "/")

                # Determine content type based on file extension
                content_type, _ = mimetypes.guess_type(local_path)
                if content_type is None:
                    # Default to binary if we can't determine the type
                    content_type = 'application/octet-stream'

                all_files.append(local_path)
                all_keys.append(s3_key)
                all_content_types.append(content_type)

        # Upload files with their respective content types
        print(f"AWSS3 >> Found {len(all_files)} files to upload from '{local_dir}'.")

        # Modified to handle content types
        self.upload_files_with_content_types(keys=all_keys, paths=all_files, content_types=all_content_types)

        print(f"AWSS3 >> Directory upload complete")
