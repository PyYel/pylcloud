import os, sys
import boto3.s3
import boto3.s3.inject
import sys

from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

AWS_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(AWS_DIR_PATH))

from aws import AWS


class AWSS3(AWS):
    """
    Standard S3 class with all purpose methods.
    """
    def __init__(self, 
                 aws_access_key_id: str,
                 aws_secret_access_key: str, 
                 aws_region_name: str,
                 bucket_name: str,
                 temp_folder_path: str = None) -> None:
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

        Note
        ----
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
                                                              aws_region_name=aws_region_name)

        # S3Client neeeds a working folder to download/upload files from a unique entry point
        if self.temp_folder_path is None:
            self.temp_folder_path = os.path.join(os.getcwd(), 'temp')
            if not os.path.exists(self.temp_folder_path):
                os.mkdir(self.temp_folder_path)
                print("S3Client >> Temporary folder created:", self.temp_folder_path)


    def ensure_key(self, key: str):
        """
        Checks if the cloud keys exist and returns a tuple of the results

        Parameters
        ----------
        - keys: the keys to check on the cloud 

        Returns:
        - key: the tested key
        - bool: whereas the key exists (True) or not (False) 
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
        - keys: the keys to check on the cloud 

        Returns:
        - dict{key: bool}
        """

        print(f"S3Client >> Verifying the existence of {len(keys)} keys on the cloud")
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
            print(f"S3Client >> Failed to verify the existence of {failures}/{len(keys)} keys")

        return results


    def download_file(self, key: str, file_idx: int = 0, path: str = None):
        """
        Downloads a file into the path folder where file is renamed fileidx_filename.fileextension

        Args:
        - key: the cloud path of the file
        - path: the local path to save the dowload the data to. If None is specified, the data is
        saved into the default temp folder
        - file_idx: the index to prefix the downloaded file with
        """

        if not path:
            path = self.temp_folder_path
            self._empty_folder()

        self.s3_client.download_file(Bucket=self.bucket_name, 
                                  Key=key, 
                                  Filename=path)


    def download_files(self, keys:list[str], paths: list[str] = None):
        """
        Downloads a list of files into the path folder where each file is renamed fileidx_filename.fileextension

        Args:
        - keys: the list of cloud paths of each file
        - path: the local path to save the dowload the data to. If None is specified, the data is
        saved into the default temp folder
        - file_idx: the index to prefix the downloaded file with
        """
        if isinstance(keys, str):
            keys = [keys]
        if not isinstance(keys, list):
            print(f"S3Client >> Argument keys is of type {type(keys)} instead of list[str]. Dowload aborted")
            return False

        if not paths:
            path = self.temp_folder_path
            paths = [os.path.join(path, f"{idx}{os.path.splitext(key)[-1]}") for idx, key in enumerate(keys)]
        else:
            if not all([os.path.exists(os.path.dirname(path)) for path in paths]):
                print("S3DataHive >> Download path do not exist. Download aborted")
                return False

        failed_files = []
        print(f"S3Client >> Downloading {len(keys)} files")
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
            print(f"S3Client >> Error downloading {len(failed_files)}/{len(keys)} files")

        return failed_files



    def download_directory(self, key:str, path:str=None):
        """
        Downloads all the files from a folder into a single output folder where
        each file is renamed fileidx_filename.fileextension.

        Parameters
        ----------
        - key: the cloud path of the folder on the S3 storage
        - path: the local path to save the dowload the data to. If None is specified, the data is
        saved into the default temp folder

        Returns
        -------
        - failed_files: the list of files that were not correctly downloaded

        Example
        -------
        >>> folder = DataHive/Data
        >>> os.listdir(folder)
        ["img1.jpg", "cat.png", "sample.wav"]
        >>> download_directory(key=folder)
        >>> os.listdir(path)
        ["0_img1.jpg", "1_cat.png", "2_sample.wav"]
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
        print(f"S3Client >> Downloading batch of content from '{key}'")#: {page}/{page_total}")
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
                                print(f"S3Client >> Error downloading file: {e}")
                                failed_files.append(future)
                pbar.update(1)

        return failed_files


    def list_keys(self, key:str=""):
        """
        Returns a list of all the files present in a bucket and returns the keys.
        """

        if not self.ensure_key(key=key):
            raise ValueError(f"S3Client >> File listing prefix '{key}' does not exist")

        paginator = self.s3_client.get_paginator('list_objects_v2')
        file_list = []
        print(f"S3Client >> Listing files in {self.bucket_name}/{key}")
        for result in paginator.paginate(Bucket=self.bucket_name, Prefix=key):
            if result.get('Contents') is not None:
                for obj in result['Contents']:
                    file_list.append(obj['Key'])

        return file_list
        


    def make_directory(self, key:str):
        """
        Creates a directory on the cloud.

        Parameters
        ----------
        - key: the folder path to create on the cloud 
        """

        if not self.ensure_key(key=key):
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key)



    def make_directories(self, keys:list[str]):
        """
        Creates directories on the cloud

        Parameters
        ----------
        - keys: the list of folders paths to create on the cloud 
        """


        failed_files = []
        print(f"S3Client >> Creating {len(keys)} folders on the cloud")
        with tqdm(total=len(keys)) as pbar:
            with ThreadPoolExecutor(max_workers=2*os.cpu_count()) as executor:
                futures = []
                for key in keys:
                    futures.append(executor.submit(self.s3_client.put_object, 
                                                    Bucket=self.bucket_name, 
                                                    Key=key))

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"S3Client >> Error making folder: {e}")
                        failed_files.append(future)
                    pbar.update(1)

        return failed_files


             
    def delete_key(self, key:str):
        """
        Deletes a file on the cloud from its key

        Parameters
        ----------
        - key: the path of the file to delete on the cloud
        """

        if self.ensure_key(key=key):
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)



    def delete_keys(self, keys:list[str]):
        """
        Deletes from the bucket the files specified in keys, 
        and returns the list of the successfully deleted files

        Parameters
        ----------
        - keys: the list of keys (cloud path) to delete
        """

        if keys:
            batched_keys = []
            for batch_start in range(0, len(keys), 1000):
                batched_keys.append(keys[batch_start:batch_start+1000])

            deleted_counter = 0
            print(f"S3Client >> Deleting {len(keys)} keys")
            for batch in tqdm(batched_keys):
                response = self.s3_client.delete_objects(Bucket=self.bucket_name, 
                                                    Delete={'Objects': [{'Key': key} for key in batch]})
                deleted_counter += len(response.get('Deleted', []))

            if deleted_counter == len(keys):
                print(f"S3Client >> All {deleted_counter} keys successfully deleted")
            else:
                print(f"S3Client >> Only {deleted_counter} out of {len(keys)} keys successfully deleted ")

            return None
        
        else:
            print(f"S3Client >> No keys were deleted as the input was empty")
            return None

        


    def cut_key(self, key_original:str, key_cut:str):
        """
        Cut from the key_original file to key_cut
        and displays the successfully cut file
        """
        dict_key_original = {
            'Bucket' : self.bucket_name,
            'Key': key_original
        }
        
        if not(self.ensure_key(key=key_original)):
            print(f'file: {key_original} is not exist')
            
        elif self.ensure_key(key=key_cut):
            print(f'file: {key_cut} is already exist')
            
        else:
            self.s3_client.copy(CopySource=dict_key_original, Bucket=self.bucket_name, Key=key_cut)
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key_original)
            print(f'file: {key_cut} was successfully cut from {key_original}')


   
    def cut_list_keys(self, keys_original:list[str], keys_cut:list[str]):
        """
        Cut from list of the keys_original file to the list of keys_cut
        and displays the list of the successfully cut files
        """
        if len(keys_original) != len(keys_cut):
            print("Not all keys are provided")
        
        else:
            for key_original, key_cut in zip(keys_original, keys_cut):
                self.cut_key(key_original, key_cut)



    def copy_key(self, key_original:str, key_copy:str):
        """
        Copy from the key_original file to key_copy
        and displays the successfully copy file
        """
        dict_key_original = {
            'Bucket' : self.bucket_name ,
            'Key': key_original
        }
        
        if not(self.ensure_key(key=key_original)):
            print(f'file: {key_original} is not exist')
            
        elif self.ensure_key(key=key_copy):
            print(f'file: {key_copy} is already exist')
            
        else:
            self.s3_client.copy(CopySource=dict_key_original, Bucket=self.bucket_name, Key=key_copy)
            print(f'file: {key_copy} was successfully copy from {key_original}')


 
    def copy_list_keys(self, keys_original:list[str], keys_copy:list[str]):
        """
        Copy from list of the key_original file to the list of key_copy
        and displays the list of the successfully copy files
        """
        if len(keys_original) != len(keys_copy):
            print("Not all keys are provided")
        
        else:
            for key_original, key_copy in zip(keys_original, keys_copy):
                self.cut_key(key_original, key_copy)


   
    def rename_key(self, key_original:str, new_name:str):
        """
        Renames from the key_original file to new_name
        and displays the successfully renamed file
        """
        dict_key_original = {
            'Bucket' : self.bucket_name,
            'Key': key_original
        }
        new_key = key_original[:-len(key_original.split("/")[-1])] + new_name
        
        if not(self.ensure_key(key=key_original)):
            print(f'file: {key_original} is not exist')
            
        elif self.ensure_key(key=new_key):
            print(f'file: {new_key} is already exist')
            
        else:
            self.s3_client.copy(CopySource=dict_key_original, Bucket=self.bucket_name, Key=new_key)
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key_original)
            print(f'file: {key_original} was successfully renamed to {new_key}')

     
    def rename_list_keys(self, keys_original:list[str], new_names:list[str]):
        """
        Renames from the list of the key_original file to the list of the new_name
        and displays the successfully renamed file
        """
        if len(keys_original) != len(new_names):
            print("Not all keys are provided")
        
        else:
            for key_original, new_name in zip(keys_original, new_names):
                self.cut_key(key_original, new_name)



    def upload_file(self, key:str, path:str):
        """
        Uploads a local file (path) into its cloud location (key)
        """
        
        if not os.path.exists(path):
            raise ValueError("S3Client >> Local file path does not exist. Check the path argument")

        self.s3_client.upload_file(Bucket=self.bucket_name, 
                                Key=key,
                                Filename=path)
        
        return None


    def upload_files(self, keys:list[str], paths:list[str]):
        """
        Uploads a list of local files (paths) into their cloud locations (keys)
        """

        if len(keys) != len(paths):
            raise ValueError(f"S3Client >> Number of keys ({len(keys)}) doesn't match the number of paths ({len(paths)}). Upload aborted.")

        failed_files = []
        print(f"S3Client >> Uploading {len(keys)} files on the cloud")
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
                        print(f"S3Client >> Error uploading file: {e}")
                        failed_files.append(future)
                    pbar.update(1)

        return None

     
    def upload_directory(self, key:str, path:str):
        """
        Uploads local directory to key from bucket
        and displays the successfully uploaded file
        """

        if not key.endswith("/"):
            key += "/"
        if not os.path.exists(path):
            raise ValueError(f"S3Client >> Upload folder '{path}' does not exist")
        file_list = os.listdir(path)

        failed_files = []
        confirm = input(f"S3Client >> Uploading files from folder '{path}' into '{key}'. Confirm [Y/n] ? ")
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
                            print(f"S3Client >> Error uploading file: {e}")
                            failed_files.append(future)
                        pbar.update(1)
        else:
            print("S3Client >> Upload aborted")

        return failed_files


if __name__ == "__main__":
    session = S3Client(aws_access_key_id=AWS_KEY_ID,
                       aws_secret_access_key=AWS_KEY_ACCESS,
                       bucket_name="capeng-dataverse")
    
    
