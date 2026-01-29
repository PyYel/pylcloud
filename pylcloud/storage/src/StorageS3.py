from typing import Union, Optional, List, Dict
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import mimetypes
import tempfile

from .Storage import Storage
from pylcloud import _config_logger


class StorageS3(Storage):
    """
    Standard S3 class with all purpose methods.
    """

    def __init__(
        self,
        bucket_name: str = "",
        aws_access_key_id: str = "",
        aws_secret_access_key: str = "",
        aws_region_name: str = "eu-west-1",
    ) -> None:
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

        Notes
        -----
        - An instance of this class can connect to only one bucket.
        Create other instances to connect to multiple buckets at once.
        - The ``/temp`` folder should be a folder that can be completely erased.
        - The `key` kwarg usually refers to a cloud path.
        - The `path` kwarg usually refers to a local path.

        Exemple
        -------
        >>> bucket_api = StorageS3(KEY_ID, KEY, 'eu-west-1', 'my_bucket_name', 'C:/Users/Me/myapp/output_folder')
        """
        super().__init__(bucket_name=bucket_name)

        self.logger = _config_logger(logs_name="StorageS3")

        self.s3_client = boto3.client(
            service_name="s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region_name,
        )

        return None

    def create_bucket(self):
        """
        Creates the chosen bucket if it does not exist.
        """
        raise NotImplementedError

    def ensure_files(self, keys: Union[str, list[str]]):
        """
        Checks if the cloud keys exist and returns a dictionnary of the results,
        where keys are the cloud keys, and the values are booleans.

        Parameters
        ----------
        keys: str | list[str]
            The keys to check on the cloud.

        Returns
        -------
        results: dict[str, bool]
            A dictionnary (dict{key: bool}) of the boolean response for each key.
        """

        def _ensure_key(key: str):
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=key
            )
            if "Contents" in response:
                return key, True
            else:
                return key, False

        if isinstance(keys, str):
            return _ensure_key(key=keys)

        self.logger.info(f"Verifying the existence of {len(keys)} keys on the cloud.")
        results = {}
        failures = 0
        with tqdm(total=len(keys)) as pbar:
            with ThreadPoolExecutor(max_workers=2 * os.cpu_count()) as executor:  # type: ignore
                futures = []
                for key in keys:
                    futures.append(executor.submit(_ensure_key, key=key))
                for future in as_completed(futures):
                    try:
                        result_key, result = future.result()
                        results[result_key] = result
                    except Exception as e:
                        failures += 1
                    pbar.update(1)

        if failures:
            self.logger.error(f"Failed to verify the existence of {failures}/{len(keys)} keys.")

        return results

    def download_files(
        self,
        keys: Union[str, list[str]],
        paths: Optional[Union[str, list[str]]] = None,
        display: bool = False,
    ):
        """
        Downloads a list of files into the path folder.

        Parameters
        ----------
        keys: str | list[str]
            The list of cloud paths of each file
        path: str | list[str], None
            The local path to save the dowload the data to. If None is specified, the data is
            saved into the default temp folder
        """

        def _download_file(key: str, path: str):
            self.s3_client.download_file(
                Bucket=self.bucket_name, Key=key, Filename=path
            )
            return None

        keys, paths = self._check_args(keys=keys, paths=paths)

        if not paths:
            path = self.tmp_dir
            paths = [
                os.path.join(path, f"{idx}{os.path.splitext(key)[-1]}")
                for idx, key in enumerate(keys)
            ]
        else:
            if not all([os.path.exists(os.path.dirname(path)) for path in paths]):
                self.logger.info("Download path do not exist. Download aborted.")
                return False

        failed_files = []
        self.logger.info(f"Downloading {len(keys)} files.")
        with tqdm(total=len(keys), disable=display) as pbar:
            with ThreadPoolExecutor(max_workers=2 * os.cpu_count()) as executor:  # type: ignore
                futures = []
                for key, path in zip(keys, paths):
                    futures.append(executor.submit(_download_file, key=key, path=path))
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        failed_files.append(future)
                    pbar.update(1)
        if failed_files:
            self.logger.error(f"An error was raised (only one is shown if many): \n {str(e)}") # type: ignore
            self.logger.error(f"Error downloading {len(failed_files)}/{len(keys)} files. See error above.")

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
            if not os.path.exists(path):
                os.mkdir(path)

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_total = 0
        for result in paginator.paginate(Bucket=self.bucket_name, Prefix=key):
            page_total += 1

        file_idx = 0
        failed_files = []
        self.logger.info(f"Downloading batch of content from '{key}'")  #: {page}/{page_total}")
        with tqdm(total=page_total) as pbar:
            for result in paginator.paginate(Bucket=self.bucket_name, Prefix=key):
                if result.get("Contents") is not None:
                    with ThreadPoolExecutor(max_workers=2 * os.cpu_count()) as executor:  # type: ignore
                        futures = []
                        for obj in result["Contents"]:
                            key = obj["Key"]
                            dirpath = os.path.join(
                                os.path.join(path),
                                f"{os.path.basename(os.path.dirname(key))}",
                            )
                            if not os.path.exists(dirpath):
                                os.mkdir(dirpath)
                            filename = os.path.basename(key)
                            futures.append(
                                executor.submit(
                                    self.s3_client.download_file,
                                    Bucket=self.bucket_name,
                                    Key=key,
                                    Filename=os.path.join(dirpath, filename),
                                )
                            )
                            file_idx += 1

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        failed_files.append(future)
                    pbar.update(1)

        if failed_files:
            self.logger.info("An error was raised (only one is shown if many): \n", e)  # type: ignore
            print(
                f"StorageS3 >> Error downloading {len(failed_files)} files. See error above."
            )

        return None

    def list_files(self, prefix: str = ""):
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

        if not self.ensure_files(keys=prefix):
            raise ValueError(
                f"StorageS3 >> File listing prefix '{prefix}' does not exist"
            )

        paginator = self.s3_client.get_paginator("list_objects_v2")
        file_list = []
        self.logger.info("Listing files in {self.bucket_name}/{prefix}")
        for result in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            if result.get("Contents") is not None:
                for obj in result["Contents"]:
                    file_list.append(obj["Key"])

        return file_list

    def delete_files(self, keys: Union[str, list[str]]):
        """
        Deletes from the bucket the files specified in keys,
        and returns the list of the successfully deleted files

        Parameters
        ----------
        keys: list[str]
            The list of keys (cloud path) to delete.
        """

        if isinstance(keys, str):
            if self.ensure_files(keys=keys):
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=keys)
                self.logger.info("Key deleted successfully.")
            else:
                self.logger.info(f"Key {keys} not found.")
            return None

        if keys != []:
            batched_keys = []
            for batch_start in range(0, len(keys), 1000):
                batched_keys.append(keys[batch_start : batch_start + 1000])

            deleted_counter = 0
            self.logger.info(f"Deleting {len(keys)} keys.")
            for batch in tqdm(batched_keys):
                response = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={"Objects": [{"Key": key} for key in batch]},
                )
                deleted_counter += len(response.get("Deleted", []))

            if deleted_counter == len(keys):
                self.logger.info(f"All {deleted_counter} keys were successfully deleted.")
            else:
                self.logger.warning(f"Only {deleted_counter} out of {len(keys)} keys were successfully deleted.")

            return None

        else:
            self.logger.info("No keys were deleted as the input was empty.")

        return None

    def edit_file(self, original_key: str, new_key: str, replace: bool = True):
        """
        Moves, renames, or copy-pastes a key, by editing its cloud path.

        TODO: parallelize

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

        if not (self.ensure_keys(keys=original_key)):
            self.logger.info("Original key {original_key} does not exist.")

        elif self.ensure_keys(keys=new_key):
            self.logger.info("New key {new_key} already exist.")

        else:
            self.s3_client.copy(
                CopySource={"Bucket": self.bucket_name, "Key": original_key},
                Bucket=self.bucket_name,
                Key=new_key,
            )
            if replace:
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=original_key)

        return None

    def upload_files(
        self,
        keys: Union[str, list[str]],
        paths: Union[str, list[str]],
        content_types: Optional[list] = None,
        display: bool = True,
    ) -> None:
        """
        Uploads multiple files to S3 with specified content types using parallel processing.

        Args:
            keys (list): list of S3 keys (destination paths)
            paths (list): list of local file paths to upload
            content_types (list): list of content types for each file

        Returns:
            None
        """

        def _upload_file(key: str, path: str, content_type: str):
            """
            Uploads a local file (path) into its cloud location (key).
            """
            if not os.path.exists(path):
                print(
                    f"StorageS3 >> Local file '{path}' does not exist. Check the path argument."
                )
                return False

            try:
                self.s3_client.upload_file(
                    Bucket=self.bucket_name,
                    Key=key,
                    Filename=path,
                    ExtraArgs={"ContentType": content_type},
                )
                return True
            except Exception as e:
                self.logger.info("Error uploading {path} to {key}: {str(e)}")
                return False

        if content_types is None:
            content_types = ["application/octet-stream"] * len(paths)

        if len(keys) != len(paths) or len(keys) != len(content_types):
            raise ValueError(
                f"StorageS3 >> The lists of keys, paths, and content_types must have the same length."
            )

        upload_tasks = list(zip(keys, paths, content_types))
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:

            future_to_idx = {
                executor.submit(_upload_file, key, path, content_type): i
                for i, (key, path, content_type) in enumerate(upload_tasks)
            }

            with tqdm(total=len(upload_tasks), disable=not display) as progress_bar:
                for future in as_completed(future_to_idx):
                    progress_bar.update(1)

        return None

    def upload_directory(self, path: str, prefix: str = "") -> None:
        """
        Uploads an entire local directory to AWS S3, preserving the directory structure
        and setting appropriate content types based on file extensions.

        Args:
            local_dir (str): Path to the local directory to upload
            s3_prefix (str, optional): Prefix to add to all S3 keys (like a folder path in S3). Defaults to "".

        Returns:
            None
        """

        path = os.path.normpath(path)

        all_files = []
        all_keys = []
        all_content_types = []
        for root, _, files in os.walk(path):
            for file in files:
                # Get the full local path
                local_path = os.path.join(root, file)

                # Calculate the S3 key by preserving directory structure
                # Remove the base directory and convert to S3 path format
                rel_path = os.path.relpath(path, path)
                s3_key = os.path.join(prefix, rel_path).replace("\\", "/")

                # Determine content type based on file extension
                content_type, _ = mimetypes.guess_type(path)
                if content_type is None:
                    # Default to binary if we can't determine the type
                    content_type = "application/octet-stream"

                all_files.append(local_path)
                all_keys.append(s3_key)
                all_content_types.append(content_type)

        self.logger.info("Found {len(all_files)} files to upload from '{path}'.")
        self.upload_files(
            keys=all_keys, paths=all_files, content_types=all_content_types
        )
        self.logger.info("Directory upload complete")

        return None

    def upload_urls(
        self, keys: Union[str, list[str]], content_types: Optional[List] = None
    ) -> list[str]:
        """
        Requests list of presigned URL for direct file upload.
        """

        if isinstance(keys, str):
            keys = [keys]

        try:

            if content_types is None:
                content_types = ["application/octet-stream"] * len(keys)

            urls = [
                self.s3_client.generate_presigned_url(
                    ClientMethod="put_object",
                    Params={
                        "Bucket": self.bucket_name,
                        "Key": key,
                        "ContentType": content_type,
                    },
                    ExpiresIn=3600,
                )
                for key, content_type in zip(keys, content_types)
            ]

            return urls

        except Exception as e:
            self.logger.info("Error generating presigned urls: {str(e)}")
            return []

    def download_urls(self, keys: Union[str, List[str]]) -> List[str]:
        """
        Generate presigned URLs for downloading files from a private S3 bucket.
        """

        if isinstance(keys, str):
            keys = [keys]

        try:
            urls = [
                self.s3_client.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={
                        "Bucket": self.bucket_name,
                        "Key": key,
                    },
                    ExpiresIn=3600,
                )
                for key in keys
            ]

            return urls

        except Exception as e:
            self.logger.info("Error generating presigned urls: {str(e)}")
            return []
