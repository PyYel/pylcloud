from typing import Union, Optional
import os


from .Storage import Storage


class StorageServer(Storage):
    """
    Standard S3 class with all purpose methods.
    """
    def __init__(self, 
                 bucket_name: str,
                 endpoint: str = "http://localhost:8000",
                 tmp_dir: Optional[str] = None) -> None:
        """
        Initiates a connection to a custom storage server.

        Parameters
        ----------
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
