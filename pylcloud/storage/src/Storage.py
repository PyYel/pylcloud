import os, sys
import hashlib
from pydantic import BaseModel 
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Union
from pathlib import Path
import mimetypes

class Storage(ABC):
    """
    Inetrnal storage services helper.
    """
    def __init__(self, bucket_name: str = "storage", tmp_dir: Optional[str] = None):
        """
        Initializes the helper.
        """
        super().__init__()

        self.bucket_name = bucket_name

        if (tmp_dir is None) or not os.path.exists(tmp_dir):
            self.tmp_dir = os.path.join(os.getcwd(), "tmp")
            os.makedirs(self.tmp_dir, exist_ok=True)
            print(f"Storage >> Temporary folder created: '{self.tmp_dir}")
        else:
            self.tmp_dir = tmp_dir

         # TODO: Add connection certificate

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


    @abstractmethod
    def create_bucket(self):
        """
        Creates the chosen bucket if it does not exist.
        """
        raise NotImplementedError
    

    @abstractmethod
    def upload_files(self, paths: Union[str, List[str]], keys: Optional[Union[str, List[str]]]):
        """
        Uploads files to the remote storage.
        """
        raise NotImplementedError


    @abstractmethod
    def download_files(self, keys: Union[str, List[str]], paths: Optional[Union[str, List[str]]]):
        """
        Downloads files from the remote storage.
        """
        raise NotImplementedError


    @abstractmethod
    def delete_files(self, keys: Union[str, List[str]]):
        """
        Deletes files from the remote storage.
        """
        raise NotImplementedError


    @abstractmethod
    def list_files(self, key: Optional[str]):
        """
        Lists files from remote storage, starting down to the root (or from the key) up to the leaves.
        """
        raise NotImplementedError


    def _check_args(self, keys: Optional[Union[str, List[str]]], paths: Optional[Union[str, List[str]]]):
        """
        
        """
        
        if isinstance(keys, str):
            keys = list(keys)
        if isinstance(paths, str):
            paths = list(paths)
        if not isinstance(keys, list):
            print(f"Storage >> Argument keys is of type {type(keys)} instead of list[str]. Operations will be aborted.")
            return [], []
        if paths is None:
            paths = [os.path.join(self.tmp_dir, path) for path in keys]
        if not all([os.path.exists(os.path.dirname(path)) for path in paths]):
            print("Storage >> Paths do not exist. Operation will be aborted.")
            return [], []
        
        return keys, paths

    def _normalize_paths(self, absolute_paths: Union[str, List[str]]) -> List[str]:
        """
        Normalize a list of file absolute paths by removing platform-specific parts
        like drive letters and home directories, and converting to POSIX-style paths.
        """

        if isinstance(absolute_paths, str):
            absolute_paths = list(absolute_paths)
        
        normalized_paths = []
        home = Path.home()
        for input_path in absolute_paths:
            path = Path(input_path).resolve()

            # Strip drive letter on Windows
            if path.drive:
                path = path.relative_to(path.anchor)

            # Strip user home directory if it's a subpath
            try:
                path = path.relative_to(home)
            except ValueError:
                pass

            normalized_paths.append(str(path).replace("\\", "/"))  # Always return POSIX-style

        return normalized_paths


    def _hash_content(self, content: str, prefixes: List[str] = []):
        """
        Hashes a document content into a unique id of format <prefixes>-<hashed_content>. This is usefull to automatically overwrite 
        a stored document when a document with the same timestamp and content is written into Elasticsearch. 

        Parameters
        ----------
        content: str
            The text content to hash.
        prefix: list[str]
            A list of prefixes (such as metadata, timestamps...) to prefix the hashed content with.

        Returns
        -------
        hashed_id: str
            The unique hashed ID.

        Examples
        --------
        >>> print(_hash_content(content='Message from Caroline: Merry Christmas!', prefixes=['2024/12/25', '103010']))
        >>> '2024/12/25-103010-4432e1c6d1c4f0db2f157d501ae242a7'
        """

        if prefixes:
            return f"{'-'.join(prefixes)}-{hashlib.md5(content.encode()).hexdigest()}"
        else:
            return hashlib.md5(content.encode()).hexdigest()
