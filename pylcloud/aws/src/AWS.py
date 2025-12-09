import boto3
import os, sys
import shutil
from typing import Optional, Union, List, Dict
from abc import ABC


class AWS(ABC):
    """
    Low-level client to connect to the AWS API.
    """

    def __init__(self) -> None:
        """
        Helper to initiates a connection to an AWS service. This is a low-level method, and should not be used directly.
        """
        super().__init__()

        return None

    def _create_client(
        self,
        aws_service_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        aws_region_name: Optional[str] = "eu-west-1",
        **kwargs,
    ):
        """
        Initiates a connection to an AWS service. This is a low-level method, and should be used as part of a boto3 function.

        Parameters
        ----------
        aws_service_name: str
            The name of the AWS service to connect to.
        aws_access_key_id: str, None
            The user's private key ID to use to connect to the AWS services.
        aws_secret_access_key: str, None
            The user's private key to use to connect to the AWS services.
        aws_session_token: str, None
            The session token to use, if needed.
        aws_region_name: str, 'eu-west-1'
            The AWS ressources region name.

        Returns
        -------
        aws_client: boto3.MODULE
            The requested service relevant boto3 module, with an already setup boto3 session.
        """

        aws_client = boto3.client(
            service_name=aws_service_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=aws_region_name,
        )

        return aws_client

    def _empty_folder(
        self, path: str, include: str | list[str] = [], exclude: str | list[str] = []
    ):
        """
        Empties a folder from all its content, files and folders.

        Parameters
        ----------
        path: str
            The path to the folder to empty.
        include: str or list[str], []
            A list of extensions or files to include to the deletion list. Extensions must be of format '.ext'. If empty,
            will try to delete any file but those whose extensions are excluded (cf. ``exclude``).
        exclude: str or list[str], []
            A list of extensions of files to exclude from deletion. Will only be used if include is empty.
        """

        if isinstance(include, str):
            include = [include]
        if isinstance(exclude, str):
            exclude = [exclude]

        # Corrects the missing '.' in from of the extension
        for extension in include:
            if not extension.startswith("."):
                # In the case extension is actually a file, the original include pattern is kept, and the .include pattern is added
                include.append(f".{extension}")
        for extension in exclude:
            if not extension.startswith("."):
                # In the case extension is actually a file, the original include pattern is kept, and the .include pattern is added
                exclude.append(f".{extension}")

        if os.path.exists(path):
            for filename in os.listdir(path):
                file_path = os.path.join(path, filename)
                file_extension = os.path.splitext(filename)[-1]
                if file_extension in exclude or filename in exclude:
                    None
                elif (os.path.splitext(filename)[-1] in include) or (include == []):
                    try:
                        # Check if it is a file and delete it
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                            os.unlink(file_path)
                        # Check if it is a directory and delete it and its contents
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                    except Exception as e:
                        print(f"Pipeline >> Failed to delete {file_path}: {e}")

        return True
