from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from typing import List, Optional, Union, Dict
import os
import shutil
import uvicorn
import re
from pydantic import BaseModel

from .Storage import Storage


# Common all class response
class StorageResponse(BaseModel):
    success: bool
    message: str
    data: dict = {}


class StorageServer(Storage):
    def __init__(self, storage_root: str = "./storage", bucket_name: str = "default"):
        """
        Creates a local file storage and starts a server to receive and distribute files.
        """

        self.storage_root = os.path.abspath(storage_root)
        self.bucket_name = bucket_name
        self.bucket_path = os.path.join(storage_root, bucket_name)

        os.makedirs(self.bucket_path, exist_ok=True)

        self.app = FastAPI()
        self._setup_routes()

        return None

    def _resolve_path(self, key: str) -> str:
        """
        Treats a key as a local path from the server root, and creates the required file structure.
        """
        resolved_path = os.path.join(self.bucket_path, key.replace("/", "\\"))
        os.makedirs(os.path.dirname(resolved_path), exist_ok=True)

        return resolved_path

    def _setup_routes(self):
        @self.app.post("/upload")
        async def upload(file: UploadFile = File(...), key: str = ""):
            """POST (uploads) a file to a bucket. Bucket name is infered from key."""
            try:
                path = self._resolve_path(key)
                with open(path, "wb") as buffer:
                    shutil.copyfileobj(file.file, buffer)
                return StorageResponse(
                    success=True, message="File uploaded successfully.", data={}
                )
            except Exception as e:
                return StorageResponse(
                    success=False, message=f"Failed to upload file: {e}", data={}
                )

        @self.app.get("/download")
        async def download(key: str):
            try:
                path = self._resolve_path(key)
                if not os.path.exists(path):
                    return StorageResponse(
                        success=False, message=f"File does not exist.", data={}
                    )
                return FileResponse(path, filename=os.path.basename(path))
            except Exception as e:
                return StorageResponse(
                    success=False, message=f"Failed to download file: {e}", data={}
                )

        @self.app.get("/buckets")
        async def list_buckets():
            buckets = []
            for root, dirname, filenames in os.walk(self.storage_root):
                buckets.append(dirname)
            return StorageResponse(
                success=True,
                message=f"Buckets name list fetched.",
                data={"buckets": buckets},
            )

        @self.app.get("/files")
        async def list_bucket_files(bucket_name: str):
            bucket_files = []
            for root, _, filenames in os.walk(
                os.path.join(self.storage_root), bucket_name
            ):
                for f in filenames:
                    full_path = os.path.join(root, f)
                    rel_path = os.path.relpath(full_path, self.storage_root)
                    bucket_files.append(rel_path)
            return StorageResponse(
                success=True,
                message=f"Files name list fetched.",
                data={"files": bucket_files},
            )

    def start(self, host="0.0.0.0", port=5001):
        uvicorn.run(self.app, host=host, port=port)

    def stop(self):
        print("Server shutdown not implemented (use CTRL+C or manage via subprocess).")
