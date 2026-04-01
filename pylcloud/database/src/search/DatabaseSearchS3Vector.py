from abc import ABCMeta
import os, sys
import boto3
import os
import json
from typing import Optional, Union, List, Dict
from urllib.parse import urlparse, parse_qs

from .DatabaseSearch import DatabaseSearch
from pylcloud import _config_logger # type: ignore


class DatabaseSearchS3Vector(DatabaseSearch):
    """
    AWS proprietary S3 Vector helper. 
    A bucket acts as a cluster; an index acts as a root prefix.
    """

    def __init__(
        self,
        bucket_name: str,
        AWS_REGION_NAME: str = os.getenv("AWS_REGION_NAME", "eu-west-1"),
        AWS_ACCESS_KEY_ID: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID", None),
        AWS_ACCESS_KEY_SECRET: Optional[str] = os.getenv("AWS_ACCESS_KEY_SECRET", None),
    ):
        super().__init__()
        self.logger = _config_logger(logs_name="DatabaseSearchS3Vector")
        
        self._creds = {
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_ACCESS_KEY_SECRET,
            "region_name": AWS_REGION_NAME
        }
        self.s3_vectors: Optional[boto3.Session] = None
        self.bucket_name = bucket_name

        self.connect_database(host=bucket_name)

        return None


    def connect_database(
        self, 
        host: str = "my-bucket", 
        user: str = "", 
        password: str = ""
    ):
        """
        Parses the 'host' parameter. Can be a simple bucket name or a full 
        AWS Console URL.

        Parameters
        ----------
        host: str
            - The bucket name
            - Or the bucket URL

        Returns
        -------
        s3_vectors: boto3.client
            The boto3 S3 Vectors client.
        """

        if "console.aws.amazon.com" in host:
            parsed = urlparse(host)
            # Extract bucket from path /s3/buckets/BUCKET_NAME
            path_parts = parsed.path.split('/')
            if "buckets" in path_parts:
                self.bucket_name = path_parts[path_parts.index("buckets") + 1]
            
            # Extract region from query params
            query_params = parse_qs(parsed.query)
            if 'region' in query_params:
                self._creds["region_name"] = query_params['region'][0]
        else:
            # Case 2: Host is just the bucket name
            self.bucket_name = host

        self.s3_vectors = boto3.client(service_name="s3vectors", **self._creds)
        
        self.logger.info(f"Connected to S3 Vector Bucket: {self.bucket_name} in {self._creds['region_name']}")
        return self.s3_vectors

    def disconnect_database(self):
        """Resets the client state."""
        self.s3_vectors = None
        self.bucket_name = None
        return None


    def create_index(
        self,
        index_name: str,
        mappings: Optional[Union[dict, str]] = None,
        settings: Optional[Union[dict, str]] = None,
        shards: int = 1,
        replicas: int = 1,
    ):
        """
        Creates index. 'mappings' can be a dict or a path to a json file.
        Expects: {"dimensions": int, "metric": "cosine"|"euclidean"}
        """
        # Handle JSON file path vs Dictionary
        config = mappings
        if isinstance(mappings, str) and mappings.endswith('.json'):
            with open(mappings, 'r') as f:
                config = json.load(f)
        
        config = config or {}
        dimensions = config.get("dimensions", 1536) # Default to Titan/Ada dims
        metric = config.get("metric", "cosine")

        self.logger.info(f"Creating S3 Vector Index: {index_name} (dims: {dimensions})")
        
        return self.s3_vectors.create_index(
            bucketName=self.bucket_name,
            indexName=index_name,
            config={
                "dimensions": dimensions,
                "metric": metric
            }
        )
    def drop_index(self, index_name: str):
        return self.s3_vectors.delete_index(
            bucketName=self.bucket_name,
            indexName=index_name
        )
    
    def describe_database(self, *args, **kwargs):
        return super().describe_database(*args, **kwargs)
    
    def send_data(self, index_name: str, data_id: str, vector: List[float], metadata: dict):
        """
        Injects a vector and its metadata into the S3 Vector Index.
        """
        return self.s3_vectors.put_vectors(
            bucketName=self.bucket_name,
            indexName=index_name,
            vectors=[{
                "id": data_id,
                "vector": vector,
                "metadata": metadata
            }]
        )
    
    def delete_data(self, *args, **kwargs):
        return super().delete_data(*args, **kwargs)
    
    def update_data(self, *args, **kwargs):
        return super().update_data(*args, **kwargs)
    
    def query_data(self, *args, **kwargs):
        return super().query_data(*args, **kwargs)

    def similarity_search(
        self,
        index_name: str,
        vector_query: List[float],
        vector_field: str = "vector",
        must_pairs: List[Dict[str, str]] = [],
        should_pairs: List[Dict[str, str]] = [],
        k: int = 5,
    ):
        """
        Performs ANN search using S3 Vector APIs.
        Supports metadata filtering via must_pairs.
        """
        # Format filters for S3 Vector syntax
        filters = {pair["key"]: pair["value"] for pair in must_pairs}
        
        response = self.s3_vectors.search_vectors(
            bucketName=self.bucket_name,
            indexName=index_name,
            vector=vector_query,
            topK=k,
            filter=filters
        )
        return response.get("hits", [])
