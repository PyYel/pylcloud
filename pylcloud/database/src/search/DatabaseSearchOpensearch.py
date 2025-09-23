import os, sys
from typing import Union, Optional, Sequence, Any
from opensearchpy import OpenSearch, helpers, NotFoundError, RequestsHttpConnection, RequestError
import json
import urllib3
import warnings
import boto3
from requests_aws4auth import AWS4Auth

# Removes unverified HTTPS SSL traffic warnings
warnings.filterwarnings('ignore', 'Connecting to .+ using TLS with verify_certs=False is insecure')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from .DatabaseSearch import DatabaseSearch


class DatabaseSearchOpensearch(DatabaseSearch):
    """
    OpenSearch Python API helper.
    Compatible with both open-source OpenSearch and AWS OpenSearch Service.
    """
    def __init__(self, 
                 host: str = "https://localhost:9200", 
                 user: str = "admin", 
                 password: str = "password",
                 aws_region: Optional[str] = None):
        """
        Initializes a connection to an OpenSearch cluster.

        Parameters
        ----------
        host: str
            The OpenSearch database address and port.
        user: str
            The name of the user to connect with (for basic auth).
        password: str
            The selected user credentials (for basic auth).
        aws_region: str, optional
            If provided, uses AWS IAM authentication instead of basic auth.

        Notes
        -----
        - If we were to compare OpenSearch and SQL naming:
            - A cluster is a database or schema.
            - An index is a table (similar to a MongoDB collection).
            - Documents are records.
            - Fields are similar to columns (although they may be nested). 
        """
        super().__init__(logs_name="DatabaseOpenSearch")

        self.host = host
        self.user = user
        self.password = password
        self.aws_region = aws_region
        self.is_aws = aws_region is not None

        try:
            self.connect_database(host=host, user=user, password=password, aws_region=aws_region)
        except Exception as e:
            self.logger.critical(f"An error occurred when connecting to '{host}': {e}")

        return None


    def connect_database(self, 
                         host: str = "https://localhost:9200",
                         user: str = "admin",
                         password: str = "password",
                         aws_region: Optional[str] = None):
        """
        Connects to the database and creates a connector object ``os``. 

        Parameters
        ----------
        host: str
            The OpenSearch database address and port.
        user: str
            The name of the user to connect with (for basic auth).
        password: str
            The selected user credentials (for basic auth).
        aws_region: str, optional
            If provided, uses AWS IAM authentication instead of basic auth.
        """
        if aws_region:
            # AWS OpenSearch Service with IAM authentication
            credentials = boto3.Session().get_credentials()
            awsauth = AWS4Auth(
                credentials.access_key,
                credentials.secret_key,
                aws_region,
                'es',
                session_token=credentials.token
            )

            self.api_os = OpenSearch(
                hosts=[host],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection
            )
            self.logger.info(f"Connected to AWS OpenSearch Service at {host} using IAM authentication")
        else:
            # Standard OpenSearch with basic auth
            self.api_os = OpenSearch(
                hosts=[host],
                http_auth=(user, password),
                use_ssl=True if host.startswith("https") else False,
                verify_certs=False
            )
            self.logger.info(f"Connected to OpenSearch at {host} using basic authentication")

        return self.api_os


    def create_table(self, *args, **kwargs):
        """See ``create_index()``."""
        self.logger.warning("Tables do not exist in NoSQL. Create an index instead.")
        return self.create_index(*args, **kwargs)

    def create_index(self, 
                     index_name: str, 
                     properties: Optional[dict[str, str]] = None, 
                     mapping_file: Optional[str] = None,
                     shards: int = 1, 
                     replicas: int = 1):
        """
        Creates an index, with either inline properties or from a JSON mapping file.

        Parameters
        ----------
        index_name: str
            The name of the index to create.
        properties: dict[str], optional
            The index properties. Format: {'field': {'type': 'dtype'}}.
        mapping_file: str, optional
            Path to a JSON file containing the full OpenSearch mapping.
        shards: int, default 1
            Number of shards.
        replicas: int, default 1
            Number of replicas.

        Notes
        -----
        If both 'properties' and 'mapping_file' are provided, the JSON file will be used.
        """

        if ' ' in index_name:
            self.logger.info(f"Index name can't contain blank spaces. Index name changed to '{index_name.replace(' ', '-')}'.")
            index_name = index_name.replace(' ', '-')

        if mapping_file:
            try:
                with open(mapping_file, 'r') as f:
                    settings = json.load(f)
                self.logger.info(f"Mapping loaded from '{mapping_file}'.")
            except Exception as e:
                self.logger.error(f"Failed to load mapping file: {e}")
                return None
        else:
            if not properties:
                self.logger.error("You must provide either 'properties' or a valid 'mapping_file'.")
                return None

            settings = {
                "settings": {
                    "number_of_shards": shards,
                    "number_of_replicas": replicas,
                },
                "mappings": {
                    "properties": properties
                },
            }

        if not self.api_os.indices.exists(index=index_name):
            self.api_os.indices.create(index=index_name, body=settings)
            self.logger.info(f"Index '{index_name}' created.")
        else:
            self.logger.info(f"Index '{index_name}' already exists.")


    def disconnect_database(self):
        """
        Closes the database linked to the connector ``os``.
        """
        # OpenSearch client doesn't require explicit disconnection
        pass


    def drop_database(self, database_name: str):
        """
        Drops all the indexes from a cluster.
        """
        self.logger.warning("Can't drop an OpenSearch database. Will drop all the indexes from this cluster instead.")
        raise NotImplementedError


    def drop_table(self, *args, **kwargs):
        """See ``drop_index()``."""
        self.logger.warning("Use drop index instead.")
        return self.drop_index(*args, **kwargs)

    def drop_index(self, index_name: str):
        """
        Deletes an index and all its content.
        """
        try:
            response = self.api_os.indices.delete(index=index_name)
            self.logger.info(f"Index '{index_name}' deleted successfully.")
        except Exception as e:
            self.logger.error(f"Failed to delete index '{index_name}': {e}")

        return None


    def delete_data(self, index_name: str, pairs: dict[str, str] = {}):
        """
        Deletes all the records from an index that match the ``pairs`` conditions.

        Parameters
        ----------
        index: str
            The name of the index to delete data from.
        pairs: dict[str]
            A dictionary of label-value pairs that a record must match to be deleted.
        """

        if pairs == {}:
            query = {"query": {"match_all": {}}}
        else:
            query = {"query": {"match": pairs}}

        try:
            response = self.api_os.delete_by_query(index=index_name, body=query)
            self.logger.debug(response)
        except Exception as e:
            self.logger.error(f"Failed to delete data from '{index_name}': {e}")

        return None


    def list_databases(self, *args, **kwargs):
        """See ``list_clusters()``."""
        self.logger.warning("Use list clusters instead.")
        return self.list_clusters(*args, **kwargs)

    def list_clusters(self):
        """
        List the databases (clusters) present on an OpenSearch server.
        """
        info = self.api_os.info()
        self.logger.info(f"Cluster info: {info}")
        return info


    def list_tables(self, *args, **kwargs):
        """See ``list_indexes()``."""
        self.logger.warning("Use list indexes instead.")
        return self.list_indexes(*args, **kwargs)

    def list_indexes(self, system_db: bool = False):
        """
        Returns a list of the names of the indexes on the connected cluster.

        Parameters
        ----------
        system_db: bool
            Whether to return the built-in system indexes or not.

        Notes
        -----
        - To list the existing databases, see ``list_databases()``.
        """
        try:
            if system_db:
                # built-in system indexes start with a dot
                indexes = [index['index'] for index in self.api_os.cat.indices(params={"format":'json'})]
                self.logger.info(f"Found indexes: {', '.join(indexes)}")
                return indexes
            else:
                indexes = [index['index'] for index in self.api_os.cat.indices(params={"format":'json'}) if not index['index'].startswith(".")]
                self.logger.info(f"Found indexes: {', '.join(indexes)}")
                return indexes
        except Exception as e:
            self.logger.error(f"Error listing indexes: {e}")
            return []


    def send_data(self, index_name: str, documents: list[dict], _ids: Optional[list[str]] = None):
        """
        Sends data to the OpenSearch index. Can handle single files, multiple files, and directories.

        Parameters
        ----------
        index_name: str
            The name of the index to send data to.
        documents: list[dict]
            The payload to inject into the index.
        _ids: list[str], None
            Overwrites auto-generated _id fields for custom indexing. 
        """
        if _ids is None: _ids = [None] * len(documents) # type: ignore

        actions = [
            {
                "_index": index_name,                           # Target indexes
                "_id": _id,                                     # Hashed id for log unicity
                "_source": document                             # Document content
            }
            for document, _id in zip(documents, _ids) # type: ignore
        ]

        self.logger.info(f"Sending {len(actions)} documents into index '{index_name}'.")
        response = helpers.bulk(self.api_os, actions, raise_on_error=False)
        self.logger.debug(response)
        if response[1]:
            self.logger.error(f"Interface error when sending documents: {response}")

        return None


    def query_data(self, 
                index_name: str, 
                must_pairs: list[dict[str, str]] = [], 
                should_pairs: list[dict[str, str]] = []):
        """
        Retrieves data from the OpenSearch DB.

        Parameters
        ----------
        index_name: str
            The name of the index to query.
        must_pairs: list[dict[str]]
            A list of ALL the label-value pairs that a record must match to be selected. Must be OpenSearch keywords fields.
        should_pairs: list[dict[str]]
            A list of AT LEAST ONE label-value pair that a record must match to be selected. Must be OpenSearch keywords fields.

        Returns
        -------
        documents: list[dict[str]]
            The list of fetched documents, as dictionaries nested with the same structure as the one found in OpenSearch.
        """

        must_conditions = []
        for must_pair in must_pairs:
            must_conditions.append({"term": must_pair})

        should_conditions = []
        for should_pair in should_pairs:
            should_conditions.append({"term": should_pair})

        query = {
            "query": {
                "bool": {
                    "must": must_conditions,
                    "should": should_conditions,
                    # At least 1 should condition must be matched. When there is no should condition input, the minimum must be set to zero
                    "minimum_should_match": 1 if should_conditions else 0
                }
            }
        }

        documents = []
        try:
            for doc in helpers.scan(self.api_os, index=index_name, query=query, size=1000):
                documents.append(doc)
            self.logger.debug(f"Field search found {len(documents)} matching documents.")
        except NotFoundError as e:
            e.info
            self.logger.error(f"Index '{e.info['error']['index']}' not found.")
            return []
        except Exception as e:
            self.logger.error(f"An error occurred during semantic search: {e}")
            return []

        return documents
    

    def update_data(self, *args, **kwargs):
        raise NotImplementedError



    def similarity_search(self, index_name: str, 
                          query_vector: list[float] = None,
                          query_text: str = None,
                          must_pairs: list[dict[str, str]] = [], 
                          should_pairs: list[dict[str, str]] = [],
                          vector_field: str = "chunk_vector",
                          text_field: str = "chunk_content",
                          vector_weight: float = 0.7,
                          text_weight: float = 0.3,
                          initial_k: int = 20,
                          final_k: int = 5
                          ) -> list[dict[str, Any]]:
        """
        Performs hybrid search in Opensearch combining vector similarity and text matching.

        Parameters
        ----------
        index_name : str
            The Opensearch index to search in.
        query_vector : list[float], optional
            The vector representation of the input query.
        query_text : str, optional
            The text query for keyword matching.
        must_pairs : list[dict[str, str]], optional
            List of field-value pairs that must match in the query.
        should_pairs : list[dict[str, str]], optional
            List of field-value pairs that should match in the query.
        vector_field : str, optional
            The field containing the vector embeddings (default is "chunk.vector").
        text_field : str, optional
            The field containing the text content (default is "chunk.content").
        vector_weight : float, optional
            Weight for the vector similarity component (default is 0.7).
        text_weight : float, optional
            Weight for the text matching component (default is 0.3).
        initial_k : int, optional
            The wideness of the search, before reranking logic (default is 20).
        final_k: int, optional
            The number of closest matches to return (default is 5).

        Returns
        -------
        documents: list[dict[str, Any]]
            A list of retrieved documents sorted by combined similarity.
        """

        if query_vector is None and query_text is None:
            error_msg = "Either query_vector or query_text must be provided"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        if final_k > initial_k: 
            final_k = initial_k
            self.logger.warning("Hybrid search 'final_k' should be smaller than 'initial_k'.")

        self.logger.debug(f"Starting hybrid search on index '{index_name}' with k={initial_k}")

        # Build must conditions
        must_conditions = []
        for must_pair in must_pairs:
            must_conditions.append({"term": must_pair})

        # Build should conditions
        should_conditions = []
        for should_pair in should_pairs:
            should_conditions.append({"term": should_pair})

        # Create the query structure
        query = {
            "size": initial_k,
            "query": {
                "bool": {
                    "must": must_conditions,
                    "should": should_conditions,
                    "minimum_should_match": 1 if should_conditions else 0,
                }
            }
        }

        # Add vector search if query_vector is provided
        if query_vector is not None:
            if not isinstance(query_vector, list) or not all(isinstance(x, (int, float)) for x in query_vector):
                self.logger.error("Similarity search 'query_vector' must be a list of numbers.")
                return []

            self.logger.debug(f"Adding vector search component with weight {vector_weight}")

            # Add kNN query with weight
            if "should" not in query["query"]["bool"]:
                query["query"]["bool"]["should"] = []
                
            query["query"]["bool"]["should"].append({
                "knn": {
                    vector_field: {
                        "vector": query_vector,
                        "k": initial_k,
                        "boost": vector_weight
                    }
                }
            })

        # Add text search if query_text is provided
        if query_text is not None:
            if not isinstance(query_text, str):
                error_msg = "query_text must be a string"
                self.logger.error(error_msg)
                raise TypeError(error_msg)

            self.logger.debug(f"Adding text search component with weight {text_weight}")

            # Add text query with weight
            if "should" not in query["query"]["bool"]:
                query["query"]["bool"]["should"] = []

            query["query"]["bool"]["should"].append({
                "script_score": {
                    "query": {
                        "match": {
                            text_field: {
                                "query": query_text,
                                "fuzziness": "AUTO"
                            }
                        }
                    },
                    "script": {
                        "source": f"_score * params.weight",
                        "params": {
                            "weight": text_weight
                        }
                    }
                }
            })

        # Ensure we match something if both vector and text components are used
        if query_vector is not None and query_text is not None:
            query["query"]["bool"]["minimum_should_match"] = 1

        try:
            self.logger.debug(f"Executing hybrid search query: {query}")
            response = self.es.search(index=index_name, body=query)
            documents = [hit for hit in response['hits']['hits']]
            self.logger.info(f"Hybrid similarity search found {len(documents)} matching documents, returning top {final_k} scorers.")
            return documents[:final_k]
        
        except ConnectionError as e:
            self.logger.error(f"Connection error during search: {str(e)}")
            return []
        except RequestError as e:
            self.logger.error(f"Invalid query error: {str(e)}")
            return []
        except NotFoundError as e:
            self.logger.error(f"Index '{index_name}' not found: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error during hybrid similarity search: {str(e)}")
            return []
