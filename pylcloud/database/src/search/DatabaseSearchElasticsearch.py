import os, sys
from typing import Union, Optional, Sequence, Any
from elasticsearch import Elasticsearch, helpers, NotFoundError, RequestError
import json
import urllib3
import warnings

# Removes unverified HTTPS SSL traffic warnings
warnings.filterwarnings(
    "ignore", "Connecting to .+ using TLS with verify_certs=False is insecure"
)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from .DatabaseSearch import DatabaseSearch
from pylcloud import _config_logger

class DatabaseSearchElasticsearch(DatabaseSearch):
    """
    Elasticsearch Python API helper.
    """

    def __init__(
        self,
        host: str = "https://localhost:9200",
        user: str = "admin",
        password: str = "password",
    ):
        """
        Initializes a connection to an Elasticsearch cluster.

        Parameters
        ----------
        host: str
            The Elasticsearch database adress and port.
        user: str
            The name of the user to connect with.
        password: str
            The selected user credentials.

        Notes
        -----
        - If we were to compare Elasticsearch and SQL naming:
            - A cluster is a database or schema.
            - An index is a table (similar to a MongoDB collection).
            - Documents are records.
            - Fields are similar to columns (although they may be nested).
        - Similarly to a MySQL server, the PyYel library flattens the connection layers. This means, when connecting
        to an Elasticsearch DB, you are directly connected to the cluster. To change of cluster, you should 'reconnect' to the server.
        In the Elasticsearch context, this is more obvious, as two clusters must always be hosted on different ports.
        """
        super().__init__()

        self.host = host
        self.user = user
        self.password = password

        self.logger = _config_logger(logs_name="DatabaseSearchElasticsearch")

        # TODO: Add connection certificate
        try:
            self.connect_database(host=host, user=user, password=password)
        except Exception as e:
            self.logger.critical(f"An error occured when connecting to '{host}': {e}")

        # Note:
        # The authentication credentials above are required to connect to Elasticsearch.
        # When connecting to Kibana server, custom users credentials should be used. They can be created from within the Kibana server
        # interface, through the elastic superuser account

        return None

    def connect_database(
        self, host: str = "127.0.0.1", user: str = "user", password: str = "password"
    ):
        """
        Connects to the database and creates a connector object ``conn``.
        """
        self.es = Elasticsearch(host, basic_auth=(user, password), verify_certs=False)
        return self.es

    def create_index(
        self,
        index_name: str,
        mappings: Optional[Union[dict, str]] = None,
        settings: Optional[Union[dict, str]] = None,
        shards: int = 1,
        replicas: int = 1,
    ):
        """
        Creates an Elasticsearch index with flexible mapping and settings inputs.

        Parameters
        ----------
        index_name: str
            The name of the index.
        mappings: dict or str, optional
            Either a dictionary of mappings or a path to a JSON mapping file.
        settings: dict or str, optional
            Either a dictionary of settings or a path to a JSON settings file.
        shards: int, default 1
            Used if 'settings' is not provided.
        replicas: int, default 1
            Used if 'settings' is not provided.
        """

        def _load_resource(resource: Any, name: str) -> dict:
            if isinstance(resource, str):
                try:
                    with open(resource, "r") as f:
                        return json.load(f)
                except Exception as e:
                    self.logger.error(f"Failed to load {name} from {resource}: {e}")
                    return {}
            return resource or {}

        # Clean index name
        if " " in index_name:
            index_name = index_name.replace(" ", "-")
            self.logger.info(f"Index name adjusted to '{index_name}'.")

        final_mappings = _load_resource(mappings, "mappings")
        if final_mappings and "properties" not in final_mappings:
            final_mappings = {"properties": final_mappings}

        final_settings = _load_resource(settings, "settings")
        if not final_settings:
            final_settings = {
                "number_of_shards": shards,
                "number_of_replicas": replicas,
            }

        body = {
            "settings": final_settings,
            "mappings": final_mappings
        }

        try:
            if not self.es.indices.exists(index=index_name):
                self.es.indices.create(index=index_name, body=body)
                self.logger.info(f"Index '{index_name}' created successfully.")
            else:
                self.logger.info(f"Index '{index_name}' already exists.")
        except Exception as e:
            self.logger.error(f"Could not create index '{index_name}': {e}")
            

    def disconnect_database(self):
        """
        Closes the database linked to the connector ``conn``.
        """
        pass

    def drop_database(self, database_name: str):
        """
        Drops all the indexes from a cluster.
        """
        self.logger.warning(
            "Can't drop an Elasticsearch. Will drop all the indexes from this cluster instead."
        )
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
            response = self.es.indices.delete(index=index_name)
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
            A dictionnary of label-value pairs that a record must match to be deleted.
        """

        if pairs == {}:
            query = {"query": {"match_all": pairs}}
        else:
            query = {"query": {"match": pairs}}

        try:
            response = self.es.delete_by_query(index=index_name, body=query)
            self.logger.debug(response)
        except Exception as e:
            self.logger.error(f"Failed to delete data from '{index_name}': {e}")

        return None

    def describe_database(self, system_db: bool = False):
        """
        Returns a list of the names of the indexes on the connected cluster.

        Parameters
        ----------
        system_db: bool
            Whereas returning the builtin databases if any, or not.

        Notes
        -----
        - Logs cluster info if supported.
        """
        info = self.es.info()
        self.logger.info(f"Cluster info: {info}")

        if system_db:
            # built-in system indexes start with a dot
            indexes = [index["index"] for index in self.es.cat.indices(format="json")]  # type: ignore
            self.logger.info(f"Found indexes: {', '.join(indexes)}")
            return indexes
        else:
            indexes = [index["index"] for index in self.es.cat.indices(format="json") if not index["index"].startswith(".")]  # type: ignore
            self.logger.info(f"Found indexes: {', '.join(indexes)}")
            return indexes

    def send_data(
        self, index_name: str, documents: list[dict], _ids: Optional[list[str]] = None
    ):
        """
        Sends data to the Elasticsearch index. Can handle single files, multiple files, and directories.

        Parameters
        ----------
        index_name: str
            The name of the index to send data to.
        documents: list[dict]
            The payload to inject into the index.
        _ids: list[str], None
            Overwrites auto-generated _id fileds for custom indexing.

        Notes
        -----
        - For a file_path of format ``path/to/file.ext``, the preprocessed data should be inside ``path/to/file`` eponymous folder/
        """
        if _ids is None:
            _ids = [None] * len(documents)  # type: ignore

        actions = [
            {
                "_index": index_name,  # Target indexes
                "_id": _id,  # Hashed id for log unicity
                "_source": document,  # Document content
            }
            for document, _id in zip(documents, _ids)  # type: ignore
        ]

        self.logger.info(f"Sending {len(actions)} documents into index '{index_name}'.")
        response = helpers.bulk(self.es, actions, raise_on_error=False)
        self.logger.debug(response)
        if response[1]:
            self.logger.error(f"Interface error when sending documents: {response}")

        return None

    def update_data(self, *args, **kwargs):
        raise NotImplementedError

    def query_data(
        self,
        index_name: str,
        must_pairs: list[dict[str, str]] = [],
        should_pairs: list[dict[str, str]] = [],
    ):
        """
        Retrieves data from the Elasticsearch DB.

        Parameters
        ----------
        index_name: str
            The name of the index to query.
        must_pairs: list[dict[str]]
            A list of ALL the label-value pairs that a record must match to be selected. Must be Elasticsearch keywords fields.
        should_pairs: list[dict[str]]
            A list of AT LEAST ONE label-value pair that a record must match to be selected. Must be Elasticsearch keywords fields.

        Returns
        -------
        documents: list[dict[str]]
            The list of fetched document, as dictionnaries nested with the same structure as the one found in Elasticsearch.

        Notes
        -----
        - When performing full text search, field keys must be specified as 'keyword': [{'my_text_field.keyword': 'value'}].

        Examples
        --------
        >>> index_name = "workspaces"
        >>> should_pairs = [{'flat_field': 'at_least_one_must_match_this'}, ...]
        >>> must_pairs = [{'nested_field.key_1': 'all_must_match_this'}, ...]
        # When calling the method, it will return a list of documents with the following structure:
        >>> retreive_from_elastic(...)
        >>> [
                {
                    '_index': '...',
                    '_id':'...',
                    '@timestamp': '...',
                    '_source': {
                        flat_field: '...',
                        nested_field: {
                            key_1: '...',
                            ...
                            }
                        }
                    }
                }, ...
            ]
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
                    "minimum_should_match": 1 if should_conditions else 0,
                }
            }
        }

        documents = []
        try:
            for doc in helpers.scan(self.es, index=index_name, query=query, size=1000):
                documents.append(doc)
            self.logger.debug(
                f"Field search found {len(documents)} matching documents."
            )
        except NotFoundError as e:
            self.logger.error(f"Index '{e.info['error']['index']}' not found.")
            return []
        except Exception as e:
            self.logger.error(f"An error occurred during semantic search: {e}")
            return []

        return documents

    def similarity_search(
        self,
        index_name: str,
        vector_query: Optional[list[float]] = None,
        vector_field: str = "vector",
        vector_weight: float = 0.7,
        text_query: Optional[str] = None,
        text_field: str = "content",
        text_weight: float = 0.3,
        must_pairs: list[dict[str, str]] = [],
        should_pairs: list[dict[str, str]] = [],
        initial_k: int = 20,
        final_k: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Performs hybrid search in Elasticsearch combining vector similarity and text matching.

        Parameters
        ----------
        index_name : str
            The Elasticsearch index to search in.
        vector_query : list[float], optional
            The vector representation of the input query.
        text_query : str, optional
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

        if vector_query is None and text_query is None:
            raise ValueError("Either vector_query or text_query must be provided")

        must_conditions = [{"term": pair} for pair in must_pairs]
        should_conditions = [{"term": pair} for pair in should_pairs]

        search_body = {
            "size": initial_k,
            "query": {
                "bool": {
                    "must": must_conditions,
                    "should": should_conditions,
                    "filter": []
                }
            }
        }

        if vector_query is not None:
            # Native 'knn' top-level parameter for speed (ANN)
            search_body["knn"] = {
                "field": vector_field,
                "query_vector": vector_query,
                "k": initial_k,
                "num_candidates": initial_k * 5,
                "boost": vector_weight
            }

        # Hybrid text search
        if text_query is not None:
            text_match = {
                "match": {
                    text_field: {
                        "query": text_query,
                        "fuzziness": "AUTO",
                        "boost": text_weight
                    }
                }
            }
            search_body["query"]["bool"]["should"].append(text_match)

        try:
            response = self.es.search(index=index_name, body=search_body)
            hits = response["hits"]["hits"]
            return hits[:final_k]

        except Exception as e:
            self.logger.error(f"Hybrid search failed: {str(e)}")
            return []

