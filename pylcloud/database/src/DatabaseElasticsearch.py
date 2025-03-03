import os, sys

from elasticsearch import Elasticsearch, helpers, NotFoundError
import ssl
import json
import ssl
import urllib3
import warnings

# Removes unverified HTTPS SSL traffic warnings
warnings.filterwarnings('ignore', 'Connecting to .+ using TLS with verify_certs=False is insecure')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from .Database import Database


class DatabaseElasticsearch(Database):
    """
    Elasticsearch Python API helper.
    """
    def __init__(self, host: str = "https://localhost:9200", user: str = "admin", password: str = "password"):
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

        # TODO: Add connection certificate
        try:
            self.connect_database(host=host, user=user, password=password)
        except Exception as e:
            print(f"DatabaseElasticsearch >> An error occured when connecting to '{host}': {e}")

        # Note:
        # The authentication credentials above are required to connect to Elasticsearch.
        # When connecting to Kibana server, custom users credentials should be used. They can be created from withing the Kibana server
        # interface, through the elastic superuser account (which are actually the credentials used above: ELASTIC_USERNAME, ELASTIC_PASSWORD)

        return None


    def _connect_database(self, *args, **kwargs):
        """See ``connect_database()``."""
        return self.connect_database(*args, **kwargs)

    def connect_database(self, 
                         host: str = "127.0.0.1",
                         user: str = "user",
                         password: str = "password"):
        """
        Connects to the database and creates a connector object ``conn``. 
        """
        self.es = Elasticsearch(host, basic_auth=(user, password), verify_certs=False)
        return self.es
    

    def _create_table(self, *args, **kwargs):
        """See ``create_index()``."""
        return self.create_index(*args, **kwargs)

    def create_index(self, index_name: str, properties: dict[str], shards: int = 1, replicas: int = 1):
        """
        Creates an index, with enforced properties.

        Parameters
        ----------
        index_name: str
            The name of the index to create.
        properties: dict[str]
            The index properties. Must be formatted as {'field':{'type': 'dtype'}}. See Examples below.
        shards: int, 1
            The number of shards to allocate.
        replicas: int, 1
            The number of replicas to create for each shard. Replicas may improve data availability and redundancy.
        
        Examples
        --------
        >>> index_name = "my_index"
        >>> properties = {
                    "title": {"type": "text"},
                    "description": {"type": "text"},
                    "timestamp": {"type": "date"},
                }
        >>> create_tables(index_name=index_name, properties=properties, shards=2, replicas=1)
        """

        if ' ' in index_name:
            print(f"DatabaseElasticsearch >> Index name can't contain blank space. Index name changed to '{index_name.replace(' ', '-')}'.")
            index_name = index_name.replace(' ', '-')

        settings = {
            "settings": {
                "number_of_shards": shards,
                "number_of_replicas": replicas,
            },
            "mappings": {
                "properties": properties
            },
        }

        # Create the index
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(index=index_name, body=settings)
            print(f"DatabaseElasticsearch >> Index '{index_name}' created.")
        else:
            print(f"DatabaseElasticsearch >> Index '{index_name}' already exists.")    


    def _disconnect_database(self, *args, **kwargs):
        """See ``disconnect_database()``."""
        return self.disconnect_database(*args, **kwargs)
    
    def disconnect_database(self):
        """
        Closes the database linked to the connector ``conn``.
        """
        pass


    def _drop_database(self, *args, **kwargs):
        """See ``drop_database()``."""
        return self.drop_database(*args, **kwargs)
    
    def drop_database(self, database_name: str):
        """
        Drops all the indexes from a cluster.
        """
        print("DatabaseElasticsearch >> Can't drop an Elasticsearch. Will drop all the indexes from this cluster instead.")
        raise NotImplementedError


    def _drop_table(self, *args, **kwargs):
        """See ``drop_index()``."""
        return self.drop_index(*args, **kwargs)
    
    def drop_index(self, index_name: str):
        """
        Deletes an index and all its content.
        """
        try:
            response = self.es.indices.delete(index=index_name)
            print(f"DatabaseElasticsearch >> Index '{index_name}' deleted successfully.")
        except Exception as e:
            print(f"DatabaseElasticsearch >> Failed to delete index '{index_name}': {e}")
        
        return None
    

    def _delete_data(self, *args, **kwargs):
        """See ``delete_data()``."""
        return self.delete_data(*args, **kwargs)
    
    def delete_data(self, index_name: str, pairs: dict[str] = {}):
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
            print(f"DatabaseElasticsearch >> Successfully deleted date from '{index_name}'.")
        except Exception as e:
            print(f"DatabaseElasticsearch >> Failed to delete data from '{index_name}': {e}")

        return None


    def _list_databases(self, *args, **kwargs):
        """See ``list_clusters()``."""
        return self.list_clusters(*args, **kwargs)
    
    def list_clusters(self):
        """
        List the databases (clusters) present on an Elasticsearch DB server.
        """
        info = self.es.info()
        print(f"DatabaseElasticsearch >> Cluster info: {info}")
        return info
    

    def _list_tables(self, *args, **kwargs):
        """See ``list_indexes()``."""
        return self.list_indexes(*args, **kwargs)

    def list_indexes(self, system_db: bool = False):
        """
        Returns a list of the names of the indexes on the connected cluster.

        Parameters
        ----------
        system_db: bool
            Whereas returning the builtin databases if any, or not.

        Notes
        -----
        - To list the existing databases, see ``list_databases()``.
        """
        if system_db:
            # built-in system indexes start with a dot
            indexes = [index['index'] for index in self.es.cat.indices(format='json')]
            print(f"DatabaseElasticsearch >> Found indexes: {', '.join(indexes)}")
            return indexes
        else:
            indexes = [index['index'] for index in self.es.cat.indices(format='json') if not index['index'].startswith(".")]
            print(f"DatabaseElasticsearch >> Found indexes: {', '.join(indexes)}")
            return indexes
        
        return None


    def _send_data(self, *args, **kwargs):
        """See ``send_data()``."""
        return self.send_data(*args, **kwargs)


    def send_data(self, index_name: str, documents: list[dict]):
        """
        Sends data to the Elasticsearch index. Can handle single files, multiple files, and directories.

        Parameters
        ----------
        index_name: str
            The name of the index to send data to.
        workspace_name: str
            The name of the workspace.
        path: str
            The list of preprocessed files to inject into Elastic. 

        Notes
        -----
        - For a file_path of format ``path/to/file.ext``, the preprocessed data should be inside ``path/to/file`` eponymous folder/
        """

        if isinstance(files_paths, str): files_paths = [files_paths]

        actions = [
            {
                "_index": index_name,                           # Target indexes
                "_id": self._hash_content(document=document),   # Hashed id for log unicity
                "_source": document                             # Document content
            }
            for document in documents
        ]

        # for action in actions:
        #     response = self.es.index(index=index_name, document=action["_source"], id=action["_id"])
        #     print(response)

        print(f"DatabaseElasticsearch >> Sending {len(actions)} documents into index '{index_name}'.")
        response = helpers.bulk(self.es, actions, raise_on_error =True)
        if response[1]:
            print(f"DatabaseElasticsearch >> API error when sending documents: {response}")

        return None


    def _query_data(self, *args, **kwargs):
        """See ``query_data()``."""
        return self.query_data(*args, **kwargs)

    def query_data(self, 
                   index_name: str, 
                   must_pairs: list[dict[str]] = [], 
                   should_pairs: list[dict[str]] = []):
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
                    "minimum_should_match": 1 if should_conditions else 0
                }
            }
        }

        documents = []
        try:
            for doc in helpers.scan(self.es, index=index_name, query=query, size=1000):
                documents.append(doc)
            print(f"DatabaseElasticsearch >> Field search found {len(documents)} matching documents.")
        except NotFoundError as e:
            print(f"APIClientElastic >> Index '{e.info['error']['index']}' not found.")
            return []
        except Exception as e:
            print(f"APIClientElastic >> An error occurred during semantic search: {e}")
            return []

        return documents


    def similarity_search(self, index_name: str, 
                          query_vector: list[float], 
                          must_pairs: list[dict[str]] = [], 
                          should_pairs: list[dict[str]] = [],
                          k: int = 5):
        """
        Performs k-NN similarity search in Elasticsearch.

        Parameters
        ----------
        index_name : str
            The Elasticsearch index to search in.
        query_vector : list
            The vector representation of the input query.
        k : int, optional
            The number of closest matches to return (default is 5).

        Returns
        -------
        list
            A list of retrieved documents sorted by similarity.
        """

        must_conditions = []
        for must_pair in must_pairs:
            must_conditions.append({"term": must_pair})

        should_conditions = []
        for should_pair in should_pairs:
            should_conditions.append({"term": should_pair})

        query = {
            "size": k,
            "query": {
                "bool": {
                    # Additionnal condition are required to reduce the knn scope, such as limiting it to a specific workspace_name
                    "must": must_conditions,
                    "should": should_conditions,
                    # At least 1 should condition must be matched. When there is no should condition input, the minimum must be set to zero
                    "minimum_should_match": 1 if should_conditions else 0,
                    "filter": {
                        "knn": {
                            "field": "chunk.vector",
                            "query_vector": query_vector,
                            "k": k,
                        }
                    }
                }
            }
        }

        try:
            response = self.es.search(index=index_name, body=query)
            documents = [hit for hit in response['hits']['hits']]
            print(f"DatabaseElasticsearch >> Vector search found {len(documents)} matching documents.")
            return documents
        except Exception as e:
            print(f"APIClientElastic >> Error: An error occurred: {e}")
            return []

