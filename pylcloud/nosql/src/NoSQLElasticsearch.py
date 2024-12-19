import os, sys

import pandas as pd

from datetime import datetime, timedelta
import hashlib

from elasticsearch import Elasticsearch, helpers, NotFoundError
import ssl
import json
import ssl
import urllib3
 
# Suppress only the InsecureRequestWarning

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

 
MAIN_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if __name__ == "__main__":
    sys.path.append(MAIN_DIR)
    os.chdir(MAIN_DIR)

from constants import ELASTIC_API_KEY, ELASTIC_CERTIFICATE, ELASTIC_PASSWORD, ELASTIC_USERNAME, HOST_URL
from api import APIClient


class APIClientElastic(APIClient):


    def __init__(self, base_url: str = HOST_URL, config_path = None):
        super().__init__(base_url, config_path)

        # TODO: Add connection certificate
        self.es = Elasticsearch(self.base_url, basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD), verify_certs=False)

        # Note:
        # The authentication credentials above are required to connect to Elasticsearch.
        # When connecting to Kibana server, custom users credentials should be used. They can be created from withing the Kibana server
        # interface, through the elastic superuser account (which are actually the credentials used above: ELASTIC_USERNAME, ELASTIC_PASSWORD)


    def _hash_log(self, log: dict):
        """
        Hashes an elastic log payload into a unique id of format <timestamp>-<hashed_content>. This is usefull to automatically overwrite a stored log when a log with 
        the same timestamp and content is written into Elasticsearch.  
        """
        return f"{log['@timestamp']}-{hashlib.md5(log['log']['message'].encode()).hexdigest()}"


    def send_to_elastic(self, logs: dict[str], labels: dict[dict[str]], levels: dict[str] = "N/A", index_names: list[str] = ["test"]):
        """
        Sends to the Elasticsearch DB.

        TODO: Add certificate to the connection (cf. __init__())

        TODO: some levels may be infered during preprocessing, so ``levels`` should be a dictionnary 
        of {timestamp: level}, just like ``logs`` ({timestamp: message}).

        Parameters
        ----------
        logs: dict[str]
            The logs where the key:value pairs are of format timestamp:content.
        labels: dict[dict[str]]
            The labels (metadatas) where the key:value pairs are of format timestamp:{label:value}
        levels: str, 'N/A'
            The log.level default value.
        index_names: list[str], ['test']
            The names of the indexes which should receive the logs data. If multiple indexes are specified,
            the records are duplicated are duplicated across all the indexes.

        Examples
        --------
        >>> logs = {"2024-10-30T08:19:19.9070573Z": "Current runner version: '2.317.0'", 
                    "2024-10-25T09:36:09.0391254Z": "Runner name: 'GH-UBU22-RE-1'"}
        >>> labels = {"2024-10-30T08:19:19.9070573Z": {"step": "Complete job", "run": "2634763"},
                      "2024-10-25T09:36:09.0391254Z": {"step": "Post Checkout PR branch and all PR commits", "run": "2634763"}}
        >>> index_name = "test" # If str, will be promotted to list(str)
        """

        if type(index_names) is str: index_names = [index_names]

        logs = [
            {
                "@timestamp": timestamp,
                "log": {
                    "level": levels, # TODO: replace with levels[timestamps] = LEVEL
                    "message": content
                },
                "metadata": labels[timestamp]
            }
            for timestamp, content in logs.items()
        ]
        
        index_names = [index_name.lower() for index_name in index_names]
        for index_name in index_names:
            # Every team has its index, so some logs must be duplicated across multiple teams
            actions = [
                {
                    "_index": index_name,                   # Target indexes
                    "_id": self._hash_log(log=log),         # Hashed id for log unicity
                    "_source": log                          # Document content
                }
                for log in logs
            ]

            response = helpers.bulk(self.es, actions)
            if response[1]:
                print(f"Errors: {response[1]}")
            break # Only the first team
        
        return True


    def retrieve_from_elastic(self, 
                              index_name: str, 
                              must_pairs: list[dict[str]] = [], 
                              should_pairs: list[dict[str]] = [], 
                              start_time: str = None, 
                              end_time: str = None,
                              days_delta: float = 1.0):
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
        start_time: str
            The start of the timestamp range. Date should be of truncated format ``%Y-%m-%d %H:%M:%S``.
        end_time: str
            The end of the timestamp range. Date should be of truncated format ``%Y-%m-%d %H:%M:%S``. If ``None``, the current time will be used.
        days_delta: float, 1.0
            In the event of a missing ``start_time``, it will be replaced with the date ``days_delta`` from now.

        Returns
        -------
        documents: list[dict[str]]
            The list of fetched document, as dictionnaries nested with the same structure as the one found in Elasticsearch.
        
        Examples
        --------
        >>> index_name = "test_repository"
        >>> start_time = "2024-10-25 10" # Will replace the missing values with :00:00
        >>> end_time = "2024-10" # Will replace the missing values with :01 00:00:00
        # A list of single key-value pairs. This will retreive any log tagged as 'Error' or 'Warning'. The '.keyword' MUST be used for Elasticsearch to find the records.
        >>> should_pairs = [{'log.level.keyword': 'Error'}, {'log.level.keyword': 'Warning'}] 
        # When calling the method, it will return a list of documents with the following structure:
        >>> retreive_from_elastic(...)
        >>> [
                {
                    '_index': '...', 
                    '_id':'...', 
                    '@timestamp': '...',
                    '_source': { 
                        'log': {
                            'message': ..., # eg: 'Cleanup process'
                            'level': ... # eg: 'Info'
                        }, 
                        'metadata': {
                            ... # eg: 'repo_owner': 'PowerLogic-Toolsuite
                        }
                    }
                }, ...
            ]
        """

        if start_time is None: start_time = datetime.now() - timedelta(days=days_delta)
        else: 
            start_time = self.convert_to_datetime(start_time)
            # Free Loki Grafana limits querries to 30 days from now # TODO: DEFAULT LOKI CONFIG IS 7DAYS
            oldest_start_time = datetime.now() - timedelta(days=30)
            if start_time < oldest_start_time:
                print(f"APIClientElastic >> Can't querry logs that are older than 30 days from now: {start_time}. Querry date changed from {oldest_start_time} to now.")
                start_time = oldest_start_time
            # Start date must be older than end date
            if end_time is not None:
                if self.convert_to_datetime(end_time) < start_time:
                    print(f"APIClientElastic >> Can't querry logs when the starting date is ulterior to the ending date: {end_time} < {start_time}. Querry aborted.")
                    return []

        if end_time is None: end_time = datetime.now()
        else: 
            end_time = self.convert_to_datetime(end_time)
            # End date can't be in the future (would easily break the querry range of 1 month)
            if end_time > datetime.now(): end_time = datetime.now()

        must_conditions = [
            {
                "range": {
                    "@timestamp": {
                        "gte": start_time,
                        "lte": end_time
                    }
                }
            }
        ]
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
                    # At least 1 should condition must be matched. When there is no shoud condition input, the minimum must be set to zero
                    "minimum_should_match": 1 if should_conditions else 0
                }
            },
            "sort": [
                {"@timestamp": "asc"}
            ],
            "size": 10000  # Adjust the size as needed
        }

        # Execute the initial search query
        try:
            response = self.es.search(index=index_name, body=query)
            documents = response['hits']['hits']
        except NotFoundError as e:
            print(f"APIClientElastic >> Warning: Index not found: {e.info['error']['index']}")
            return []
        except Exception as e:
            print(f"APIClientElastic >> Error: An error occurred: {e}")
            return []

        # Retrieve all the documents using search_after
        while len(response['hits']['hits']) > 0:
            last_hit = response['hits']['hits'][-1]
            search_after = last_hit['sort']

            query['search_after'] = search_after
            response = self.es.search(index=index_name, body=query)
            documents.extend(response['hits']['hits'])

        return documents
    

    def update_documents(self, documents: list[str]):
        """
        Updates inplace a list of documents with its new content. The documents must at least feature the ``_index``, ``_id``, 
        and ``_source`` fields.

        This is useful when 
        """

        actions = [
            {
                "_op_type": "index",  # Use "index" to create or replace the document
                "_index": doc['_index'],
                "_id": doc['_id'],
                "_source": doc['_source']
            }
            for doc in documents
        ]

        response = helpers.bulk(self.es, actions)
        if response[1]:
            print(f"Errors: {response[1]}")


    def delete_where(self, index: str, pairs: dict[str] = {}):
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
        
        # Delete documents matching the query
        response = self.es.delete_by_query(index=index, body=query)
        print("Delete response:", response)

        return True


    def delete_index(self, index: str):

        try:
            response = self.es.indices.delete(index=index)
            print(f"Index '{index}' deleted successfully.")
        except Exception as e:
            print(f"Failed to delete index '{index}': {e}")
        
        return True

    def list_indexes(self):
        """Returns a list of the non-builtin indexes names."""
        return [index['index'] for index in self.es.cat.indices(format='json') if not index['index'].startswith(".")]


if __name__ == "__main__":

    api = APIClientElastic()
    print(api.list_indexes())

