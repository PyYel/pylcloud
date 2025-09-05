import os, sys

import ssl

# ssl._create_default_https_context = ssl._create_unverified_context

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))

from database import DatabaseOpenSearch

# Test import and init
api_os = DatabaseOpenSearch(host="http://localhost:9200")

# api_os.drop_index(index_name="test")

api_os.create_index(index_name="test", properties={"name": {"type": "keyword"}, "age": {"type": "float"}})

indexes = api_os.list_indexes()

if indexes:
    api_os.send_data(index_name="test", documents=[{"name": "john2", "age": 10}])
    print([response["_source"] for response in api_os.query_data(index_name="test")])