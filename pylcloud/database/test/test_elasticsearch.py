import os, sys
from dotenv import load_dotenv
import ssl
import random
import time
import pprint

# ssl._create_default_https_context = ssl._create_unverified_context

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))

from database import DatabaseSearchElasticsearch

load_dotenv(os.path.join(DATABASE_DIR_PATH, "src", "search", ".env"))


# Read write prod user
api_os = DatabaseSearchElasticsearch(
    host="http://localhost:9200",
    user=os.getenv("ELASTICSEARCH_USER", ""),
    password=os.getenv("ELASTICSEARCH_PASSWORD", ""),
)

api_os.drop_index(index_name="test")
api_os.drop_index(index_name="test")
api_os.drop_index(index_name="test-vect")


api_os.create_index(
    index_name="test",
    mappings={"name": {"type": "keyword"}, "age": {"type": "float"}},
)

indexes = api_os.list_indexes()


api_os.send_data(index_name="test", documents=[{"name": "john1", "age": 10}, {"name": "john2", "age": 20}])

api_os.delete_data(index_name="test", pairs={"name": "john1"})

time.sleep(1) # time for os indexing
print([response["_source"] for response in api_os.query_data(index_name="test")])


api_os.create_index(
    index_name="test-vect",
    mappings={
        "name": {"type": "keyword"}, 
        "vector": {
            "type": "dense_vector",
            "dims": 256,
        },
    }
)


test_vect = [random.random() for k in range(256)]

api_os.send_data(index_name="test-vect", documents=[{"name": "john1", "vector": test_vect}])

time.sleep(1)
results = api_os.similarity_search(
    index_name="test-vect", 
    vector_query=test_vect, 
    vector_field="vector",
    text_field="name",
    text_query="not_john",
    text_weight=1
)
pprint.pprint([result["_source"] for result in results], depth=2)
