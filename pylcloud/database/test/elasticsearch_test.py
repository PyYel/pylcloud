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
api_es = DatabaseSearchElasticsearch(
    host="http://localhost:9200",
    user=os.getenv("ELASTICSEARCH_USER", ""),
    password=os.getenv("ELASTICSEARCH_PASSWORD", ""),
)

api_es.drop_index(index_name="test")
api_es.drop_index(index_name="test")
api_es.drop_index(index_name="test-vect")


api_es.create_index(
    index_name="test",
    mappings={"name": {"type": "keyword"}, "age": {"type": "float"}},
)

indexes = api_es.describe_database()


api_es.send_data(index_name="test", documents=[{"name": "john1", "age": 10}, {"name": "john2", "age": 20}])

api_es.delete_data(index_name="test", pairs={"name": "john1"})

time.sleep(1) # time for os indexing
print([response["_source"] for response in api_es.query_data(index_name="test")])


api_es.create_index(
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

api_es.send_data(index_name="test-vect", documents=[{"name": "john1", "vector": test_vect}])

time.sleep(1)
results = api_es.similarity_search(
    index_name="test-vect", 
    vector_query=test_vect, 
    vector_field="vector",
    text_field="name",
    text_query="not_john",
    text_weight=1
)
pprint.pprint([result["_source"] for result in results], depth=2)
