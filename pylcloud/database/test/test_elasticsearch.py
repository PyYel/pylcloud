import os, sys

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))

from database import DatabaseElasticsearch

# Test import and init
elasic_api = DatabaseElasticsearch()

print(elasic_api._hash_content(content='Message from Caroline: Merry Christmast!', prefixes=['2024/12/25', '103010']))
# elasic_api.create_table(index_name="test")

indexes = elasic_api.list_indexes()
elasic_api.list_clusters()

if indexes:
    elasic_api.query_data(index_name=indexes[0])