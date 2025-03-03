import os, sys

NOSQL_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(NOSQL_DIR_PATH))

from nosql import NoSQL, NoSQLElasticsearch, NoSQLMongoDB

# Test import and init
mongodb_api = NoSQLMongoDB()

elasic_api = NoSQLElasticsearch()

print(elasic_api._hash_content(content='Message from Caroline: Merry Christmast!', prefixes=['2024/12/25', '103010']))
# elasic_api.create_table(index_name="test")

indexes = elasic_api.list_indexes()
elasic_api.list_clusters()

if indexes:
    elasic_api.query_data(index_name=indexes[0])