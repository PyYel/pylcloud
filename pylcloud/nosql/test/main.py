import os, sys

NOSQL_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(NOSQL_DIR_PATH))

from nosql import NoSQL, NoSQLElasticsearch

# Test import and init
NoSQL()
NoSQLElasticsearch("")