import os, sys

from pylcloud.database import *

# This is a test file. Run it with pyyel as an installed package.

for cls in [
    DatabaseDocumentMongoDB,
    DatabaseGraphJenafuseki,
    DatabaseGraphNeo4j,
    DatabaseRelationalMySQL,
    DatabaseRelationalPostgreSQL,
    DatabaseRelationalSQLite,
    DatabaseSearchElasticsearch,
    DatabaseSearchOpensearch,
]:
    try:
        cls()
    except Exception as e:
        print(e)
