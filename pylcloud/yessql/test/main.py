import os, sys

YESSQL_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(YESSQL_DIR_PATH))

from yessql import YesSQL, YesSQLMySQL, YesSQLSQLite

# Test import and init
YesSQL()
YesSQLMySQL()
YesSQLSQLite()