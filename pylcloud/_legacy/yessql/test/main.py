import os, sys

YESSQL_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(YESSQL_DIR_PATH))

from yessql import YesSQL, YesSQLMySQL, YesSQLSQLite

# Test import and init
# YesSQL()
mysql = YesSQLMySQL()
sqlite = YesSQLSQLite()

mysql.list_databases(display=True)
# mysql.drop_database()
# mysql.connect_database(user="admin", database_name="another_db")
# mysql.drop_database()
# mysql.list_databases(display=True)

mysql.list_tables(display=True)