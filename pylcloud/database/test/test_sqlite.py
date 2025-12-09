import os, sys

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))

from database import DatabaseSQLite

# Test import and init
db = DatabaseSQLite(database_path=os.path.join(os.path.dirname(__file__), "test_db.db"))
db.create_table(table_name="test", column_definitions=["idx", "name", "role"])
db.list_tables()
