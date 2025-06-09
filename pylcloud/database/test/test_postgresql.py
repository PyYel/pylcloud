import os, sys

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))

from database import DatabasePostgreSQL

# Test import and init
db = DatabasePostgreSQL(host="localhost", database_name="test-db")
db.list_databases(display=True)
db.list_tables(display=True)
# db.create_database(database_name="test-db")
# db.connect_database(database_name="test-db")
db.create_table(table_name="test", column_definitions=["id_field", "value-field"])
db.execute_file(file_path=r"C:\Users\nayel\Projets\HyperRAG-backend\server\backend\config\postgres_setup.sql")