import os, sys

from dotenv import load_dotenv

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))

load_dotenv(os.path.join(os.path.dirname(DATABASE_DIR_PATH), ".env"))

from database import DatabasePostgreSQL

# Test import and init
if "local" in sys.argv:    
    db = DatabasePostgreSQL(host="localhost", database_name="test-db")
    db.list_databases(display=True)
    db.list_tables(display=True)
    # db.create_database(database_name="test-db")
    # db.connect_database(database_name="test-db")
    db.create_table(table_name="test", column_definitions=["id_field", "value-field"])
    # db.execute_file(file_path=os.path.join(os.path.dirname(__file__), "postgres_setup.sql"))

if "aws" in sys.argv:
    db = DatabasePostgreSQL(database_name="test",
                            host=os.getenv("RDS_HOST"),
                            aws_region_name=os.getenv("AWS_REGION_NAME"),
                            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                            aws_secret_access_key=os.getenv("AWS_ACCESS_KEY_SECRET"))
    db.create_database(database_name="test-db-2")
