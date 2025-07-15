import os, sys

from dotenv import load_dotenv

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(DATABASE_DIR_PATH)), ".env"))

from database import DatabasePostgreSQL

# Test import and init
if "local" in sys.argv:    
    db = DatabasePostgreSQL(host="localhost", schema_name="test-db", user="admin", password="password")
    db.list_databases(display=True)
    db.list_tables(display=True)
    # db.create_database(schema_name="test-db")
    # db.connect_database(schema_name="test-db")
    db.create_table(table_name="test", column_definitions=["id_field", "value-field"])
    # db.execute_file(file_path=os.path.join(os.path.dirname(__file__), "postgres_setup.sql"))

if "aws" in sys.argv:
    db = DatabasePostgreSQL(schema_name=os.getenv("RDS_SCHEMA"),
                            host=os.getenv("RDS_HOST"),
                            user=os.getenv("RDS_USER"),
                            password=os.getenv("RDS_PASSWORD"),
                            aws_region_name=os.getenv("AWS_REGION_NAME"),
                            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                            aws_secret_access_key=os.getenv("AWS_ACCESS_KEY_SECRET"))
    db.connect_database(schema_name="test-db", create_if_not_exists=True)
    db.list_databases(display=True)
    db._create_iam_user(user="RDSUser", schema_name="test-db")

    db = DatabasePostgreSQL(schema_name="test-db",
                            host=os.getenv("RDS_HOST"),
                            user="RDSUser", # For an existing IAM user named 'RDSUser'
                            password=None, # None implies an IAM auth
                            aws_region_name=os.getenv("AWS_REGION_NAME"),
                            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                            aws_secret_access_key=os.getenv("AWS_ACCESS_KEY_SECRET"))
    db.list_tables(display=True)
    db.create_table(table_name="demo", column_definitions=["id_index", "value_column"])
    db.list_tables(display=True)
    db.send_data(table_name="demo", id_index=12, value_column="this is a string")
    print(db.query_data(SELECT="*", FROM="demo"))

