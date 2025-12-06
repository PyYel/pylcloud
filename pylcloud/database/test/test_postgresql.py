import os, sys

from dotenv import load_dotenv

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(DATABASE_DIR_PATH)), ".env"))

from database import DatabaseRelationalPostgreSQL

# Test import and init
if "local" in sys.argv:
    db = DatabaseRelationalPostgreSQL(
        host="localhost",
        schema="public",
        user="admin",
        password="password",
        database="postgres",
    )
    db._init_db()
    db.describe(display=True)
    db.create_table(
        table_name="test", column_definitions=["id_field", "forbidden-field-format"]
    )
    db.create_table(
        table_name="test",
        column_definitions=["id SERIAL PRIMARY KEY", "name VARCHAR(100) NOT NULL"],
    )
    db.describe(display=True)

if "aws" in sys.argv:
    # Direct connect as root
    # db = DatabaseRelationalPostgreSQL(schema_name=os.getenv("RDS_SCHEMA", ""),
    #                         host=os.getenv("RDS_HOST", ""),
    #                         user=os.getenv("RDS_USER", ""),
    #                         password=os.getenv("RDS_PASSWORD"),
    #                         connection_timeout=10,
    #                         aws_region_name=os.getenv("AWS_REGION_NAME"),
    #                         aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    #                         aws_secret_access_key=os.getenv("AWS_ACCESS_KEY_SECRET"))
    # db.connect_database(schema_name="test-db", create_if_not_exists=True)
    # db.list_databases(display=True)
    # db._create_iam_user(user="RDSUser", schema_name="test-db")
    # db.list_schemas(display=True)

    # Standard connect for AWS RDS using IAM User auth
    db = DatabaseRelationalPostgreSQL(
        schema_name="test-db",
        host=os.getenv("RDS_HOST", ""),
        user="RDSUser",  # For an existing IAM user named 'RDSUser'
        password=None,  # None implies an IAM auth
        connection_timeout=10,
        aws_region_name=os.getenv("AWS_REGION_NAME"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_ACCESS_KEY_SECRET"),
    )
    db.list_tables(display=True)
    db.create_table(
        table_name="demo",
        column_definitions=["id SERIAL PRIMARY KEY", "name VARCHAR(100) NOT NULL"],
    )
    db.list_tables(display=True)
    db.send_data(table_name="demo", id=12, name="this is a string")
    print(db.query_data(SELECT="*", FROM="demo"))
