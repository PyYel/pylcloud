import os, sys

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))

from database import DatabaseRelationalMySQL

# Test import and init
db = DatabaseRelationalMySQL(host="10.24.160.80", database_name="datahive")
print(db.list_databases(system_db=True))
print(db.list_tables())
print(db.query_data(SELECT="dataset_name", FROM="datasets"))
print(
    db.query_data(
        SELECT="*", FROM="datapoints", WHERE="dataset_name", VALUES=("Audio dataset",)
    )
)
print(len(db.query_data(SELECT="*", FROM="datapoints")))
