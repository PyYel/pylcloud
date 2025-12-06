import os, sys

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))

from database import DatabaseDocumentMongoDB

# Test import and init
db = DatabaseDocumentMongoDB()
