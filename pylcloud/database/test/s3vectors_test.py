import os, sys
from dotenv import load_dotenv
import ssl
import random
import time
import pprint

# ssl._create_default_https_context = ssl._create_unverified_context

DATABASE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(DATABASE_DIR_PATH))

from database import DatabaseSearchS3Vector

load_dotenv(os.path.join(DATABASE_DIR_PATH, "src", "search", ".env"))

api = DatabaseSearchS3Vector(
    bucket_name="vector-275376003127",
    AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID", ""),
    AWS_ACCESS_KEY_SECRET=os.getenv("AWS_ACCESS_KEY_SECRET", ""),
    AWS_REGION_NAME=os.getenv("AWS_REGION_NAME", "eu-west-1"),
    
)

api.create_index(index_name="test", mappings={"name": "text"})