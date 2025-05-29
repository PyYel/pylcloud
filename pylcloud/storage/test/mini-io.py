import os, sys

MAIN_DIR = os.path.dirname(os.path.dirname((os.path.dirname(__file__))))
if __name__ == "__main__":
    sys.path.append(MAIN_DIR)

from storage import StorageMiniIO

# test upload of this __file__.py
dir = os.path.dirname(__file__)
api_miniio = StorageMiniIO(bucket_name="test")
api_miniio.create_bucket()
api_miniio.upload_files(paths=__file__)