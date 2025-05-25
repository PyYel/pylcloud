import os, sys

MAIN_DIR = os.path.dirname((os.path.dirname(__file__)))
if __name__ == "__main__":
    sys.path.append(MAIN_DIR)

from pylcloud.storage import StorageServer


storage_api = StorageServer()