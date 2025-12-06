import os, sys


MAIN_DIR_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if __name__ == "__main__":
    sys.path.append(MAIN_DIR_PATH)
