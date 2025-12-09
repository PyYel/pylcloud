import os, sys
from pylcloud.storage import *

# This is a test file. Run it with pyyel as an installed package.

for cls in [StorageMinIO, StorageS3]:
    try:
        cls()
    except Exception as e:
        print(e)
