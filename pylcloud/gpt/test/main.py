import os, sys

from pylcloud.gpt import GPTAWS, GPTAzure, GPTServer

# This is a test file. Run it with pyyel as an installed package.

for cls in [GPTServer, GPTAWS, GPTAzure]:
    try:
        cls()
    except Exception as e:
        print(e)
