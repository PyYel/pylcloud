import os, sys

from pylcloud.aws import *

# This is a test file. Run it with pyyel as an installed package.

for cls in [
    AWSBedrockKnowledgeBase,
    AWSBedrockModels,
    # AWSECR, # Requires SSO login
    AWSTranscribe,
]:
    try:
        cls()
    except Exception as e:
        print(e)
