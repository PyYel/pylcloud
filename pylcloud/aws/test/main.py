import os, sys

AWS_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(AWS_DIR_PATH))

from aws import AWSBedrockKnowledgeBase, AWSBedrockModels, AWSS3

# Test import and init
AWSS3("", "", "")
AWSBedrockKnowledgeBase("", "")
AWSBedrockModels("", "")