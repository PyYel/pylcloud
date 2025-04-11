import os, sys
from dotenv import load_dotenv

AWS_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(AWS_DIR_PATH))

from aws import AWSBedrockKnowledgeBase, AWSBedrockModels, AWSS3
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(AWS_DIR_PATH)), ".env"))

# Test import and init
api_s3 = AWSS3("", "", "")
api_kb = AWSBedrockKnowledgeBase("", "")
api_bedrock = AWSBedrockModels(os.getenv("AWS_KEY_ID"), os.getenv("AWS_KEY_ACCESS"), aws_region_name="eu-west-1")

