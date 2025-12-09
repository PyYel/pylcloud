# Use the native inference API to send a text message to Anthropic Claude.

import boto3
import json

from botocore.exceptions import ClientError

import os, sys
from dotenv import load_dotenv

AWS_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(AWS_DIR_PATH))

from aws import AWSBedrockKnowledgeBase, AWSBedrockModels, AWSS3

load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(os.path.dirname(AWS_DIR_PATH)), ".env")
)

# # Test import and init

api_bedrock = AWSBedrockModels(
    os.getenv("AWS_ACCESS_KEY_ID"),
    os.getenv("AWS_ACCESS_KEY_SECRET"),
    aws_region_name="eu-west-1",
)


response = api_bedrock.return_query(
    model_name="claude-3.5-sonnet", user_prompt="who are you?", display=False
)
# print(response["text"])
print(response["usage"])

response = api_bedrock.return_query(
    model_name="claude-3-haiku",
    user_prompt="what is this ??",
    files=os.path.join(os.path.dirname(__file__), "test.png"),
    display=True,
)
# print(response["text"])
print(response["usage"])

response = ""
for stream in api_bedrock.yield_query(
    model_name="claude-3-haiku", user_prompt="tell me about ww2 history", display=False
):
    if isinstance(stream, str):
        # print(stream)
        response += stream
    else:
        # print(response)
        print(stream["usage"])
