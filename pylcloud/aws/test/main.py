import os, sys

from pylcloud.aws import AWSBedrockKnowledgeBase, AWSBedrockModels

# Test import and init
api_kb = AWSBedrockKnowledgeBase("", "")
api_bedrock = AWSBedrockModels(
    os.getenv("AWS_ACCESS_KEY_ID", ""),
    os.getenv("AWS_ACCESS_KEY_SECRET", ""),
    aws_region_name="eu-west-1",
)
