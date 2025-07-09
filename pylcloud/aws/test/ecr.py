
import os, sys
from dotenv import load_dotenv

AWS_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(AWS_DIR_PATH))

from aws import AWSECR


load_dotenv(os.path.join(os.path.dirname(os.path.dirname(AWS_DIR_PATH)), ".env"))

aws_ecr = AWSECR(profile_name=os.getenv("AWS_PROFILE_NAME"), region=os.getenv("AWS_REGION_NAME"), account_id=os.getenv("AWS_ACCOUNT_ID"))
aws_ecr.push_image_to_ecr(local_image_name="sparql-pipeline", repo_name="sparql-pipeline")
# aws_ecr.pull_image_from_ec2()