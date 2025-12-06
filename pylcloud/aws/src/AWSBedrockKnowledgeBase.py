import boto3
import json
import os, sys
import uuid

from .AWS import AWS


class AWSBedrockKnowledgeBase(AWS):
    """
    AWS Bedrock RAG related services helper.
    """

    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_region_name: str = "us-east-1",
        aws_session_token: str = uuid.uuid4(),
    ) -> None:
        """
        Initiates a connection to the Bedrock Knowledge Base related services.

        This is dedicated to knowledge bases (RAG). For straight model inference, see ``AWSBedrockModels``.

        Parameters
        ----------
        aws_access_key_id: str
            The user's private key ID to use to connect to the AWS services.
        aws_secret_access_key: str
            The user's private key to use to connect to the AWS services.
        aws_region_name: str, 'us-east-1'
            The AWS ressources region name.
        aws_session_token: str, 'uuid.uuid4()'
            A unique session token.

        Notes
        -----

        Exemple
        -------
        >>> bedrock_kb_api = AWSBedrockKnowledgeBase(KEY_ID, KEY, 'eu-west-1', SESSION_TOKEN)
        """
        super().__init__()

        # The agent is used to retreive KB content by using an encoding model
        self.bedrock_agent_client = self._create_client(
            aws_service_name="bedrock-agent-runtime",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=aws_region_name,
        )

        # The runtime client is a more straight-forward approach, that directly connects to the models
        self.bedrock_runtime_client = self._create_client(
            aws_service_name="bedrock-runtime",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            aws_region_name=aws_region_name,
        )

        return None

    def retreive_similar(self, knwoledge_base_id: str, prompt: str):
        """
        Embeds a prompt and returns the S3 location of similar references.
        """

        model_id = "amazon.titan-text-premier-v1:0"
        model_arn = f"arn:.AWS:bedrock:us-east-1::foundation-model/{model_id}"

        models = self.list_foundation_models()
        # print([model["modelId"] for model in models if model["modelId"].startswith("amazon")])
        model_arn = [
            model["modelArn"] for model in models if model["modelId"] == model_id
        ][0]

        response = self.bedrock_agent_client.retrieve_and_generate(
            input={"text": prompt},
            retrieveAndGenerateConfiguration={
                "type": "KNOWLEDGE_BASE",
                "knowledgeBaseConfiguration": {
                    "knowledgeBaseId": knwoledge_base_id,
                    "modelArn": model_arn,
                },
            },
        )

        response_files = [ref["retrievedReferences"] for ref in response["citations"]]
        response_files = [
            ref[0]["location"]["s3Location"]["uri"] for ref in response_files
        ]

        return response_files

    def list_agent_knowledge_bases(self, agent_id, agent_version):
        """
        List the knowledge bases associated with a version of an Amazon Bedrock Agent.

        :param agent_id: The unique identifier of the agent.
        :param agent_version: The version of the agent.
        :return: The list of knowledge base summaries for the version of the agent.
        """

        knowledge_bases = []

        paginator = self.client.get_paginator("list_agent_knowledge_bases")
        for page in paginator.paginate(
            agentId=agent_id,
            agentVersion=agent_version,
            PaginationConfig={"PageSize": 10},
        ):
            knowledge_bases.extend(page["agentKnowledgeBaseSummaries"])

        return knowledge_bases

    def list_foundation_models(self, display: bool = False):
        """
        List the available Amazon Bedrock foundation models.

        Parameters
        ----------
        display: bool, False
            Whereas to print in the terminal the model list, or not.

        Returns
        -------
        models_list: list[str]
            The list of available bedrock foundation models.
        """

        response = self.bedrock_runtime_client.list_foundation_models()
        models_list = response["modelSummaries"]
        if display:
            print(models_list)

        return models_list
