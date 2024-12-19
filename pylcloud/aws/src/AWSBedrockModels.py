import os, sys
import boto3
import json
import uuid

AWS_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(AWS_DIR_PATH))

from aws import AWS


class AWSBedrockModels(AWS):
    """
    AWS Bedrock general inference services helper.
    """
    def __init__(self, 
                 aws_access_key_id: str,
                 aws_secret_access_key: str, 
                 aws_region_name: str = 'us-west-1',
                 aws_session_token: str = uuid.uuid4()
                 ) -> None:
        """
        Initiates a connection to the Bedrock service. 
        
        This is dedicated to fundation models inference. For RAG and knowledge bases, see ``AWSBedrockKnowledgeBase``.

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

        Note
        ----
        - Connects to both 'bedrock' and 'bedrock-runtime' services endpoints.

        Exemple
        -------
        >>> bedrock_models_api = AWSBedrockModels(KEY_ID, KEY, 'eu-west-1', SESSION_TOKEN)
        """
        super().__init__()

        self.bedrock_client = self._create_client(aws_service_name="bedrock",
                                                  aws_access_key_id=aws_access_key_id,
                                                  aws_secret_access_key=aws_secret_access_key,
                                                  aws_region_name=aws_region_name,
                                                  aws_session_token=aws_session_token)

        self.bedrock_runtime_client = self._create_client(aws_service_name="bedrock-runtime",
                                                          aws_access_key_id=aws_access_key_id,
                                                          aws_secret_access_key=aws_secret_access_key,
                                                          aws_region_name=aws_region_name,
                                                          aws_session_token=aws_session_token)

        return None


    def evaluate_model(self, model_id: str, prompt_text: str, display: bool = False):
        """
        Function to interact with an AWS Bedrock model using boto3.

        Parameters
        ----------
        model_id: str
            The ID of the model in AWS Bedrock to invoke. 
            See ``list_fundation_models()`` for the exact ID name.
        prompt_text: str 
            The input text to send to the model.
        display: bool, False
            Whereas to print in the terminal the model response, or not.

        Returns
        -------
        response_body: dict
            Response from the AWS Bedrock model.
        """

        payload = {
            "inputText": prompt_text
        }
        
        response = self.bedrock_runtime_client.invoke_model(
            modelId=model_id,           # The model to invoke. See list_fundation_models for the exact ID name.
            accept='application/json',  
            contentType='application/json',  
            body=json.dumps(payload)    # Converts the payload to a JSON string
        )
        
        response_body = json.loads(response['body'].read())
        if display: print(response_body)

        return response_body
        

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
        if display: print(models_list)

        return models_list
    
