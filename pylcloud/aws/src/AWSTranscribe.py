
import os, sys

import requests
import time

from .AWS import AWS


class AWSTranscribe(AWS):
    """
    An helper that simplifies calls to the AWS Transcribe API.
    """
    def __init__(self, 
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 bucket_name = str,
                 aws_region_name: str ="us-west-1",
                 ):
        """
        Initiates a connection to a given S3 bucket.

        Parameters
        ----------
        aws_access_key_id: str
            The user's private key ID to use to connect to the AWS services.
        aws_secret_access_key: str
            The user's private key to use to connect to the AWS services.
        bucket_name: str
            The name of the bucket to fetch and send data to.
        aws_region_name: str, 'us-east-1'
            The AWS ressources region name.

        Notes
        -----
        - An instance of this class can connect to only one bucket.
        Create other instances to connect to multiple buckets at once.
        - The ``/tmp`` folder should be a folder that can be completely erased.
        - The `key` kwarg usually refers to a cloud path.
        - The `path` kwarg usually refers to a local path.

        Exemple
        -------
        >>> transcribe_api = AWSTranscribe(KEY_ID, KEY, 'my_bucket_name', 'eu-west-1')
        """

        super().__init__()

        self.transcribe_client = self._create_client(aws_region_name='transcribe', 
                                                     aws_access_key_id=aws_access_key_id, 
                                                     aws_secret_access_key=aws_secret_access_key, 
                                                     region_name=aws_region_name)
        self.bucket_name = bucket_name

        return None 
    

    def _map_language(self, language: str = None):
        """
        Maps a language into its AWS code. See supported languages below.

        Parameters
        ----------
        language: str, None
            The language name. Supported languages are: 
            ``'english'``, ``'french'``.
        
        Returns
        -------
        mapping[language]: str, 'en-US'
            The mapped language code. Errors default to english.
        """

        mapping = {
            "english": "en-US",
            "french": "fr-FR"
        }

        if language is None:
            return "en-US"
        else:
            try:
                return mapping[language]
            except:
                print(f"AWSTranscribe >> Error when mapping unsupported language '{language}'. Defaulting to 'en-US' (english).")
                return "en-US"


    def transcribe(self, key: str, language: str = None):
        """
        Calls an AWS Transcribe job to transcribe a mp3 file located on a S3 bucket.
        # TODO: thread the function 

        Parameters
        ----------
        key: str
            The key of the file on the S3.
        """

        job_name = f"ti-ragbot-{time.time()}"
        job_uri = f"s3://{self.bucket_name}/{key}"

        MediaFormat = os.path.splitext(job_uri)[-1][1:] # retreives .ext from key and removes the dot


        if language is not None:
            response = self.transcribe_client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': job_uri},
                MediaFormat=MediaFormat,
                IdentifyLanguage=False,
                LanguageCode=self._map_language(language)
            )
        else:
            response = self.transcribe_client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': job_uri},
                MediaFormat=MediaFormat,
                IdentifyLanguage=True
            )

        # Check the status of the transcription job
        while True:
            status = self.transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
            if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                break
            # Sleep to reduce API calls
            time.sleep(1)

        # AWS keeps a remote transcription history by default
        self.transcribe_client.delete_transcription_job(TranscriptionJobName=job_name)

        if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
            transcript_url = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
            response = requests.get(transcript_url)
            return response.json()
        return {}
    
        # self.transcribe_client.delete_transcription_job(TranscriptionJobName=job_name)
        # return {}
