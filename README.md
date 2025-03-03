# PyYel-CloudOps

The PyYel Cloud Operations extension. This repository regroups tools to simplify and speed up Python application deployment process.

1. [Quick start](#quick-start) : how to install PyYel-CloudOps
1. [Content](#content)
    - [AWS](#aws) : Amazon Web Services API tools
    - [Database](#database) : data management and servers tools
1. [Notes](#notes)

## Quick start
1. Clone the repository and Install the library.

``` bash
(your_env) your_path/PyYel-CloudOps> pip install .
```

2. Import the library into you code.

``` python
import pylcloud
```

3. Import the relevant features.

``` python
from pylcloud.aws import AWSS3
from pylcloud.database import DatabaseElasticsearch
```

## Content

The content of pylcloud. Unless specified diferently, all the modules may be directly imported into Python code as libraries.

### AWS

Tools built upon [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) for simplified interaction with the Amazon Web Service API.

|Module|Description|Type|
|------|-----------|----|
|AWSS3|Simplified AWS S3 API for any data type|Data & NoSQL|
|AWSBedrockModels|Simplified AWS Bedrock API for LLMs prompting|Generative AI|
|AWSBedrockKnowledgeBase|Simplified AWS Bedrock knowledge base (vector database) API for RAG management|AI & Generative AI|
|AWSTranscribe|Simplified AWS Transcribe API for audio transcription|Generative AI|

### Database

Tools for simplified interactions with common database technologies (SQL/NoSQL).

|Module|Description|Type|
|------|-----------|----|
|DatabaseElasticsearch|Simplified Elasticsearch API for document storage|NoSQL & AI|
|DatabaseMongoDB|TODO|NoSQL|
|DatabaseMySQL|Simplified MySQL Server API for structured database|SQL|
|DatabaseSQLite|Simplified SQLite local DB API for structured database|SQL|


## Notes

See also [***PyYel-DevOps***](https://github.com/PyYel/PyYel-DevOps) and [***PyYel-MLOps***](https://github.com/PyYel/PyYel-MLOps) for development and AI tools.