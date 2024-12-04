# PyYel-CloudOps

The PyYel Cloud Operations extension. This repository regroups tools to simplify and speed up Python application deployment process.

## Quick start
1. Clone the repository and Install the library.

``` bash
your_path/PyYel-CloudOps> pip install .
```

2. Import the library into you code.

``` python
import pylcloud
```

3. Import the relevant features.

``` python
from pylcloud.s3 import S3Client
from pylcloud.elasticsearch import ElasticsearchClient
```

## Content

The content of pylcloud. Unless specified diferently, all the modules may be directly imported into Python code as libraries.

### S3

A high-level API that simplifies AWS S3 API calls.

### Bedrock

A high-level API that simplifies AWS Bedrock API calls.

### Elasticsearch

A high-level API that simplifies Elasticsearch API calls. Offers deployment tools and help too.

### MySQL

A high-level API that simplifies MySQL API calls. Offers deployment tools and help too.

## Note

See also [***PyYel-DevOps***](https://github.com/PyYel/PyYel-DevOps) and [***PyYel-MLOps***](https://github.com/PyYel/PyYel-MLOps) for development and AI tools.