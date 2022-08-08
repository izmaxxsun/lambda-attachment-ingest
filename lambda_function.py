import boto3
import json
import base64
from elasticsearch import Elasticsearch, helpers
import urllib3

INDEX_NAME = "sandbox-s3"
class S3File:
    def __init__(self, name, body):
        self.name = name
        self.body = body

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib3.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # Connect to ES
    es = Elasticsearch(cloud_id="<CLOUD_ID_GOES_HERE>",api_key="<API_KEY_GOES_HERE")
    result = es.ping()
    print(result)

    # Index S3 file to ES
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read()
        data = base64.b64encode(body)
        b64_data = data.decode('UTF-8')
        s3_file = S3File(key, b64_data).__dict__

        print("CONTENT TYPE: " + response['ContentType'])
        print("KEY: " + key)

        index_response = es.index(
            index = INDEX_NAME,
            id=key,
            document=s3_file
        )

        print("response:", index_response)

        return index_response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e