import boto3
from botocore.config import Config
from boto3.session import Session

# Configuration Parameters
aws_access_key_id="USER_KEY"
aws_secret_access_key="SECRET_KEY"
endpoint_url="http://IP_ADRESS:9020"
signature_version='s3v4'
connect_timeout=3 
read_timeout=3
retries={"max_attempts": 1}
s3 = {
    "addressing_style": "path",
}
bucket = 'bucket1'
key = "file1"

# Self signed url parameters. ClientMethod should point at the desired boto3.client method.

ClientMethod = 'get_object'
# ClientMethod = 'list_buckets'
# ClientMethod = 'put_object'
Params = {'Bucket': bucket,'Key': key}
ExpiresIn = 3600

class S3(object):

    def set_config(self, read_timeout=read_timeout, connect_timeout=connect_timeout, 
                   signature_version=signature_version, retries=retries, s3=s3 ):
        
        self.config = Config(read_timeout=read_timeout, connect_timeout=connect_timeout, 
                  signature_version=signature_version, retries=retries, s3=s3)

    def create_session(self, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key):
        self.session = Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    def create_client(self, endpoint_url=endpoint_url):
        self.client = self.session.client('s3', endpoint_url=endpoint_url, config=self.config)

    def generate_presigned_url(self, ClientMethod=ClientMethod, Params=Params, ExpiresIn=ExpiresIn):
        response = self.client.generate_presigned_url(ClientMethod=ClientMethod, Params=Params, ExpiresIn=ExpiresIn)
        print(response)

    def generate_presigned_post(self, Bucket=bucket, Key=key):
        response = self.client.generate_presigned_post(Bucket=Bucket, Key=Key, Fields=None, Conditions=None, ExpiresIn=ExpiresIn)
        print(response)

if __name__ == '__main__':

    s3Connection = S3()
    s3Connection.set_config(read_timeout=read_timeout, connect_timeout=connect_timeout, 
                  signature_version=signature_version, retries=retries, s3=s3)
    
    s3Connection.create_session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3Connection.create_client(endpoint_url=endpoint_url)

    s3Connection.generate_presigned_url(ClientMethod=ClientMethod, Params=Params, ExpiresIn=ExpiresIn)
    # s3Connection.generate_presigned_post(Bucket=bucket, Key=key)
