import io
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import os
import logging
from dotenv import load_dotenv
from typing import Optional


logger = logging.getLogger(__name__)

load_dotenv()

aws_access_key_id: Optional[str] = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key: Optional[str] = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name: Optional[str] = os.getenv('BUCKET_NAME')
endpoint_url: Optional[str] = os.getenv('ENDPOINT_URL')
processed_tag: str = 'processedToInflux'

# Singleton class to connect to an object storage service
class ObjectStorageConnector:
    _instance: Optional['ObjectStorageConnector'] = None

    def __new__(cls, *args, **kwargs) -> 'ObjectStorageConnector':
        if not cls._instance:
            cls._instance = super(ObjectStorageConnector, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, 'initialized'):
            self.s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
            self.bucket_name = bucket_name
            self.initialized = True

    def is_healthy(self) -> bool:
        try:
            self.s3_client.list_buckets()
            # logger.info("Connected to S3")
            return True
        except NoCredentialsError:
            logger.error("Credentials not available")
            return False
        except PartialCredentialsError:
            logger.error("Incomplete credentials provided")
            return False
        
    def get_file(self, filename: str) -> io.BytesIO:
        logger.debug("Getting file")
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=filename)
            return io.BytesIO(response.get('Body').read())
        except FileNotFoundError:
            logger.error(f"The file {filename} was not found")
            return io.BytesIO()
        except NoCredentialsError:
            logger.error("Credentials not available")
            return io.BytesIO()
        except PartialCredentialsError:
            logger.error("Incomplete credentials provided")
            return io.BytesIO()
        
        
    def list_unprocessed_files(self) -> list:
        logger.debug("Listing unprocessed files")
        try:
            listResponse = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            unprocessed_files = []
            for obj in listResponse.get('Contents', []):
                filename = obj.get('Key')
                # get metadata
                metadata = self.s3_client.get_object_tagging(Bucket=self.bucket_name, Key=filename)
                tags = metadata.get('TagSet', [])
                if not any(tag.get('Key') == processed_tag for tag in tags):
                    # logger.info(f"File {filename} is unprocessed")
                    unprocessed_files.append(filename)
            return unprocessed_files
        except FileNotFoundError:
            logger.error("The bucket was not found")
            return []
        except NoCredentialsError:
            logger.error("Credentials not available")
            return []
        except PartialCredentialsError:
            logger.error("Incomplete credentials provided")
            return []
        
    def mark_file_as_processed(self, filename: str) -> None:
        logger.debug("Marking file as processed")
        try:
            # get old tags 
            metadata = self.s3_client.get_object_tagging(Bucket=self.bucket_name, Key=filename)
            tags = metadata.get('TagSet', [])
            # add new tag
            tags.append({'Key': processed_tag, 'Value': 'true'})
            self.s3_client.put_object_tagging(
                Bucket=self.bucket_name,
                Key=filename,
                Tagging={
                    'TagSet': tags
                }
            )
        except FileNotFoundError:
            logger.error(f"The file {filename} was not found")
        except NoCredentialsError:
            logger.error("Credentials not available")
        except Exception as e:
            logger.error(f"Error marking file as processed: {e}")
    
    def push_to_storage(self, file_data: io.BytesIO, file_name: str) -> None:
        logger.debug("Pushing to storage")
        try:
            self.s3_client.upload_fileobj(file_data, self.bucket_name, str(file_name))
            logger.info(f"File {file_name} uploaded to {self.bucket_name}/{file_name}")
        except FileNotFoundError:
            logger.error(f"The file {file_name} was not found")
        except NoCredentialsError:
            logger.error("Credentials not available")
        except PartialCredentialsError:
            logger.error("Incomplete credentials provided")

# Usage example:
# connector = ObjectStorageConnector()
# connector.push_to_storage('path/to/your/file.txt', b'file_data')
