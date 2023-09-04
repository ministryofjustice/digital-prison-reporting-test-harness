import sys
import os
from utils.Handles import get_payload
from utils.Kinesis import Kinesis
from utils.Config import get_config
from utils.S3 import S3
from utils.Athena import Athena
from utils.DataGenerator import get_data

# Arrange

profileName = get_config("aws", "profile")
stream = Kinesis(get_config("kinesis", "stream"))
rawBucketName= get_config("s3", "raw_zone_bucket")
structuredBucketName= get_config("s3", "structured_zone_bucket")
curatedBucketName= get_config("s3", "curated_zone_bucket")
tableName="offenders"
output_bucket=get_config("s3", "output_bucket")
payload=get_data("OFFENDERS","HAPPY_PATH")

zone_repo_raw=s3handle=S3(profile_name=profileName, bucket_name=rawBucketName)
zone_repo_structured=s3handle=S3(profile_name=profileName, bucket_name=structuredBucketName)
zone_repo_curated=s3handle=S3(profile_name=profileName, bucket_name=curatedBucketName)
athena_table = Athena(profileName, 'curated',
                        "dpr-artifact-store-development", "dpr-artifact-store-development")
query = 'SELECT count(*) FROM "{}"."{}"'.format(
    "curated", "oms_owner_offenders")


def test_post_message_to_stream():
    response = stream.send_stream(data=payload, profile_name=profileName)
    assert response.find("Status_Code :: 200") != -1
    
def test_raw_zone_source_exists():
  
  assert zone_repo_raw.does_source_exist("nomis") == True
    
def test_structured_zone_source_exists():
  
  assert zone_repo_structured.does_source_exist("nomis") == True
   
def test_curated_zone_source_exists():
  assert zone_repo_curated.does_source_exist("nomis") == True
  
def test_raw_zone_table_exists():
  zone_repo_curated.does_table_exist("nomis", tableName)
    
def test_structured_table_exists():
  zone_repo_structured.does_table_exist("nomis", tableName)
    
def test_curated_table_exists():
  zone_repo_curated.does_table_exist("nomis", tableName)
  
def test_curated_data_exists():
  assert len(athena_table.run_query(query)) > 0
  