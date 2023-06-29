import sys
import os
from utils.Handles import get_payload
from utils.Kinesis import Kinesis
from utils.Config import getConfig
from utils.S3 import S3
from utils.Athena import Athena
from utils.DataGenerator import get_data

# Arrange

profileName = getConfig("aws", "profile")
stream = Kinesis(getConfig("kinesis", "stream"))
rawBucketName= getConfig("s3","raw_zone_bucket")
structuredBucketName= getConfig("s3","structured_zone_bucket")
curatedBucketName= getConfig("s3","curated_zone_bucket")
tableName="offenders"
output_bucket=getConfig("s3","output_bucket")
payload=get_data("OFFENDERS","HAPPY_PATH")

zone_repo_raw=s3handle=S3(profilename=profileName,bucketName=rawBucketName)
zone_repo_structured=s3handle=S3(profilename=profileName,bucketName=structuredBucketName)
zone_repo_curated=s3handle=S3(profilename=profileName,bucketName=curatedBucketName)
athena_table = Athena(profileName, 'curated',
                        "dpr-artifact-store-development", "dpr-artifact-store-development")
query = 'SELECT count(*) FROM "{}"."{}"'.format(
    "curated", "oms_owner_offenders")


def test_post_message_to_stream():
    response = stream.send_stream(data=payload, profileName=profileName)
    assert response.find("Status_Code :: 200") != -1
    
def test_raw_zone_source_exists():
  
  assert zone_repo_raw.doesSourceExist("nomis") == True
    
def test_structured_zone_source_exists():
  
  assert zone_repo_structured.doesSourceExist("nomis") == True  
   
def test_curated_zone_source_exists():
  assert zone_repo_curated.doesSourceExist("nomis") == True
  
def test_raw_zone_table_exists():
  zone_repo_curated.doesTableExist("nomis",tableName)
    
def test_structured_table_exists():
  zone_repo_structured.doesTableExist("nomis",tableName)   
    
def test_curated_table_exists():
  zone_repo_curated.doesTableExist("nomis",tableName) 
  
def test_curated_data_exists():
  assert len(athena_table.runQuery(query)) > 0
  