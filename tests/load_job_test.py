import sys
import os
from utils.Handles import get_payload
from utils.Kinesis import Kinesis
from utils.Config import getConfig
from utils.S3 import S3
from utils.Athena import Athena

# Arrange

profileName = getConfig("aws", "profile")
stream = Kinesis(getConfig("kinesis", "stream"))
rawBucketName= getConfig("s3","raw_zone_bucket")
structuredBucketName= getConfig("s3","structured_zone_bucket")
curatedBucketName= getConfig("s3","curated_zone_bucket")
tableName="offenders"
payload=get_payload(tableName)
zone_repo_raw=s3handle=S3(profilename=profileName,bucketName=rawBucketName)
zone_repo_structured=s3handle=S3(profilename=profileName,bucketName=structuredBucketName)
zone_repo_curated=s3handle=S3(profilename=profileName,bucketName=curatedBucketName)
athena_table = Athena(profileName, 'dpr-320-test',
                        "dpr-304-test", "athena_test")
query = 'SELECT count(*) FROM "{}"."{}"'.format(
    "dpr-320-test", "offenders")


def test_post_message_to_stream():
    response = stream.send_stream(data=payload, profileName=profileName)
    assert response["ResponseMetadata"]["HTTPStatusCode"]  ==  200
    
def test_raw_zone_source_exists():
  
  assert zone_repo_raw.doesSourceExist("nomis") == True
    
def test_structured_zone_source_exists():
  assert zone_repo_structured.doesSourceExist("nomis") == True  
    
def test_curated_zone_source_exists():
  assert zone_repo_curated.doesSourceExist("nomis") == True
  
def test_raw_zone_table_exists():
  zone_repo_curated.doesTableExist("nomis",tableName)
    
def test_structured_table_exists():
  zone_repo_structured.doesTableExist("nomis",tableName,"structured")   
    
def test_curated_table_exists():
  zone_repo_curated.doesTableExist("nomis",tableName,"curated") 
  
def test_curated_data_exists():
  assert len(athena_table.runQuery(query)) > 0