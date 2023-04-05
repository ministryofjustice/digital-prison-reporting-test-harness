import sys
import os
sys.path.append(os.pardir)
from utils.handles import get_payload
from utils.Kinesis import Kinesis
from utils.Config import getConfig
from utils.S3 import S3
from utils.Athena import Athena

# Arrange

profileName = getConfig("aws", "profile")
stream = Kinesis(getConfig("kinesis", "stream"))
bucketName= getConfig("s3","bucket")
tableName="offenders"
payload=get_payload(tableName)
zone_repo=s3handle=S3(profilename=profileName,bucketName=bucketName)
athena_table = Athena(profileName, 'dpr-320-test',
                        "dpr-304-test", "athena_test")
query = 'SELECT count(*) FROM "{}"."{}"'.format(
    "dpr-320-test", "offenders")


def test_post_message_to_stream():
    response = stream.send_stream(data=payload, profileName=profileName)
    assert response["ResponseMetadata"]["HTTPStatusCode"]  ==  200
    
def test_raw_zone_source_exists():
  assert zone_repo.doesSourceExist("nomis") == True
    
def test_structured_zone_source_exists():
  assert zone_repo.doesSourceExist("nomis") == True  
    
def test_curated_zone_source_exists():
  assert zone_repo.doesSourceExist("nomis") == True
  
def test_raw_zone_table_exists():
  zone_repo.doesTableExist("nomis",tableName)
    
def test_structured_table_exists():
  zone_repo.doesTableExist("nomis",tableName,"structured")   
    
def test_curated_table_exists():
  zone_repo.doesTableExist("nomis",tableName,"curated") 
  
def test_curated_data_exists():
  assert len(athena_table.runQuery(query)) > 0