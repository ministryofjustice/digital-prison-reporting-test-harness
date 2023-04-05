import sys
import os
import json
import random
import os
sys.path.append(os.pardir)
from utils.Kinesis import Kinesis
from utils.Config import getConfig
from utils.S3 import S3
from utils.Athena import Athena

# Arrange

profileName = getConfig("aws", "profile")
stream = Kinesis(getConfig("kinesis", "stream"))
bucketName= getConfig("s3","bucket")
tableName="offenders"

def post_message_to_stream():
    Kinesis().send_stream()
    