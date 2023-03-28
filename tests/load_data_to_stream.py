import json
import base64,sys,os
sys.path.append(os.pardir)
from utils.config import getConfig
from utils.kinesis import KinesisStream



# GIVEN: ARRANGE

profileName = getConfig("aws", "profile")
stream = KinesisStream(getConfig("kinesis", "stream"))

# STUB TABLE DATA

tableName = "OFFENDER_BOOKINGS"
# payloadFile = "agency_locations_happy_path.json"c
# payloadFile = "offenders_happy_path.json"
# payloadFile = "agency_internal_locations_happy_path.json"


payloadRepo_happy_path = os.pardir+'/payloads/happy_scenarios'
payloadRepo_error_scenarios = os.pardir+'payloads/error_scenarios'


for filename in os.listdir(payloadRepo_happy_path):
    if filename.startswith(tableName):
        with open(os.path.join(payloadRepo_happy_path, filename), 'r') as f:
            payload = json.dumps(json.load(f))
            base64data = base64.b64encode(payload.encode())

print("\n"+"Data source {} : Table Name {}".format("NOMIS", tableName)+"\n")

# WHEN:POST DATA TO KINESIS STREAM : ACT

for i in range(20):
    response = stream.send_stream(data=payload, profileName=profileName)
    print("Record ID -- {}  ShardId -- {}  SequenceNumber -- {}  HTTPStatusCode -- {}".format(i+1,
          response["ShardId"], response["SequenceNumber"], response["ResponseMetadata"]["HTTPStatusCode"]))

# THEN: Deltalake created in S3 RAW ZONE :ASSERT
