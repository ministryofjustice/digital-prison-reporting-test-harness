import sys,os
sys.path.append(os.pardir)
from utils.kinesis import KinesisStream
import base64,json,os

# GIVEN: ARRANGE

stream = KinesisStream("dpr-297-datastream3")
profileName = "hari_profile"
    
# STUB TABLE DATA

tableName = "OFFENDER_BOOKINGS"
# payloadFile = "agency_locations_happy_path.json"
# payloadFile = "offenders_happy_path.json"
# payloadFile = "agency_internal_locations_happy_path.json"


payloadRepo_happy_path = '/Users/hari.shanmugam/ministryofjustice/dpr_test_harness/payloads/happy_paths'

payloadRepo_error_scenarios = '/Users/hari.shanmugam/ministryofjustice/dpr_test_harness/payloads/error_scenarios'


for filename in os.listdir(payloadRepo_happy_path):
    if filename.startswith(tableName):
        with open(os.path.join(payloadRepo_happy_path, filename), 'r') as f:
            payload = json.dumps(json.load(f))
            base64data = base64.b64encode(payload.encode())

print("\n"+"Data source {} : Table Name {}".format("NOMIS",tableName)+"\n")

# WHEN:POST DATA TO KINESIS STREAM : ACT

for i in range(20):
    response = stream.send_stream(data=payload, profileName=profileName)
    print("Record ID -- {}  ShardId -- {}  SequenceNumber -- {}  HTTPStatusCode -- {}".format(i+1,response["ShardId"],response["SequenceNumber"],response["ResponseMetadata"]["HTTPStatusCode"]))

# THEN: Deltalake created in S3 RAW ZONE :ASSERT