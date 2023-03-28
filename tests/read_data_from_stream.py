import sys,os
sys.path.append(os.pardir)
from utils.kinesis import KinesisStream

# stream= KinesisStream("dpr-kinesis-partitioned-ingestor-spike")
stream= KinesisStream("dpr-297-datastream")
profileName= "hari_profile"

response=stream.read_stream(profileName="hari_profile",shardId="shardId-000000000000",   
                            sequenceNumber="49638967058681094876531550543190409781960531882977263618")
print(response)

