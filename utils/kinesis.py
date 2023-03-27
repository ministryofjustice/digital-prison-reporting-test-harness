import json, uuid, boto3, base64
from boto3 import Session


# json="{ \"data\": {}, \"metadata\": { \"timestamp\":  \"$iso_timestamp\", \"record-type\":  \"data\", \"operation\":  \"insert\", \"partition-key-type\": \"primary-key\", \"partition-key-value\":  \"OMS_OWNER.AGENCY_INTERNAL_LOCATIONS.134401\", \"schema-name\":  \"OMS_OWNER\", \"table-name\": \"AGENCY_INTERNAL_LOCATIONS\" }}"

# stream="dpr-kinesis-partitioned-ingestor-spike"

class KinesisStream :
    
    def __init__(self, stream):
        self.stream = stream

    def _connected_client(self,profileName):
        
        """ Connect to Kinesis Streams """
        session = Session(profile_name=profileName)
        return session.client('kinesis')

    def send_stream(self, data,profileName,partition_key=None):
        """
        data: python dict containing your data.
        partition_key:  set it to some fixed value if you want processing order
                        to be preserved when writing successive records.
                        
                        If your kinesis stream has multiple shards, AWS hashes your
                        partition key to decide which shard to send this record to.
                        
                        Ignore if you don't care for processing order
                        or if this stream only has 1 shard.
                        
                        If your kinesis stream is small, it probably only has 1 shard anyway.
        """

        # If no partition key is given, assume random sharding for even shard write load
        if partition_key == None:
            partition_key = str(uuid.uuid4())

        client = self._connected_client(profileName)
        return client.put_record(
            StreamName=self.stream,
            Data=data,
            PartitionKey=partition_key
        )
        
    def  read_stream(self,profileName,shardId,sequenceNumber):
     client = self._connected_client(profileName)   
     shard_iterator= client.get_shard_iterator(
         StreamName=self.stream,
         ShardId=shardId,
         ShardIteratorType='AT_SEQUENCE_NUMBER',
         StartingSequenceNumber=sequenceNumber
     )['ShardIterator']
     
     while True:
         records_response=client.get_records(ShardIterator=shard_iterator,Limit=5)
         
         records=records_response['Records']
         
         for record in records:
            #  streamData= json.loads(record['Data'])
               streamData=base64.b64decode(record['Data']).decode('UTF-8')
               if 'offender_bookings'in streamData:
                   return streamData
                #    print(streamData)
                   
         shard_iterator=records_response['NextShardIterator']    
         
              