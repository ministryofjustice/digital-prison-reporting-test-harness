import uuid
import base64
import json,os
from boto3 import Session
class Kinesis:

    def __init__(self, stream):
        self.stream = stream

    def _connected_client(self, profileName):
        session = Session(profile_name=profileName)
        return session.client('kinesis')

    def stream_name(self):
        return self.stream

    def send_stream(self, data, profileName, partition_key=None):

        if partition_key == None:
            partition_key = str(uuid.uuid4())

        client = self._connected_client(profileName)
        response= client.put_record(
            StreamName=self.stream,
            Data=data,
            PartitionKey=partition_key
        )
        return "Status_Code :: {}  Shard_Id :: {} Sequence_Number :: {}".format(response['ResponseMetadata']['HTTPStatusCode'],response['ShardId'],response['SequenceNumber'])

    def read_stream(self, profileName, shardId, sequenceNumber):
        client = self._connected_client(profileName)
        shard_iterator = client.get_shard_iterator(
            StreamName=self.stream,
            ShardId=shardId,
            ShardIteratorType='AT_SEQUENCE_NUMBER',
            StartingSequenceNumber=sequenceNumber
        )['ShardIterator']

        while True:
            records_response = client.get_records(
                ShardIterator=shard_iterator, Limit=5)

            records = records_response['Records']

            for record in records:
                streamData = base64.b64decode(record['Data']).decode('UTF-8')
                if 'offender_bookings' in streamData:
                    return streamData

            shard_iterator = records_response['NextShardIterator']