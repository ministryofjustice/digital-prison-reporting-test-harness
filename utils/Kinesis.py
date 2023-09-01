import base64
import json

from boto3 import Session


class Kinesis:

    def __init__(self, stream):
        self.stream = stream

    @staticmethod
    def _connected_client(profile_name):
        session = Session(profile_name=profile_name)
        return session.client('kinesis')

    def stream_name(self):
        return self.stream

    def send_stream(self, data, profile_name, partition_key=None):

        if partition_key is None:
            # partition_key = str(uuid.uuid4())
            partition_key = json.loads(data)['metadata']['partition-key-value']
        client = self._connected_client(profile_name)
        response = client.put_record(
            StreamName=self.stream,
            Data=data,
            PartitionKey=partition_key
        )
        return "Status_Code :: {}  Shard_Id :: {} Sequence_Number :: {}".format(
            response['ResponseMetadata']['HTTPStatusCode'], response['ShardId'], response['SequenceNumber'])

    def read_stream(self, profile_name, shard_id, sequence_number):
        client = self._connected_client(profile_name)
        shard_iterator = client.get_shard_iterator(
            StreamName=self.stream,
            ShardId=shard_id,
            ShardIteratorType='AT_SEQUENCE_NUMBER',
            StartingSequenceNumber=sequence_number
        )['ShardIterator']

        while True:
            records_response = client.get_records(
                ShardIterator=shard_iterator, Limit=5)

            records = records_response['Records']

            for record in records:
                stream_data = base64.b64decode(record['Data']).decode('UTF-8')
                if 'offender_bookings' in stream_data:
                    return stream_data

            shard_iterator = records_response['NextShardIterator']
