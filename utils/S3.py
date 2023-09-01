from datetime import datetime, timedelta
from urllib.parse import urlparse

from boto3 import Session


class S3:

    def __init__(self, profile_name, bucket_name):
        self.profileName = profile_name
        self.bucketName = bucket_name

    def client(self):
        session = Session(profile_name=self.profileName)
        return session.resource('s3')

    def does_source_exist(self, source_name, resource_zone="raw"):
        resource_list = []
        bucket_name = self.client().Bucket(self.bucketName)
        # s3://dpr-raw-zone-development/nomis/

        resource_path = "s3://{}/{}/".format(bucket_name.name, source_name)

        print(resource_path)
        parsed_uri = urlparse(resource_path)
        resource_path = parsed_uri.path.lstrip('/')
        objects = bucket_name.objects.filter(Prefix=resource_path)
        for obj in objects:
            resource_list.append(obj.key)
        is_folder_present = [resource_path in item for item in resource_list]
        return True if is_folder_present else False

    def does_table_exist(self, source_name, table_name):
        resource_list = []

        bucket_name = self.client().Bucket(self.bucketName)

        #  s3://dpr-raw-zone-development/nomis/agency_locations/
        resource_path = "s3://{}/{}/{}/".format(
            bucket_name.name, source_name, table_name)

        parsed_uri = urlparse(resource_path)
        resource_path = parsed_uri.path.lstrip('/')
        objects = bucket_name.objects.filter(Prefix=resource_path)
        for obj in objects:
            resource_list.append(obj.key)
        is_folder_present = [resource_path in item for item in resource_list]

        return True if is_folder_present else False

    def get_keys_from_table(self, source_name, table_name):

        bucket_name = self.client().Bucket(self.bucketName)

        resource_path = "s3://{}/{}/{}/{}/".format(
            bucket_name.name, resource_zone, source_name, table_name)
        print(resource_path)
        parsed_uri = urlparse(resource_path)
        resource_path = parsed_uri.path.lstrip('/')

        time_range = datetime.utcnow() - timedelta(days=20)

        print("Time Range", time_range)

        key_list = ["s3://{}/".format(self.bucketName) + obj.key for obj in bucket_name.objects.filter(
            Prefix=resource_path) if
                   obj.last_modified.replace(tzinfo=None) >= time_range and obj.key.endswith('parquet')]

        return key_list

    def get_keys_from_s3_raw_bucket(self, source_name, table_name):
        bucket = self.client().Bucket(self.bucketName)
        folder_path = "raw/{}/".format(source_name)

        for obj in bucket.objects.all():
            if obj.key.endswith(folder_path):
                print('Folder', obj.key)
            else:
                print('File', obj.key)
