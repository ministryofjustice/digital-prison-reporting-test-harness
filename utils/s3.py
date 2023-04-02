from boto3 import Session
from urllib.parse import urlparse
from datetime import datetime, timedelta

class S3:

    def __init__(self, profilename, bucketName):
        self.profileName = profilename
        self.bucketName = bucketName

    def client(self):
        session = Session(profile_name=self.profileName)
        return session.resource('s3')

    def doesSourceExist(self, sourceName, resourceZone="raw"):
        resourceList = []
        bucketName = self.client().Bucket(self.bucketName)
        # "s3://dpr-297-raw-zone/raw/{}".format(sourceName)
        resourcePath = "s3://{}/{}/{}/".format(
            bucketName.name, resourceZone, sourceName)
        print(resourcePath)
        parsed_uri = urlparse(resourcePath)
        resourcePath = parsed_uri.path.lstrip('/')
        print("RESOURCE PATH", resourcePath)
        objects = bucketName.objects.filter(Prefix=resourcePath)
        objects = bucketName.objects.filter(Prefix=resourcePath)
        for obj in objects:
            resourceList.append(obj.key)
        isFolderPresent = [resourcePath in item for item in resourceList]
        return True if isFolderPresent else False

    def doesTableExist(self, sourceName, tableName, resourceZone="raw"):
        resourceList = []

        bucketName = self.client().Bucket(self.bucketName)
        # "s3://dpr-297-raw-zone/raw/{}".format(sourceName)

        resourcePath = "s3://{}/{}/{}/{}/".format(
            bucketName.name, resourceZone, sourceName, tableName)
        print(resourcePath)
        parsed_uri = urlparse(resourcePath)
        resourcePath = parsed_uri.path.lstrip('/')
        print("RESOURCE PATH", resourcePath)
        objects = bucketName.objects.filter(Prefix=resourcePath)
        for obj in objects:
            resourceList.append(obj.key)
        isFolderPresent = [resourcePath in item for item in resourceList]

        return True if isFolderPresent else False

    def getKeysFromTable(self, sourceName, tableName, resourceZone="raw"):

        keyList = []
        bucketName = self.client().Bucket(self.bucketName)

        resourcePath = "s3://{}/{}/{}/{}/".format(
            bucketName.name, resourceZone, sourceName, tableName)
        print(resourcePath)
        parsed_uri = urlparse(resourcePath)
        resourcePath = parsed_uri.path.lstrip('/')

        time_range = datetime.utcnow()-timedelta(days=20)

        print("Time Range", time_range)

        # keyList=[obj.key for obj in bucketName.objects.filter(Prefix=resourcePath) if obj.last_modified.replace(tzinfo= None) >= time_range and obj.key.endswith('parquet')]

        keyList = ["s3://{}/".format(self.bucketName)+obj.key for obj in bucketName.objects.filter(
            Prefix=resourcePath) if obj.last_modified.replace(tzinfo=None) >= time_range and obj.key.endswith('parquet')]

        return keyList

    def getKeysFromS3RawBucket(self, sourceName, tableName):
        bucket = self.client().Bucket(self.bucketName)
        folderPath = "raw/{}/".format(sourceName)
        tablePath = "raw/{}/{}/load/".format(sourceName, tableName)

        for obj in bucket.objects.all():
            if (obj.key.endswith(folderPath)):
                print('Folder', obj.key)
            else:
                print('File', obj.key)
