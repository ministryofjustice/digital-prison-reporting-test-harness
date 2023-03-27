import json, uuid, boto3, base64
from boto3 import Session
from urllib.parse import urlparse

class s3utils :
    
    def __init__(self, profilename,bucketName):
        self.profileName = profilename
        self.bucketName=bucketName

    def _connected_client(self):
        
        """ Connect to S3 """
        session = Session(profile_name=self.profileName)
        return session.resource('s3')
    
    def doesSourceExist(self,sourceName,resourceZone="raw"):
        resourceList=[]
        bucketName=self._connected_client().Bucket(self.bucketName)
        # "s3://dpr-297-raw-zone/raw/{}".format(sourceName)
        resourcePath= "s3://{}/{}/{}/".format(bucketName.name,resourceZone,sourceName)
        print(resourcePath)            
        parsed_uri=urlparse(resourcePath)
        resourcePath=parsed_uri.path.lstrip('/')
        print("RESOURCE PATH", resourcePath)
        objects=bucketName.objects.filter(Prefix=resourcePath)
        objects=bucketName.objects.filter(Prefix=resourcePath)
        for obj in objects:
                resourceList.append(obj.key)
        isFolderPresent=[resourcePath in item for item in resourceList]
        return True if isFolderPresent else False
    
    def doesTableExist(self,sourceName,tableName,resourceZone="raw"):
        resourceList=[]
        
        bucketName=self._connected_client().Bucket(self.bucketName)
        # "s3://dpr-297-raw-zone/raw/{}".format(sourceName)
        
        resourcePath= "s3://{}/{}/{}/{}/".format(bucketName.name,resourceZone,sourceName,tableName)
        print(resourcePath)            
        parsed_uri=urlparse(resourcePath)
        resourcePath=parsed_uri.path.lstrip('/')
        print("RESOURCE PATH", resourcePath)
        objects=bucketName.objects.filter(Prefix=resourcePath)
        for obj in objects:
                resourceList.append(obj.key)
        isFolderPresent=[resourcePath in item for item in resourceList]
        return True if isFolderPresent else False

    def getKeysFromS3RawBucket(self,operation,sourceName,tableName):
        bucket=self._connected_client().Bucket(self.bucketName)
        folderPath= "raw/{}/".format(sourceName)
        tablePath="raw/{}/{}/load/".format(sourceName,tableName)
        
        for obj in bucket.objects.all():
            if(obj.key.endswith(folderPath)):
                print('Folder',obj.key)
            else:
                print('File',obj.key)
                
                    
    
