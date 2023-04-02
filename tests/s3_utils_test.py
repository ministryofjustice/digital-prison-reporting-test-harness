import sys,os
sys.path.append(os.pardir)
from utils.S3 import S3
from utils.Config import getConfig

profileName=getConfig("aws","profile")
bucketName=getConfig("s3","bucket")

s3handle=S3(profilename=profileName,bucketName=bucketName)

result=s3handle.getKeysFromTable("nomis","offenders","raw")

print(result)


