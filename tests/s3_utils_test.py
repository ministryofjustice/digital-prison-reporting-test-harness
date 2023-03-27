import sys,os
sys.path.append(os.pardir)
from utils.s3 import s3utils
from utils.config import getConfig

profileName=getConfig("aws","profile")
bucketName=getConfig("s3","bucket")

s3handle=s3utils(profilename=profileName,bucketName=bucketName)

result= s3handle.doesTableExist("nomis","offenders","raw")

