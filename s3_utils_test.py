from utils.s3 import s3utils

s3handle=s3utils('hari_profile','dpr-297-raw-zone')

# s3handle.getKeysFromS3RawBucket("load","nomis","offenders")

# result=s3handle.doesSourceFolderExists("nomis")
# result1=s3handle.doesSourceFolderExists("offenders")

# print(result)

# result=s3handle.doesS3UriExists("s3://dpr-297-raw-zone/raw/nomis/offender_bookings1/")
  
result= s3handle.doesSourceExist("public1","raw")
 
# result= s3handle.doesTableExist("nomis","offenders","raw")

print(result)