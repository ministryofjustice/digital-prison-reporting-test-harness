from boto3 import Session

session = Session(profile_name='hari_profile')
s3 = session.resource('s3')
for bucket in s3.buckets.all():
    print(bucket.name)