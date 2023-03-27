import boto3
import pyarrow.parquet as pq

aws_profile="hari_profile"
s3_uri="s3://dpr-183-structured-development/nomis/offender_bookings/part-00000-40d7239a-66d2-4f8f-9ef6-bf7f984150ae-c000.snappy.parquet"

s3_bucket=s3_uri.split('/')[2]
s3_key='/'.join(s3_uri.split('/')[3:])

session= boto3.Session(profile_name=aws_profile)

s3=session.resource('s3')

obj=s3.Object(s3_bucket,s3_key)
file_obj= obj.get()['Body'].read()  

table=pq.read_table(file_obj)

df=table.to_pandas()



# print(s3_bucket)
# print(s3_key)
