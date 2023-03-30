from pyspark.sql import SparkSession
from delta.tables import *


spark = SparkSession.builder.appName("Test_Parquet").master("local[*]")\
    .config("spark.hadoop.fs.s3a.impl",    "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.2.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.180").getOrCreate()
    
s3_path = "s3a://dpr-320/raw/nomis/offender_bookings/load/part-00000-e4883eaf-3d6b-40f7-9179-4fa9c2332d15-c000.snappy.parquet"
spark._jsc.hadoopConfiguration().set(
    "com.amazonaws.services.s3.enableV4",  "true")
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
"com.amazonaws.auth.profile.ProfileCredentialsProvider")
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl",
                                     "org.apache.hadoop.fs.s3a.S3A")
spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "ASIA3HFAZ5PV4TIP45RG")
spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "Z//YiYxbHspPwxwb2TvgN5uiDeNJ90LDKwjgQH1N")

df = spark.read.parquet(s3_path)

print("File data",df.first())