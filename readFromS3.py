import os
import boto3

# References:
#https://realpython.com/python-boto3-aws-s3/

AWS_ACCESS_KEY_ID = os.environ['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = os.environ['aws_secret_access_key']


# Read from S3
BUCKET_NAME='datasample1'
KEY='blood_data_cleaned.csv'

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)
s3_resource = session.resource('s3')
my_bucket = s3_resource.Bucket(BUCKET_NAME) #subsitute this for your s3 bucket name.


files = list(my_bucket.objects.filter(Prefix=KEY))
file = files[0]
print(file.key)
body=file.get()['Body'].read()
# print(body)
# bytes to string
contents=body.decode("utf-8")
print(contents)

