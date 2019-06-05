import os
import boto3 # boto3 is the name of the Python SDK for AWS

# References:
# aws s3 : https://realpython.com/python-boto3-aws-s3/
# Functions : https://www.codementor.io/kaushikpal/user-defined-functions-in-python-8s7wyc8k2


# Read from S3
def readfromS3(BUCKET_NAME, KEY):
    AWS_ACCESS_KEY_ID = os.environ['aws_access_key_id']
    AWS_SECRET_ACCESS_KEY = os.environ['aws_secret_access_key']

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_resource = session.resource('s3')
    my_bucket = s3_resource.Bucket(BUCKET_NAME)  # subsitute this for your s3 bucket name.

    files = list(my_bucket.objects.filter(Prefix=KEY))
    file_obj = files[0]
    print(file_obj.key)
    body = file_obj.get()['Body'].read()
    # print(body)
    # bytes to string
    contents = body.decode("utf-8")
    print(contents)  # as string
    # Iterate over each line
    # for line in contents.splitlines():
    #     print(line)

    lines = list(contents.splitlines());
    # print(lines[len(lines) - 1])

    # Change some data
    line_split = lines[len(lines) - 1].split(',')
    line_split[len(line_split) - 1] = "YES"
    lines[len(lines) - 1] = ",".join(line_split)
    print(lines[len(lines) - 1])
    return lines


BUCKET_NAME = 'datasample1'
KEY = 'blood_data_cleaned.csv'

lines=readfromS3(BUCKET_NAME,KEY)
print(lines)




