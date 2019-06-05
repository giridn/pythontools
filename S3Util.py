import os
import boto3 # boto3 is the name of the Python SDK for AWS

# References:
# https://docs.python.org/3/tutorial/index.html
# aws s3 : https://realpython.com/python-boto3-aws-s3/
# aws s3 official : https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-s3.html
# Functions : https://www.codementor.io/kaushikpal/user-defined-functions-in-python-8s7wyc8k2
# pycharm-intellisense-for-boto3 : https://stackoverflow.com/questions/31555947/pycharm-intellisense-for-boto3


def getS3Resource():
    AWS_ACCESS_KEY_ID = os.environ['aws_access_key_id']
    AWS_SECRET_ACCESS_KEY = os.environ['aws_secret_access_key']
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_resource = session.resource('s3')
    return s3_resource


# Download file from s3 : https://realpython.com/python-boto3-aws-s3/#downloading-a-file
def downloadFilefromS3(BUCKET_NAME, KEY):
    s3_resource = getS3Resource()
    file_local_name = f'/tmp/{KEY}'
    fileObj_s3 = s3_resource.Object(BUCKET_NAME, KEY)
    fileObj_s3.download_file(file_local_name)
    return file_local_name


# Read from S3
def readfromS3(BUCKET_NAME, KEY):
    s3_resource = getS3Resource()
    my_bucket = s3_resource.Bucket(BUCKET_NAME)  # subsitute this for your s3 bucket name.

    files = list(my_bucket.objects.filter(Prefix=KEY))
    file_obj = files[0]
    # print(file_obj.key)
    body = file_obj.get()['Body'].read()
    # print(body)

    # bytes to string
    contents = body.decode("utf-8")
    # print(contents)  # as string

    # Iterate over each line
    # for line in contents.splitlines():
    #     print(line)

    lines = list(contents.splitlines())
    # print(lines[len(lines) - 1])

    return lines


# Write to S3 : https://realpython.com/python-boto3-aws-s3/#uploading-a-file
def writetoS3(bucket_name, key, filename):
    s3_resource = getS3Resource()
    my_bucket = s3_resource.Bucket(bucket_name)
    my_bucket.upload_file(Filename=filename, Key=key)
    return


BUCKET_NAME = 'datasample1'
KEY = 'blood_data_cleaned.csv'

lines = readfromS3(BUCKET_NAME, KEY)
print(lines)

# https://docs.python.org/3/tutorial/inputoutput.html#reading-and-writing-files
file_local_name = downloadFilefromS3(BUCKET_NAME, KEY)
with open(file_local_name, 'r') as file_local:
    lines = [line.rstrip('\n') for line in file_local]
    print(lines)


# Change some data
line_split = lines[len(lines) - 1].split(',')
line_split[len(line_split) - 1] = "\"YES\""
lines[len(lines) - 1] = ",".join(line_split)
print(lines[len(lines) - 1])
with open(file_local_name, 'w') as file_local:
    lines = file_local.write('\n'.join(lines))

# file_local_name = f'/tmp/{KEY}'
writetoS3(BUCKET_NAME, KEY, file_local_name)



