import os
import boto3 # boto3 is the name of the Python SDK for AWS

# References:
# https://docs.python.org/3/tutorial/index.html
# aws s3 : https://realpython.com/python-boto3-aws-s3/
# aws s3 official : https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-s3.html
# Functions : https://www.codementor.io/kaushikpal/user-defined-functions-in-python-8s7wyc8k2
# pycharm-intellisense-for-boto3 : https://stackoverflow.com/questions/31555947/pycharm-intellisense-for-boto3


def getS3Resource():
    aws_access_key_id = os.environ['aws_access_key_id']
    aws_secret_access_key = os.environ['aws_secret_access_key']
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    s3_resource = session.resource('s3')
    return s3_resource


# Download file from s3 : https://realpython.com/python-boto3-aws-s3/#downloading-a-file
def downloadFilefromS3(bucket_name, key):
    s3_resource = getS3Resource()
    file_local_name = f'/tmp/{KEY}'
    fileObj_s3 = s3_resource.Object(bucket_name, key)
    fileObj_s3.download_file(file_local_name)
    print(f'Downloaded {key} from bucket {bucket_name} to {file_local_name}')
    return file_local_name

# Upload file to S3 : https://realpython.com/python-boto3-aws-s3/#uploading-a-file
def uploadFiletoS3(bucket_name, key, filename):
    s3_resource = getS3Resource()
    my_bucket = s3_resource.Bucket(bucket_name)
    my_bucket.upload_file(Filename=filename, Key=key)
    print(f'Uploaded {filename} to bucket {bucket_name} as {key}')
    return

# Read from S3
def readfromS3(bucket_name, key):
    s3_resource = getS3Resource()
    print(f'Reading {key} from bucket {bucket_name}')
    my_bucket = s3_resource.Bucket(bucket_name)  # subsitute this for your s3 bucket name.

    files = list(my_bucket.objects.filter(Prefix=key))
    file_obj = files[0]
    # print(file_obj.key)
    body = file_obj.get()['Body'].read()
    # print(body)

    # bytes to string    contents = body.decode("utf-8")
    # print(contents)  # as string

    # Iterate over each line
    # for line in contents.splitlines():
    #     print(line)

    lines = list(contents.splitlines())
    # print(lines[len(lines) - 1])

    return lines


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
uploadFiletoS3(BUCKET_NAME, KEY, file_local_name)



