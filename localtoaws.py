# import boto3
# import logging
# # AWS S3 configuration
# AWS_ACCESS_KEY = ""
# AWS_SECRET_KEY = ""
# AWS_S3_BUCKET_NAME = ""
# AWS_REGION = "us-east-1"
# LOCAL_FILE = 'example.txt'
# NAME_FOR_S3 = 'example.txt'
# # Setup logger
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("S3Uploader")
# def main():
#     logger.info('Starting the S3 upload script')
#     print('Starting the S3 upload script')
#     logger.info('Creating S3 client')
#     print('Creating S3 client')
#     s3_client = boto3.client(
#         service_name='s3',
#         region_name=AWS_REGION,
#         aws_access_key_id=AWS_ACCESS_KEY,
#         aws_secret_access_key=AWS_SECRET_KEY
#     )
#     logger.info('Uploading file to S3')
#     print('Uploading file to S3')
#     s3_client.upload_file(LOCAL_FILE, AWS_S3_BUCKET_NAME, NAME_FOR_S3)
#     logger.info('Successfully uploaded the file to S3')
#     print('Successfully uploaded the file to S3')
# if __name__ == '__main__':
#     main()
# working code




# import boto3
# import pandas as pd
# from io import StringIO
# # AWS S3 configuration
# AWS_ACCESS_KEY = ''
# AWS_SECRET_KEY = "/"
# AWS_S3_BUCKET_NAME = ''
# AWS_REGION = 'us-east-1'
# INPUT_FOLDER = 'input/'
# OUTPUT_FOLDER = 'output/'
# FILE_NAME = 'MOCK_DATA.csv'
# def read_csv_from_s3(access_key, secret_key, region, bucket_name, folder, file_name):
#     s3_client = boto3.client(
#         's3',
#         aws_access_key_id=access_key,
#         aws_secret_access_key=secret_key,
#         region_name=region
#     )
#     response = s3_client.get_object(Bucket=bucket_name, Key=folder + file_name)
#     status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
#     if status == 200:
#         print(f"Successfully fetched file from S3: {file_name}")
#         csv_content = response['Body'].read().decode('utf-8')
#         return pd.read_csv(StringIO(csv_content))
#     else:
#         print(f"Failed to fetch file from S3. Status - {status}")
#         return None
# def upload_csv_to_s3(access_key, secret_key, region, bucket_name, folder, file_name, dataframe):
#     csv_buffer = StringIO()
#     dataframe.to_csv(csv_buffer, index=False)
#     s3_resource = boto3.resource(
#         's3',
#         aws_access_key_id=access_key,
#         aws_secret_access_key=secret_key,
#         region_name=region
#     )
#     s3_resource.Object(bucket_name, folder + file_name).put(Body=csv_buffer.getvalue())
#     print(f"Successfully uploaded file to S3: {folder + file_name}")
# def main():
#     df = read_csv_from_s3(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, AWS_S3_BUCKET_NAME, INPUT_FOLDER, FILE_NAME)
#     if df is not None:
#         # Add a new column to the dataframe
#         df['NewColumn'] = 'NewValue'
#         # Save to the output folder in the same bucket
#         upload_csv_to_s3(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, AWS_S3_BUCKET_NAME, OUTPUT_FOLDER, FILE_NAME, df)
#     else:
#         print("No dataframe to process")
# if __name__ == '__main__':
#     main()
### working code











#
#
#
# import boto3
# import json
# import logging
#
# # Setup logger
# logger = logging.getLogger("SecretsManagerLogger")
# logger.setLevel(logging.INFO)
#
# # Define the ARN of the secret
# SECRET_ARN = 'https://signin.aws.amazon.com/switchrole?roleName=stsroledecemberborn&account=851725359488'
# REGION_NAME = 'us-east-1'
#
# def get_secret(secret_arn, region_name):
#     # Create a Secrets Manager client
#      = boto3.client(
#         service_name='secretsmanager',
#         region_name=region_name
#     )
#
#     try:
#         # Fetch the secret value
#         response = client.get_secret_value(
#             SecretId=secret_arn
#         )
#         secret = response['SecretString']
#         return json.loads(secret)
#     except Exception as e:
#         logger.error(f"Error retrieving secret: {e}", exc_info=True)
#         return None
#
# def main():
#     secret_value = get_secret(SECRET_ARN, REGION_NAME)
#     if secret_value:
#         logger.info(f"Secret Value: {secret_value}")
#     else:
#         logger.error("Failed to retrieve the secret.")
#
# if __name__ == '__main__':
#     main()
