# import boto3
#
# AWS_ACCESS_KEY = ""
# AWS_SECRET_KEY = "/"
# AWS_S3_BUCKET_NAME = "december2024learning"
# AWS_REGION = "us-east-1"
#
# LOCAL_FILE = 'example.txt'
# NAME_FOR_S3 = 'example.txt'
#
# def main():
#     print('in main method')
#
#     s3_client = boto3.client(
#         service_name='s3',
#         region_name=AWS_REGION,
#         aws_access_key_id=AWS_ACCESS_KEY,
#         aws_secret_access_key=AWS_SECRET_KEY
#     )
#
#     response = s3_client.upload_file(LOCAL_FILE, AWS_S3_BUCKET_NAME, NAME_FOR_S3)
#     print(f'Upload file response: {response}')
#
# if __name__ == '__main__':
#     main()






import boto3
import json
import logging

# Setup logger
logger = logging.getLogger("SecretsManagerLogger")
logger.setLevel(logging.INFO)

# Define the ARN of the secret
SECRET_ARN = 'https://signin.aws.'
REGION_NAME = 'us-east-1'

def get_secret(secret_arn, region_name):
    # Create a Secrets Manager client
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        # Fetch the secret value
        response = client.get_secret_value(
            SecretId=secret_arn
        )
        secret = response['SecretString']
        return json.loads(secret)
    except Exception as e:
        logger.error(f"Error retrieving secret: {e}", exc_info=True)
        return None

def main():
    secret_value = get_secret(SECRET_ARN, REGION_NAME)
    if secret_value:
        logger.info(f"Secret Value: {secret_value}")
    else:
        logger.error("Failed to retrieve the secret.")

if __name__ == '__main__':
    main()


