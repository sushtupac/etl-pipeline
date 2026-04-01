import boto3

ENDPOINT = 'http://localhost:4566'
REGION = 'us-east-1'

def get_client(service):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )

def setup():
    s3 = get_client('s3')
    
    buckets = [
        'sales-raw-data',
        'sales-processed-data',
        'sales-reports'
    ]
    
    for bucket in buckets:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"Created bucket: {bucket}")
        except Exception as e:
            if 'BucketAlreadyExists' in str(e) or 'BucketAlreadyOwnedByYou' in str(e):
                print(f"Bucket already exists: {bucket}")
            else:
                raise e
    
    print("\nUploading raw data to S3...")
    s3.upload_file(
        'raw_data.csv',
        'sales-raw-data',
        'incoming/sales_2024_01.csv'
    )
    print("Uploaded raw_data.csv to s3://sales-raw-data/incoming/sales_2024_01.csv")
    
    print("\nSetup complete. Buckets ready.")
    print("Run etl.py next.")

if __name__ == '__main__':
    setup()