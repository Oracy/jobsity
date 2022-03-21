import boto3
import io
import pandas as pd

def read_csv():
    s3 = boto3.client(
        's3',
        aws_access_key_id='AKIAQS57MX5HNPBDFLHG',
        aws_secret_access_key= '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'
    )
    bucket = 'jobsitytest'
    key = 'consume/trips.csv'
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df


def move_csv():
    s3 = boto3.resource(
        's3',
        aws_access_key_id='AKIAQS57MX5HNPBDFLHG',
        aws_secret_access_key= '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'
    )
    bucket = 'jobsitytest'
    old_key = 'consume/trips.csv'
    new_key = 'done/trips.csv'
    s3.Object(bucket, new_key).copy_from(CopySource=f"{bucket}/{old_key}")
    s3.Object(bucket, old_key).delete()


def create_csv():
    aws_access_key_id='AKIAQS57MX5HNPBDFLHG',
    aws_secret_access_key= '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'
    df = pd.DataFrame()
    bucket = 'jobsitytest'
    df.to_csv(
    f"s3://{bucket}/arquiv_teste.csv",
    index=False,
    storage_options={
        "key": aws_access_key_id,
        "secret": aws_secret_access_key,
    },
)

create_csv()