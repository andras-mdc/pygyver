""" Module to ETL data from/to S3"""
import os
import logging
import pandas as pd
import boto3
from botocore.exceptions import ClientError

def s3_get_file_json(file_name):
    """ Gets file from S3 """
    try:
        client = boto3.client('s3')
        bucket_name = os.getenv("AWS_S3_BUCKET")
        root = os.getenv("AWS_S3_ROOT")
        path_to_file = root + file_name
        logging.info("Getting %s from %s", path_to_file, bucket_name)
        s3_object = client.get_object(
            Bucket=bucket_name,
            Key=path_to_file
        )
        s3_object_body = s3_object['Body'].read()
        s3_object_body = s3_object_body.decode()
        logging.info("Loading %s to pandas dataframe", path_to_file)
        data = []
        chunks = pd.read_json(
            s3_object_body,
            lines=True,
            chunksize=10000
        )
        for chunk in chunks:
            data.append(chunk)
        data = pd.concat(data)
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            logging.warning("File does not exist")
            data = pd.DataFrame()
        else:
            raise
    return data
