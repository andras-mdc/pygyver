""" Module to ETL data from/to S3"""
import os
import io
import json
import boto3
import logging
import pandas as pd
from pygyver.etl.lib import s3_default_root
from botocore.exceptions import ClientError
from pygyver.etl.lib import s3_default_bucket
from pygyver.etl.lib import remove_first_slash


def s3_get_file_json(file_name):  # to be removed after the code migration in waxit
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


class S3Executor:

    def __init__(
        self,
        root=s3_default_root(),
        bucket_name=s3_default_bucket()
    ):
        # uses credentials in ~/.aws/credentials
        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')
        self.connection = self.resource.meta.client
        self.bucket_name = bucket_name
        self.root = root
        self.bucket = self.resource.Bucket(bucket_name)

    def get_object(self, file):
        """ return the object """
        try:
            logging.info("Getting %s from %s", file, self.bucket)
            obj = self.bucket.Object(file)
            return obj.get()

        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                logging.warning("File does not exist")
            else:
                raise

    def get_file(self, file):
        """ return the file """
        return self.get_object(file)['Body'].read().decode('utf8')

    def load_json_to_df(self, file):
        """  load json into a Pandas DataFrame """
        s3_object_body = self.get_file(file)
        logging.info("Loading %s to pandas dataframe", file)
        data = json.loads(s3_object_body)
        df = pd.DataFrame.from_dict(data)
        return df

    def load_csv_to_df(self, file, chunksize=None):
        """  load json into a Pandas DataFrame """
        s3_object_body = self.get_file(file)
        logging.info("Loading %s to pandas dataframe", file)
        data = []
        chunks = pd.read_csv(
            io.StringIO(s3_object_body),
            chunksize=chunksize
        )
        if chunksize is None:
            return chunks

        for chunk in chunks:            
            data.append(chunk)

        data = pd.concat(data)
        return data

    def upload_file(self, file_name, bucket=None, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        if bucket is None:
            bucket = self.bucket_name
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        try:
            self.client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def create_bucket(self, bucket_name):
        """
        Param:      path
        Returns:    the bucket's objects as a list
        """
        # session = boto3.session.Session()
        bucket_response = self.connection.create_bucket(
                Bucket=bucket_name)
        return bucket_name, bucket_response

    def get_objects(self, prefix=''):
        """
        Param:      prefix
        Returns:    the bucket's objects as a list
        """
        objs = self.bucket.objects.all()
        return list(filter(lambda x: prefix in x.key, objs))

    def ls(self, prefix=''):        
        """
        Param:      prefix
        Returns:    the list of keys that matches the search
        """

        result = self.client.list_objects(
            Bucket=self.bucket_name,
            Prefix=self.root
        )
        if prefix == '':
            deep = 0
        else:
            deep = remove_first_slash(prefix[:-1]).count("/") + 1

        dict_list = list(filter(lambda x: prefix in x['Key'], result.get('Contents', [])))
        key_list = [d['Key'] for d in dict_list if 'Key' in d]
        key_list = list(map(remove_first_slash, key_list))        
        return list(set([d.split("/")[deep] for d in key_list if deep < len(d.split("/"))]))

    def get_list(self, search=''):
        """
        Param:      search
        Returns:    the list of keys that matches the search
        """

        result = self.client.list_objects(
            Bucket=self.bucket_name,
            Prefix=self.root
        )
        dict_list = list(filter(lambda x: search in x['Key'], result.get('Contents', [])))
        return [d['Key'] for d in dict_list if 'Key' in d]

    def read_json_files(self, prefix: str):
        list_json_files = []
        objects = self.bucket.objects.filter(Prefix=prefix)
        files = [obj.key for obj in objects if obj.key.endswith(".json")]
        for file in files:
            list_json_files.append(self.get_file(file))
