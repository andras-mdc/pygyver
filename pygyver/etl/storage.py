""" Module to ETL data from/to S3"""
import os
import io
import json
import logging
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from botocore.exceptions import ClientError
import boto3
from pygyver.etl.lib import gcs_default_project, gcs_default_bucket,  \
    bq_token_file_valid, bq_token_file_path, s3_default_root, s3_default_bucket, remove_first_slash

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

class GCSExecutor:
    """
    GCSExecutor
    """
    def __init__(
            self,
            project_id=gcs_default_project(),
        ):

        self.client = None
        self.credentials = None
        self.project_id = project_id
        self.auth()

    def auth(self):
        """
        Authentificates using the access token
        """
        bq_token_file_valid()
        self.credentials = service_account.Credentials.from_service_account_file(
            bq_token_file_path()
        )
        self.client = storage.Client(
            credentials=self.credentials,
            project=self.project_id
        )

    def set_bucket(self,
                   gcs_bucket=gcs_default_bucket()
                  ):
        """
        Set the GCS bucket if provided. If not, set GCS_BUCKET env as GCS bucket
        """
        self.bucket = self.client.get_bucket(gcs_bucket)

    def df_to_gcs(self,
                  gcs_path,
                  df,
                  gcs_bucket=gcs_default_bucket(),
                  index=False
                 ):
        """
        Writes pandas Dataframe to csv on GCS
        gcs_path: path to file in GCS
        df: pandas Dataframe to be uploaded
        gcs_bucket: the GCS bucket, by default uses the GCS_BUCKET env
        index: keeps the Dataframe index in file
        """
        self.set_bucket(gcs_bucket=gcs_bucket)
        self.bucket.blob(gcs_path).upload_from_string(df.to_csv(index=index), 'text/csv')

    def download_file(self,
                      gcs_path,
                      file_path,
                      gcs_bucket=gcs_default_bucket()
                     ):
        """
        Downloads file from GCS
        gcs_path: path to file in GCS
        file_path: path to write to, on the local machine
        gcs_bucket: the GCS bucket, by default uses the GCS_BUCKET env
        """
        self.set_bucket(gcs_bucket=gcs_bucket)
        self.bucket.blob(gcs_path).download_to_filename(file_path)

    def upload_file(self,
                    file_path,
                    gcs_path,
                    gcs_bucket=gcs_default_bucket()
                   ):
        """
        Uploads file to GCS
        gcs_path: path write to in GCS
        file_path: path to file on the local machine
        gcs_bucket: the GCS bucket, by default uses the GCS_BUCKET env
        """
        self.set_bucket(gcs_bucket=gcs_bucket)
        self.bucket.blob(gcs_path).upload_from_filename(file_path)

    def csv_to_df(self,
                  gcs_path,
                  gcs_bucket=gcs_default_bucket()
                 ):
        """
        Reads csv from GCS to pandas Dataframe
        gcs_path: path to file in GCS
        gcs_bucket: the GCS bucket, by default uses the GCS_BUCKET env
        """
        self.set_bucket(gcs_bucket=gcs_bucket)
        self.bucket.blob(gcs_path).download_to_filename('temp_gcs_file.csv')
        df = pd.read_csv('temp_gcs_file.csv')
        os.remove("temp_gcs_file.csv")
        return df

    def list_files(self,
                   gcs_directory,
                   gcs_bucket=gcs_default_bucket()
                  ):
        """
        Returns a list of blobs in GCS directory
        gcs_directory: path to directory in GCS
        gcs_bucket: the GCS bucket, by default uses the GCS_BUCKET env
        """
        self.set_bucket(gcs_bucket=gcs_bucket)
        file_list = [file.name for file in self.bucket.list_blobs(prefix=gcs_directory)]
        return file_list

    def delete_directory(self,
                         gcs_directory,
                         gcs_bucket=gcs_default_bucket()
                        ):
        """
        Deletes all files within a GCS directory recursively
        gcs_directory: path to directory in GCS
        gcs_bucket: the GCS bucket, by default uses the GCS_BUCKET env
        """
        self.set_bucket(gcs_bucket=gcs_bucket)
        for blob in self.bucket.list_blobs(prefix=gcs_directory):
            blob.delete()
