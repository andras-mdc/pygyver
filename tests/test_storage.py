import unittest
import botocore
import pandas as pd
from moto import mock_s3
import pygyver.etl.storage as storage

MY_BUCKET = "my_bucket"


@mock_s3
class Test_S3Executor(unittest.TestCase):

    """ Test """
    def setUp(self):
        """ Test """
        try:
            my_s3 = storage.S3Executor()
            my_s3.resource.meta.client.head_bucket(Bucket=MY_BUCKET)
        except botocore.exceptions.ClientError:
            pass
        else:
            err = "{bucket} should not exist.".format(bucket=MY_BUCKET)
            raise EnvironmentError(err)
        my_s3.create_bucket(bucket_name=MY_BUCKET)
        my_s3.upload_file(
            file_name="tests/data/bar.json",
            bucket=MY_BUCKET,
            object_name="mock_data/bar.json"
        )
        my_s3.upload_file(
            file_name="tests/data/foo.json",
            bucket=MY_BUCKET,
            object_name="mock_data/foo.json"
        )
        my_s3.upload_file(
            file_name="tests/data/non_json.txt",
            bucket=MY_BUCKET,
            object_name="non_json.txt"
        )
        my_s3.upload_file(
            file_name="tests/json/test_s3_json_file.json",
            bucket=MY_BUCKET,
            object_name="json/test_s3_json_file.json"
        )
        my_s3.upload_file(
            file_name="tests/csv/test_s3_csv_to_df.csv",
            bucket=MY_BUCKET,
            object_name="csv/test_s3_csv_to_df.csv"
        )

    def list_objects(self):
        """ Test """
        self.s3 = storage.S3Executor()
        return self.s3.get_list()

    def tearDown(self):
        """ Test """
        my_s3_tear_down = storage.S3Executor(bucket_name=MY_BUCKET)
        for key in my_s3_tear_down.get_objects():
            key.delete()
        my_s3_tear_down.bucket.delete()

    def test_s3_executor(self):
        my_s3 = storage.S3Executor(bucket_name=MY_BUCKET)
        with open("tests/data/foo.json", "r") as file:
            file_content = file.readline()
        file.close()
        self.assertEqual(
                my_s3.get_file("mock_data/foo.json"),
                file_content,
                "create_bucket, upload_file, get_file: ok"
        )
        self.assertListEqual(
            my_s3.get_list(search='mock_data'),
            ['mock_data/bar.json', 'mock_data/foo.json'],
            "get_list: ok"
        )
        self.assertEqual(
            my_s3.ls().sort(reverse=True),
            ["mock_data", 'non_json.txt'].sort(reverse=True),
            "ls methods no parameter - ok")
        self.assertEqual(
            my_s3.ls(prefix='mock_data').sort(reverse=True),
            ['bar.json', 'foo.json'].sort(reverse=True),
            "ls methods with parameter - ok")

    def test_load_json_to_df(self):
        my_s3 = storage.S3Executor(bucket_name=MY_BUCKET)
        with open("tests/json/test_s3_json_file.json", "r") as file:
            expected_df = pd.read_json(file)
        file.close()
        self.assertTrue(
                expected_df.equals(my_s3.load_json_to_df("json/test_s3_json_file.json")),
                "load_json_to_df: ok"
        )

    def test_load_csv_to_df(self):
        my_s3 = storage.S3Executor(bucket_name=MY_BUCKET)
        with open("tests/csv/test_s3_csv_to_df.csv", "r") as file:
            expected_df = pd.read_csv(file)
        file.close()
        self.assertTrue(
                expected_df.equals(my_s3.load_csv_to_df("csv/test_s3_csv_to_df.csv")),
                "load_json_to_df without chuncksize: ok"
        )
        self.assertTrue(
                expected_df.equals(my_s3.load_csv_to_df("csv/test_s3_csv_to_df.csv", chunksize=1)),
                "load_json_to_df with chuncksize: ok"
        )


if __name__ == "__main__":
    unittest.main()
