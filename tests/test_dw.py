""" DW Tests """
import os
import logging
import gspread
import unittest
import pandas as pd
from unittest import mock
from pygyver.etl import dw
from google.cloud import bigquery
from google.cloud import exceptions
from pandas.testing import assert_frame_equal
from pygyver.etl.lib import bq_token_file_path
from pygyver.etl.dw import BigQueryExecutorError
from oauth2client.service_account import ServiceAccountCredentials
from pygyver.etl.storage import GCSExecutor
from pandas.testing import assert_frame_equal


def get_existing_partition_query_mock(dataset_id, table_id):
    d = {'partition_id': ["20200101", "20200102", "20200103"]}
    return pd.DataFrame(data=d)


class test_read_sql(unittest.TestCase):
    """ Test """
    def test_class_read_sql(self):        
        """ Test """
        sql = dw.read_sql(
            file="tests/sql/read_sql.sql",
            param1="type",
            param2="300",
            param3="shipped_date",
            param4='trying'
        )
        self.assertEqual(
            sql,
            'select type, shipped_date from `table1` where amount > 300',
            "read_sql unit test"
        )
        sql = dw.read_sql(
            file="tests/sql/read_sql.sql"
            )
        self.assertTrue(
            sql == 'select {param1}, {param3} from `table1` where amount > {param2}',
            "read_sql unit test no opt parameters"
        )

        with self.assertRaises(KeyError):
            dw.read_sql(
                file="tests/sql/read_sql.sql",
                partition_date='20200101'
            )

    def test_class_read_sql_with_dry_run_dataset_prefix(self):        
        """ Test """
        sql = dw.read_sql(
            file="tests/sql/read_sql_dry_run_dataset_prefix.sql",
            param1="type",
            param2="300",
            param3="shipped_date",
            param4='trying',
            dry_run_dataset_prefix='1001'
        )
        self.assertEqual(
            sql,
            'select type, shipped_date from `1001_data.table1` and `1001_data.table2` and `1001_data.table3` where amount > 300',
            "read_sql add suffix to dataset: ok"
        )

        
class BigQueryExecutorDatasets(unittest.TestCase):
    """ Test """
    def setUp(self):
        """ Test """
        try:
            self.create_my_dataset()
        except exceptions.Conflict as exc:
            logging.info(exc)
        self.bq_client = dw.BigQueryExecutor()

    def create_my_dataset(self):
        """ Test """
        client = bigquery.Client()
        dataset_id = "{}.my_dataset".format(os.environ['BIGQUERY_PROJECT'])
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)

    def tearDown(self):
        """ Test """
        client = bigquery.Client()
        dataset_id = "{}.my_dataset".format(os.environ['BIGQUERY_PROJECT'])
        dataset = bigquery.Dataset(dataset_id)
        client.delete_dataset(dataset)

    def test_dataset_exists(self):
        """ Test """
        self.assertTrue(self.bq_client.dataset_exists("my_dataset"),
                        "Dataset does not exists")

    def test_dataset_does_not_exists(self):
        """ Test """
        self.assertFalse(
            self.bq_client.dataset_exists("my_dataset_which_does_not_exists"),
            "Dataset exists"
        )

class BigQueryExecutorTables(unittest.TestCase):
    """ Test """
    def setUp(self):
        """ Test """
        try:
            self.create_my_table()
        except exceptions.Conflict as exc:
            logging.info(exc)
        self.bq_client = dw.BigQueryExecutor()

    def create_my_table(self):
        """ Test """
        client = bigquery.Client()
        dataset_id = "{}.test_bq_executor_table".format(os.environ['BIGQUERY_PROJECT'])
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        table_ref = dataset.table('my_table')
        table = bigquery.Table(table_ref)
        table = client.create_table(table)

    def test_table_exists(self):
        """ Test """
        self.assertTrue(
            self.bq_client.table_exists(
                dataset_id="test_bq_executor_table",
                table_id="my_table"
            ),
            "Table does not exists"
        )

    def test_table_does_not_exists(self):
        """ Test """
        self.assertFalse(
            self.bq_client.table_exists(
                dataset_id="test_bq_executor_table_2",
                table_id="my_table_2"
            ),
            "Table exists"
        )

    def test_initiate_table(self):
        """ Test """
        self.assertFalse(
            self.bq_client.table_exists(
                dataset_id='test_bq_executor_table',
                table_id='test'
            )
        )
        self.bq_client.initiate_table(
            dataset_id='test_bq_executor_table',
            table_id='test',
            schema_path='tests/schema/initiate_table.json'
        )
        self.assertTrue(
            self.bq_client.table_exists(
                dataset_id='test_bq_executor_table',
                table_id='test'
            )
        )

    def tearDown(self):
        """ Test """
        self.bq_client.delete_table(
            dataset_id='test_bq_executor_table',
            table_id='test'
        )

class BigQueryExecutorDatasetCreation(unittest.TestCase):
    """ Test """
    def setUp(self):
        """ Test """
        self.bq_client = dw.BigQueryExecutor()

    def tearDown(self):
        """ Test """
        client = bigquery.Client()
        dataset_id = "{}.test_bq_create_dataset".format(os.environ['BIGQUERY_PROJECT'])
        dataset = bigquery.Dataset(dataset_id)
        client.delete_dataset(dataset)

    def test_create_dataset(self):
        self.assertFalse(
            self.bq_client.dataset_exists('test_bq_create_dataset')
        )
        self.bq_client.create_dataset('test_bq_create_dataset')
        self.assertTrue(
            self.bq_client.dataset_exists('test_bq_create_dataset')
        )

class BigQueryExecutorDatasetDeletion(unittest.TestCase):
    """
    Testing different scenarios
    """
    def setUp(self):
        """ Test """
        self.bq_client = dw.BigQueryExecutor()
        self.bq_client.create_dataset('test_bq_delete_dataset')

    def test_delete_empty_dataset(self):
        self.bq_client.delete_dataset('test_bq_delete_dataset')
        self.assertFalse(
            self.bq_client.dataset_exists('test_bq_delete_dataset'),
            "Dataset was not deleted"
        )

    def test_delete_non_empty_dataset(self):
        self.bq_client.initiate_table(
            dataset_id='test_bq_delete_dataset',
            table_id='test',
            schema_path='tests/schema/initiate_table.json'
        )
        with self.assertRaises(exceptions.BadRequest):
            self.bq_client.delete_dataset('test_bq_delete_dataset')
        self.bq_client.delete_dataset(
            dataset_id='test_bq_delete_dataset',
            delete_contents=True
        )
        self.assertFalse(
            self.bq_client.dataset_exists('test_bq_delete_dataset'),
            "Dataset was not deleted"
        )

class BigQueryExecutorExecutes(unittest.TestCase):
    """ Test """
    def setUp(self):
        """ Test """
        self.bq_client = dw.BigQueryExecutor()

    def test_execute_sql(self):
        """ Test """
        result = self.bq_client.execute_sql(
            "SELECT 'test' AS value"
        )
        self.assertEqual(
            result['value'][0],
            'test'
        )

class BigQueryExecutorExecutesPatch(unittest.TestCase):
    """
    Testing different scenarios
    """
    def setUp(self):
        """ Test """
        try:
            self.create_my_dataset()
        except exceptions.Conflict as exc:
            logging.info(exc)
        self.db = dw.BigQueryExecutor()
        self.db.initiate_table(
            dataset_id='test',
            table_id='table_to_patch',
            schema_path='tests/schema/orig_table.json'
        )

    def create_my_dataset(self):
        """ Test """
        client = bigquery.Client()
        dataset_id = "{}.test".format(os.environ['BIGQUERY_PROJECT'])
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)

    def test_identity_new_fields_no_field(self):
        """ Tests that using same schema, no new field identified"""
        lf = self.db.identify_new_fields(
            dataset_id='test',
            table_id='table_to_patch',
            schema_path='tests/schema/orig_table.json'
        )
        self.assertEqual(lf, [])

    def test_identity_new_fields_2_fields(self):
        """ Tests that using another schema, 2 new fields are identified"""
        lf = self.db.identify_new_fields(
            dataset_id='test',
            table_id='table_to_patch',
            schema_path='tests/schema/table_plus_2.json'
        )
        self.assertEqual(len(lf), 2)

    def test_apply_patch_error(self):
        """ Tests that if trying to update type, apply_patch fails"""
        with self.assertRaises(exceptions.BadRequest):
            self.db.apply_patch(
                dataset_id='test',
                table_id='table_to_patch',
                schema_path='tests/schema/orig_table_error.json'
            )

    def test_apply_patch(self):
        """ Tests that apply_patch can add both descriptions and new fields"""
        old_schema = self.db.get_table_schema(
            dataset_id='test',
            table_id='table_to_patch'
        )
        self.assertEqual(len(old_schema), 2)
        self.assertEqual(old_schema[0].description, "")
        new_schema_length = self.db.apply_patch(
            schema_path='tests/schema/table_plus_2.json',
            dataset_id='test',
            table_id='table_to_patch'
        )
        new_schema = self.db.get_table_schema(
            dataset_id='test',
            table_id='table_to_patch'
        )
        self.assertEqual(new_schema_length, 4)
        self.assertEqual(new_schema[0].description, "My description")

    def tearDown(self):
        """ Test """
        self.db.delete_table(
            dataset_id='test',
            table_id='table_to_patch'
        )

class BigQueryExecutorTableCreation(unittest.TestCase):
    """
    Testing different scenarios
    """
    def setUp(self):
        """ Test """
        try:
            self.create_my_dataset()
        except exceptions.Conflict as exc:
            logging.info(exc)
        self.db = dw.BigQueryExecutor()
        if self.db.table_exists('test', 'a_table_that_does_not_exists'):
            self.db.delete_table('test', 'a_table_that_does_not_exists')
        self.db.initiate_table(
            dataset_id='test',
            table_id='test_table_creation',
            schema_path='tests/schema/initiate_table.json'
        )
        self.db.initiate_table(
            dataset_id='test',
            table_id='my_partition_table',
            schema_path='tests/schema/orig_table.json',
            partition=True,
            clustering=None
        )
        self.env = mock.patch.dict('os.environ', {'BIGQUERY_START_DATE': '2020-01-01', 'BIGQUERY_END_DATE': '2020-01-05'})

    def create_my_dataset(self):
        """ Test """
        client = bigquery.Client()
        dataset_id = "{}.test".format(os.environ['BIGQUERY_PROJECT'])
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)

    def test_set_partition_name(self):
        """ Tests that using same schema, no new field identified"""
        table_id = 'my_table'
        date = '20200101'
        partition_name = self.db.set_partition_name(table_id, date)
        self.assertEqual(
            partition_name,
            "my_table$20200101"
        )

    def test_set_partition_name_invalid_date(self):
        """ Tests that using same schema, no new field identified"""
        table_id = 'my_table'
        date = '2020-01-01'
        with self.assertRaises(ValueError):
            partition_name = self.db.set_partition_name(table_id, date)

    def test_get_existing_partition_dates(self):
        with mock.patch('pygyver.etl.dw.BigQueryExecutor.get_existing_partition_query') as mock_get_existing_partition_query:
            mock_get_existing_partition_query.side_effect = get_existing_partition_query_mock
            existing_partition_dates = self.db.get_existing_partition_dates(
                dataset_id='test',
                table_id='test_table_creation'
            )
            self.assertEqual(
                existing_partition_dates,
                ['20200101', '20200102', '20200103'],
                "Wrong existing partition dates"
            )

    def test_get_partition_dates(self):
        partition_dates = self.db.get_partition_dates(
            start_date="2020-01-01",
            end_date="2020-01-05",
            existing_dates=['20200101', '20200102', '20200103']
        )
        self.assertEqual(
            partition_dates,
            ['20200104', '20200105'],
            "Wrong partition dates"
        )

    def test_create_partition_table(self):
        with self.env:
            self.db.create_partition_table(
                dataset_id='test',
                table_id="my_partition_table",
                sql="SELECT 'Angus MacGyver' AS fullname, 2 AS age"
            )

        number_of_partitions = self.db.execute_sql(
            "SELECT FORMAT_DATE('%Y%m%d', DATE(_PARTITIONTIME)) as partition_id FROM test.my_partition_table GROUP BY 1"
        )
        self.db.delete_table(
            dataset_id='test',
            table_id="my_partition_table"
        )
        self.assertEqual(
            number_of_partitions.shape[0],
            5,
            "Wrong number of partitions created"
        )

    def test_create_table_raises_errors(self):
        with self.assertRaises(BigQueryExecutorError):
            self.db.create_table(
                dataset_id='test',
                table_id='my_normal_table',
                schema_path='tests/schema/initiate_table.json'
            )

    def test_create_table(self):
        self.db.create_table(
            dataset_id='test',
            table_id='my_normal_table',
            schema_path='tests/schema/orig_table.json',
            sql="SELECT 'Angus MacGyver' AS fullname, 2 AS age"
        )
        self.assertTrue(
            self.db.table_exists(
                dataset_id='test',
                table_id='my_normal_table'
            ),
            "Table was not created"
        )
        self.db.delete_table(
            dataset_id='test',
            table_id='my_normal_table'
        )

    def test_create_table_with_param(self):
        self.db.create_table(
            dataset_id='test',
            table_id='my_param_table',
            schema_path='tests/schema/orig_table.json',            
            file="tests/sql/sql_with_parameters.sql",
            who="'Angus MacGyver'",
        )
        self.assertTrue(
            self.db.table_exists(
                dataset_id='test',
                table_id='my_param_table'
            ),
            "Table was not created"
        )
        df1 = self.db.execute_sql("select * from test.my_param_table")
        df2 = self.db.execute_sql("SELECT 'Angus MacGyver' AS fullname, 2 AS age")       
        self.assertTrue(
            df1.equals(df2),
            "parameter for create table: ok"
        )

    def tearDown(self):
        self.db.delete_table(
            dataset_id='test',
            table_id='my_param_table'
        )
        self.db.delete_table(
            dataset_id='test',
            table_id='my_normal_table'
        )



class BigQueryLoadDataframe(unittest.TestCase):
    """ Test """

    def setUp(self):
        self.db = dw.BigQueryExecutor()
        self.db.initiate_table(
            table_id='load_dataframe',
            dataset_id='test',
            schema_path='tests/schema/test_load_dataframe.json'
        )

    def test_load_dataframe_on_existing_table(self):
        """ Test """
        data = pd.DataFrame(data={'my_date_string': ["20200101", "20200102", "20200103"]})

        self.db.load_dataframe(
            df=data,
            table_id='load_dataframe',
            dataset_id='test'
        )

        result = self.db.execute_sql(
            "SELECT * FROM test.load_dataframe"
        )

        assert_frame_equal(
            result,
            data
        )

    def test_load_dataframe_on_non_existing_table_with_schema(self):
        """ Test """
        data = pd.DataFrame(data={'my_date_string': ["20200101", "20200102", "20200103"]})

        self.db.load_dataframe(
            df=data,
            table_id='load_dataframe_non_existing_schema',
            dataset_id='test',
            schema_path='tests/schema/test_load_dataframe.json'
        )

        result = self.db.execute_sql(
            "SELECT * FROM test.load_dataframe_non_existing_schema"
        )

        assert_frame_equal(
            result,
            data
        )

    def tearDown(self):
        self.db.delete_table(
            table_id='load_dataframe',
            dataset_id='test'
        )
        self.db.delete_table(
            table_id='load_dataframe_non_existing_schema',
            dataset_id='test'
        )

class BigQueryLoadJSONfile(unittest.TestCase):
    """ Test """

    def setUp(self):
        self.db = dw.BigQueryExecutor()
        self.db.initiate_table(
            table_id='load_json_file',
            dataset_id='test',
            schema_path='tests/schema/test_load_json.json'
        )
    def test_load_json_file_on_existing_table(self):
        """ Test """

        self.db.load_json_file(
            file='tests/json/test_json_file.json',
            table_id='load_json_file',
            dataset_id='test'
        )

        result = self.db.execute_sql(
            "SELECT * FROM test.load_json_file"
        )
        data = pd.read_json('tests/json/test_json_file.json', lines=True)
        assert_frame_equal(
            result,
            data
        )

    def test_load_json_file_on_non_existing_table(self):
        """ Test """
        self.db.load_json_file(
            file='tests/json/test_json_file.json',
            table_id='load_json_file_non_existing_table',
            dataset_id='test',
            schema_path='tests/schema/test_load_json.json'
        )

        result = self.db.execute_sql(
            "SELECT * FROM test.load_json_file_non_existing_table"
        )
        data = pd.read_json('tests/json/test_json_file.json', lines=True)
        assert_frame_equal(
            result,
            data
        )

    def test_load_json_file_on_non_existing_table_without_schema(self):
        """ Test """
        with self.assertRaises(Exception):
            self.db.load_json_file(
                file='tests/json/test_json_file.json',
                table_id='load_json_non_existing_schema',
                dataset_id='test'
                )

    def tearDown(self):
        self.db.delete_table(
            table_id='load_json_file',
            dataset_id='test'
        )
        self.db.delete_table(
            table_id='load_json_file_non_existing_table',
            dataset_id='test'
        )

# class BigQueryInsertJSONRows(unittest.TestCase):
#     """ Test """
#     def setUp(self):
#         self.db = dw.BigQueryExecutor()
#         self.db.initiate_table(
#             table_id='insert_json_rows',
#             dataset_id='test',
#             schema_path='tests/schema/test_insert_json_rows.json'
#         )
#         self.data = [
#             {"name": "John", "age": 30, "car": ''},
#             {"name": "James", "age": 35, "car": 'Toyota'}
#         ]

#     def test_insert_json_rows(self):
#         """ Test """
#         self.assertEqual(
#             self.db.execute_sql(
#                 "select count(*) AS row_count FROM test.insert_json_rows"
#             )['row_count'][0],
#             0
#         )
#         self.db.insert_rows_json(
#             dataset_id='test',
#             table_id='insert_json_rows',
#             rows=self.data
#         )
#         self.assertEqual(
#              self.db.execute_sql(
#                 "select count(*) AS row_count FROM test.insert_json_rows"
#             )['row_count'][0],
#             2
#         )
        
#     def tearDown(self):
#         self.db.delete_table(
#             dataset_id='test',
#             table_id='insert_json_rows'      
#         )  

class BigQueryLoadJSONData(unittest.TestCase):
    """ Test """
    def setUp(self):
        self.db = dw.BigQueryExecutor()
        self.db.initiate_table(
            table_id='load_json_flat',
            dataset_id='test',
            schema_path='tests/schema/test_load_json_flat.json'
        )
        self.db.initiate_table(
            table_id='load_json_nested',
            dataset_id='test',
            schema_path='tests/schema/test_load_json_nested.json'
        )
        self.data_flat = [{"name": "John", "age": 30, "car": ''},
                     {"name": "James", "age": 35, "car": 'Toyota'}]
        self.data_nested = [
                {
                    "name": "John",
                    "age": 30,
                    "cars": [{
                        "car": 'Toyota',
                        "year": 2003},
                        {
                        "car": "BMW",
                        "year": 2010}]
                },
                {
                    "name": "Jane",
                    "age": 35,
                    "cars": [{
                        "car": 'Fiat',
                        "year": 2012},
                        {
                        "car": "Kia",
                        "year": 2015}]
                }
            ]

    def test_load_json_data_on_existing_flat_table(self):
        """ Test """
        old_schema = self.db.get_table_schema(
            table_id='load_json_flat',
            dataset_id='test'
        )
        self.db.load_json_data(
            json=self.data_flat,
            table_id='load_json_flat',
            dataset_id='test'
        )
        new_schema = self.db.get_table_schema(
            table_id='load_json_flat',
            dataset_id='test'
        )
        result = self.db.execute_sql(
            "SELECT * FROM test.load_json_flat"
        )
        data = pd.DataFrame(self.data_flat)
        assert_frame_equal(
            result,
            data,
            check_like=True
        )
        self.assertNotEqual(
            old_schema,
            new_schema
        )

    def test_load_json_data_on_existing_flat_table_with_schema(self):
        """ Test """
        old_schema = self.db.get_table_schema(
            table_id='load_json_flat',
            dataset_id='test'
        )
        self.db.load_json_data(
            json=self.data_flat,
            table_id='load_json_flat',
            dataset_id='test',
            schema_path='tests/schema/test_load_json_flat.json'
        )
        new_schema = self.db.get_table_schema(
            table_id='load_json_flat',
            dataset_id='test'
        )
        self.assertEqual(
            old_schema,
            new_schema
        )

    def test_load_json_on_existing_nested_table(self):
        """ Test """

        self.db.load_json_data(
            json= self.data_nested,
            table_id='load_json_nested',
            dataset_id='test'
        )

        result = self.db.execute_sql(
            "SELECT * FROM test.load_json_nested"
        )
        data = pd.DataFrame(self.data_nested)
        result = result[result.columns.sort_values().values]
        data = data[data.columns.sort_values().values]
        assert_frame_equal(
            result,
            data
        )

    def test_load_json_on_non_existing_flat_table(self):
        """ Test """
        self.db.load_json_data(
            json=self.data_flat,
            table_id='load_json_non_existing_flat_table',
            dataset_id='test',
            schema_path='tests/schema/test_load_json_flat.json'
        )

        result = self.db.execute_sql(
            "SELECT * FROM test.load_json_non_existing_flat_table"
        )
        data = pd.DataFrame(self.data_flat)
        assert_frame_equal(
            result,
            data,
            check_like=True
        )

    def test_load_json_on_non_existing_nested_table(self):
        """ Test """
        self.db.load_json_data(
            json=self.data_nested,
            table_id='load_json_non_existing_nested_table',
            dataset_id='test',
            schema_path='tests/schema/test_load_json_nested.json'
        )

        result = self.db.execute_sql(
            "SELECT * FROM test.load_json_non_existing_nested_table"
        )
        data = pd.DataFrame(self.data_nested)
        result = result[result.columns.sort_values().values]
        data = data[data.columns.sort_values().values]
        assert_frame_equal(
            result,
            data
        )

    def test_load_json_on_non_existing_table_without_schema(self):
        """ Test """
        with self.assertRaises(Exception):
            self.db.load_json_data(
                json=self.data_flat,
                table_id='load_json_non_existing_schema',
                dataset_id='test'
                )

    def tearDown(self):
        self.db.delete_table(
            table_id='load_json_flat',
            dataset_id='test'
        )
        self.db.delete_table(
            table_id='load_json_nested',
            dataset_id='test'
        )
        self.db.delete_table(
            table_id='load_json_non_existing_flat_table',
            dataset_id='test'
        )
        self.db.delete_table(
            table_id='load_json_non_existing_nested_table',
            dataset_id='test'
        )

class BigQueryLoadGCS(unittest.TestCase):
    """ Test """

    def setUp(self):
        self.gcs = GCSExecutor()
        self.db = dw.BigQueryExecutor()
        self.data = pd.DataFrame(
            data={
                "first_name": ["Boris", "Emmanuel", "Angela"], 
                "age": [55, 42, 65]
                }
            )
        self.gcs.df_to_gcs(
            df=self.data,
            gcs_path='test-bigquery-load/test.csv'
        )
        
    def test_load_gcs_autodetect(self):
        """ Test """

        self.db.load_gcs(
            gcs_path='test-bigquery-load/test.csv',
            table_id='load_gcs',
            dataset_id='test'
        )

        result = self.db.execute_sql(
            "SELECT * FROM test.load_gcs"
        )

        assert_frame_equal(
            result,
            self.data
        )


    def test_load_gcs_schema(self):
        """ Test """

        self.db.load_gcs(
            gcs_path='test-bigquery-load/test.csv',
            table_id='load_gcs',
            dataset_id='test',
            schema_path='tests/schema/test_load_gcs.json'
        )

        result = self.db.execute_sql(
            "SELECT * FROM test.load_gcs"
        )

        assert_frame_equal(
            result,
            self.data
        )

    def tearDown(self):
        self.db.delete_table(
            table_id='load_gcs',
            dataset_id='test'
        )

class BigQueryExportGCS(unittest.TestCase):
    """ Test """

    def setUp(self):
        self.gcs = GCSExecutor()
        self.db = dw.BigQueryExecutor()
        self.data = pd.DataFrame(
            data={
                "first_name": ["Boris", "Emmanuel", "Angela"], 
                "age": [55, 42, 65]
                }
            )
        self.db.load_dataframe(
            df=self.data,
            dataset_id='test',
            table_id='export_gcs'
        )
        
    def test_extract_table_to_gcs(self):
        """ Test """
        self.db.extract_table_to_gcs(
            dataset_id='test',
            table_id='export_gcs',
            gcs_path='test-bigquery-export/test.csv'
        )

        result = self.gcs.csv_to_df(
            gcs_path='test-bigquery-export/test.csv'
        )

        assert_frame_equal(
            result,
            self.data
        )

    def tearDown(self):
        self.db.delete_table(
            dataset_id='test',
            table_id='export_gcs'
        )

class BigQueryExecutorTableTruncate(unittest.TestCase):
    """
    Test
    """
    def setUp(self):
        """ Test """
        self.bq_client = dw.BigQueryExecutor()
        self.bq_client.initiate_table(
            dataset_id='test',
            table_id='bq_truncate_table',
            schema_path='tests/schema/orig_table.json',
            partition=True,
            clustering=None
        )
        self.env = mock.patch.dict('os.environ', {'BIGQUERY_START_DATE': '2020-01-01', 'BIGQUERY_END_DATE': '2020-01-05'})
        with self.env:
            self.bq_client.create_partition_table(
                dataset_id='test',
                table_id='bq_truncate_table',
                sql="SELECT 'Angus MacGyver' AS fullname, 2 AS age"
            )

    def tearDown(self):
        """ Test """
        self.bq_client.delete_table(
            dataset_id='test',
            table_id='bq_truncate_table'
        )

    def test_truncate_table(self):
        """ Test """
        self.bq_client.truncate_table(
            dataset_id='test',
            table_id='bq_truncate_table'
        )
        truncated_table = bigquery.Client().get_table(
            self.bq_client.get_table_ref(
                dataset_id='test',
                table_id='bq_truncate_table'
            )
        )
        self.assertEqual(
            truncated_table.partitioning_type,
            'DAY'
        )
        self.assertEqual(
            len(truncated_table.schema),
            2
        )
        self.assertEqual(
            truncated_table.num_rows,
            0
        )

class BigQueryExecutorCreateTableStructure(unittest.TestCase):
    """
    Testing different scenarios
    """
    def setUp(self):
        """ Test """
        self.bq_client = dw.BigQueryExecutor()

        if not self.bq_client.dataset_exists('test'):
            self.bq_client.create_dataset('test')

        self.bq_client.create_table(
            dataset_id='test',
            table_id='bq_copy_table_source',
            schema_path='tests/schema/orig_table.json',
            sql="SELECT 'Angus MacGyver' AS fullname, 2 AS age"
        )
        self.bq_client.create_table(
            dataset_id='test',
            table_id='bq_copy_table_target',
            schema_path='tests/schema/orig_table.json',
            sql="SELECT 'Angus MacGyver' AS fullname, 2 AS age"
        )

    def test_copy_table_succeeded(self):
        self.bq_client.copy_table_structure(
            source_project_id=self.bq_client.project_id,
            source_dataset_id='test',
            source_table_id='bq_copy_table_source',
            dest_dataset_id='test',
            dest_table_id='bq_copy_table_source3'
        )
        self.assertTrue(
            self.bq_client.table_exists(
                dataset_id='test',
                table_id='bq_copy_table_source3'),
            "table structure properly copied"
        )
    
    def test_copy_table_does_not_error_and_delete_dest_table(self):
        try:
            self.bq_client.copy_table_structure(
            source_dataset_id='test',
            source_table_id='does_not_exist',
            dest_dataset_id='test',
            dest_table_id='bq_copy_table_target'
        )
        except:
            self.fail("it should not fail!")
        
        self.assertTrue(
            not self.bq_client.table_exists(
            dataset_id='test',
            table_id='bq_copy_table_target'),
            "local table deleted even though not in source project"
        )

    def tearDown(self):
        """ Test """
        self.bq_client.delete_table(
            dataset_id='test',
            table_id='bq_copy_table_source'
        )
        self.bq_client.delete_table(
            dataset_id='test',
            table_id='bq_copy_table_source_2'
        )
        self.bq_client.delete_table(
            dataset_id='test',
            table_id='bq_copy_table_source_3'
        )

class BigQueryExecutorTableCopy(unittest.TestCase):
    """
    Testing different scenarios
    """
    def setUp(self):
        """ Test """
        self.bq_client = dw.BigQueryExecutor()
        self.bq_client.create_table(
            dataset_id='test',
            table_id='bq_copy_table_source',
            schema_path='tests/schema/orig_table.json',
            sql="SELECT 'Angus MacGyver' AS fullname, 2 AS age"
        )
        self.bq_client.create_table(
            dataset_id='test',
            table_id='bq_copy_table_source_2',
            schema_path='tests/schema/table_plus_2.json',
            sql="SELECT 'Jack Dalton' AS fullname, 4 AS age, 'ABC' AS postcode"
        )

    def tearDown(self):
        """ Test """
        self.bq_client.delete_table(
            dataset_id='test',
            table_id='bq_copy_table_source'
        )
        self.bq_client.delete_table(
            dataset_id='test',
            table_id='bq_copy_table_source_2'
        )
        self.bq_client.delete_dataset(
            'test_bq_copy_table',
            delete_contents=True
        )

    def test_copy_table(self):
        """ Test """
        self.assertFalse(
            self.bq_client.dataset_exists('test_bq_copy_table')
        )
        self.bq_client.copy_table(
            source_project_id=os.environ['BIGQUERY_PROJECT'],
            source_dataset_id='test',
            source_table_id='bq_copy_table_source_2',
            dest_dataset_id='test_bq_copy_table',
            dest_table_id='dest'
        )
        self.assertTrue(
            self.bq_client.table_exists(
                dataset_id='test_bq_copy_table',
                table_id='dest'
            )
        )
        self.bq_client.copy_table(
            source_project_id=os.environ['BIGQUERY_PROJECT'],
            source_dataset_id='test',
            source_table_id='bq_copy_table_source',
            dest_dataset_id='test_bq_copy_table',
            dest_table_id='dest'
        )
        data = self.bq_client.execute_sql("SELECT fullname FROM `test_bq_copy_table.dest`")
        self.assertEqual(data['fullname'][0], "Angus MacGyver")

class BigQueryExecutorListFunctions(unittest.TestCase):
    """ Test """
    def setUp(self):
        """ Test """
        self.bq_client = dw.BigQueryExecutor()

    def tearDown(self):
        """ Test """
        self.bq_client.delete_dataset(
            'test_bq_list_functions',
            delete_contents=True
        )

    def test_list_datasets(self):
        datasets = self.bq_client.list_datasets()
        datasets.append('test_bq_list_functions')
        expected_outcome = sorted(datasets)

        self.bq_client.create_dataset('test_bq_list_functions')
        self.assertEqual(
            self.bq_client.list_datasets(),
            expected_outcome
        )

    def test_list_tables(self):
        self.bq_client.create_dataset('test_bq_list_functions')
        self.bq_client.initiate_table(
            dataset_id='test_bq_list_functions',
            table_id='bq_list_tables_Z',
            schema_path='tests/schema/initiate_table.json'
        )
        self.bq_client.initiate_table(
            dataset_id='test_bq_list_functions',
            table_id='bq_list_tables_A',
            schema_path='tests/schema/initiate_table.json'
        )
        self.assertEqual(
            self.bq_client.list_tables('test_bq_list_functions'),
            ['bq_list_tables_A', 'bq_list_tables_Z']
        )

class BigQueryExecutorCheckDQ(unittest.TestCase):
    """ Test """
    def setUp(self):
        """ Test """
        self.bq_client = dw.BigQueryExecutor()
        self.bq_client.create_table(
            dataset_id='test',
            table_id='bq_check_dq',
            sql="""
                SELECT 'spam' AS col1, 'ham' AS col2 UNION ALL
                SELECT 'spam', 'eggs' UNION ALL
                SELECT 'ham', 'eggs'
            """
        )

    def tearDown(self):
        """ Test """
        self.bq_client.delete_table(
            dataset_id='test',
            table_id='bq_check_dq'
        )

    def test_check_dq(self):
        """ Test """
        self.assertEqual(
            self.bq_client.count_rows(
                dataset_id='test',
                table_id='bq_check_dq'
            ),
            3
        )
        self.assertEqual(
            self.bq_client.count_columns(
                dataset_id='test',
                table_id='bq_check_dq'
            ),
            2
        )
        self.assertEqual(
            self.bq_client.count_duplicates(
                dataset_id='test',
                table_id='bq_check_dq',
                primary_key=['col1']
            ),
            1
        )
        with self.assertRaises(AssertionError):
            self.bq_client.assert_unique(
                dataset_id='test',
                table_id='bq_check_dq',
                primary_key=['col1']
            )
        with self.assertLogs(level='WARNING'):
            self.bq_client.assert_unique(
                dataset_id='test',
                table_id='bq_check_dq',
                primary_key=['col1'],
                ignore_error=True
            )
        self.bq_client.assert_unique(
            dataset_id='test',
            table_id='bq_check_dq',
            primary_key=['col1', 'col2']
        )

class BigQueryCheckSql(unittest.TestCase):
    """ Test """
    def setUp(self):
        """ Test """
        self.bq_client = dw.BigQueryExecutor()
        
        
    def test_check_sql_output_ok(self):
        """ Test """
        cte = """`staging.table1` AS (
                    SELECT 'A001' AS id, '102461350' AS order_reference, 'sofa' AS sku UNION ALL
                    SELECT 'A003' AS id,'1600491919' AS order_reference, 'sofa' AS sku UNION ALL
                    SELECT 'A002' AS id,'1600491918' AS order_reference, 'chair' AS sku
                ),
                `staging.table2` AS (
                    SELECT  100 AS price, 'A001' AS id, UNION ALL
                    SELECT  200 AS price, 'A002' AS id UNION ALL
                    SELECT  300 AS price, 'A003' AS id 
                ),
                `expected_output` AS (
                    SELECT 'A001' AS id,'sofa' AS sku, 100 AS price, '102461350' AS order_reference UNION ALL
                    SELECT 'A002' AS id,'chair' AS sku, 200 AS price, '1600491918' AS order_reference UNION ALL
                    SELECT 'A003' AS id,'sofa' AS sku, 300 AS price, '1600491919' AS order_reference  
                )"""
        sql = """
                SELECT a.*, b.price from  `staging.table1` a LEFT JOIN `staging.table2` b on a.id = b.id
                """
        try:
            self.bq_client.assert_acceptance(
                sql=sql,
                cte=cte
            )

        except AssertionError:
            self.fail("run_checks() raised AssertionError unexpectedly!")


    def test_assert_acceptance_ko(self):
        """ Test """
        cte = """`staging.table1` AS (
                    SELECT 'A001' AS id,'102461350' AS order_reference,'sofa' AS sku UNION ALL
                    SELECT 'A002','1600491918','chair'
                ),
                `staging.table2` AS (
                    SELECT 'A001' AS id, 100 AS price UNION ALL
                    SELECT 'A002', 200
                ),
                `expected_output` AS (
                    SELECT 'A001' AS id,'102461350' AS order_reference,'sofa' AS sku, 100 AS price UNION ALL
                    SELECT 'A003','1600491918','chair', 200) """
        sql = """
                SELECT a.*, b.price from  `staging.table1` a LEFT JOIN `staging.table2` b on a.id = b.id
                """        
        with self.assertRaises(AssertionError):
            self.bq_client.assert_acceptance(
                sql=sql,
                cte=cte
            )

class BigQueryExecutorLoadGoogleSheet(unittest.TestCase):
    """ Test """
    def setUp(self):
        self.bq_client = dw.BigQueryExecutor()
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
            ]
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            bq_token_file_path(),
            self.scope
            )
        self.client = gspread.authorize(self.credentials)
        self.id = self.client.create('test_spreadsheet').id
        self.test_data = open('tests/csv/test_load_to_gs.csv', 'r').read()
        self.client.import_csv(self.id, self.test_data)

    def test_load_google_sheet_to_bigquery(self):
        self.bq_client.load_google_sheet(
            googlesheet_key=self.id,
            table_id='table1',
            dataset_id='test'
        )

        result = self.bq_client.execute_sql(
            "SELECT * FROM test.table1"
        )

        test_df = pd.read_csv('tests/csv/test_load_to_gs.csv')
        assert_frame_equal(
            result,
            test_df
        )

    def tearDown(self):
        self.bq_client.delete_table(
            dataset_id='test',
            table_id='table1'
        )


class BigQueryExecutorGetTableClusteringFields(unittest.TestCase):
    """ Test """
    def setUp(self):
        self.db = dw.BigQueryExecutor()
    
    def test_get_table_clustering_fields_table1(self):
        self.db.initiate_table(
            dataset_id='test',
            table_id='table1',
            schema_path='tests/schema/initiate_table.json'
        )
        clustering_fields = self.db.get_table_clustering_fields(
            dataset_id='test',
            table_id='table1'
        )
        self.assertIsNone(
            clustering_fields,
            "Clustering fields is not None"
        )

    def test_get_table_clustering_fields_table2(self):
        self.db.initiate_table(
            dataset_id='test',
            table_id='table2',
            schema_path='tests/schema/initiate_table.json',
            partition=True
        )
        clustering_fields = self.db.get_table_clustering_fields(
            dataset_id='test',
            table_id='table2'
        )
        self.assertIsNone(
            clustering_fields,
            "Clustering fields is not None"
        )

    def test_get_table_clustering_fields_table3(self):
        self.db.initiate_table(
            dataset_id='test',
            table_id='table3',
            schema_path='tests/schema/initiate_table.json',
            partition=True,
            clustering=['survey_id', 'language_code']
        )
        clustering_fields = self.db.get_table_clustering_fields(
            dataset_id='test',
            table_id='table3'
        )
        self.assertEqual(
            clustering_fields,
            ['survey_id', 'language_code'],
            "Clustering fields is not ['survey_id', 'language_code']"
        )

    def tearDown(self):
        self.db.delete_table(
            dataset_id='test',
            table_id='table1'
        )
        self.db.delete_table(
            dataset_id='test',
            table_id='table2'
        )
        self.db.delete_table(
            dataset_id='test',
            table_id='table3'
        )

class BigQueryExecutorGetTablePartititonType(unittest.TestCase):
    """ Test """
    def setUp(self):
        self.db = dw.BigQueryExecutor()
    
    def test_get_table_clustering_fields_table1(self):
        self.db.initiate_table(
            dataset_id='test',
            table_id='table1',
            schema_path='tests/schema/initiate_table.json'
        )
        partition_type = self.db.get_table_partitioning_type(
            dataset_id='test',
            table_id='table1'
        )
        self.assertIsNone(
            partition_type,
            "Partition Type is not None"
        )

    def test_get_table_clustering_fields_table2(self):
        self.db.initiate_table(
            dataset_id='test',
            table_id='table2',
            schema_path='tests/schema/initiate_table.json',
            partition=True
        )
        partition_type = self.db.get_table_partitioning_type(
            dataset_id='test',
            table_id='table2'
        )
        self.assertEqual(
            partition_type,
            "DAY"
        )

    def tearDown(self):
        self.db.delete_table(
            dataset_id='test',
            table_id='table1'
        )
        self.db.delete_table(
            dataset_id='test',
            table_id='table2'
        )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
