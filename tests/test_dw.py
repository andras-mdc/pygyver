""" DW Tests """
import os
import pandas as pd
import unittest
from unittest import mock
import logging
from google.cloud import bigquery
from google.cloud import exceptions
from pygyver.etl import dw
from pygyver.etl.dw import BigQueryExecutorError

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


if __name__ == "__main__":
    unittest.main()
