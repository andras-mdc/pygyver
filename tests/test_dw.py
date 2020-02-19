""" DW Tests """
import os
import unittest
import logging
from google.cloud import bigquery
from google.cloud import exceptions
from etl import dw

class test_read_sql(unittest.TestCase):
    """ Test """
    def test_class_read_sql(self):
        """ Test """
        sql = dw.read_sql(
            file="tests/sql/read_sql.sql",
            param1="type",
            param2="300",
            param3="shipped_date"
        )
        self.assertEqual(
            sql,
            'select type, shipped_date from table1 where amount > 300',
            "read_sql unit test"
        )
        sql = dw.read_sql(
            file="tests/sql/read_sql.sql"
            )
        self.assertTrue(
            sql == 'select {param1}, {param3} from table1 where amount > {param2}',
            "read_sql unit test no opt parameters"
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
            self.bq_client.table_exists("test_bq_executor_table", "my_table"),
            "Table does not exists"
        )

    def test_table_does_not_exists(self):
        """ Test """
        self.assertFalse(
            self.bq_client.table_exists("test_bq_executor_table_2", "my_table_2"),
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

    # def test_execute_file(self):
    #     result = self.bq_client.execute_file(
    #         file="tests/sql/execute_file.sql",
    #         param1 = "type"
    #     )
    #     print(result)

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


if __name__ == "__main__":
    unittest.main()
