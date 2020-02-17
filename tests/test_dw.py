import os
print(os.getcwd())

from etl import dw 
import unittest
import json
import filecmp as cmp
from google.cloud import bigquery
from google.cloud import exceptions
import logging

class test_read_sql(unittest.TestCase):

    def test_class_read_sql(self):
        sql = dw.read_sql(
                file="tests/sql/read_sql.sql", 
                param1 = "type",
                param2 = "300",
                param3 = "shipped_date"
            )
        self.assertTrue(sql == 'select type, shipped_date from table1 where amount > 300' , "read_sql unit test")

        sql = dw.read_sql(
            file="tests/sql/read_sql.sql"
            )
        self.assertTrue(sql == 'select {param1}, {param3} from table1 where amount > {param2}' , "read_sql unit test no opt parameters")

class BigQueryExecutorDatasets(unittest.TestCase):

    def setUp(self):
        try:
            self.create_my_dataset()
        except exceptions.Conflict as e:
            logging.info(e)
        self.bq_client = dw.BigQueryExecutor()

    def create_my_dataset(self):
        client = bigquery.Client()
        dataset_id = "{}.my_dataset".format(os.environ['BIGQUERY_PROJECT'])
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)

    def tearDown(self):
        client = bigquery.Client()
        dataset_id = "{}.my_dataset".format(os.environ['BIGQUERY_PROJECT'])
        dataset = bigquery.Dataset(dataset_id)
        client.delete_dataset(dataset)

    def test_dataset_exists(self):
        self.assertTrue(self.bq_client.dataset_exists("my_dataset"),
                        "Dataset does not exists")

    def test_dataset_does_not_exists(self):
        self.assertFalse(self.bq_client.dataset_exists("my_dataset_which_does_not_exists"),
                        "Dataset exists")

class BigQueryExecutorTables(unittest.TestCase):

    def setUp(self):
        try:
            self.create_my_table()
        except exceptions.Conflict as e:
            logging.info(e)
        self.bq_client = dw.BigQueryExecutor()

    def create_my_table(self):
        client = bigquery.Client()
        dataset_id = "{}.test_bq_executor_table".format(os.environ['BIGQUERY_PROJECT'])
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        table_ref = dataset.table('my_table')
        table = bigquery.Table(table_ref)
        table = client.create_table(table)

    def test_table_exists(self):
        self.assertTrue(self.bq_client.table_exists("test_bq_executor_table", "my_table"),
                        "Table does not exists")

    def test_table_does_not_exists(self):
        self.assertFalse(self.bq_client.table_exists("test_bq_executor_table_2", "my_table_2"), "Table exists")

    def tearDown(self):
        client = bigquery.Client()
        dataset_id = "{}.test_bq_executor_table".format(os.environ['BIGQUERY_PROJECT'])
        dataset = bigquery.Dataset(dataset_id)
        table_ref = dataset.table('my_table')
        table = bigquery.Table(table_ref)

if __name__ == "__main__":
    unittest.main()