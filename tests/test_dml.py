import os
import sys
import unittest
import logging
from pygyver.etl import dw
from google.cloud import bigquery
from google.cloud import exceptions

logger = logging.getLogger()
logger.level = logging.DEBUG

class TestExcecuteDML(unittest.TestCase):

    def setUp(self):
        self.client = bigquery.Client()
        self.bqe = dw.BigQueryExecutor()
        try:
            self.create_my_table()
        except exceptions.Conflict as exc:
            logging.info(exc)
            
    def tearDown(self):
        """ Test """
        self.bqe.delete_dataset(
            dataset_id='test_bq_executor_dml',
            delete_contents=True
        )

    def create_my_table(self):
        dataset_id = "{}.test_bq_executor_dml".format(os.environ['BIGQUERY_PROJECT'])
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        self.client.create_dataset(dataset)
        table_ref = dataset.table('test')
        schema = [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("col", "STRING", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        self.client.create_table(table)


    def test_case_insert_delete(self):
        
        query = """
        INSERT INTO test_bq_executor_dml.test
        SELECT 1, 'test'
        """

        self.bqe.execute_dml(sql=query)

        assert_sql = "select * from test_bq_executor_dml.test"

        job = self.client.query(assert_sql)
        result = job.result()

        assert len(list(result)) == 1

        query = """
        UPDATE test_bq_executor_dml.test
        SET col = 'updated'
        WHERE id = 1
        """

        self.bqe.execute_dml(sql=query)

        job = self.client.query("select col from test_bq_executor_dml.test where id = 1")
        result = job.result()

        assert list(result)[0][0] == 'updated'

        query = """
        DELETE FROM test_bq_executor_dml.test
        WHERE id = 1
        """

        self.bqe.execute_dml(sql=query)

        job = self.client.query(assert_sql)
        result = job.result()

        assert len(list(result)) == 0

    def test_update_from_file(self):
        
        self.bqe.execute_dml(file="tests/sql/execute_dml.sql")

        assert_sql = "select * from test_bq_executor_dml.test"

        job = self.client.query(assert_sql)
        result = job.result()

        assert len(list(result)) == 1
