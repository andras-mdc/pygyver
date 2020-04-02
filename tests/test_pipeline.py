import time
import asyncio
import unittest
from pygyver.etl.dw import BigQueryExecutor
import pygyver.etl.pipeline as pl


class TestPipelineExecutor(unittest.TestCase):

    def setUp(self):
        self.bq_client = BigQueryExecutor()

    def sleep_2_sec(self, num):
        asyncio.sleep(2)
        return "ciao"

    def test_time_is_right(self):
        start_time = time.time()
        list_values = [
            {"num": "1"}, {"num": "2"}, {"num": "3"},
            {"num": "4"}, {"num": "5"}, {"num": "6"},
            {"num": "7"}, {"num": "8"}
            ]
        result = asyncio.run(
            pl.execute_parallel(
                self.sleep_2_sec,
                list_values)
            )
        end_time = time.time()
        self.assertLess(end_time-start_time, 3, "right execution time")
        self.assertEqual(len(result), 8, "all executed")

    def test_run_queries(self):
        args = [{"table_id": "table1", "dataset_id": "test", "file": "tests/sql/table1.sql"},
                {"table_id": "table2", "dataset_id": "test", "file": "tests/sql/table1.sql"}]
        asyncio.run(pl.execute_parallel(self.bq_client.create_table, args))
        self.assertTrue(
            self.bq_client.table_exists(
                table_id='table1',
                dataset_id="test"),
            "Table1 exists")
        self.assertTrue(
            self.bq_client.table_exists(
                table_id='table2',
                dataset_id="test"
                ),
            "Table2 exists")

    def test_create_tables(self):
        test_pipeline = pl.PipelineExecutor("tests/yaml/mock_run_batch.yaml")
        test_pipeline.create_tables(test_pipeline.yaml[0])
        self.assertTrue(self.bq_client.table_exists(table_id='test_run_batch_table_1',dataset_id="test"), "test_run_batch_table_1 exists")
        self.assertTrue(self.bq_client.table_exists(table_id='test_run_batch_table_2',dataset_id="test"), "test_run_batch_table_2 exists")

    def test_run_batch(self):
        test_pipeline = pl.PipelineExecutor("tests/yaml/mock_run_batch.yaml")
        test_pipeline.run_batch(test_pipeline.yaml[0])
        self.assertTrue(self.bq_client.table_exists(table_id='test_run_batch_table_1',dataset_id="test"), "test_run_batch_table_1 exists")
        self.assertTrue(self.bq_client.table_exists(table_id='test_run_batch_table_2',dataset_id="test"), "test_run_batch_table_2 exists")

    def tearDown(self):
        if self.bq_client.table_exists(table_id='table1', dataset_id='test'):
            self.bq_client.delete_table(table_id='table1', dataset_id='test')
        if self.bq_client.table_exists(table_id='table2', dataset_id='test'):
            self.bq_client.delete_table(table_id='table2', dataset_id='test')
        if self.bq_client.table_exists(table_id='test_run_batch_table_1', dataset_id='test'):
            self.bq_client.delete_table(table_id='test_run_batch_table_1', dataset_id='test')
        if self.bq_client.table_exists(table_id='test_run_batch_table_2', dataset_id='test'):
            self.bq_client.delete_table(table_id='test_run_batch_table_2', dataset_id='test')


if __name__ == '__main__':
    unittest.main()
