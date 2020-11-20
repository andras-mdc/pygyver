import time
import asyncio
import logging
import unittest
from unittest import mock
from google.cloud import bigquery
from pygyver.etl.dw import BigQueryExecutor
import pygyver.etl.pipeline as pl
from pygyver.etl.lib import add_dataset_prefix
from pygyver.etl.lib import bq_default_project
from pygyver.etl.lib import bq_default_prod_project

class TestPipelineExtractuUnitTests(unittest.TestCase):

    def test_extract_unit_tests(self):
        list_batches = [
                        {
                            "batch": None,
                            "desc": "create table1 & table2 in staging",
                            "tables": [
                                {
                                "table_desc": "table1",
                                "create_table": {
                                    "table_id": "table1",
                                    "file": "tests/sql/unit_table1.sql"
                                },
                                "pk": [
                                    "col1",
                                    "col2"
                                ],
                                "mock_data": {
                                    "mock_file": "sql/unit_table1_mocked.sql"
                                }
                                },
                                {
                                "table_desc": "table2",
                                "create_table": {
                                    "table_id": "table2",
                                    "file": "tests/sql/unit_table2.sql"
                                },
                                "pk": [
                                    "col1",
                                    "col2"
                                ],
                                "mock_data": {
                                    "mock_file": "sql/unit_table2_mocked.sql"
                                }
                                }
                            ]
                            },
                            {
                            "batch": None,
                            "desc": "create table3 & table4 in staging",
                            "tables": [
                                {
                                "table_desc": "table3",
                                "create_table": {
                                    "table_id": "table3",
                                    "file": "tests/sql/unit_table3.sql"
                                },
                                "pk": [
                                    "col1",
                                    "col2"
                                ],
                                "mock_data": {
                                    "mock_file": "sql/unit_table3_mocked.sql"
                                }
                                },
                                {
                                "table_desc": "table4",
                                "create_table": {
                                    "table_id": "table4",
                                    "file": "tests/sql/unit_table4.sql"
                                },
                                "pk": [
                                    "col1",
                                    "col2"
                                ],
                                "mock_data": {
                                    "mock_file": "sql/unit_table4_mocked.sql",
                                    "output_table_name": "output_test_table"
                                }
                                }
                            ]
                            }
                        ]


        self.assertCountEqual(
            pl.extract_unit_tests(list_batches),
            [
                { "table_id": "table1", "file" : "tests/sql/unit_table1.sql", "mock_file": "sql/unit_table1_mocked.sql"},
                { "table_id": "table2", "file" : "tests/sql/unit_table2.sql", "mock_file": "sql/unit_table2_mocked.sql"},
                { "table_id": "table3", "file" : "tests/sql/unit_table3.sql", "mock_file": "sql/unit_table3_mocked.sql"},
                { "table_id": "table4", "file" : "tests/sql/unit_table4.sql", "mock_file": "sql/unit_table4_mocked.sql", "output_table_name": "output_test_table"},
            ],
            "unit tests well extracted" )


    def test_extract_unit_test_value(self):
        self.assertCountEqual(
            pl.extract_unit_test_value(
                    [
                        { "file" : "tests/sql/unit_table1.sql", "mock_file": "tests/sql/unit_table1_mocked.sql"},
                        { "file" : "tests/sql/unit_table1.sql", "mock_file": "tests/sql/unit_table1_mocked.sql", "output_table_name": "output_test_table"},
                    ]
                ),
            [
                { "sql" : "sql test 1", "cte": "mock_sql test 1",
                "file" : "tests/sql/unit_table1.sql", "mock_file": "tests/sql/unit_table1_mocked.sql"},
                { "sql" : "sql test 1", "cte": "mock_sql test 1", "output_table_name": "output_test_table",
                "file" : "tests/sql/unit_table1.sql", "mock_file": "tests/sql/unit_table1_mocked.sql", "output_table_name": "output_test_table"}
            ],
            "unit tests values well extracted" )

    def test_extract_unit_test_value_with_sql_param(self):
        self.assertCountEqual(
            pl.extract_unit_test_value(
                    [
                        { "file" : "tests/sql/sql_with_parameters.sql", "mock_file": "tests/sql/unit_table1_mocked.sql", "who": "'Angus MacGyver'"},
                        { "file" : "tests/sql/unit_table1.sql", "mock_file": "tests/sql/unit_table1_mocked.sql", "output_table_name": "output_test_table"},
                    ]
                ),
            [
                { "sql" : "SELECT 'Angus MacGyver' AS fullname, 2 AS age", "cte": "mock_sql test 1",
                "file" : "tests/sql/sql_with_parameters.sql", "mock_file": "tests/sql/unit_table1_mocked.sql",  "who": "'Angus MacGyver'"},
                { "sql" : "sql test 1", "cte": "mock_sql test 1", "output_table_name": "output_test_table",
                "file" : "tests/sql/unit_table1.sql", "mock_file": "tests/sql/unit_table1_mocked.sql", "output_table_name": "output_test_table"}
            ],
            "unit tests values well extracted" )

    def test_extract_unit_test_value_with_sql_mock_param(self):
        self.assertCountEqual(
            pl.extract_unit_test_value(
                    [
                        { "file" : "tests/sql/sql_with_parameters.sql", "mock_file": "tests/sql/unit_table1_mocked.sql", "who": "'Angus MacGyver'", "mock_who": "'Simone F'"},
                        { "file" : "tests/sql/unit_table1.sql", "mock_file": "tests/sql/unit_table1_mocked.sql", "output_table_name": "output_test_table"},
                    ]
                ),
            [
                { "sql" : "SELECT 'Simone F' AS fullname, 2 AS age", "cte": "mock_sql test 1",
                "file" : "tests/sql/sql_with_parameters.sql", "mock_file": "tests/sql/unit_table1_mocked.sql",  "who": "'Angus MacGyver'", "mock_who": "'Simone F'"},
                { "sql" : "sql test 1", "cte": "mock_sql test 1", "output_table_name": "output_test_table",
                "file" : "tests/sql/unit_table1.sql", "mock_file": "tests/sql/unit_table1_mocked.sql", "output_table_name": "output_test_table"}
            ],
            "unit tests values well extracted" )

    def test_extract_unit_test_value_with_partition(self):
        self.maxDiff = None
        res = pl.extract_unit_test_value(
                    [
                        { "file" : "tests/sql/sql_with_partition_date.sql", "mock_file": "tests/sql/unit_table1_mocked.sql", "who": "'Angus MacGyver'", "mock_partition_date": "2020-01-01"},
                        { "file" : "tests/sql/unit_table1.sql", "mock_file": "tests/sql/unit_table1_mocked.sql", "output_table_name": "output_test_table"},
                    ]
                )
        expected_result =  [
                { "sql" : "SELECT 'Angus MacGyver' AS fullname, 2 AS age, TIMESTAMP(\"2020-01-01 00:00:00\") AS _PARTITIONTIME where DATE(_PARTITIONTIME) = PARSE_DATE(\"%Y%m%d\", \'2020-01-01\')",
                'cte': 'mock_sql test 1',
                "file" : "tests/sql/sql_with_partition_date.sql", "mock_file": "tests/sql/unit_table1_mocked.sql",  "who": "'Angus MacGyver'", "mock_partition_date": "2020-01-01"},
                { "sql" : "sql test 1", "cte": "mock_sql test 1", "output_table_name": "output_test_table",
                "file" : "tests/sql/unit_table1.sql", "mock_file": "tests/sql/unit_table1_mocked.sql", "output_table_name": "output_test_table"}
            ]
        self.assertCountEqual(
            res,
            expected_result,
            "unit tests values well extracted"
        )

    def test_run_unit_tests_ok(self):
        self.p_ex = pl.PipelineExecutor("tests/yaml/unit_tests.yaml")
        try:
            self.p_ex.run_unit_tests()
        except AssertionError:
            self.fail("run_unit_tests() raised AssertionError unexpectedly!")

class TestPipelineUnitTestsErrorRaised(unittest.TestCase):

    def setUp(self):
        self.db = BigQueryExecutor()
        self.p_ex = pl.PipelineExecutor("tests/yaml/unit_tests_fail.yaml")

    def test_run_unit_tests_error(self):
        with self.assertRaises(AssertionError):
            self.p_ex.run_unit_tests()

    def tearDown(self):
        self.db.delete_table(
            table_id='table2',
            dataset_id='test'
        )
        self.db.delete_table(
            table_id='table3',
            dataset_id='test'
        )
        self.db.delete_table(
            table_id='table4',
            dataset_id='test'
        )

class TestPipelineExecutorRunBatch(unittest.TestCase):

    def setUp(self):
        self.bq_exec = BigQueryExecutor()
        self.p_ex = pl.PipelineExecutor("tests/yaml/test_dummy.yaml")
        self.bq_client = bigquery.Client()

    def test_run_batch_create_tables(self):
        batch = {
            "desc": "create table1 & table2 in staging",
            "tables":
            [
                {
                    "table_desc": "table1",
                    "create_table": {
                        "table_id": "table1",
                        "dataset_id": "test",
                        "description": "some descriptive text here",
                        "file": "tests/sql/table1.sql"
                    },
                    "pk": ["col1", "col2"],
                    "mock_data": "sql/table1_mocked.sql"
                },
                {
                    "table_desc": "table2",
                    "create_table": {
                        "table_id": "table2",
                        "dataset_id": "test",
                        "file": "tests/sql/table2.sql"
                    },
                    "pk": ["col1", "col2"],
                    "mock_data": "sql/table1_mocked.sql"
                }
            ]
        }
        self.p_ex.run_batch(batch)
        self.assertTrue(
            self.bq_exec.table_exists(
                table_id='table1',
                dataset_id="test"),
            "Tables are created")

        table_ref = self.bq_exec.get_table_ref(dataset_id='test', table_id='table1', project_id=bq_default_project())
        table = self.bq_client.get_table(table_ref)  # API request

        self.assertTrue(
            table.description == "some descriptive text here",
            "The 'description' is not the same"
        )

        self.assertTrue(
            self.bq_exec.table_exists(
                table_id='table2',
                dataset_id="test"
                ),
            "Tables are created")


    def test_run_batch_create_gs_tables(self):
        batch = {
            "desc": "load test spreadsheet into bigquery",
            "sheets": [
                {
                    "table_desc": "ref gs_test_table1",
                    "create_gs_table": {
                         "table_id": "gs_test_table1",
                        "dataset_id": "test",
                        "sheet_name": "input",
                        "googlesheet_uri": "https://docs.google.com/spreadsheets/d/19Jmapr9G1nrMcW2QTpY7sOvKYaFXnw5krK6dD0GwEqU/edit#gid=0"
                    }
                }
            ]
        }
        self.p_ex.create_gs_tables(batch)
        self.assertTrue(
            self.bq_exec.table_exists(
                table_id='gs_test_table1',
                dataset_id="test"),
            "gs_test_table1 does NOT exists")

    def test_run_batch_load_google_sheets(self):
        batch = {
            "desc": "load test spreadsheet into bigquery",
            "sheets": [
                {
                    "table_desc": "ref sheet1",
                    "load_google_sheet": {
                        "table_id": "ref_sheet1",
                        "dataset_id": "test",
                        "sheet_name": "input",
                        "googlesheet_uri": "https://docs.google.com/spreadsheets/d/19Jmapr9G1nrMcW2QTpY7sOvKYaFXnw5krK6dD0GwEqU/edit#gid=0"
                    }
                },
                {
                    "table_desc": "ref sheet2",
                    "load_google_sheet": {
                        "table_id": "ref_sheet2",
                        "dataset_id": "test",
                        "sheet_name": "input",
                        "description": "foo bar",
                        "googlesheet_uri": "https://docs.google.com/spreadsheets/d/19Jmapr9G1nrMcW2QTpY7sOvKYaFXnw5krK6dD0GwEqU/edit#gid=0"
                    }
                }
            ]
        }


        self.p_ex.load_google_sheets(batch)
        self.assertTrue(
            self.bq_exec.table_exists(
                table_id='ref_sheet1',
                dataset_id="test"),
            "ref_sheet1 does NOT exists")

        self.assertTrue(
            self.bq_exec.table_exists(
                table_id='ref_sheet2',
                dataset_id="test"),
            "ref_sheet2 does NOT exists")

        table_ref = self.bq_exec.get_table_ref(dataset_id='test', table_id='ref_sheet2', project_id=bq_default_project())
        table = self.bq_client.get_table(table_ref)  # API request

        self.assertTrue(
            table.description == "foo bar",
            "The 'description' is not the same"
        )

    def test_run_batch_create_partition_tables(self):
        # A table must be created first, only then it can be partitioned
        batch = {
            "desc": "create partition_table1",
            "tables":
                [
                    {
                        "table_desc": "creating table",
                        "create_table": {
                            "table_id": "partition_table1",
                            "dataset_id": "test",
                            "file": "tests/sql/table1.sql"
                        },
                        "pk": ["col1", "col2"],
                        "mock_data": "sql/table1_mocked.sql"
                    },
                    {
                        "table_desc": "creating partition table",
                        "create_partition_table": {
                            "table_id": "partition_table1",
                            "dataset_id": "test",
                            "description": "some descriptive text here",
                            "file": "tests/sql/table1.sql",
                            "partition_dates": []
                        },
                        "pk": ["col1", "col2"],
                        "mock_data": "sql/table1_mocked.sql"
                    }
                ]
        }

        self.p_ex.run_batch(batch)
        self.assertTrue(
            self.bq_exec.table_exists(
                table_id='partition_table1',
                dataset_id="test"),
            "Partition table is created")

        table_ref = self.bq_exec.get_table_ref(dataset_id='test', table_id='partition_table1', project_id=bq_default_project())
        table = self.bq_client.get_table(table_ref)  # API request

        self.assertTrue(
            table.description == "some descriptive text here",
            "The 'description' is not the same"
        )


    def tearDown(self):
        if self.bq_exec.table_exists(table_id='table1', dataset_id='test'):
            self.bq_exec.delete_table(table_id='table1', dataset_id='test')
        if self.bq_exec.table_exists(table_id='table2', dataset_id='test'):
            self.bq_exec.delete_table(table_id='table2', dataset_id='test')
        if self.bq_exec.table_exists(table_id='test_run_batch_table_1', dataset_id='test'):
            self.bq_exec.delete_table(table_id='test_run_batch_table_1',dataset_id='test')
        if self.bq_exec.table_exists(table_id='test_run_batch_table_2', dataset_id='test'):
            self.bq_exec.delete_table(table_id='test_run_batch_table_2', dataset_id='test')
        if self.bq_exec.table_exists(table_id='gs_test_table1', dataset_id='test'):
            self.bq_exec.delete_table(table_id='gs_test_table1', dataset_id='test')
        if self.bq_exec.table_exists(table_id='ref_sheet1', dataset_id='test'):
            self.bq_exec.delete_table(table_id='ref_sheet1', dataset_id='test')
        if self.bq_exec.table_exists(table_id='ref_sheet2', dataset_id='test'):
            self.bq_exec.delete_table(table_id='ref_sheet2', dataset_id='test')
        if self.bq_exec.table_exists(table_id='partition_table1', dataset_id='test'):
            self.bq_exec.delete_table(table_id='partition_table1', dataset_id='test')


class TestPipelineExecutorRun(unittest.TestCase):

    def setUp(self):
        self.bq_client = BigQueryExecutor()
        self.p_ex = pl.PipelineExecutor("tests/yaml/test_run.yaml")
        self.bq_client.create_dataset(dataset_id='test')

    def test_run_completed_no_error(self):

        self.p_ex.run()

        self.assertTrue(
            self.bq_client.table_exists(
                table_id='ref_sheet1',
                dataset_id="test"
            ),
            "test.ref_sheet1 exists"
        )

        self.assertTrue(
            self.bq_client.table_exists(
                table_id='ref_sheet2',
                dataset_id="test"
            ),
            "test.ref_sheet2 exists"
        )

        self.assertTrue(
            self.bq_client.table_exists(
                table_id='gs_test_table1',
                dataset_id="test"
            ),
            "test.gs_test_table1 exists"
        )

        self.assertTrue(
            self.bq_client.table_exists(
                table_id='table1',
                dataset_id="test"
            ),
            "test.table1 exists"
        )

        self.assertTrue(
            self.bq_client.table_exists(
                table_id='table2',
                dataset_id="test"
            ),
            "test.table2 exists"
        )


    def tearDown(self):
        if self.bq_client.table_exists(table_id='table1', dataset_id='test'):
            self.bq_client.delete_table(table_id='table1', dataset_id='test')
        if self.bq_client.table_exists(table_id='table2', dataset_id='test'):
            self.bq_client.delete_table(table_id='table2', dataset_id='test')
        if self.bq_client.table_exists(table_id='ref_sheet1', dataset_id='test'):
            self.bq_client.delete_table(table_id='ref_sheet1', dataset_id='test')
        if self.bq_client.table_exists(table_id='ref_sheet2', dataset_id='test'):
            self.bq_client.delete_table(table_id='ref_sheet2', dataset_id='test')
        if self.bq_client.table_exists(table_id='gs_test_table1', dataset_id='test'):
            self.bq_client.delete_table(table_id='gs_test_table1', dataset_id='test')

class TestPipelineCopyTableStructure(unittest.TestCase):

    def setUp(self):
        self.bq_client = BigQueryExecutor()
        self.p_ex = pl.PipelineExecutor("tests/yaml/unit_tests_fail.yaml")
        self.p_ex.dataset_prefix = "1001_"
        add_dataset_prefix(self.p_ex.yaml, self.p_ex.dataset_prefix)

        if self.bq_client.table_exists(dataset_id='reporting', table_id='out_product'):
            self.bq_client.delete_table(dataset_id='reporting', table_id='out_product')
        if self.bq_client.table_exists(dataset_id='reporting', table_id='out_saleorder'):
            self.bq_client.delete_table(dataset_id='reporting', table_id='out_saleorder')

    def test_copy_prod_structure(self):
        self.p_ex.copy_prod_structure(['copper-actor-127213.reporting.out_product', 'reporting.out_saleorder'])
        self.assertTrue(
            self.bq_client.table_exists(dataset_id='1001_reporting', table_id='out_product') and
            self.bq_client.table_exists(dataset_id='1001_reporting', table_id='out_saleorder'),
            "all table's structure are copied"
        )

class TestPipelineDryRun(unittest.TestCase):
    def setUp(self):
        self.bq_client = BigQueryExecutor()
        self.p_ex = pl.PipelineExecutor("tests/yaml/test_dry_run.yaml")
        self.p_ex.dataset_prefix = "1001_"
        add_dataset_prefix(self.p_ex.yaml, self.p_ex.dataset_prefix)
        self.bq_client.create_dataset(dataset_id=str(self.p_ex.dataset_prefix) + "test")

    def test_dry_run(self):
        self.p_ex.run()
        self.assertTrue(
            self.bq_client.table_exists(
                dataset_id='1001_test',
                table_id='table1'
            )
        )

    def tearDown(self):
        self.p_ex.dry_run_clean()

class TestPipelineDryRunWithArgs(unittest.TestCase):

    def setUp(self):
        self.bq_client = BigQueryExecutor()
        self.bq_client.create_dataset(
            dataset_id='4001_my_dataset_dry_run_with_args'
        )
        self.bq_client.create_dataset(
            dataset_id='4001_test'
        )
        self.p_ex = pl.PipelineExecutor(
            yaml_file= "tests/yaml/test_dry_run_with_args.yaml",
            my_string_arg='one',
            my_dataset_arg='my_dataset_dry_run_with_args'
        )
        self.p_ex.dataset_prefix = "4001_"

        add_dataset_prefix(obj=self.p_ex.yaml, dataset_prefix=self.p_ex.dataset_prefix, kwargs={'my_string_arg': 'one', 'my_dataset_arg': 'my_dataset_dry_run_with_args'})


    def test_dry_run(self):
        self.p_ex.run()
        self.assertTrue(
            self.bq_client.table_exists(
                dataset_id='4001_my_dataset_dry_run_with_args',
                table_id='table1'
            )
        )

    def tearDown(self):
        self.p_ex.dry_run_clean()


class TestExecute_parallel(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)

    def sleep_2_sec(self, num):
        time.sleep(2)
        a = 3 + 2
        return "ciao"

    def test_time_is_right(self):
        start_time = time.time()
        list_values = [
            {"num": "1"}, {"num": "2"}, {"num": "3"},
            {"num": "4"}, {"num": "5"}, {"num": "6"},
            {"num": "7"}, {"num": "8"}
            ]

        pl.execute_parallel(
                    self.sleep_2_sec,
                    list_values)

        end_time = time.time()
        self.assertLess(end_time-start_time, 3, "right execution time")

    def bug(self, **kwargs):
        for key, value in kwargs.items():
            if value == "error":
                raise Exception("error something")
            else:
                return 1

    def test_error_raised(self):
        with self.assertRaises(Exception):
            pl.execute_parallel(
                    self.bug,
                    [{"status": "ok"}, {"status": "ok"}, {"status": "error"}, {"status": "ok"}]
                )

class TestPipelinePerformance(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        self.bq_client = BigQueryExecutor()
        self.p_ex = pl.PipelineExecutor("tests/yaml/test_performance.yaml")
        self.p_ex.dataset_prefix = "1008_"
        add_dataset_prefix(self.p_ex.yaml, self.p_ex.dataset_prefix)
        self.bq_client.create_dataset(dataset_id=str(self.p_ex.dataset_prefix) + "test")

    def test_dry_pipeline_performance(self):


        start_time = time.time()
        self.p_ex.run()
        end_time = time.time()
        self.assertLess(end_time - start_time, 10, "pipeline performance is bad")

    def tearDown(self):
        self.p_ex.dry_run_clean()


class TestRunReleases(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        self.bq_client = BigQueryExecutor()
        self.p_ex = pl.PipelineExecutor("tests/yaml/test_run.yaml")

    def test_run_releases(self):
        self.p_ex.run_releases(release_date="2020-01-01")
        self.assertTrue(
            self.bq_client.table_exists(
                table_id="table1",
                dataset_id="test"
            ),
            "Table was not created"
        )

    def test_run_releases_with_dataset_prefix(self):
        self.p_ex.dataset_prefix = "2001_"
        self.p_ex.run_releases(release_date="2020-01-01")
        self.assertTrue(
            self.bq_client.table_exists(
                table_id="table1",
                dataset_id="2001_test"
            ),
            "Table was not created"
        )

    def tearDown(self):
        if self.bq_client.table_exists(table_id='table1', dataset_id='test'):
            self.bq_client.delete_table(table_id='table1', dataset_id='test')
        if self.bq_client.dataset_exists("2001_test"):
            self.bq_client.delete_dataset("2001_test", delete_contents=True)


if __name__ == '__main__':
    unittest.main()
