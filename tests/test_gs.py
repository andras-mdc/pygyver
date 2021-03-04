""" GS Tests """
import filecmp
import os
import pandas as pd
import unittest
from pandas.testing import assert_frame_equal
from pygyver.etl import gs


class GoogleSpreadsheetLoadDataToDataframe(unittest.TestCase):
    """ Test """
    def setUp(self):
        self.client = gs.gspread_client()

        self.spreadsheet = self.client.create('test_spreadsheet')
        self.worksheet = self.spreadsheet.add_worksheet(title='new_worksheet', rows=3, cols=3, index=1)
        self.test_df = pd.read_csv('tests/csv/test_load_gs.csv', thousands=',')
        self.worksheet.update([self.test_df.columns.values.tolist()] + self.test_df.values.tolist())

    def tearDown(self):
        self.client.del_spreadsheet(self.spreadsheet.id)

    def test_load_gs_to_dataframe(self):
        result_by_sheet_name = gs.load_gs_to_dataframe(self.spreadsheet.id, sheet_name='new_worksheet', thousands=',')
        assert_frame_equal(
            result_by_sheet_name,
            self.test_df
        )

        result_by_sheet_index = gs.load_gs_to_dataframe(self.spreadsheet.id, sheet_index=1, thousands=',')
        assert_frame_equal(
            result_by_sheet_index,
            self.test_df
        )


class GoogleSpreadsheetLoadDataToSchemaJson(unittest.TestCase):
    """
    Test spreadsheet to schema json, generated v expected output.
    """

    def setUp(self):

        self.gen_source = "tests/csv/test_load_gs_schema.csv"

        self.client = gs.gspread_client()

        self.spreadsheet = self.client.create("test_spreadsheet")
        self.worksheet = self.spreadsheet.add_worksheet(title="schema", rows=14, cols=4, index=1)
        self.test_df = pd.read_csv(self.gen_source, thousands=",", keep_default_na=False)
        self.worksheet.update([self.test_df.columns.values.tolist()] + self.test_df.values.tolist())

    def test_load_gs_to_json_schema(self):

        self.gen_output = "tests/json/test_schema_gen.json"
        self.exp_output = "tests/json/test_schema.json"

        # 1. generate output
        gs.load_gs_to_json_schema(self.spreadsheet.id, self.gen_output)

        # 2. compare generated v expected output
        comp = filecmp.cmp(self.gen_output, self.exp_output, shallow=False)
        self.assertTrue(comp, "Expected schema doesn't match the created file")

    def tearDown(self):
        self.client.del_spreadsheet(self.spreadsheet.id)
        if os.path.exists(self.gen_output):
            os.remove(self.gen_output)


class GoogleSpreadsheetLoadDataToMockSql(unittest.TestCase):
    """
    Test spreadsheet to mock cte sql, generated v expected output.
    """

    def setUp(self):

        self.gen_source = "tests/csv/test_load_gs_mock.csv"

        self.client = gs.gspread_client()

        self.spreadsheet = self.client.create("test_spreadsheet")
        self.worksheet = self.spreadsheet.add_worksheet(title="acceptance", rows=7, cols=13, index=1)
        self.test_df = pd.read_csv(self.gen_source, thousands=",", keep_default_na=False)
        self.worksheet.update([self.test_df.columns.values.tolist()] + self.test_df.values.tolist())

    def test_load_gs_to_sql_mock(self):

        self.gen_output = "tests/sql/test_mock_gen.sql"
        self.exp_output = "tests/sql/test_mock.sql"

        # 1. generate output
        gs.load_gs_to_sql_mock(self.spreadsheet.id, self.gen_output)

        # 2. compare generated v expected output
        comp = filecmp.cmp(self.gen_output, self.exp_output, shallow=False)
        self.assertTrue(comp, "Expected sql mock doesn't match the created file")

    def tearDown(self):
        self.client.del_spreadsheet(self.spreadsheet.id)
        if os.path.exists(self.gen_output):
            os.remove(self.gen_output)


if __name__ == "__main__":
    unittest.main()
