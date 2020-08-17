""" GS Tests """
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
import gspread
from pygyver.etl import gs
from pygyver.etl.lib import bq_token_file_path


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


if __name__ == "__main__":
    unittest.main()
