""" GS Tests """
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pygyver.etl import gs
from pygyver.etl.lib import bq_token_file_path


class GoogleSpreadsheetLoadDataToDataframe(unittest.TestCase):
    """ Test """
    def setUp(self):
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

    def test_load_gs_to_dataframe(self):
        result = gs.load_gs_to_dataframe(self.id)
        test_df = pd.read_csv('tests/csv/test_load_to_gs.csv')
        assert_frame_equal(
            result,
            test_df
        )


if __name__ == "__main__":
    unittest.main()
