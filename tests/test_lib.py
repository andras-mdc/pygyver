""" Test for lib functions """
import os
import unittest
from unittest import mock
from etl.lib import bq_token_file_valid

def bq_token_file_path_exists_mock(token_path):
    """
    Mock bq_token_file_path_exist to return True
    """
    return True

class FunctionsinLib(unittest.TestCase):
    """
    Unittest class for lib functions
    """
    def setUp(self):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'my_path/my_token.json'

    def tearDown(self):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ''

    def test_bq_token_file_valid_when_does_not_exists(self):
        """
        Tests that bq_token_file_valid raises an ValueError when the file does not exists.
        """
        with self.assertRaises(ValueError):
            bq_token_file_valid()

    def test_bq_token_file_valid(self):
        """
        Tests that bq_token_file_valid is True when the file exists (mocked).
        """
        with mock.patch('etl.lib.bq_token_file_path_exists') as mock_bq_token_file_path_exists:
            mock_bq_token_file_path_exists.side_effect = bq_token_file_path_exists_mock
            self.assertTrue(
                bq_token_file_valid(),
                "Token file is not valid"
            )

if __name__ == "__main__":
    unittest.main()
