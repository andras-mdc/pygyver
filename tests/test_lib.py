""" Test for lib functions """
import os
import unittest
from unittest import mock
from pygyver.etl.lib import bq_token_file_valid
from pygyver.etl.lib import extract_args
from pygyver.etl.lib import remove_first_slash
from pygyver.etl.lib import add_dataset_id_prefix



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
        self.credential_back_up = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'my_path/my_token.json'

    def tearDown(self):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credential_back_up

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
        with mock.patch('pygyver.etl.lib.bq_token_file_path_exists') as mock_bq_token_file_path_exists:
            mock_bq_token_file_path_exists.side_effect = bq_token_file_path_exists_mock
            self.assertTrue(
                bq_token_file_valid(),
                "Token file is not valid"
            )

    def test_remove_first_slash(self):
        self.assertEqual(
                remove_first_slash("/sandbox"), "sandbox",
                "first slash not removed - wrong"
            )
        self.assertEqual(
                remove_first_slash("sandbox"), "sandbox",
                "no first slash - ok"
            )
        self.assertEqual(
                remove_first_slash(""), "",
                "empty string - ok"
            )

    def test_extract_args_1_param(self):
        content = [
                        {
                            "table_desc": "table1",
                            "create_table": {
                                "table_id": "table1",
                                "dataset_id": "test",              
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
                            "pk": ["col1"],
                            "mock_data": "sql/table1_mocked.sql"
                        }
                    ]        
        self.assertEqual(
            extract_args(content, "pk"),
            [["col1", "col2"], ["col1"]],
            "extracted ok"
            )
        self.assertEqual(
            extract_args(content, "create_table"),
            [
                {
                    "table_id": "table1",
                    "dataset_id": "test",
                    "file": "tests/sql/table1.sql"
                },
                {
                    "table_id": "table2",
                    "dataset_id": "test",
                    "file": "tests/sql/table2.sql"
                }
            ],
            "extracted ok"
            )

    def test_add_dataset_id_prefix(self):
        self.yaml = {
                        "desc": "test",
                        "tables":
                        [
                            {
                                "table_desc": "table1",
                                "create_table": {
                                    "table_id": "table1",
                                    "dataset_id": "test",              
                                },                      
                            },
                            {
                                "table_desc": "table2",
                                "create_table": {
                                    "table_id": "table2",
                                    "dataset_id": "test",                                    
                                },                                    
                            }
                        ]
                    }
        add_dataset_id_prefix(
                    self.yaml,
                    prefix='1234'
                )
        self.assertEqual(
                self.yaml
                , 
                {
                    "desc": "test",
                    "tables":
                    [
                        {
                            "table_desc": "table1",
                            "create_table": {
                                "table_id": "table1",
                                "dataset_id": "1234_test",              
                            },                      
                        },
                        {
                            "table_desc": "table2",
                            "create_table": {
                                "table_id": "table2",
                                "dataset_id": "1234_test",                                    
                            },                                    
                        }
                    ]                
                },
                "prefix properly added to dataset_id"
            )
   

if __name__ == "__main__":
    unittest.main()
