
import data_prep.tdd_utility as tu
import unittest
import json
import filecmp as cmp

class test_mockdata(unittest.TestCase):

    def test_class_load_file(self):
        json_output = tu.load_file(
            file="data_prep/test/data/csv/repeated_records.csv", 
            schema="data_prep/test/data/schema/repeated_records_schema.json"
        )
        result = json_output.to_new_line_delimiter_file("data_prep/test/data/json/output.json")
        self.assertTrue(result==0, "unit test to_new_line_delimiter_file() - return value")
        self.assertTrue(
            cmp.cmp(
                "data_prep/test/data/json/output.json",
                "data_prep/test/data/json/repeated_records_expected.json"), 
            'unit test to_new_line_delimiter_file() - created file')
        
if __name__ == "__main__":
    unittest.main()