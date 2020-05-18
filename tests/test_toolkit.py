""" GS Tests """
import unittest
from pygyver.etl.toolkit import convert_list_for_sql


class Toolkit(unittest.TestCase):
    """ Test """

    def test_convert_list_for_sql(self):
        self.assertEqual(
            convert_list_for_sql([1, 2, 3]),
            "1, 2, 3"
        ) 
        self.assertEqual(
            convert_list_for_sql(['Simone', 'Dan']),
            "'Simone', 'Dan'"
        ) 

if __name__ == "__main__":
    unittest.main()
