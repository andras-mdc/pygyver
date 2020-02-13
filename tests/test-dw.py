
from etl import dw 
import unittest
import json
import filecmp as cmp

class test_read_sql(unittest.TestCase):

    def test_class_read_sql(self):
        sql = dw.read_sql(
                file="tests/sql/read_sql.sql", 
                param1 = "type",
                param2 = "300",
                param3 = "shipped_date"
            )
        self.assertTrue(sql == 'select type, shipped_date from table1 where amount > 300' , "read_sql unit test")

        sql = dw.read_sql(
            file="tests/sql/read_sql.sql"
            )
        self.assertTrue(sql == 'select {param1}, {param3} from table1 where amount > {param2}' , "read_sql unit test no opt parameters")
        
if __name__ == "__main__":
    unittest.main()