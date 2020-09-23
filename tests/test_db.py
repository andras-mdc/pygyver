""" DB Tests """
import logging
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from pygyver.etl.toolkit import configure_logging
from pygyver.etl import db


class DBExecutorTest(unittest.TestCase):
    """ Test """
    def setUp(self):
        self.test_df = pd.DataFrame(
            data={
                "fullname": ["Angus MacGyver", "Jack Dalton"],
                "age": [2, 4],
                "date_of_birth": [pd.Timestamp('2018-01-01'), pd.Timestamp('2016-01-01')],
                "iq": [176.5, 124.0]
            }
        )

    def setup_test_db(self, url):
        self.engine = create_engine(url, poolclass=NullPool)
        self.test_df.to_sql(
            "table_1",
            con=self.engine,
            if_exists="replace",
            index=False
        )

    def tearDown(self):
        self.engine.dispose()

    def test_execute_query_postgres(self):
        url = "postgresql+pg8000://user:password@postgres-test:5432/testing"
        self.setup_test_db(url)
        db_client = db.DBExecutor(url=url)
        result_df = db_client.execute_query(sql="SELECT * FROM table_1 ORDER BY fullname")
        assert_frame_equal(
            result_df,
            self.test_df
        )

    def test_execute_query_mysql(self):
        url = "mysql+pymysql://user:password@mysql-test:3306/testing"
        self.setup_test_db(url)
        db_client = db.DBExecutor(url=url)
        result_df = db_client.execute_query(sql="SELECT * FROM table_1 ORDER BY fullname")
        assert_frame_equal(
            result_df,
            self.test_df
        )


if __name__ == "__main__":
    configure_logging()
    unittest.main()
