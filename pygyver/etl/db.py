""" SQL Alchemy Wrapper """
import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.engine.url import URL
from pygyver.etl.toolkit import read_sql_file


class DBExecutorError(Exception):
    pass


class DBExecutor:
    """ Database handler
    Note:
        Sqlalchemy's built-in drivers should be supported
        Additional drivers can be added using requirements.txt
        Postgres(pg8000) and MySQL(pymysql) are included in tests

    Args:
        name (str): Defines connection using env vars with this prefix.
        url (str): Defines connection using SQLAlchemy database URL format.

    Attributes:
        name (str): Defines connection using env vars with this prefix.
        url (str): Defines connection using SQLAlchemy database URL format.
        safe_url (str): url with password removed for logging
        engine: SQLAlchemy Engine object

    Returns:
        DBExecutor object.

    Examples:
        >>> db = DBExecutor("ERP")
        >>> db = DBExecutor(url="mysql+pymysql://username:password@host:port/database")

    References:
        https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls
    """
    def __init__(self, name=None, url=None):
        if url:
            self.url = url
        else:
            if name:
                self.url = URL(
                    drivername=os.getenv(f"{name}_DRIVERNAME"),
                    username=os.getenv(f"{name}_USERNAME"),
                    password=os.getenv(f"{name}_PASSWORD"),
                    host=os.getenv(f"{name}_HOST"),
                    port=os.getenv(f"{name}_PORT"),
                    database=os.getenv(f"{name}_DATABASE")
                )
            else:
                raise DBExecutorError("Either database name or url must be provided")
        # remove password from url string for logging
        self.safe_url = repr(self.url)

        try:
            self.engine = create_engine(self.url, poolclass=NullPool)
        except Exception as error:
            logging.error(f"Failed to connect to {self.safe_url}: {error}")
            raise error


    def clean_up(self):
        self.engine.dispose()


    def execute_query(self, sql=None, file=None, **kwargs):
        """ Executes a SQL query and loads it as a DataFrame.

        Args:
            sql (str): SQL query (or table name).
            file (str): path to the SQL file relative to PROJECT_ROOT env var.
            **kwargs: can be applied to pass parameters into a SQL file

        Returns:
            Query result as a DataFrame.
        """
        if sql is None:
            if file:
                sql = read_sql_file(file, **kwargs)
            else:
                raise DBExecutorError("Either SQL or file containing the SQL must be provided")

        with self.engine.connect() as con:
            try:
                logging.info(f"Running SQL query on {self.safe_url}")
                df = pd.read_sql(sql, con)
            except Exception as error:
                logging.error(error)
                raise error
            finally:
                con.close()

        return df
