""" Module containing bigquery object for Python """

import os
from google.cloud import bigquery


def read_sql(file, *args, **kwargs):
    """ Read sql query.
    Parameters:
        argument1 (sql_file): path to the sql (SQL query): "select .. {param2} .. {param1} .. {paramN}"
        param1=value1
        param2=value2
        paranN=valueN
    Returns:
        (SQL query): "select .. value2 .. value1 .. valueN
    """
    file = open(file, 'r')
    sql = file.read()
    file.close()
    if len(kwargs)>0:
        sql = sql.format(**kwargs)
    return sql

class BigQueryExecutor:
    """ BigQuery handler
    Parameters:
        project_id (sql_file): BigQuery Project. Defaults to BIGQUERY_PROJECT environment variable.
    """
    def __init__(self, project_id=os.environ['BIGQUERY_PROJECT']):

        self.client = bigquery.Client()
        self.project_id = project_id
