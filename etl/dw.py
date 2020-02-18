""" Module containing bigquery object for Python """

import os
import logging
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from etl.lib import bq_token_file_valid, bq_default_project

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
    def __init__(self, project_id=bq_default_project()):
        """
        Initiates the object.
        Required: GOOGLE_APPLICATION_CREDENTIALS (env variable).
        """
        self.client = None
        self.project_id = project_id
        self.auth()

    def auth(self):
        bq_token_file_valid()
        self.client = bigquery.Client(
            project = self.project_id
        )

    def dataset_exists(self, dataset_id):
        """
        Checks if a BigQuery dataset exists
        Arguments:
        - dataset_id (string): the BigQuery dataset ID
        """
        dataset = self.client.dataset(dataset_id)
        try:
            self.client.get_dataset(dataset)
            return True
        except NotFound:
            return False

    def table_exists(self, dataset_id, table_id):
        """
        Checks if a BigQuery table exists
        Arguments:
        - dataset_id (string): the BigQuery dataset ID
        - table_id (string): the BigQuery table ID
        """
        dataset = self.client.dataset(dataset_id)
        table_ref = dataset.table(table_id)
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def delete_table(self, dataset_id, table_id):
        """ Delete a BigQuery table.
        Parameters:
        dataset_id: the BigQuery dataset ID
        table_id: the BigQuery table ID
        """
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            self.client.delete_table(table_ref)
            logging.info(
                'Table %s:%s.%s deleted.',
                self.project_id,
                dataset_id,
                table_id
            )
            time.sleep(1)
        except NotFound as error:
            logging.error(error)
