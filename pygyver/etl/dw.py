""" Module containing bigquery object for Python """

import os
import logging
import time
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from google.api_core import exceptions
from pygyver.etl.lib import bq_token_file_valid
from pygyver.etl.lib import bq_token_file_path
from pygyver.etl.lib import bq_default_project
from pygyver.etl.lib import read_table_schema_from_file

def read_sql(file, *args, **kwargs):
    """ Read sql query.
    Parameters:
        argument1 (sql_file): path to the sql (SQL query):
         "select .. {param2} .. {param1} .. {paramN}"
        param1=value1
        param2=value2
        paranN=valueN
    Returns:
        (SQL query): "select .. value2 .. value1 .. valueN
    """
    path_to_file = os.path.join(os.getenv("PROJECT_ROOT"), file)
    file = open(path_to_file, 'r')
    sql = file.read()
    file.close()
    if len(kwargs) > 0:
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
        self.credentials = None
        self.project_id = project_id
        self.auth()

    def auth(self):
        """
        Authentificate using the access token
        """
        bq_token_file_valid()
        self.credentials = service_account.Credentials.from_service_account_file(
            os.path.join("PROJECT_ROOT", bq_token_file_path())
        )
        self.client = bigquery.Client(
            credentials=self.credentials,
            project=self.project_id
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

    def initiate_table(self, dataset_id, table_id, schema_path, partition=False, clustering=None):

        """  Creates dataset_name.table_name in BigQuery.
        Arguments:
            - schema_path (string): full path to the schema file to be used to create the table.
            - dataset_name (string): target dataset
            - table_name (string): Optional. If not specify, will use the file name specify in
            the schema file.
        """

        if self.table_exists(dataset_id, table_id):
            logging.info("Table %s.%s already exists in project %s",
                         dataset_id,
                         table_id,
                         self.project_id)
            self.apply_patch(dataset_id, table_id, schema_path)
        else:
            dataset = self.client.dataset(dataset_id)
            schema = read_table_schema_from_file(schema_path)
            table = bigquery.Table(dataset.table(table_id), schema=schema)
            if partition:
                table.partitioning_type = 'DAY'
                table.clustering_fields = clustering
            try:
                table = self.client.create_table(table)
                logging.info(
                    'Created table %s.%s in in project %s',
                    dataset_id,
                    table_id,
                    self.project_id
                )
            except exceptions.Conflict as error:
                logging.error(error)

    def get_table_schema(self, dataset_id, table_id):
        '''
        return SchemaField values
        '''
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table_schema = self.client.get_table(table_ref).schema
        return table_schema

    def identify_new_fields(self, dataset_id, table_id, schema_path):
        """ identifies new fields from a schema file """
        list_field = []
        schema_a = self.get_table_schema(dataset_id, table_id)
        schema_b = read_table_schema_from_file(schema_path)
        field_list_a = [schema_field.name for schema_field in schema_a]
        for schema_field in schema_b:
            if schema_field.name not in field_list_a:
                list_field.append(schema_field)
        return list_field

    def append_field(self, dataset_id, table_id, field):
        '''
        field: schema field object
        i.e. SchemaField('postcode', 'STRING', 'NULLABLE', None, ())
        '''
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)  # API request

        original_schema = table.schema
        new_schema = original_schema[:]  # creates a copy of the schema
        new_schema.append(field)

        table.schema = new_schema
        table = self.client.update_table(table, ["schema"])  # API request
        assert len(table.schema) == len(original_schema) + 1 == len(new_schema)
        return 0

    def apply_patch(self, dataset_id, table_id, schema_path):
        '''
        this function identifies and appends all the new fields to the original table
        '''
        logging.info("Attempting patch")
        logging.info("Checking for new fields...")
        new_fields = self.identify_new_fields(dataset_id, table_id, schema_path)
        if new_fields != []:
            logging.info("New fields to be added:")
            logging.info(new_fields)
            for field in new_fields:
                self.append_field(
                    dataset_id,
                    table_id,
                    field=field
                )
            logging.info("Done!")
        else:
            logging.info("No field to be added")

        logging.info("Checking for schema update...")
        self.update_schema(
            dataset_id=dataset_id,
            table_id=table_id,
            schema_path=schema_path
        )
        return len(
            self.get_table_schema(
                dataset_id=dataset_id,
                table_id=table_id
            )
            )

    def update_schema(self, dataset_id, table_id, schema_path):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)  # API request
        new_schema = read_table_schema_from_file(schema_path)
        if table.schema == new_schema:
            print("No changes needed")
        else:
            assert len(table.schema) == len(new_schema)
            table.schema = new_schema
            try:
                table = self.client.update_table(table, ["schema"])  # API request
                return 0
            except exceptions.BadRequest as error:
                raise error

    def execute_sql(self, sql, project_id=bq_default_project(), dialect='standard'):
        """ Execute sql query.

        Parameters:
        argument1 (sql): the sql query
        argument2 (dialect): the sql dialect ('legacy' or 'standard')

        Returns:
        a dataframe including the query results
        """
        data = pd.read_gbq(
            sql,
            project_id=project_id,
            credentials=self.credentials,
            dialect=dialect
        )

        return data

    def execute_file(self, file, project_id=bq_default_project(),
                     dialect='standard', *args, **kwargs):
        """ Execute sql file.

        Parameters:
        argument1 (file_path): the path to the SQL file
        argument2 (dialect): the sql dialect ('legacy' or 'standard')
        Returns:
        a dataframe including the query results
        """
        sql = read_sql(file, *args, **kwargs)
        data = self.execute_sql(sql, project_id, dialect)
        return data
