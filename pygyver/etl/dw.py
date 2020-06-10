""" Module containing BigQueryExecutor for Python """
import os
import re
import logging
import time
import json
import pandas as pd
from pandas._testing import assert_frame_equal
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from google.api_core import exceptions
from pygyver.etl.lib import bq_token_file_valid
from pygyver.etl.lib import bq_token_file_path
from pygyver.etl.lib import bq_default_project
from pygyver.etl.lib import bq_default_dataset
from pygyver.etl.lib import gcs_default_bucket
from pygyver.etl.lib import read_table_schema_from_file
from pygyver.etl.lib import bq_start_date
from pygyver.etl.lib import bq_end_date
from pygyver.etl.lib import set_write_disposition, set_priority
from pygyver.etl.toolkit import date_lister
from pygyver.etl.toolkit import validate_date
from pygyver.etl.gs import load_gs_to_dataframe


class BigQueryExecutorError(Exception):
    pass


def print_kwargs_params(func):
    def inner(*args, **kwargs):
        logging.info("Keyword args applied to the template:")
        for key, value in kwargs.items():
            if key in forbiden_kwargs():
                raise KeyError("{} is a forbidden keyword argument.".format(key))
        for key, value in kwargs.items():
            logging.info("%s = %s" % (key, value))
        return func(*args, **kwargs)
    return inner


def forbiden_kwargs():
    return ['partition_date']


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'

@print_kwargs_params
def read_sql(file, *args, **kwargs):
    ''' Read SQL file and apply arguments/keywors arguments.

    Args:
        file (string): path to the SQL file from PROJECT_ROOT environment variable.
        *kwargs can be passed if some parameters are to be passed.

    Returns:
        a SQL formated with **kwargs if applicable.

    Example:
        With SQL as:
            "select .. {param2} .. {param1} .. {paramN}"
        *kwargs as:
            param1=value1
            param2=value2
            paranN=valueN
        The functions returns:
            "select .. value2 .. value1 .. valueN"
    '''
    path_to_file = os.path.join(os.getenv("PROJECT_ROOT"), file)
    file = open(path_to_file, 'r')
    sql = file.read()
    file.close()
    if kwargs.get('dry_run_dataset_prefix', None) is not None:
        for index, dataset in enumerate(sql.split("`")):
            if index%2==1 and "." in dataset:
                sql = sql.replace(
                    "`" + sql.split("`")[index] + "`",
                    "`" + str(kwargs.get('dry_run_dataset_prefix', None)) + "_" +  sql.split("`")[index] + "`",
                    1
                )

    if len(kwargs) > 0:
        sql = sql.format_map(SafeDict(**kwargs))
    return sql


class BigQueryExecutor:
    """ BigQuery handler

    Parameters:
        project_id (sql_file): BigQuery Project. Defaults to BIGQUERY_PROJECT environment variable.

    Required:
        GOOGLE_APPLICATION_CREDENTIALS (env variable).

    Attributes:
        client (Client object)
        credentials (Credentials object)
        project_id (string): BigQuery Project. Defaults to BIGQUERY_PROJECT environment variable.

    Returns:
        a BigQueryExecutor object.
    """
    def __init__(self, project_id=bq_default_project()):
        """
        Parameters:
            project_id (sql_file): BigQuery Project. Defaults to BIGQUERY_PROJECT environment variable.
        """
        self.client = None
        self.credentials = None
        self.auth()

    def auth(self):
        """ Sets BigQuery client.
        """
        bq_token_file_valid()
        self.credentials = service_account.Credentials.from_service_account_file(
            bq_token_file_path()
        )
        self.client = bigquery.Client(
            project = bq_default_project(),
            credentials=self.credentials
        )


    def get_dataset_ref(self,  dataset_id, project_id=bq_default_project()):
        """ Returns BigQuery DatasetReference object.

        Parameters:
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.

        Returns:
            bigquery.dataset.DatasetReference object
        """
        return bigquery.dataset.DatasetReference(
            project = project_id,
            dataset_id = dataset_id
        )

    def get_table_ref(self, dataset_id, table_id, project_id=bq_default_project()):
        """ Returns BigQuery Table reference object.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.

        Returns:
            Returns BigQuery Table reference object.
        """
        dataset_ref = self.get_dataset_ref(dataset_id, project_id = project_id)
        return dataset_ref.table(table_id)

    def dataset_exists(self, dataset_id=bq_default_dataset(),project_id=bq_default_project()):
        """ Checks if a BigQuery dataset exists.

        Parameters:
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.

        Returns:
            True is the dataset exists, False otherwise
        """
        dataset_ref = self.get_dataset_ref(dataset_id,project_id=project_id)
        try:
            self.client.get_dataset(dataset_ref)
            return True
        except NotFound:
            return False

    def delete_dataset(self, dataset_id=bq_default_dataset(),project_id=bq_default_project(),delete_contents=False):
        """ Deletes a BigQuery dataset.

        Parameters:
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            delete_contents (bool): If True, delete all the tables in the dataset.
            If False and the dataset contains tables, the request will fail. Default is False.
        """
        dataset_ref = self.get_dataset_ref(dataset_id,project_id=project_id)
        try:
            self.client.delete_dataset(
                dataset_ref,
                delete_contents=delete_contents
            )
            logging.info(
                "Dataset %s:%s deleted",
                project_id,
                dataset_id
            )
            time.sleep(1)
        except exceptions.Conflict as error:
            logging.error(error)


    def create_dataset(self, dataset_id=bq_default_dataset(), project_id=bq_default_project(), **kwargs):
        """ Creates a BigQuery dataset if the dataset does not exists. Otherwise pass.

        Parameters:
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
        """
        dataset_ref = self.get_dataset_ref(dataset_id,project_id=project_id)
        if self.dataset_exists(dataset_id,project_id=project_id):
            logging.info(
                "Dataset %s already exists in project %s",
                dataset_id,
                project_id
            )
        else:
            try:
                self.client.create_dataset(dataset_ref)
                logging.info(
                    "Created dataset %s in in project %s",
                    dataset_id,
                    project_id
                )
            except exceptions.Conflict as error:
                logging.error(error)

    def table_exists(self, table_id, dataset_id=bq_default_dataset(), project_id=bq_default_project()):
        """ Checks if a BigQuery table exists.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            

        Returns:
            True is the table exists, False otherwise.
        """
        table_ref = self.get_table_ref(dataset_id, table_id, project_id=project_id)
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def delete_table(self, table_id, dataset_id=bq_default_dataset(),project_id=bq_default_project()):
        """ Delete a BigQuery table.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            
        """
        try:
            table_ref = self.get_table_ref(dataset_id, table_id,project_id=project_id)
            self.client.delete_table(table_ref)
            logging.info(
                'Table %s:%s.%s deleted.',
                project_id,
                dataset_id,
                table_id
            )
            time.sleep(1)
        except NotFound as error:
            logging.error(error)

    def initiate_table(self,
                       table_id,
                       schema_path,
                       dataset_id=bq_default_dataset(),
                       project_id=bq_default_project(),
                       partition=False,
                       partition_field='_PARTITIONTIME',
                       clustering=None):
        """ Initiate a BigQuery table. If the table already exists, compares the schema_path and apply a patch if there is a schema change.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            schema_path (string: Path to the BigQuery table schema from the PROJECT_ROOT environement variable.
            partition (bool): Specify whether the BigQuery table is partioned. Default to False.
            partition_field (string): Specify partition_field if table partioned. Defaults to "_PARTITIONTIME".
            clustering (list): List of clustering fields. Defaults to None.
        """
        if self.table_exists(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id):
            logging.info("Table %s.%s already exists in project %s",
                         dataset_id,
                         table_id,
                         project_id)
            self.apply_patch(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                schema_path=schema_path
            )
        else:
            schema = read_table_schema_from_file(schema_path)
            table = bigquery.Table(
                self.get_table_ref(dataset_id, table_id, project_id = project_id),
                schema=schema
                )
            if partition:
                table.partitioning_type = 'DAY'
                if partition_field and partition_field != '_PARTITIONTIME':
                    if isinstance(partition_field, str):
                        job_config.time_partitioning = bigquery.table.TimePartitioning(
                            field=partition_field
                        )
                    else:
                        raise ValueError("partition_field should be a string")
            table.clustering_fields = clustering
            try:
                table = self.client.create_table(table)
                logging.info(
                    'Created table %s.%s in in project %s',
                    dataset_id,
                    table_id,
                    project_id
                )
            except exceptions.Conflict as error:
                logging.error(error)

    def create_table(self, table_id, dataset_id=bq_default_dataset(),project_id=bq_default_project(), sql=None, file=None,
                     write_disposition='WRITE_TRUNCATE', use_legacy_sql=False,
                     location='US', schema_path='',
                     partition=False,
                     partition_field='_PARTITIONTIME',
                     clustering=None,
                     priority='INTERACTIVE',
                     **kwargs):
        """ create a bigquery table from a sql query """

        if sql is None and file is None:
            raise BigQueryExecutorError("EIther sql or file must be provided")

        if sql is None:
            sql = read_sql(file, **kwargs)

        if schema_path != '':
            self.initiate_table(
                table_id=table_id,
                schema_path=schema_path,
                partition=partition,
                partition_field=partition_field,
                dataset_id=dataset_id, 
                project_id=project_id
            )
            if write_disposition == "WRITE_TRUNCATE":
                self.truncate_table(dataset_id=dataset_id, table_id=table_id,project_id=project_id)
                write_disposition = "WRITE_EMPTY"
        else:
            pass

        job_config = bigquery.QueryJobConfig()
        job_config.destination = self.get_table_ref(dataset_id, table_id,project_id=project_id)
        job_config.write_disposition = set_write_disposition(write_disposition)
        job_config.use_legacy_sql = use_legacy_sql
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        job_config.priority = set_priority(priority)
        if partition:
            if not partition_field or partition_field == '_PARTITIONTIME':
                job_config.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY
                )
                job_config.clustering_fields = clustering
            elif isinstance(partition_field, str):
                job_config.time_partitioning = bigquery.table.TimePartitioning(
                    field=partition_field
                )
            else:
                raise ValueError("partition_field should be a string")

        query_job = self.client.query(
            sql,
            location=location,
            job_config=job_config
        )
        if priority == 'INTERACTIVE':
            query_job.result()
            logging.info(
            'Query results loaded to table %s:%s.%s',
                project_id,
                dataset_id,
                table_id
            )
        
        return query_job


    def create_partition_table(self,
                               table_id,
                               dataset_id=bq_default_dataset(),
                               project_id=bq_default_project(),
                               sql=None,
                               file=None,
                               use_legacy_sql=False,
                               write_disposition='WRITE_TRUNCATE',
                               partition_dates=None,
                               partition_field='_PARTITIONTIME',
                               clustering=None,
                               priority="INTERACTIVE",
                               schema_path="",
                               **kwargs
                              ):
        """
        Partition to be generated are either passed through partition_dates or automatically generated using existing partitions.
        To filter on a specific partition, the filter DATE(_PARTITIONTIME) = {partition_date} can be used in your sql query.
        """
        if sql is None and file is None:
            raise BigQueryExecutorError("EIther sql or file must be provided")
        if sql is None:
            sql = read_sql(file, **kwargs)

        if schema_path != "":
            self.initiate_table(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                schema_path=schema_path,
                partition=True,
                partition_field=partition_field,
                clustering=clustering
            )

        if not self.table_exists(dataset_id=dataset_id, table_id=table_id,project_id=project_id):
            raise BigQueryExecutorError("To create a partition, please initiate the table first using initiate_table.")

        if partition_dates is None:
            existing_dates = self.get_existing_partition_dates(
                table_id=table_id,
                project_id=project_id,
                dataset_id=dataset_id
            )
            dates = self.get_partition_dates(
                start_date=bq_start_date(),
                end_date=bq_end_date(),
                existing_dates=existing_dates
            )
        else:
            self.validate_partition_dates(
                partition_dates=partition_dates
            )
            dates = partition_dates

        jobs = []
        for date in dates:
            partition_name = self.set_partition_name(table=table_id, date=date)
            logging.info("Updating partition: %s", partition_name)
            job = self.create_table(
                sql=self.apply_partition_filter(
                    sql=sql,
                    date=date
                ),
                dataset_id=dataset_id,
                project_id=project_id,
                table_id=partition_name,
                write_disposition=write_disposition,
                use_legacy_sql=use_legacy_sql,
                partition=True,
                partition_field=partition_field,
                clustering=clustering,
                priority=priority
            )
            jobs.append(job)

        if priority == 'BATCH':
            for job in jobs:
                job.result()


    def apply_partition_filter(self, sql, date):
        """ Apply partition_date to the SQL query.

        Parameters:
            sql (string): the SQL query
            partition_date (string): the partiton date

        Returns:
            Formatted SQL query.
        """
        return sql.format(
            partition_date=date
        )

    def validate_partition_dates(self, partition_dates):
        """ Validates the partition_dates parameter. Checks whether it is a list and the elements are in the right format.

        Parameters:
            partition_dates (list of string): list of partition_dates
            partition_date (string): the partiton date

        Raises:
            BigQueryExecutorError if the partition_dates parameter in not a list.
            ValueError if incorrect date format.
        """
        if not isinstance(partition_dates, list):
            raise BigQueryExecutorError("Partition dates need to be a list of date eg ['YYYYmmdd']")
        else:
            for date in partition_dates:
                validate_date(date=date, format='%Y%m%d')

    def set_partition_name(self, table, date):
        """ Validates the date and sets the partition_name associated.

        Parameters:
            table (string): table name
            date (string): date with format '%Y%m%d'

        Returns:
            Partition name

        Raises:
            BigQueryExecutorError if the partition_dates parameter in not a list.
            ValueError if incorrect date format.
        """
        validate_date(date=date, format='%Y%m%d')
        return table + "$" + date.replace("-", "")

    def get_partition_dates(self, start_date, end_date, existing_dates):
        """ Returns the partitions dates required based on start_date, end_date and existing_dates.

        Parameters:
            start_date (string): First partition required.
            end_date (string): Last partition required.
            existing_dates (list of string): List of existing partition.

        Returns:
            List of partition dates required.
        """
        partition_dates = []
        required_dates = date_lister(start_date=start_date, end_date=end_date)
        if existing_dates == []:
            for date in required_dates:
                partition_date = date.replace("-", "")
                partition_dates.append(partition_date)
        else:
            for date in required_dates:
                partition_date = date.replace("-", "")
                if partition_date not in existing_dates:
                    partition_dates.append(partition_date)
        return partition_dates

    def get_existing_partition_query(self, dataset_id, table_id,project_id = bq_default_project()):
        """ Gets existing partitions from BigQuery as dataframe.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.

        Returns:
            DataFrame of existing partitions
        """
        sql = """ SELECT
                    FORMAT_DATE('%Y%m%d', DATE(_PARTITIONTIME)) AS partition_id
                  FROM
                    `{dataset_id}.{table_id}`
                  GROUP BY
                    1 """.format(
                        dataset_id=dataset_id,
                        table_id=table_id
                        )
        return self.execute_sql(
            sql=sql, 
            project_id=project_id
        )

    def get_existing_partition_dates(self, table_id, dataset_id=bq_default_dataset(),project_id=bq_default_project()):
        """ Gets existing partitions.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.

        Returns:
            List of existing partition dates.
        """
        if not self.table_exists(dataset_id=dataset_id, table_id=table_id,project_id=project_id):
            existing_partition_dates = []
        else:
            res = self.get_existing_partition_query(dataset_id=dataset_id, table_id=table_id)
            # checks that res has number of rows > 0
            if res.shape[0] > 0:
                existing_partition_dates = res['partition_id'].to_list()
            # if not, no existing partitions
            else:
                existing_partition_dates = []
        return existing_partition_dates

    def get_table_schema(self, table_id, dataset_id=bq_default_dataset(), project_id=bq_default_project()):
        """ Gets table schema object

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            

        Returns:
            Table schema.
        """
        table_ref = self.get_table_ref(dataset_id, table_id, project_id=project_id)
        table_schema = self.client.get_table(table_ref).schema
        return table_schema

    def get_table_partitioning_type(self, table_id, dataset_id=bq_default_dataset(), project_id=bq_default_project()):
        """ Gets table partitioning_type

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.

        Returns:
            partitioning_type is the table is partioned, None otherwise.
        """
        table_ref = self.get_table_ref(dataset_id, table_id, project_id=project_id)
        partitioning_type = self.client.get_table(table_ref).partitioning_type
        return partitioning_type

    def get_table_clustering_fields(self, table_id, dataset_id=bq_default_dataset(), project_id=bq_default_project()):
        """ Gets table clustering_fields

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.

        Returns:
            list of clustering fields is the table has cluster fields, None otherwise.
        """
        table_ref = self.get_table_ref(dataset_id, table_id, project_id=project_id)
        clustering_fields = self.client.get_table(table_ref).clustering_fields
        return clustering_fields

    def identify_new_fields(self, table_id, schema_path, dataset_id=bq_default_dataset(),project_id=bq_default_project()):
        """ Identify new fields in based on a schema file.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            schema_path (string): Path to the schema to compare to.

        Returns:
            List of new fields.
        """
        list_field = []
        schema_a = self.get_table_schema(
            table_id=table_id,
            dataset_id=dataset_id,
            project_id=project_id
        )
        schema_b = read_table_schema_from_file(schema_path)
        field_list_a = [schema_field.name for schema_field in schema_a]
        for schema_field in schema_b:
            if schema_field.name not in field_list_a:
                list_field.append(schema_field)
        return list_field

    def append_field(self, table_id, field, dataset_id=bq_default_dataset(),project_id=bq_default_project()):
        """ Append fields to a BigQuery table.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            field (string): Schema field object.

        """
        table_ref = self.get_table_ref(dataset_id, table_id,project_id=project_id)
        table = self.client.get_table(table_ref)  # API request

        original_schema = table.schema
        new_schema = original_schema[:]  # creates a copy of the schema
        new_schema.append(field)

        table.schema = new_schema
        table = self.client.update_table(table, ["schema"])  # API request
        assert len(table.schema) == len(original_schema) + 1 == len(new_schema)
        return 0

    def apply_patch(self, table_id, schema_path, dataset_id=bq_default_dataset(),project_id=bq_default_project()):
        """ Apply a patch to a BigQuery Table if required.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            schema_path (string): Path to schema file to compare to.

        Returns:
            Lenght of new schema
        """
        logging.info("Attempting patch")
        logging.info("Checking for new fields...")
        new_fields = self.identify_new_fields(
            table_id=table_id,
            schema_path=schema_path,
            project_id=project_id,
            dataset_id=dataset_id
        )
        if new_fields != []:
            logging.info("New fields to be added:")
            logging.info(new_fields)
            for field in new_fields:
                self.append_field(
                    field=field,
                    table_id=table_id,
                    dataset_id=dataset_id,
                    project_id=project_id
                )
            logging.info("Done!")
        else:
            logging.info("No field to be added")

        logging.info("Checking for schema update...")
        self.update_schema(
            table_id=table_id,
            schema_path=schema_path,
            dataset_id=dataset_id,
            project_id=project_id
        )
        return len(
            self.get_table_schema(
                table_id=table_id,
                dataset_id=dataset_id,
                project_id=project_id
            )
            )

    def update_schema(self, table_id, schema_path, dataset_id=bq_default_dataset(),project_id=bq_default_project()):
        """ Perform a schema update. Used to update descriptions.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            schema_path (string): Path to schema file to compare to.

        Raises:
            BadRequest if the update fails.
        """
        table_ref = self.get_table_ref(dataset_id, table_id,project_id = project_id)
        table = self.client.get_table(table_ref)  # API request
        new_schema = read_table_schema_from_file(schema_path)
        if table.schema == new_schema:
            logging.info("No changes needed")
        else:
            assert len(table.schema) == len(new_schema)
            table.schema = new_schema
            try:
                table = self.client.update_table(table, ["schema"])  # API request
                return 0
            except exceptions.BadRequest as error:
                raise error

    def execute_sql(self, sql, project_id=bq_default_project(), dialect='standard'):
        """ Executes a SQL query and loads it as a DataFrame.

        Parameters:
            sql (string): SQL Query.
            project_id (string): BigQuery Project ID.
            dialect (string): BigQuery dialect. Defaults to standard.

        Returns:
            Query result as a DataFrame.
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
        """ Executes a SQL file and loads it as a DataFrame.

        Parameters:
            file (string): Path to SQL file.
            project_id (string): BigQuery Project ID.
            dialect (string): BigQuery dialect. Defaults to standard.

        **kwargs can be passed if the SQL file contains arguments formatted with {}.

        Forbidden kwargs:
            partition_date

        Returns:
            Query result as a DataFrame.
        """
        sql = read_sql(file, *args, **kwargs)
        data = self.execute_sql(
            sql=sql,
            project_id=project_id,
            dialect=dialect
        )
        return data

    def load_dataframe(self, df, table_id, dataset_id=bq_default_dataset(),project_id=bq_default_project(), schema_path='', write_disposition="WRITE_TRUNCATE"):
        """ Loads DataFrame to BigQuery table.

        Parameters:
            df (pd.DataFrame): Pandas DataFrame
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            schema_path (string): Path to schema file.
            write_disposition (string): Write disposition. Can be one of WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY.
        """
        if schema_path != '':
            self.initiate_table(
                table_id=table_id,
                dataset_id=dataset_id,
                project_id=project_id,
                schema_path=schema_path
            )
            schema = read_table_schema_from_file(schema_path)
        else:
            schema = None

        data = df.rename(columns=lambda cname: cname.replace('.', '_'))
        table_ref = self.get_table_ref(dataset_id, table_id,project_id=project_id)
        job_config = bigquery.LoadJobConfig(schema=schema)
        job_config.write_disposition = set_write_disposition(write_disposition)
        job = self.client.load_table_from_dataframe(
            data,
            table_ref,
            job_config=job_config
        )
        job.result()


    def load_google_sheet(self, googlesheet_key, table_id, dataset_id=bq_default_dataset(),project_id=bq_default_project(), **kwargs):
        """ Loads Google Sheet to BigQuery table. If the table already exists, overwrites.

        Parameters:
            googlesheet_key (string): Google Sheet Key
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
        """
        df = load_gs_to_dataframe(googlesheet_key)
        if self.table_exists(table_id, dataset_id,project_id=project_id):
            self.delete_table(table_id, dataset_id,project_id=project_id)
        self.load_dataframe(
            df,
            table_id,
            dataset_id, 
            project_id=project_id
            )

    def load_json_file(self, file, table_id, dataset_id=bq_default_dataset(),project_id=bq_default_project(), schema_path='', write_disposition="WRITE_TRUNCATE"):
        """ Loads JSON file to BigQuery table.

        Parameters:
            file (string): Path to JSON file.
            project_id (string): BigQuery Project ID.
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            schema_path (string): Path to schema file.
            write_disposition (string): Write disposition. Can be one of WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY.
        """
        if schema_path != '':
            self.initiate_table(
                table_id=table_id,
                schema_path=schema_path,
                dataset_id=dataset_id, 
                project_id=project_id
            )
            schema = read_table_schema_from_file(schema_path)
        else:
            schema = None

        if self.table_exists(
                table_id=table_id,
                dataset_id=dataset_id,
                project_id=project_id
        ):
            table_ref = self.get_table_ref(dataset_id, table_id,project_id=project_id)
            job_config = bigquery.LoadJobConfig(schema=schema)
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.write_disposition = set_write_disposition(write_disposition)
            with open(file, mode='rb') as data:
                job = self.client.load_table_from_file(
                    file_obj=data,
                    destination=table_ref,
                    location='US',
                    job_config=job_config
                )
                job.result()
        else:
            raise Exception("Please initiate %s:%s.%s or pass the schema file",project_id ,dataset_id, table_id)

    def load_json_data(self, json, table_id, dataset_id=bq_default_dataset(),project_id=bq_default_project(), schema_path='', write_disposition="WRITE_TRUNCATE"):
        """ Loads JSON data to BigQuery table.

        Parameters:
            json (string): JSON data.
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            schema_path (string): Path to schema file.
            write_disposition (string): Write disposition. Can be one of WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY.
        """
        if schema_path != '':
            self.initiate_table(
                table_id=table_id,
                schema_path=schema_path,
                dataset_id=dataset_id, 
                project_id=project_id
            )
            schema = read_table_schema_from_file(schema_path)
        else:
            schema = None

        if self.table_exists(
                table_id=table_id,
                dataset_id=dataset_id, 
                project_id=project_id
        ):
            table_ref = self.get_table_ref(dataset_id, table_id,project_id=project_id)
            job_config = bigquery.LoadJobConfig(schema=schema)
            job_config.write_disposition = set_write_disposition(write_disposition)
            job = self.client.load_table_from_json(
                json_rows=json,
                destination=table_ref,
                location='US',
                job_config=job_config
            )
            job.result()
        else:
            raise Exception("Please initiate %s:%s.%s or pass the schema file",project_id,dataset_id, table_id)

    def load_gcs(self, dataset_id, table_id, gcs_path, gcs_bucket=gcs_default_bucket(),project_id=bq_default_project(), location='US', schema_path='', header=True, write_disposition='WRITE_TRUNCATE'):
        """ Loads Google Cloud Storage CSV file into a BigQuery table.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            gcs_bucket (string): Google Cloud Storage Bucket.
            gcs_path (string): Google Cloud Storage Path.
            location (string): Defaults to 'US'.
            schema NBD
            header (bool): Defaults to True.
            write_disposition (string): Write disposition. Can be one of WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY.
        """

        if isinstance(gcs_path, list):
            uri = ["gs://{}/{}".format(gcs_bucket, p) for p in gcs_path]
            uri_out = uri[0]
        else:
            uri = "gs://{}/{}".format(gcs_bucket, gcs_path)
            uri_out = uri

        job_config = bigquery.LoadJobConfig()

        if schema_path != '':
            self.initiate_table(
                table_id=table_id,
                schema_path=schema_path,
                dataset_id=dataset_id,
                project_id=project_id
            )
            job_config.schema = read_table_schema_from_file(schema_path)
            if header:
                job_config.skip_leading_rows = 1
        else:
            job_config.schema = None
            job_config.autodetect = True

        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.write_disposition = write_disposition

        table_ref = self.get_table_ref(
            dataset_id=dataset_id,
            table_id=table_id, 
            project_id=project_id
        )

        load_job = self.client.load_table_from_uri(
            uri,
            table_ref,
            job_config=job_config,
            location=location
        )

        load_job.result()  # Waits for job to complete.

        logging.info(
            'Loaded %s loaded to %s:%s.%s',
            uri,
            project_id,
            dataset_id,
            table_id
        )

    def insert_rows_json(self, dataset_id, table_id, rows,project_id=bq_default_project()):
        """ Insert rows into a table via the streaming API.
        Requires the table to exists in BigQuery.

        Arguments:
            - project_id (string): BigQuery project ID
            - dataset_id (string): BigQuery dataset ID
            - table_id (string): BigQuery table ID
            - rows (one of: list of tuples, list of dictonaries): Row data to be inserted.
            If a list of tuples is given, each tuple should contain data for each schema
            field on the current table and in the same order as the schema fields.
            If a list of dictionaries is given, the keys must include all required fields
            in the schema. Keys which do not correspond to a field in the schema are ignored.
        """
        table_ref = self.get_table_ref(
            dataset_id=dataset_id,
            table_id=table_id, 
            project_id=project_id
        )
        try:
            error_response = self.client.insert_rows_json(
                table_ref,
                rows,
                ignore_unknown_values=True
            )
            if error_response == []:
                logging.info(
                    'Loaded %s row(s) into %s:%s.%s',
                    len(rows),
                    project_id,
                    dataset_id,
                    table_id
                )
            else:
                if hasattr(error_response, 'errors'):
                    for error in error_response.errors:
                        logging.warning(' - ' + error)
                        logging.warning(' - ' + rows)
                else:
                    logging.warning(json.dumps(error_response))

        except Exception:
            logging.exception('BigQuery Exception', exc_info=True)

    def extract_table_to_gcs(self, dataset_id, table_id, gcs_path,project_id=bq_default_project(), gcs_bucket=gcs_default_bucket(), location='US', shard=False):
        """ Extract BigQuery table into Google Cloud Storage.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
            gcs_bucket (string): Google Cloud Storage Bucket.
            gcs_path (string): Google Cloud Storage Path.
            location (string): Defaults to 'US'.
            shard (bool)
        """

        uri = "gs://{}/{}".format(gcs_bucket, gcs_path)
        if shard:
            uri = re.sub('\\.', '*.', uri)

        table_ref = self.get_table_ref(
            dataset_id=dataset_id,
            table_id=table_id, 
            project_id=project_id
        )

        job = self.client.extract_table(
            table_ref,
            uri,
            location=location
        )

        job.result()  # Waits for job to complete.

        logging.info(
            'Table %s:%s.%s loaded to %s',
            project_id,
            dataset_id,
            table_id,
            uri
        )

    def truncate_table(self, table_id, dataset_id=bq_default_dataset(),project_id=bq_default_project()):
        """ Delete all records from table but preserve structure e.g. schema, partitioning, clustering, description, labels, etc.

        Parameters:
            table_id (string): BigQuery table ID.
            dataset_id (string): BigQuery dataset ID.
            project_id (string): BigQuery project ID.
        """
        job_config = bigquery.QueryJobConfig()
        job_config.destination = self.get_table_ref(dataset_id, table_id,project_id=project_id)
        job_config.write_disposition = set_write_disposition("WRITE_TRUNCATE")
        query_job = self.client.query(
            query=f"SELECT * FROM `{dataset_id}.{table_id}` LIMIT 0",
            job_config=job_config
        )
        query_job.result()
        logging.info(
            'Table %s:%s.%s has been truncated',
            project_id,
            dataset_id,
            table_id
        )

    # this is a placeholder - no tests.
    def copy_table_structure(
        self,
        source_table_id,
        dest_table_id,
        source_dataset_id=bq_default_dataset(),
        dest_dataset_id=bq_default_dataset(),
        source_project_id=bq_default_project(),
        dest_project_id=bq_default_project()
    ):


        if not self.dataset_exists(dest_dataset_id):
            self.create_dataset(dest_dataset_id)

        if self.table_exists(table_id=dest_table_id, dataset_id=dest_dataset_id):
            self.delete_table(table_id=dest_table_id, dataset_id=dest_dataset_id)

        if self.table_exists(table_id=source_table_id, dataset_id=source_dataset_id, project_id=source_project_id):

            schema = self.get_table_schema(
                table_id=source_table_id,
                dataset_id=source_dataset_id,
                project_id=source_project_id
            )
            table_def = "{}.{}.{}".format(dest_project_id, dest_dataset_id, dest_table_id)
            table = bigquery.Table(table_def, schema=schema)
            clustering_fields = self.get_table_clustering_fields(
                table_id=source_table_id,
                dataset_id=source_dataset_id,
                project_id=source_project_id
            )
            partition_type = self.get_table_partitioning_type(
                table_id=source_table_id,
                dataset_id=source_dataset_id,
                project_id=source_project_id
            )
            if not clustering_fields is None:
                table.clustering_fields = clustering_fields
            if not partition_type is None:
                table.partitioning_type = partition_type
            self.client.create_table(table) # Make an API request.
            logging.info(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )



    def copy_table(self, source_table_id, dest_table_id,
                   source_dataset_id=bq_default_dataset(), dest_dataset_id=bq_default_dataset(),
                   source_project_id=bq_default_project(),dest_project_id = bq_default_project(), write_disposition='WRITE_TRUNCATE'):
        """ Copy a BigQuery table.

        Parameters:
            source_project_id (string): Source BigQuery Project ID.
            source_dataset_id (string): Source BigQuery Dataset ID.
            source_table_id (string): Source BigQuery table ID.
            dest_project_id (string): Source BigQuery Project ID.
            dest_dataset_id (string): Destination BigQuery Dataset ID.
            dest_table_id (string): Destination BigQuery table ID.
            write_disposition (string): Write disposition. Can be one of WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY.

        Limitations:
            Destination project is the default, i.e. specified in `local.env`
            Destination dataset must reside in the same location as source (US, EU, etc.)

        Permissions:
            The email address specified in `access_token.json` must have read permissions for the source
        """
        source_table_ref = self.get_table_ref(source_dataset_id, source_table_id,project_id=source_project_id)
        dest_table_ref = self.get_table_ref(dest_dataset_id, dest_table_id,project_id=dest_project_id)

        if not self.dataset_exists(dest_dataset_id):
            self.create_dataset(dest_dataset_id)

        job_config = bigquery.CopyJobConfig()
        job_config.write_disposition = set_write_disposition(write_disposition)
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        job = self.client.copy_table(
            source_table_ref,
            dest_table_ref,
            project=source_project_id,
            job_config=job_config
        )
        job.result()
        logging.info(
            'Table %s:%s.%s copied to %s:%s.%s',
            source_project_id,
            source_dataset_id,
            source_table_id,
            dest_project_id,
            dest_dataset_id,
            dest_table_id
        )

    def list_datasets(self, project_id=bq_default_project(), include_all=False):
        """ Lists datasets in project

        Parameters:
            project_id (string): BigQuery project ID
            include_all (boolean): True if results include hidden datasets. Defaults to False.

        Returns:
            A sorted list of dataset ids

        Permissions:
            The email address specified in `access_token.json` must have read permissions for the project
        """
        try:
            datasets = list(self.client.list_datasets(project=project_id, include_all=include_all))
            dataset_ids = [dataset.dataset_id for dataset in datasets]
            return dataset_ids
        except NotFound as error:
            logging.error(error)

    def list_tables(self, dataset_id=bq_default_dataset(), project_id=bq_default_project()):
        """ Lists tables in dataset

        Parameters:
            project_id (string): BigQuery project ID
            dataset_id (string): BigQuery dataset ID

        Returns:
            A sorted list of table ids

        Permissions:
            The email address specified in `access_token.json` must have read permissions for the project
        """
        try:
            dataset_ref = self.get_dataset_ref(dataset_id, project_id = project_id)
            tables = self.client.list_tables(dataset_ref)
            table_ids = [table.table_id for table in tables]
            return table_ids
        except NotFound as error:
            logging.error(error)

    def count_rows(self, table_id, dataset_id=bq_default_dataset(),project_id=bq_default_project()):
        """ Count rows in table

        Parameters:
            project_id (string): BigQuery project ID.
            dataset_id (string): BigQuery dataset ID.
            table_id (string): BigQuery table ID.

        Returns:
            Number of rows in a BigQuery table.
        """
        table_ref = self.get_table_ref(dataset_id, table_id,project_id = project_id)
        table = self.client.get_table(table_ref)
        return table.num_rows

    def count_columns(self, table_id, dataset_id=bq_default_dataset(),project_id=bq_default_project()):
        """ Count columns in table

        Parameters:
            project_id (string): BigQuery project ID.
            dataset_id (string): BigQuery dataset ID.
            table_id (string): BigQuery table ID.

        Returns:
            Number of columns in a BigQuery table.
        """
        table_ref = self.get_table_ref(dataset_id, table_id,project_id = project_id)
        table = self.client.get_table(table_ref)
        return len(table.schema)

    def count_duplicates(self, table_id, primary_key: list, dataset_id=bq_default_dataset(),project_id=bq_default_project()):
        """ Count duplicate rows in primary key

        Parameters:
            project_id (string): BigQuery project ID.
            dataset_id (string): BigQuery dataset ID.
            table_id (string): BigQuery table ID.
            primary_key: a list of one or more column names, e.g. ['col1', 'col2']

        Returns:
            non-negative integer
        """
        data = self.execute_sql(
            f"""
            SELECT
                COALESCE(SUM(dup_count), 0) AS dup_total
            FROM (
                SELECT
                    {', '.join(primary_key)},
                    (COUNT(*) - 1) AS dup_count
                FROM
                    `{dataset_id}.{table_id}`
                GROUP BY
                    {', '.join(primary_key)}
            )
            """, 
            project_id = project_id
        )
        return data['dup_total'].values[0]

    def assert_unique(self, table_id, primary_key: list, dataset_id=bq_default_dataset(),project_id=bq_default_project(), ignore_error=False, **kwargs):
        """ Assert uniqueness of primary key in table

        Parameters:
            project_id (string): BigQuery project ID.
            dataset_id (string): BigQuery dataset ID.
            table_id (string): BigQuery table ID.
            primary_key: a list of one or more column names, e.g. ['col1', 'col2']
            ignore_error: boolean flag to prevent error being raised, useful for debugging

        Returns:
            Nothing if there are no duplicate rows in primary key
            Raise and log AssertionError if there are duplicate rows and ignore_error=False (default)
            Log a warning if there are dupicate rows and ignore_error=True (debugging)
        """
        if self.count_duplicates(table_id, primary_key, dataset_id,project_id = project_id) != 0:
            msg = "Table %s:%s.%s is not unique on %s" % (
                project_id,
                dataset_id,
                table_id,
                ', '.join(primary_key)
            )
            if ignore_error:
                logging.warning(msg)
            else:
                raise AssertionError(msg)

    def assert_acceptance(self, sql, cte, output_table_name='expected_output', **kwargs):
        try:
            sql_extract_output_table = "WITH {} ( SELECT * FROM `{}` )".format(cte, output_table_name)

            df_result = self.execute_sql(sql_extract_output_table)
        except:
            raise AssertionError("Bad SQL or CTE")
        composite_sql = "WITH {} ( {} )".format(cte, sql)
        df = self.execute_sql(composite_sql)
        df = df.apply(lambda x: x.sort_values().values)
        df_result = df_result.apply(lambda x: x.sort_values().values)
        assert_frame_equal(df, df_result, check_like=True)
