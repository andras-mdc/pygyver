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
from pygyver.etl.lib import bq_default_dataset
from pygyver.etl.lib import read_table_schema_from_file
from pygyver.etl.lib import bq_start_date
from pygyver.etl.lib import bq_end_date
from pygyver.etl.lib import set_write_disposition, set_priority
from pygyver.etl.toolkit import date_lister
from pygyver.etl.toolkit import validate_date

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

@print_kwargs_params
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
            bq_token_file_path()
        )
        self.client = bigquery.Client(
            credentials=self.credentials,
            project=self.project_id
        )

    def get_dataset_ref(self, dataset_id):
        return bigquery.dataset.DatasetReference(
            self.project_id,
            dataset_id
        )

    def get_table_ref(self, dataset_id, table_id):
        dataset_ref = self.get_dataset_ref(dataset_id)
        return dataset_ref.table(table_id)

    def dataset_exists(self, dataset_id=bq_default_dataset()):
        """
        Checks if a BigQuery dataset exists
        Arguments:
        - dataset_id (string): the BigQuery dataset ID
        """
        dataset_ref = self.get_dataset_ref(dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            return True
        except NotFound:
            return False

    def delete_dataset(self, dataset_id=bq_default_dataset(), delete_contents=False):
        """
        Delete a BigQuery dataset.
        Arguments:
        - dataset_id (string): the BigQuery dataset ID
        - delete_contents (boolean): removes all content from the dataset if is set to TRUE
        """
        dataset_ref = self.get_dataset_ref(dataset_id)
        try:
            self.client.delete_dataset(
                dataset_ref,
                delete_contents=delete_contents
            )
            logging.info(
                "Dataset %s.%s deleted",
                self.project_id,
                dataset_id
            )
            time.sleep(1)
        except exceptions.Conflict as error:
            logging.error(error)

    def create_dataset(self, dataset_id=bq_default_dataset()):
        """ 
        Create a BigQuery dataset.
        Arguments:
        - dataset_id (string): the BigQuery dataset ID
        """
        dataset_ref = self.get_dataset_ref(dataset_id)
        if self.dataset_exists(dataset_id):
            logging.info(
                "Dataset %s already exists in project %s",
                dataset_id,
                self.project_id
            )
        else:
            try:
                self.client.create_dataset(dataset_ref)
                logging.info(
                    "Created dataset %s in in project %s",
                    dataset_id,
                    self.project_id
                )
            except exceptions.Conflict as error:
                logging.error(error)

    def table_exists(self, table_id, dataset_id=bq_default_dataset()):
        """
        Checks if a BigQuery table exists
        Arguments:
        - dataset_id (string): the BigQuery dataset ID
        - table_id (string): the BigQuery table ID
        """
        table_ref = self.get_table_ref(dataset_id, table_id)
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def delete_table(self, table_id, dataset_id=bq_default_dataset()):
        """ Delete a BigQuery table.
        Parameters:
        dataset_id: the BigQuery dataset ID
        table_id: the BigQuery table ID
        """
        try:
            table_ref = self.get_table_ref(dataset_id, table_id)
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

    def initiate_table(self, table_id, schema_path, dataset_id=bq_default_dataset(), partition=False, clustering=None):

        """  Creates dataset_name.table_name in BigQuery.
        Arguments:
            - schema_path (string): full path to the schema file to be used to create the table.
            - dataset_name (string): target dataset
            - table_name (string): Optional. If not specify, will use the file name specify in
            the schema file.
        """

        if self.table_exists(
                dataset_id=dataset_id,
                table_id=table_id):
            logging.info("Table %s.%s already exists in project %s",
                         dataset_id,
                         table_id,
                         self.project_id)
            self.apply_patch(
                dataset_id=dataset_id,
                table_id=table_id,
                schema_path=schema_path
            )
        else:
            schema = read_table_schema_from_file(schema_path)
            table = bigquery.Table(
                self.get_table_ref(dataset_id, table_id), 
                schema=schema
                )
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

    def create_table(self, table_id, dataset_id=bq_default_dataset(), sql=None, file=None,
                     write_disposition='WRITE_TRUNCATE', use_legacy_sql=False,
                     location='US', schema_path='',
                     partition=False,
                     partition_field='_PARTITIONTIME', 
                     clustering=None,
                     priority='INTERACTIVE'):
        """ create a bigquery table from a sql query """

        if sql is None and file is None:
            raise BigQueryExecutorError("EIther sql or file must be provided")
        if sql is None:
            sql = read_sql(file)

        if schema_path != '':
            self.initiate_table(
                dataset_id=dataset_id,
                table_id=table_id,
                schema_path=schema_path,
                partition=partition
            )
            if write_disposition == "WRITE_TRUNCATE":
                self.truncate_table(dataset_id=dataset_id, table_id=table_id)
                write_disposition = "WRITE_EMPTY"
        else:
            pass

        job_config = bigquery.QueryJobConfig()
        job_config.destination = self.get_table_ref(dataset_id, table_id)
        job_config.write_disposition = set_write_disposition(write_disposition)
        job_config.use_legacy_sql = use_legacy_sql
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        job_config.priority = set_priority(priority)
        if partition:
            if partition_field == '_PARTITIONTIME':
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
        query_job.result()
        logging.info(
            'Query results loaded to table %s:%s.%s',
            self.project_id,
            dataset_id,
            table_id
        )

    def create_partition_table(self,
                               table_id,
                               dataset_id=bq_default_dataset(),
                               sql=None,
                               file=None,
                               use_legacy_sql=False,
                               write_disposition='WRITE_TRUNCATE',
                               partition_dates=None,
                               partition_field="_PARTITIONTIME",
                               clustering=None,
                               priority="INTERACTIVE"
                               ):
        """
        Partition to be generated are either passed through partition_dates or automatically generated using existing partitions.
        To filter on a specific partition, the filter DATE(_PARTITIONTIME) = {partition_date} can be used in your sql query.
        """
        if sql is None and file is None:
            raise BigQueryExecutorError("EIther sql or file must be provided")
        if sql is None:
            sql = read_sql(file)

        if not self.table_exists(dataset_id=dataset_id, table_id=table_id):
            raise BigQueryExecutorError("To create a partition, please initiate the table first using initiate_table.")

        if partition_dates is None:
            existing_dates = self.get_existing_partition_dates(
                dataset_id=dataset_id,
                table_id=table_id
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

        for date in dates:
            partition_name = self.set_partition_name(table=table_id, date=date)
            logging.info("Updating partition: %s", partition_name)
            self.create_table(
                sql=self.apply_partition_filter(
                    sql=sql,
                    date=date
                ),
                dataset_id=dataset_id,
                table_id=partition_name,
                write_disposition=write_disposition,
                use_legacy_sql=use_legacy_sql,
                partition=True,
                partition_field=partition_field,
                clustering=clustering,
                priority=priority
            )

    def apply_partition_filter(self, sql, date):
        return sql.format(
            partition_date=date
        )

    def validate_partition_dates(self, partition_dates):
        if not isinstance(partition_dates, list):
            raise BigQueryExecutorError("Partition dates need to be a list of date eg ['YYYYmmdd']")
        else:
            for date in partition_dates:
                validate_date(date=date, format='%Y%m%d')

    def set_partition_name(self, table, date):
        validate_date(date=date, format='%Y%m%d')
        return table + "$" + date.replace("-", "")

    def get_partition_dates(self, start_date, end_date, existing_dates):
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

    def get_existing_partition_query(self, dataset_id, table_id):
        sql = """ SELECT 
                    FORMAT_DATE('%Y%m%d', DATE(_PARTITIONTIME)) AS partition_id 
                  FROM 
                    {dataset_id}.{table_id} 
                  GROUP BY 
                    1 """.format(
                        dataset_id=dataset_id, 
                        table_id=table_id
                        )
        return self.execute_sql(
            sql = sql
        )

    def get_existing_partition_dates(self, table_id, dataset_id=bq_default_dataset()):
        if not self.table_exists(dataset_id=dataset_id, table_id=table_id):
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

    def get_table_schema(self, table_id, dataset_id=bq_default_dataset()):
        '''
        return SchemaField values
        '''
        table_ref = self.get_table_ref(dataset_id, table_id)
        table_schema = self.client.get_table(table_ref).schema
        return table_schema

    def identify_new_fields(self, table_id, schema_path, dataset_id=bq_default_dataset()):
        """ identifies new fields from a schema file """
        list_field = []
        schema_a = self.get_table_schema(
            table_id=table_id,
            dataset_id=dataset_id
        )
        schema_b = read_table_schema_from_file(schema_path)
        field_list_a = [schema_field.name for schema_field in schema_a]
        for schema_field in schema_b:
            if schema_field.name not in field_list_a:
                list_field.append(schema_field)
        return list_field

    def append_field(self, table_id, field, dataset_id=bq_default_dataset()):
        '''
        field: schema field object
        i.e. SchemaField('postcode', 'STRING', 'NULLABLE', None, ())
        '''
        table_ref = self.get_table_ref(dataset_id, table_id)
        table = self.client.get_table(table_ref)  # API request

        original_schema = table.schema
        new_schema = original_schema[:]  # creates a copy of the schema
        new_schema.append(field)

        table.schema = new_schema
        table = self.client.update_table(table, ["schema"])  # API request
        assert len(table.schema) == len(original_schema) + 1 == len(new_schema)
        return 0

    def apply_patch(self, table_id, schema_path, dataset_id=bq_default_dataset()):
        '''
        this function identifies and appends all the new fields to the original table
        '''
        logging.info("Attempting patch")
        logging.info("Checking for new fields...")
        new_fields = self.identify_new_fields(
            dataset_id=dataset_id,
            table_id=table_id,
            schema_path=schema_path
        )
        if new_fields != []:
            logging.info("New fields to be added:")
            logging.info(new_fields)
            for field in new_fields:
                self.append_field(
                    dataset_id=dataset_id,
                    table_id=table_id,
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

    def update_schema(self, table_id, schema_path, dataset_id=bq_default_dataset()):
        table_ref = self.get_table_ref(dataset_id, table_id)
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
        data = self.execute_sql(
            sql=sql,
            project_id=project_id,
            dialect=dialect
        )
        return data

    def load_dataframe(self, df, table_id, dataset_id=bq_default_dataset(), schema_path='', write_disposition="WRITE_TRUNCATE"):
        '''
        Loads pandas dataframe to BigQuery.
        '''
        if schema_path != '':
            self.initiate_table(
                table_id=table_id,
                dataset_id=dataset_id,
                schema_path=schema_path
            )
            schema = read_table_schema_from_file(schema_path)
        else:
            schema = None

        if self.table_exists(
                table_id=table_id,
                dataset_id=dataset_id
        ):
            data = df.rename(columns=lambda cname: cname.replace('.', '_'))
            table_ref = self.get_table_ref(dataset_id, table_id)
            job_config = bigquery.LoadJobConfig(schema=schema)
            job_config.write_disposition = set_write_disposition(write_disposition)
            job = self.client.load_table_from_dataframe(
                data,
                table_ref,
                job_config=job_config
            )
            job.result()
        else:
            raise Exception("Please initiate %s.%s or pass the schema file", dataset_id, table_id)

    def load_json_file(self, file, table_id, dataset_id=bq_default_dataset(), schema_path='', write_disposition="WRITE_TRUNCATE"):
        '''
        Loads JSON file (new line delimited) to BigQuery.
        '''
        if schema_path != '':
            self.initiate_table(
                table_id=table_id,
                schema_path=schema_path,
                dataset_id=dataset_id
            )
            schema = read_table_schema_from_file(schema_path)
        else:
            schema = None

        if self.table_exists(
                table_id=table_id,
                dataset_id=dataset_id
        ):
            table_ref = self.get_table_ref(dataset_id, table_id)
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
            raise Exception("Please initiate %s.%s or pass the schema file", dataset_id, table_id)

    def load_json_data(self, json, table_id, dataset_id=bq_default_dataset(), schema_path='', write_disposition="WRITE_TRUNCATE"):
        '''
        Loads JSON data to BigQuery.
        '''
        if schema_path != '':
            self.initiate_table(
                table_id=table_id,
                schema_path=schema_path,
                dataset_id=dataset_id
            )
            schema = read_table_schema_from_file(schema_path)
        else:
            schema = None
    
        if self.table_exists(
                table_id=table_id,
                dataset_id=dataset_id
        ):
            table_ref = self.get_table_ref(dataset_id, table_id)
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
            raise Exception("Please initiate %s.%s or pass the schema file", dataset_id, table_id)
    
    def truncate_table(self, table_id, dataset_id=bq_default_dataset()):
        """
        Delete all records from table but preserve structure
        e.g. schema, partitioning, clustering, description, labels, etc.
        """
        job_config = bigquery.QueryJobConfig()
        job_config.destination = self.get_table_ref(dataset_id, table_id)
        job_config.write_disposition = set_write_disposition("WRITE_TRUNCATE")
        query_job = self.client.query(
            query=f"SELECT * FROM `{dataset_id}.{table_id}` LIMIT 0",
            job_config=job_config
        )
        query_job.result()
        logging.info(
            'Table %s:%s.%s has been truncated',
            self.project_id,
            dataset_id,
            table_id
        )

    def copy_table(self, source_table_id, dest_table_id,
                   source_dataset_id=bq_default_dataset(), dest_dataset_id=bq_default_dataset(),
                   source_project_id=bq_default_project(), write_disposition='WRITE_TRUNCATE'):
        """
        Copy a BigQuery table
        
        Limitations:
        - Destination project is the default, i.e. specified in `local.env`
        - Destination dataset must reside in the same location as source (US, EU, etc.)

        Permissions:
        - The email address specified in `access_token.json` must have read permissions for the source
        """
        source_dataset_ref = bigquery.dataset.DatasetReference(source_project_id, source_dataset_id)
        source_table_ref = source_dataset_ref.table(source_table_id)
        dest_table_ref = self.get_table_ref(dest_dataset_id, dest_table_id)

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
            self.project_id,
            dest_dataset_id,
            dest_table_id
        )

    def count_rows(self, table_id, dataset_id=bq_default_dataset()):
        """
        Count rows in table
        
        Returns:
        non-negative integer
        """
        table_ref = self.get_table_ref(dataset_id, table_id)
        table = self.client.get_table(table_ref)
        return table.num_rows
    
    def count_columns(self, table_id, dataset_id=bq_default_dataset()):
        """
        Count columns in table
        
        Returns:
        non-negative integer
        """
        table_ref = self.get_table_ref(dataset_id, table_id)
        table = self.client.get_table(table_ref)
        return len(table.schema)

    def count_duplicates(self, table_id, primary_key: list, dataset_id=bq_default_dataset()):
        """
        Count duplicate rows in primary key

        Arguments:
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
                    {dataset_id}.{table_id}
                GROUP BY
                    {', '.join(primary_key)}
            )
            """
        )
        return data['dup_total'].values[0]

    def assert_unique(self, table_id, primary_key: list, dataset_id=bq_default_dataset(), ignore_error=False, **kwargs):
        """
        Assert uniqueness of primary key in table

        Arguments:
        primary_key: a list of one or more column names, e.g. ['col1', 'col2']
        ignore_error: boolean flag to prevent error being raised, useful for debugging

        Returns:
        - Nothing if there are no duplicate rows in primary key
        - Raise and log AssertionError if there are duplicate rows and ignore_error=False (default)
        - Log a warning if there are dupicate rows and ignore_error=True (debugging)
        """
        if self.count_duplicates(table_id, primary_key, dataset_id) != 0:
            msg = "Table %s:%s.%s is not unique on %s" % (
                self.project_id,
                dataset_id,
                table_id,
                ', '.join(primary_key)
            )
            if ignore_error:
                logging.warning(msg)
            else:
                raise AssertionError(msg)
