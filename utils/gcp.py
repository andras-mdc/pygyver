import os
import re
import tempfile
import sqlparse
from google.cloud import bigquery
from google.cloud import storage


def read_sql(sql_path,
             sql_params={}):
    """
    Reads a .sql file, removes comments, and (optional) substitutes a dict of parameters
    :param sql_path: path/to/script.sql
    :param sql_params: example {'param_1': 3, 'param_2': ['a','b','c']}
    :return: string
    """
    # read the sql, remove the comments:
    with open(sql_path, 'r') as f:
        sql = sqlparse.format(f.read(), strip_comments=True)

    # substitute parameters:
    for key in sql_params.keys():
        if isinstance(sql_params[key], list):
            if isinstance(sql_params[key][0], str):
                sql_params[key] = '\',\''.join(sql_params[key])
                sql_params[key] = '\'{}\''.format(sql_params[key])
            else:
                sql_params[key] = ','.join([str(item) for item in sql_params[key]])

        sql = re.sub(key, str(sql_params[key]), sql)

    return sql


def query_bigquery(sql_path,
                   sql_params={},
                   location='US'):
    """
    :param sql_path: path to a local .sql file
    :param sql_params: parameters to substitute in the query
    :param location: query location for BigQuery
    :return: pandas dataframe if results=True
    """

    # read the sql, remove the comments:
    sql = read_sql(sql_path, sql_params)

    bigquery_client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    query_job = bigquery_client.query(sql,
                                      location=location,
                                      job_config=job_config)

    query_job.result()  # Waits for job to complete.

    return query_job.to_dataframe()


def exec_bigquery(sql_path,
                  dataset,
                  sql_params={},
                  table=None,
                  write_disposition='WRITE_EMPTY',
                  location='US'):

    """
    :param sql_path: path to a local .sql file
    :param sql_params: parameters to substitute in the query
    :param dataset:
    :param table: Name (Optional)
    :param write_disposition: examples:
                'WRITE_TRUNCATE' overwrites anything existing
                'WRITE_APPEND' appends to anything existing
                'WRITE_EMPTY' exits with error if table already exists
    :param location: query location for BigQuery
    :return: pandas dataframe if results=True
    """

    # read the sql, remove the comments:
    sql = read_sql(sql_path, sql_params)

    bigquery_client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    job_config.write_disposition = write_disposition

    if table is None:
        table = next(tempfile._get_candidate_names())
    table_ref = bigquery_client.dataset(dataset).table(table)
    job_config.destination = table_ref

    query_job = bigquery_client.query(sql,
                                      location=location,
                                      job_config=job_config)

    query_job.result()  # Waits for job to complete.

    print('Table written to {}'.format(':'.join([dataset, table])))


def export_bigquery(dataset,
                    table,
                    gcs_path,
                    gcs_bucket=os.getenv('GCS_BUCKET'),
                    location='US'):
    """
    Exports data from a BigQuery table to GCS
    :param dataset:
    :param table:
    :param gcs_path:
    :param gcs_bucket:
    :param location:
    :return:
    """

    uri = "gs://{}/{}".format(gcs_bucket, gcs_path)

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset)
    table_ref = dataset_ref.table(table)

    extract_job = bigquery_client.extract_table(table_ref,
                                                uri,
                                                location=location)

    extract_job.result()  # Waits for job to complete.

    print('Exported {}.{} to {}'.format(dataset, table, uri))


def import_bigquery(dataset,
                    table,
                    gcs_path,
                    gcs_bucket=os.getenv('GCS_BUCKET'),
                    location='US',
                    schema=[],
                    header=True,
                    write_disposition='WRITE_APPEND'):
    """
    Imports data from GCS to a BigQuery table, auto-detecting the schema
    :param dataset: BigQuery dataset to write to
    :param table: BigQuery table to write to
    :param gcs_path: where the GCS CSV resides
    :param gcs_bucket: the GCS bucket
    :param location: data location e.g. 'US'
    :param schema: list of tuples (colname, dtype)
    :param header: does the CSV contain a header?
    :param write_disposition: examples:
                'WRITE_TRUNCATE' overwrites anything existing
                'WRITE_APPEND' appends to anything existing
                'WRITE_EMPTY' exits with error if table already exists
    :return:
    """

    uri = "gs://{}/{}".format(gcs_bucket, gcs_path)

    bigquery_client = bigquery.Client()

    job_config = bigquery.LoadJobConfig()
    if len(schema) == 0:
        job_config.autodetect = True
    else:
        job_config.schema = [bigquery.SchemaField(colname, dtype) for colname, dtype in schema]
        if header:
            job_config.skip_leading_rows = 1

    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = write_disposition

    dataset_ref = bigquery_client.dataset(dataset)
    table_ref = dataset_ref.table(table)

    load_job = bigquery_client.load_table_from_uri(uri,
                                                   table_ref,
                                                   job_config=job_config,
                                                   location=location)

    load_job.result()  # Waits for job to complete.

    print('Imported {} to {}.{}'.format(uri, dataset, table))


def upload_gcs(file_path,
               gcs_path,
               gcs_bucket=os.getenv('GCS_BUCKET')):
    """
    :param file_path: path to file on the local machine
    :param gcs_path: path to write to, within gcs bucket
    :param gcs_bucket: the gcs bucket
    :return:
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(file_path)


def download_gcs(gcs_path,
                 file_path,
                 gcs_bucket=os.getenv('GCS_BUCKET')):
    """
    :param gcs_path: path to file within the gcs bucket
    :param file_path: path to write to, on the local machine
    :param gcs_bucket: the gcs bucket
    :return:
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.download_to_filename(file_path)


def df_to_gcs(gcs_path,
              df,
              gcs_bucket=os.getenv('GCS_BUCKET'),
              index=False):
    """
    Writes a Dataframe to Google Cloud Storage directly
    :param gcs_path:
    :param df:
    :param gcs_bucket:
    :return:
    """

    client = storage.Client()
    bucket = client.get_bucket(gcs_bucket)
    bucket.blob(gcs_path).upload_from_string(df.to_csv(index=index), 'text/csv')


