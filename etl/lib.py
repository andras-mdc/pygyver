""" Functions used in different places ;) """
import os
from os import path
from google.cloud import bigquery
from etl.toolkit import validate_date
from datetime import date, timedelta

def set_write_disposition(write_disposition):
    """ Sets bigquery.WriteDisposition based on write_disposition """
    if write_disposition == 'WRITE_APPEND':
        return bigquery.WriteDisposition.WRITE_APPEND
    elif write_disposition == 'WRITE_EMPTY':
        return bigquery.WriteDisposition.WRITE_EMPTY
    elif write_disposition == 'WRITE_TRUNCATE':
        return bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        raise KeyError("{} is not a valid write_disposition key".format(write_disposition))

def bq_token_file_path():
    """
    Returns GOOGLE_APPLICATION_CREDENTIALS if env is set
    """
    return os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')

def bq_token_file_path_exists(token_path):
    """
    Returns True if the file exists, False otherwise
    """
    return path.exists(os.path.join("PYTHONPATH", token_path))

def bq_token_file_valid():
    """
    Checks whether the token file is valid.
    """
    token_path = bq_token_file_path()
    if token_path == '':
        raise ValueError(
            "Please set GOOGLE_APPLICATION_CREDENTIALS to the path to the access token from the PYTHONPATH."
        )
    elif bq_token_file_path_exists(token_path) is False:
        raise ValueError(
            "Token file could not be found. Please reset your GOOGLE_APPLICATION_CREDENTIALS/PYTHONPATH env vars. Current:",
            token_path
        )
    else:
        return True

def bq_use_legacy_sql():
    return os.environ.get('BIGQUERY_LEGACY_SQL', 'TRUE')

def bq_default_project():
    return os.environ.get('BIGQUERY_PROJECT', '')

def bq_default_dataset():
    return os.environ.get('BIGQUERY_DATASET', '')

def bq_billing_project():
    return bq_default_project()

def bq_start_date():
    start_date = os.environ.get('BIGQUERY_START_DATE', '2016-01-01')
    validate_date(
        start_date,
        error_msg = "Invalid BIGQUERY_START_DATE: {} should be YYYY-MM-DD".format(start_date)
    )
    return start_date

def bq_end_date():
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = os.environ.get('BIGQUERY_END_DATE', yesterday)
    validate_date(
        end_date,
        error_msg = "Invalid BIGQUERY_END_DATE: {} should be YYYY-MM-DD".format(end_date)
    )
    return end_date
