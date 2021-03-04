""" Functions used in different places ;) """
import json
import numpy as np
import os
import pandas as pd
import re
import string
from datetime import date, datetime, timedelta
from google.cloud import bigquery
from inspect import stack
from pygyver.etl import gs
from pygyver.etl.toolkit import validate_date
from typing import List, Tuple


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


def set_priority(priority):
    """ Sets bigquery.QueryPriority based on write_disposition """
    if priority == 'BATCH':
        return bigquery.QueryPriority.BATCH
    elif priority == 'INTERACTIVE':
        return bigquery.QueryPriority.INTERACTIVE
    else:
        raise KeyError("{} is not a valid priority key".format(priority))


def read_table_schema_from_file(path):
    """
    Read table schema from json file.

    Arguments:
        - path: full path to schema file from folder pipelines
    """
    full_path = os.path.join(os.environ.get("PROJECT_ROOT"), path)
    with open(full_path, 'r') as file_path:
        json_schema = json.load(file_path)
        return bigquery.schema._parse_schema_resource({'fields': json_schema})


def bq_token_file_path():
    """
    Returns GOOGLE_APPLICATION_CREDENTIALS if env is set
    """
    return os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')


def bq_token_file_path_exists(token_path):
    """
    Returns True if the file exists, False otherwise
    """
    return os.path.exists(token_path)


def bq_token_file_valid():
    """
    Checks whether the token file is valid.
    """
    token_path = bq_token_file_path()
    if token_path == '':
        raise ValueError(
            "Please set GOOGLE_APPLICATION_CREDENTIALS to the path to the access token."
        )
    elif bq_token_file_path_exists(token_path) is False:
        raise ValueError(
            "Token file could not be found. Please reset your GOOGLE_APPLICATION_CREDENTIALS env var. Current:",
            token_path
        )
    else:
        return True


def bq_use_legacy_sql():
    """
    Returns BIGQUERY_LEGACY_SQL if env is set
    """
    return os.environ.get('BIGQUERY_LEGACY_SQL', 'TRUE')


def bq_default_project():
    """
    Returns BIGQUERY_PROJECT if env is set
    """
    return os.environ.get('BIGQUERY_PROJECT', '')


def bq_default_prod_project():
    """
    Returns BIGQUERY_PROD_PROJECT if env is set or 'copper-actor-127213' if not
    """
    return os.environ.get('BIGQUERY_PROD_PROJECT', 'copper-actor-127213')


def bq_default_dataset():
    """
    Returns BIGQUERY_DATASET if env is set
    """
    return os.environ.get('BIGQUERY_DATASET', '')


def bq_billing_project():
    """
    Returns BIGQUERY_PROJECT if env is set
    """
    return bq_default_project()


def bq_start_date():
    """
    Returns BIGQUERY_START_DATE if env is set. Defaults to '2016-01-01'.
    """
    start_date = os.environ.get('BIGQUERY_START_DATE', '2016-01-01')
    validate_date(
        start_date,
        error_msg="Invalid BIGQUERY_START_DATE: {} should be YYYY-MM-DD".format(start_date)
    )
    return start_date


def bq_end_date():
    """
    Returns BIGQUERY_LEGACY_SQL if env is set. Defaults to Yesterday.
    """
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = os.environ.get('BIGQUERY_END_DATE', yesterday)
    validate_date(
        end_date,
        error_msg="Invalid BIGQUERY_END_DATE: {} should be YYYY-MM-DD".format(end_date)
    )
    return end_date


def remove_first_slash(word=''):
    if word == '':
        return word
    if word[0] == '/':
        return word[1:]
    return word


# AWS
def s3_default_bucket():
    """
    Returns AWS_S3_BUCKET if env is set
    """
    return os.getenv('AWS_S3_BUCKET', '')


def s3_default_root():
    """
    Returns AWS_S3_ROOT if env is set
    """
    return os.getenv('AWS_S3_ROOT', '')


def extract_args(content, to_extract: str, kwargs={}):
    if len(kwargs) > 0:
        for x in content:
            apply_kwargs(x.get(to_extract), kwargs)
    return [x.get(to_extract, '') for x in content if x.get(to_extract, '') != '']


def apply_kwargs(orig, kwargs):
    if orig is not None:
        for key_, value_ in orig.items():
            for kwargs_key, kwargs_value in kwargs.items():
                if '$' + kwargs_key == value_:
                    orig.update({key_: kwargs_value})


def gcs_default_project():
    """
    Returns GOOGLE_CLOUD_PROJECT if env is set
    """
    return os.environ.get('GCS_PROJECT', '')


def gcs_default_bucket():
    """
    Returns GCS_BUCKET if env is set
    """
    return os.environ.get('GCS_BUCKET', '')


def add_dataset_prefix(obj, dataset_prefix: str, kwargs={}):

    if isinstance(obj, list):
        for i in obj:
            add_dataset_prefix(i, dataset_prefix, kwargs)

    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, list):
                for i in v:
                    add_dataset_prefix(i, dataset_prefix, kwargs)

            if isinstance(v, dict):
                apply_kwargs(v, kwargs)
                add_dataset_prefix(v, dataset_prefix, kwargs)
            else:
                if k == 'dataset_id':
                    obj[k] = dataset_prefix + obj[k]


def get_dataset_prefix():
    """
    Call this method from a release script listed in pipeline.yaml
    The dataset_prefix can then be added to target dataset_id's to pass dry run tests

    Returns:
        if release module is run from PipelineExecutor:
            return dataset_prefix (string or None)
        else:
            return None
    """
    parent_locals = [
        f[0].f_locals for f in stack()
        if "pygyver/etl/pipeline.py" in f.filename
        and f.function == "run_python_file"
    ]

    if parent_locals:
        dataset_prefix = parent_locals[0].get('_dataset_prefix', None)
    else:
        dataset_prefix = None

    return dataset_prefix


# google sheet to json schema / sql mock
def _convert_column_to_number(
    col_letter: str
) -> int:
    """
    Convert spreadsheet-style column letter to its corresponding number,
    starting with 0.

    Args:
        col_letter: e.g. A, L, AB

    Returns:
        col_number: e.g. 0, 11, 27
    """
    col_number = 0
    for c in col_letter:
        if c in string.ascii_letters:
            col_number = col_number * 26 + (ord(c.upper()) - ord('A')) + 1
    return col_number - 1


def _extract_cell_range_params(
    cell_range: str
) -> Tuple[int, int, List[int]]:
    """
    Extract params to filter the correct cell range from a table.

    Args:
        cell_range - spreadsheet range e.g. B1:C10

    Returns:
        Tuple[
            skiprows - number rows to skip e.g. 0, 3, 10
            nrows - number of rows to insert e.g. 5, 10, 12
            usecols - list of column numbers to insert e.g [0, 1, 2, 3]
        ]
    """
    # extract column and row values
    try:
        cr_1, cr_2 = cell_range.split(":")
        c_1 = re.findall(r"[a-zA-Z]+", cr_1)[0].upper()
        c_2 = re.findall(r"[a-zA-Z]+", cr_2)[0].upper()
        d_1 = int(re.findall(r"\d+", cr_1)[0])
        d_2 = int(re.findall(r"\d+", cr_2)[0])
        cs = sorted([
            _convert_column_to_number(c_1),
            _convert_column_to_number(c_2)
        ])
    except IndexError:
        raise Exception(
            "Incorrect input, required format is e.g. 'B3:L10'"
        )
    # params for gspread
    skiprows = min([d_1, d_2]) - 1
    nrows = abs(d_1 - d_2)
    usecols = list(
        range(
            cs[0],
            cs[1] + 1
        )
    )
    return skiprows, nrows, usecols


def _extract_table_columns(
    key: str,
    sheet_name: str = "acceptance",
    sheet_index: int = 1
) -> dict:
    """
    Extract the table names and column indices from the full gs df.

    Args:
        key: 44-digit spreadsheet id from the url e.g. '1DR1...k-xDU'
        sheet_name: name of the sheet within the spreadsheet e.g. 'Sheet1'
        sheet_index: zero-based index where the sheet is within the spreadsheet

    Returns:
        table_details: table_name-[col_idx] key-value pairs
    """
    # load gs
    df = gs.load_gs_to_dataframe(
        key=key,
        sheet_name=sheet_name,
        sheet_index=sheet_index,
        evaluate_formulas=True
    )
    df = df.dropna(
        axis=1,
        how="all"
    )
    # extract details
    cols = list(df.columns)
    table_names = [
        x for x in cols if
        ("." in x) & ("Unnamed" not in x)
    ]
    table_idx_1 = [
        cols.index(x) for x in table_names
    ]
    table_idx_2 = (
        table_idx_1 + [len(cols)]
    )[1:]

    table_details = {
        v: list(range(table_idx_1[k], table_idx_2[k]))
        for k, v in enumerate(table_names)
    }
    return table_details


def _extract_table_schema(
    key: str,
    cols: List,
    sheet_name: str = "acceptance",
    sheet_index: int = 1
) -> dict:
    """
    Extract the table schema from the full gs df using the column indices.

    Args:
        key: 44-digit spreadsheet id from the url e.g. '1DR1...k-xDU'
        cols: column indices to insert e.g. [3, 4, 5, 6]
        sheet_name: name of the sheet within the spreadsheet e.g. 'Sheet1'
        sheet_index: zero-based index where the sheet is within the spreadsheet

    Returns:
        schema_dict: column name-sql column type as key-value pairs
            e.g. {'uid': 'STRING', 'clicks': 'INTEGER'}
    """
    schema_df = gs.load_gs_to_dataframe(
        key=key,
        usecols=cols,
        skiprows=1,
        nrows=1,
        sheet_name=sheet_name,
        sheet_index=sheet_index,
        evaluate_formulas=True
    )
    schema_df.columns = [
        x.split(".")[0] for x in schema_df.columns
    ]
    schema_dict = dict(
        zip(
            list(schema_df.columns),
            list(schema_df.iloc[0, :])
        )
    )
    return schema_dict


def _extract_table_data(
    key: str,
    cols: List,
    schema: dict,
    sheet_name: str = "acceptance",
    sheet_index: int = 1
) -> pd.DataFrame:
    """
    Extract the table data from the full gs df using the column indices.

    Args:
        key: 44-digit spreadsheet id from the url e.g. '1DR1...k-xDU'
        cols: column indices to insert e.g. [3, 4, 5, 6]
        schema: column name-python type as key-value pairs
            e.g. {'uid': str, 'clicks': 'Int64'}
        sheet_name: name of the sheet within the spreadsheet e.g. 'Sheet1'
        sheet_index: zero-based index where the sheet is within the spreadsheet

    Returns:
        df: pandas dataframe of the selected cell range
    """
    df = gs.load_gs_to_dataframe(
        key=key,
        usecols=cols,
        dtype=schema,
        names=list(schema.keys()),
        sheet_name=sheet_name,
        sheet_index=sheet_index,
        skiprows=3,
        evaluate_formulas=True
    )
    df = df.dropna(how="all")

    return df


def _schema_sql_to_bq_compatibility(
    schema_dict: dict
) -> dict:
    """
    Convert sql schema to be compatible with the bq ui.

    Args:
        schema_dict: column name-sql column type as key-value pairs
            e.g. {'uid': 'STRING', 'clicks': 'INTEGER'}

    Returns:
        schema_dict: column name-sql column type as key-value pairs
            e.g. {'uid': 'STRING', 'clicks': 'INT64'}
    """
    for k, v in schema_dict.items():
        if v == "INTEGER":
            schema_dict[k] = "INT64"
        elif v == "FLOAT":
            schema_dict[k] = "FLOAT64"

    return schema_dict


def _extract_schema_json(
    schema_path: str
) -> dict:
    """
    Extract the sql schema from the json as key-value dict pairs.

    Args:
        schema_path: path to the schema file e.g. 'path/file.json'

    Returns:
        schema_dict: column name-sql column type as key-value pairs
            e.g. {'uid': 'STRING', 'clicks': 'INTEGER'}
    """
    with open(schema_path) as json_file:
        json_data = json.load(json_file)

    schema_dict = {x["name"]: x["type"] for x in json_data}

    schema_dict = _schema_sql_to_bq_compatibility(schema_dict)

    return schema_dict


def _schema_sql_to_py(
    schema_sql: dict
) -> dict:
    """
    Convert sql schema to python/pandas format.

    Args:
        schema_sql: column name-sql type as key-value pairs
            e.g. {'uid': 'STRING', 'clicks': 'INTEGER'}

    Returns:
        schema_py: column name-python type as key-value pairs
            e.g. {'uid': str, 'clicks': 'Int64'}

    Notes:
        - defaults to string if type not in the converter dict
    """
    schema_sql_to_py = {
        "BOOLEAN": str,  # cast error if using pandas boolean
        "DATE": datetime,
        "DATETIME": datetime,
        "FLOAT": np.float64,
        "FLOAT64": np.float64,
        "INT64": "Int64",
        "INTEGER": "Int64",  # pandas integer type that's nullable
        "NUMERIC": np.float64,
        "STRING": str,
        "TIME": str,
        "TIMESTAMP": str
    }
    schema_py = {
        k: schema_sql_to_py[v]
        if v in schema_sql_to_py
        else str
        for k, v in schema_sql.items()
    }
    return schema_py


def _field_to_cte(
    idx: int,
    field: tuple,
    field_type: str
) -> str:
    """
    Convert a single field to its string equivalent for a cte.

    Args:
        idx: row number, used to define if it's a first line
        field: DataFrame cell as tuple(column_name, value)
            e.g. tuple('store_code', 'gb')
        field_type: sql data type e.g. 'STRING', 'INTEGER'

    Returns:
        field_cte: field in sql cte format as str
            - first row e.g. "CAST('gb' AS STRING) AS country_code"
            - latter rows e.g. "CAST('gb' AS STRING)"
    """
    # handle nulls
    f_format = "NULL" if pd.isna(field[1]) else f"'{field[1]}'"

    # first line - include column name
    field_cte = f"CAST({f_format} AS {field_type})"
    if idx == 0:
        field_cte = f"{field_cte} AS {field[0]}"

    return field_cte


def _row_to_cte(
    idx: int,
    row: pd.core.series.Series,
    schema_sql: dict
) -> str:
    """
    Convert a DataFrame row to its string equivalent for a cte.

    Args:
        idx: row number, used to define if it's a first line
        row: DataFrame row object
        schema_sql: column name-sql type as key-value pairs
            e.g. {'uid': 'STRING', 'clicks': 'INTEGER'}

    Returns:
        line_cte: row in sql cte format as str
    """
    line = ", ".join([
        _field_to_cte(idx, r, schema_sql[r[0]]) for r in row.iteritems()
    ])
    if idx == 0:
        line_cte = f"\tSELECT {line}"
    else:
        line_cte = f"\tUNION ALL\n\tSELECT {line}"

    return line_cte


def _df_to_cte(
    df: pd.DataFrame,
    schema_sql: dict,
    cte_name: str
) -> str:
    """
    Convert a DataFrame to a cte format.

    Args:
        df: DataFrame to convert
        schema_sql: column name-sql type as key-value pairs
            e.g. {'uid': 'STRING', 'clicks': 'INTEGER'}
        cte_name: full name (dataset_id.table_id) of the cte table
            e.g. data.country_product

    Returns:
        cte: the complete cte as str
    """
    lines = "\n".join([
        _row_to_cte(idx, row, schema_sql) for idx, row in df.iterrows()
    ])
    cte = f"`{cte_name}` AS (\n{lines}\n)"

    return cte
