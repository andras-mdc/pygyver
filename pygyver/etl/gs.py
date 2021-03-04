""" Google Spreadsheet utility """
import gspread
import json
import pandas as pd
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe
from pygyver.etl import lib
from pygyver.etl.toolkit import (
    create_folder,
    split_full_path,
    write_text_file
)


def gspread_client():
    """ Sets BigQuery client.
    """
    lib.bq_token_file_valid()
    credentials = service_account.Credentials.from_service_account_file(
        lib.bq_token_file_path(),
        scopes=[
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
    )
    gc = gspread.Client(auth=credentials)
    gc.session = AuthorizedSession(credentials)
    return gc


def load_gs_to_dataframe(key, sheet_name='', sheet_index=0, **kwargs):
    '''
    Loads Google Spreadsheet to a pandas dataframe

    Args:
        key (str): Google Sheets key a.k.a. Spreadsheet ID
        sheet_name (str): Name of the sheet within the Spreadsheet
        sheet_index (int): Zero-based index where the sheet is within the Spreadsheet
        **kwargs: Any that are supported by this Pandas version’s text parsing readers, such as pandas.read_csv
            e.g. delimiter=None, header='infer', names=None, index_col=None, etc.
            For more info, see docs for gspread-dataframe and pandas.read_csv

    Returns:
        Spreadsheet as a DataFrame.
    '''
    client = gspread_client()

    if sheet_name != '':
        sheet = client.open_by_key(key).worksheet(sheet_name)
    else:
        sheet = client.open_by_key(key).get_worksheet(sheet_index)
    data = get_as_dataframe(sheet, **kwargs)
    return data


def load_gs_range_to_dataframe(
    key: str,
    cell_range: str,
    sheet_name: str = '',
    sheet_index: int = 0,
    **kwargs
) -> pd.DataFrame:
    """
    Load a range from a Google Spreadsheet to a pandas DataFrame.

    Args:
        key: 44-digit spreadsheet id from the url e.g. 1DR1...k-xDU
        cell_range: data range including column names e.g. B1:C10
        sheet_name: name of the sheet within the spreadsheet e.g. Sheet1
        sheet_index: zero-based index where the sheet is within the spreadsheet
        **kwargs: any that are supported by this pandas version’s text parsing
            readers, such as pandas.read_csv

    Returns:
        df: pandas dataframe of the selected cell range

    Notes:
        - only one of sheet_name/sheet_index is required, defaults to the first
            sheet but prioritizing sheet_name if both are given

    Requirements:
        - share gs with the service account
        - gs api enabled for the project
    """
    # df filter params
    skiprows, nrows, usecols = lib._extract_cell_range_params(cell_range)

    df = load_gs_to_dataframe(
        key=key,
        sheet_name=sheet_name,
        sheet_index=sheet_index,
        skiprows=skiprows,
        nrows=nrows,
        usecols=usecols,
        **kwargs
    )
    return df


def load_gs_to_json_schema(
    key: str,
    schema_path: str,
    cell_range: str = None,
    sheet_name: str = "schema",
    sheet_index: int = 0,
):
    """
    Load table schema from google spreadsheet and write as json.

    Args:
        key: 44-digit spreadsheet id from the url e.g. '1DR1...k-xDU'
        schema_path: path to the schema file (the output to be written)
            e.g. 'path/schema.json'
        cell_range: data range including column names e.g. 'B1:C10'
        sheet_name: name of the sheet within the spreadsheet e.g. 'Sheet1'
        sheet_index: zero-based index where the sheet is within the spreadsheet
        column_order: the order of columns given for the schema, can only
            contain any of

    Notes:
        - if no 'cell_range' is defined, the sheet has to be otherwise empty
        - only one of sheet_name/sheet_index is required, defaults to the first
            sheet but prioritizing sheet_name if both are given
        - the gs load required the python data types otherwise it's making
            incorrect transformations
        - 'column mode' will be set as 'NULLABLE'

    Requirements:
        - share gs with the service account
        - gs api enabled for the project
    """
    # load gs
    if cell_range:
        df = load_gs_range_to_dataframe(
            key=key,
            cell_range=cell_range,
            sheet_name=sheet_name,
            sheet_index=sheet_index,
            evaluate_formulas=True
        )
    else:
        df = load_gs_to_dataframe(
            key=key,
            sheet_name=sheet_name,
            sheet_index=sheet_index,
            evaluate_formulas=True
        )
    # find header / drop empty rows and columns
    df = df.dropna(how="all", axis=1).dropna(how="all", axis=0)
    if any("Unnamed" in x for x in df.columns):
        df = df.rename(columns=df.iloc[0]).drop(df.index[0])

    # validate column names
    assert set(df.columns).issubset(["name", "type", "mode", "description"]), (
        "columns should be 'name', 'type', 'mode', 'description'"
    )
    assert set(["name", "type"]).issubset(list(df.columns)), (
        "'name' and 'type' are mandatory columns"
    )
    if "mode" not in df.columns:
        df["mode"] = "NULLABLE"
    else:
        df["mode"] = df["mode"].fillna("NULLABLE")
    if "description" not in df.columns:
        df["description"] = ""
    else:
        df["description"] = df["description"].fillna("")
    df = df[[
        "name",
        "type",
        "mode",
        "description"
    ]]
    # write
    f_path, f_name = split_full_path(schema_path)
    if f_path:
        create_folder(f_path)

    json_data = []
    for _, row in df.iterrows():
        json_data.append(
            {
                "name": row["name"],
                "type": row["type"],
                "mode": row["mode"],
                "description": row["description"]
            }
        )
    with open(schema_path, "w") as f:
        json.dump(json_data, f, indent=4)
    print(f"Table schema file created at '{schema_path}.'")


def load_gs_to_sql_mock(
    key: str,
    mock_path: str,
    sheet_name: str = "acceptance",
    sheet_index: int = 1
):
    """
    Load mock data from google spreadsheet and write as cte sql.

    Args:
        key: 44-digit spreadsheet id from the url e.g. '1DR1...k-xDU'
        mock_path: path to the mock data file (the output for the cte)
            e.g. 'path/mock.sql'
        sheet_name: name of the sheet within the spreadsheet e.g. 'Sheet1'
        sheet_index: zero-based index where the sheet is within the spreadsheet

    Notes:
        - only one of sheet_name/sheet_index is required, defaults to the first
            sheet but prioritizing sheet_name if both are given
        - the gs load required the python data types otherwise it's making
            incorrect transformations

    Requirements:
        - share gs with the service account
        - gs api enabled for the project
    """
    # get table list and info
    table_columns = lib._extract_table_columns(
        key=key,
        sheet_name=sheet_name,
        sheet_index=sheet_index
    )
    # loop through tables
    ctes = []
    for table_name, cols in table_columns.items():

        # get schema
        schema_sql = lib._extract_table_schema(
            key=key,
            cols=cols,
            sheet_name=sheet_name,
            sheet_index=sheet_index
        )
        schema_py = lib._schema_sql_to_py(schema_sql)
        schema_sql = lib._schema_sql_to_bq_compatibility(schema_sql)

        # get table and convert to cte
        df = lib._extract_table_data(
            key=key,
            cols=cols,
            schema=schema_py,
            sheet_name=sheet_name,
            sheet_index=sheet_index
        )
        cte = lib._df_to_cte(
            df=df,
            schema_sql=schema_sql,
            cte_name=table_name
        )
        ctes.append(cte)

    # write
    f_path, f_name = split_full_path(mock_path)
    if f_path:
        create_folder(f_path)

    content = ",\n\n".join(ctes)
    write_text_file(
        content=content,
        file_path=mock_path
    )
    print(f"Mock CTE file created at '{mock_path}.'")
