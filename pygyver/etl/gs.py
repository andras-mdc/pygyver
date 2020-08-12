""" Google Spreadsheet utility """
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from pygyver.etl.lib import bq_token_file_path


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
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(bq_token_file_path(), scope)
    client = gspread.authorize(credentials)
    if sheet_name != '':
        sheet = client.open_by_key(key).worksheet(sheet_name)
    else:
        sheet = client.open_by_key(key).get_worksheet(sheet_index)
    data = get_as_dataframe(sheet, **kwargs)
    return data
