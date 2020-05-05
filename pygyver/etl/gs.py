""" Google Spreadsheet utility """
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pygyver.etl.lib import bq_token_file_path


def load_gs_to_dataframe(key, sheet_name='', sheet_index=0):
    '''
    Loads Google Spreadsheet to a pandas dataframe
    '''
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(bq_token_file_path(), scope)
    client = gspread.authorize(credentials)
    if sheet_name != '':
        sheet = client.open_by_key(key).worksheet(sheet_name)
        recs = sheet.get_all_records()
        data = pd.DataFrame(recs)
    else:
        sheet = client.open_by_key(key).get_worksheet(sheet_index)
        recs = sheet.get_all_records()
        data = pd.DataFrame(recs)
    return data
