import os
import sys
import json
from dotenv import load_dotenv
from gspread import authorize
from pandas import DataFrame, concat
from gspread.exceptions import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials as sac

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)
load_dotenv()
FILE_PATH = os.path.join(PROJECT_ROOT, os.getenv("CREDENTIALS_PATH"))


def read_gsheets(gsheets_url: str, sheets_to_read: list) -> DataFrame:
    """
    Fetch data from Google Sheets

    Args:
        gsheets_url: string, full link https://docs.google.com/spreadsheets/d/XXX
        sheets_to_read: list-like object of strings, each value is the sheet name to fetch

    Returns:
        DataFrame, append of all sheets specified from Google Sheets
    """

    with open(FILE_PATH, "r") as f:
        cred = json.load(f)

    client = sac.from_json_keyfile_dict(cred, "https://spreadsheets.google.com/feeds")
    session = authorize(client)
    book = session.open_by_url(gsheets_url)

    to_read = book.worksheets() if sheets_to_read is None else sheets_to_read
    df = DataFrame()
    for sheet in to_read:
        try:
            data = book.worksheet(sheet)
            sheets_data = DataFrame(data.get_values())
            sheets_data.rename(columns=sheets_data.iloc[0], inplace=True)
            sheets_data.drop(sheets_data.index[0], inplace=True)
            df = concat([df, sheets_data], axis=0)
        except WorksheetNotFound:
            print(f'Sheetname: "{sheet}" was not found at\n{gsheets_url}')
    return df
