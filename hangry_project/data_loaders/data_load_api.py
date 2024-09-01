import os
import json
import requests
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    def get_google_sheet_data(spreadsheet_id,sheet_name, api_key):
        # Construct the URL for the Google Sheets API
        url = f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{sheet_name}!A1:Z?alt=json&key={api_key}'

        try:
            # Make a GET request to retrieve data from the Google Sheets API
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the JSON response
            data = response.json()
            return data

        except requests.exceptions.RequestException as e:
            # Handle any errors that occur during the request
            print(f"An error occurred: {e}")
            return None

    # configurations
    spreadsheet_id = '1DhbDtw4TwVHF3qnwle_MwXjqQLQOdeAO8PxrJPe22zU'
    api_key = os.environ['SHEET_API_KEY']
    sheet_name_1 = "Menu"
    sheet_name_2 = "Order"
    sheet_name_3 = "Promotion"

    sheet_data = get_google_sheet_data(spreadsheet_id,sheet_name_1, api_key)
    df_menu = pd.DataFrame(sheet_data['values'][1:], columns=sheet_data['values'][0])

    sheet_data = get_google_sheet_data(spreadsheet_id,sheet_name_2, api_key)
    df_order = pd.DataFrame(sheet_data['values'][1:], columns=sheet_data['values'][0])

    sheet_data = get_google_sheet_data(spreadsheet_id,sheet_name_3, api_key)
    df_promotion = pd.DataFrame(sheet_data['values'][1:], columns=sheet_data['values'][0])

    return {'df_menu':df_menu.to_json(orient='records'),'df_order':df_order.to_json(orient='records'),'df_promotion':df_promotion.to_json(orient='records')}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
