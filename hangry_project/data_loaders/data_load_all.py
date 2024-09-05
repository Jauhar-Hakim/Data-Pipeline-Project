import os
import json
import pymysql
import requests
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

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

def data_source_local():
    file_directories = 'newdata'
    df_menu = pd.read_csv(file_directories+'/Menu.csv')
    df_order = pd.read_csv(file_directories+'/Order.csv')
    df_promotion = pd.read_csv(file_directories+'/Promotion.csv')
    return df_menu, df_order, df_promotion

def data_source_api():
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
    
    return df_menu, df_order, df_promotion

def data_source_mysql():
    # configurations
    mysqlpass = os.environ['MYSQL_PASS']
    timeout = 10
    connection = pymysql.connect(
    charset="utf8mb4",
    connect_timeout=timeout,
    cursorclass=pymysql.cursors.DictCursor,
    db="rawdata",
    host="hangrytest-hangrytest.h.aivencloud.com",
    password=mysqlpass,
    read_timeout=timeout,
    port=21843,
    user="avnadmin",
    write_timeout=timeout,
    )
    cursor = connection.cursor()

    cursor.execute("""
        SELECT  menu_id,
                brand,
                name,
                price,
                cogs,effective_date
        FROM Menu;
    """)
    df_menu = pd.DataFrame(cursor.fetchall())

    cursor.execute("""
        SELECT  order_id,
                menu_id,
                quantity,
                sales_date
        FROM Sales_Order;
    """)
    df_order = pd.DataFrame(cursor.fetchall())

    cursor.execute("""
        SELECT  start_date,
                end_date,
                disc_value,
                max_disc 
        FROM Promotion;
    """)
    df_promotion = pd.DataFrame(cursor.fetchall())

    return df_menu, df_order, df_promotion

@data_loader
def data_load_all(*args, **kwargs):
    """
    Load data from local, api, and database.
    """
    if kwargs['source']=='local':
        df_menu, df_order, df_promotion = data_source_local()

    elif kwargs['source']=='api':
        df_menu, df_order, df_promotion = data_source_api()
    
    elif kwargs['source']=='mysql':
        df_menu, df_order, df_promotion = data_source_mysql()

    return {'df_menu':df_menu.to_json(orient='records'),'df_order':df_order.to_json(orient='records'),'df_promotion':df_promotion.to_json(orient='records')}

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'