import os
import json
import pymysql
import requests
import sqlite3
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def connect_mysql():
    # configurations
    mysqlpass = os.environ['MYSQL_PASS']
    timeout = 10

    connection = pymysql.connect(
    charset="utf8mb4",
    connect_timeout=timeout,
    cursorclass=pymysql.cursors.DictCursor,
    db="staging",
    host="hangrytest-hangrytest.h.aivencloud.com",
    password=mysqlpass,
    read_timeout=timeout,
    port=21843,
    user="avnadmin",
    write_timeout=timeout,
    )
    return connection

def connect_local(database):
    db_file=database+'.db'
    connection = sqlite3.connect(db_file)
    return connection

def query_staging(connection):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT  menu_id,
                brand,
                name,
                price,
                cogs,
                effective_date
        FROM Menu_Staging;
    """)
    df_menu = pd.DataFrame(cursor.fetchall())

    cursor.close()
    connection.close()

    return df_menu

@data_loader
def data_load_staging_menu(*args, **kwargs):
    """
    Load data from database.
    """
    if kwargs['staging']=='local':
        connection = connect_local('stag')
        df_menu = query_staging(connection)
        df_menu.columns=['menu_id','brand','name','price','cogs','effective_date']

    elif kwargs['staging']=='mysql':
        connection = connect_mysql()
        df_menu = query_staging(connection)

    return df_menu

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'