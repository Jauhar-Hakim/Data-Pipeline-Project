import os
import json
import pymysql
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def data_load_mysql(*args, **kwargs):

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
        SELECT * FROM Menu;
    """)
    df_menu = pd.DataFrame(cursor.fetchall())

    cursor.execute("""
        SELECT * FROM Sales_Order;
    """)
    df_order = pd.DataFrame(cursor.fetchall())

    cursor.execute("""
        SELECT * FROM Promotion;
    """)
    df_promotion = pd.DataFrame(cursor.fetchall())

    return {'df_menu':df_menu.to_json(orient='records'),'df_order':df_order.to_json(orient='records'),'df_promotion':df_promotion.to_json(orient='records')}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
