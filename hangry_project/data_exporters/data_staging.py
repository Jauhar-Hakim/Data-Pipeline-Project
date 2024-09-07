import os
import json
import sqlite3
import pymysql
import numpy as np
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

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

def create_database_mysql(connection):
    cursor = connection.cursor()
    # Create Mysql Database
    cursor.execute("""
        CREATE DATABASE IF NOT EXISTS staging;
    """)
    cursor.execute("""
        USE staging;
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS
            Menu_Staging (id INTEGER PRIMARY KEY AUTO_INCREMENT,
                            menu_id INTEGER,
                            brand VARCHAR(32),
                            name VARCHAR(128),
                            price INTEGER,
                            cogs INTEGER,
                            effective_date DATE,
                            UNIQUE(menu_id, effective_date));
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS
            Order_Staging (id INTEGER PRIMARY KEY AUTO_INCREMENT,
                            order_id INTEGER,
                            menu_id INTEGER,
                            quantity INTEGER,
                            sales_date DATE,
                            UNIQUE(order_id, menu_id, sales_date),
                            FOREIGN KEY (menu_id) REFERENCES Menu_Staging(menu_id));
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS
            Promotion_Staging (id INTEGER PRIMARY KEY,
                                start_date DATE,
                                end_date DATE,
                                disc_value DECIMAL(5,3),
                                max_disc INTEGER,
                                UNIQUE(id, start_date, end_date));
        """)
    
def create_database_local(connection):
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS
            Menu_Staging (id INTEGER PRIMARY KEY AUTOINCREMENT,
                            menu_id INTEGER,
                            brand VARCHAR(32),
                            name VARCHAR(128),
                            price INTEGER,
                            cogs INTEGER,
                            effective_date DATE,
                            UNIQUE(menu_id, effective_date));
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS
            Order_Staging (id INTEGER PRIMARY KEY AUTOINCREMENT,
                            order_id INTEGER,
                            menu_id INTEGER,
                            quantity INTEGER,
                            sales_date DATE,
                            UNIQUE(order_id, menu_id, sales_date));
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS
            Promotion_Staging (id INTEGER,
                                start_date DATE,
                                end_date DATE,
                                disc_value DECIMAL(5,3),
                                max_disc INTEGER,
                                UNIQUE(id, start_date, end_date));
        """)

def upsert_df_menu_local(row):
    create_unique_index_query = '''
    CREATE UNIQUE INDEX IF NOT EXISTS idx_menuid_effective_date
    ON Menu_Staging (menu_id, effective_date)
    '''
    upsert_query = '''
        INSERT INTO Menu_Staging (menu_id, brand, name, price, cogs, effective_date)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(menu_id, effective_date) DO UPDATE SET
            brand = excluded.brand,
            name = excluded.name,
            price = excluded.price,
            cogs = excluded.cogs
    '''
    connection=connect_local('staging')
    cursor = connection.cursor()
    cursor.execute(create_unique_index_query)
    cursor.execute(upsert_query, (row['menu_id'], row['brand'], row['name'], row['price'], row['cogs'], row['effective_date']))
    connection.commit()

def upsert_df_menu_mysql(row):
    upsert_query = '''
    INSERT INTO Menu_Staging (menu_id, brand, name, price, cogs, effective_date)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        brand = VALUES(brand),
        name = VALUES(name),
        price = VALUES(price),
        cogs = VALUES(cogs)
    '''
    connection=connect_mysql()
    cursor = connection.cursor()
    cursor.execute(upsert_query, (row['menu_id'], row['brand'], row['name'], row['price'], row['cogs'], row['effective_date']))
    connection.commit()

def upsert_df_order_local(row):
    create_unique_index_query = '''
    CREATE UNIQUE INDEX IF NOT EXISTS idx_order_id_menu_id_sales_date
    ON Order_Staging (order_id, menu_id, sales_date)
    '''
    upsert_query = '''
        INSERT INTO Order_Staging (order_id, menu_id, quantity, sales_date)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(order_id, menu_id, sales_date) DO UPDATE SET quantity = excluded.quantity
    '''
    connection=connect_local('staging')
    cursor = connection.cursor()
    cursor.execute(create_unique_index_query)
    cursor.execute(upsert_query, (row['order_id'], row['menu_id'], row['quantity'], row['sales_date']))
    connection.commit()

def upsert_df_order_mysql(row):
    upsert_query = '''
    INSERT INTO Order_Staging (order_id, menu_id, quantity, sales_date)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE quantity = VALUES(quantity)
    '''
    connection=connect_mysql()
    cursor = connection.cursor()
    cursor.execute(upsert_query, (row['order_id'], row['menu_id'], row['quantity'], row['sales_date']))
    connection.commit()

def upsert_df_promotion_local(row):
    create_unique_index_query = '''
    CREATE UNIQUE INDEX IF NOT EXISTS idx_id_start_date_end_date
    ON Promotion_Staging (id, start_date, end_date)
    '''
    upsert_query = '''
        INSERT INTO Promotion_Staging (id, start_date, end_date, disc_value, max_disc)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(id, start_date, end_date) DO UPDATE SET
            disc_value = excluded.disc_value,
            max_disc = excluded.max_disc
    '''
    connection=connect_local('staging')
    cursor = connection.cursor()
    cursor.execute(create_unique_index_query)
    cursor.execute(upsert_query, (row['id'], row['start_date'], row['end_date'], row['disc_value'], row['max_disc']))
    connection.commit()

def upsert_df_promotion_mysql(row):
    upsert_query = '''
    INSERT INTO Promotion_Staging (id, start_date, end_date, disc_value, max_disc)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        disc_value = VALUES(disc_value),
        max_disc = VALUES(max_disc)
    '''
    connection=connect_mysql()
    cursor = connection.cursor()
    cursor.execute(upsert_query, (row['id'], row['start_date'], row['end_date'], row['disc_value'], row['max_disc']))
    connection.commit()

@data_exporter
def export_data_to_mysql(data, *args, **kwargs):
    """
    Template for exporting data to a MySQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#mysql
    """
    df_menu_list = json.loads(data['df_menu'])
    df_order_list = json.loads(data['df_order'])
    df_promotion_list = json.loads(data['df_promotion'])

    df_menu = pd.DataFrame(df_menu_list)
    df_order = pd.DataFrame(df_order_list)
    df_promotion = pd.DataFrame(df_promotion_list)

    if kwargs['staging']=='mysql':
        connection=connect_mysql()
        create_database_mysql(connection)
        cursor = connection.cursor()
        df_menu.apply(upsert_df_menu_mysql, axis=1)
        df_order.apply(upsert_df_order_mysql, axis=1)
        df_promotion.apply(upsert_df_promotion_mysql, axis=1)
    else:
        connection=connect_local('staging')
        create_database_local(connection)
        cursor = connection.cursor()
        df_menu.apply(upsert_df_menu_local, axis=1)
        df_order.apply(upsert_df_order_local, axis=1)
        df_promotion.apply(upsert_df_promotion_local, axis=1)

    # # Query the data
    select_query = 'SELECT * FROM Menu_Staging'
    cursor.execute(select_query)
    display(pd.DataFrame(cursor.fetchall()))

    select_query = 'SELECT * FROM Order_Staging'
    cursor.execute(select_query)
    display(pd.DataFrame(cursor.fetchall()))

    select_query = 'SELECT * FROM Promotion_Staging'
    cursor.execute(select_query)
    display(pd.DataFrame(cursor.fetchall()))

    cursor.close()
    connection.close()