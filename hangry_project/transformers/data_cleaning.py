import os
import json
import numpy as np
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def fill_missing_values_with_median(df,columns):
    for col in columns:
        values = sorted(df[col].dropna().tolist())
        median_value = values[math.floor(len(values) / 2)]
        df[[col]] = df[[col]].fillna(median_value)
    return df

def drop_duplicate(df,subset_column):
    return df.drop_duplicates(subset=subset_column,keep='last')

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    df_menu_list = json.loads(data['df_menu'])
    df_order_list = json.loads(data['df_order'])
    df_promotion_list = json.loads(data['df_promotion'])

    df_menu = pd.DataFrame(df_menu_list)
    df_order = pd.DataFrame(df_order_list)
    df_promotion = pd.DataFrame(df_promotion_list)

    #drop duplicate and keep last
    df_menu = drop_duplicate(df_menu,subset_column=['menu_id','brand','name','effective_date'])
    df_order = drop_duplicate(df_order,subset_column=['order_id','menu_id'])
    df_promotion = drop_duplicate(df_promotion,subset_column=['start_date','end_date'])

    ###
    #Handling Missing-Value
    ###
    #price, cogs, effective_date in df_menu
    #quantity and sales_date in df_order
    if kwargs['missing']=='isolate':
        df_menu=df_menu.dropna(subset=['price','cogs','effective_date'])
        df_order=df_order.dropna(subset=['quantity','sales_date'])
    else:
        df_menu=fill_missing_values_with_median(df_menu,['price','cogs'])
        df_order=fill_missing_values_with_median(df_order,['quantity'])

        df_menu['effective_date']=df_menu['effective_date'].fillna(method='ffill')
        df_order['sales_date'] = df_order.groupby('order_id')['sales_date'].fillna(method='ffill')
        df_order=df_order.dropna(subset=['sales_date'])
    #other column that can't do fillna
    df_menu=df_menu.dropna(subset=['menu_id','brand','name'])
    df_order=df_order.dropna(subset=['order_id','menu_id'])
    df_promotion=df_promotion.dropna()

    ###Drop Noise (Incosistencies) Data
    df_menu = df_menu[~(df_menu.select_dtypes(include=[np.number]) < 0).any(axis=1)]
    df_order = df_order[~(df_order.select_dtypes(include=[np.number]) < 0).any(axis=1)]
    df_promotion = df_promotion[~(df_promotion.select_dtypes(include=[np.number]) < 0).any(axis=1)]

    #equalizing sales_date in each order_id
    df_order['sales_date'] = df_order.groupby('order_id')['sales_date'].transform('first')

    #I won't do anything to outlier

    return {'df_menu':df_menu.to_json(orient='records'),'df_order':df_order.to_json(orient='records'),'df_promotion':df_promotion.to_json(orient='records')}

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
