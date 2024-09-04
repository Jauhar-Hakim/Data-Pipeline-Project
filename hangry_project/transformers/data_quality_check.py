import json
import logging
import numpy as np
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def log_missing_values(data, df_name):
    missing_values = data.isnull().sum()
    for column, count in missing_values.items():
        if count > 0:
            logger.warning(f'{df_name}: Missing values in {column}: {count} (for row {list(data[data.isna().any(axis=1)].index)} with {count/data.shape[0]:.3f}%)')

def log_inconsistencies(data, df_name):
    for column in data.select_dtypes(include=[np.number]):
        negative_values = (data[column] < 0).sum()
        if negative_values > 0:
            logger.error(f'{df_name}: Inconsistent data in {column}: {negative_values} negative values found (for row {list(data[data[column] < 0].index)} with {negative_values/data.shape[0]:.3f}%)')

def log_outliers(data, df_name):
    from scipy.stats import zscore
    z_scores = np.abs(zscore(data.select_dtypes(include=[np.number])))
    outliers = (z_scores > 3).sum()
    for column, count in zip(data.columns, outliers):
        if count > 0:
            logger.info(f'{df_name}: Outliers detected in {column}: {count} potential outliers (for row {list(z_scores[z_scores[column] > 3].index)} with {count/data.shape[0]:.3f}%)')

def log_data_quality(df_menu, df_order, df_promotion):
    log_missing_values(df_menu, "df_menu")
    log_inconsistencies(df_menu, "df_menu")
    log_outliers(df_menu, "df_menu")

    log_missing_values(df_order, "df_order")
    log_inconsistencies(df_order, "df_order")
    log_outliers(df_order, "df_order")

    log_missing_values(df_promotion, "df_promotion")
    log_inconsistencies(df_promotion, "df_promotion")
    log_outliers(df_promotion, "df_promotion")

@transformer
def transform(data, *args, **kwargs):
    """
    Data Quality Checking
    1. Check missing value
    2. Check noise data (inconsistencies)
    3. Check outliers
    """
    df_menu_list = json.loads(data['df_menu'])
    df_order_list = json.loads(data['df_order'])
    df_promotion_list = json.loads(data['df_promotion'])

    df_menu = pd.DataFrame(df_menu_list)
    df_order = pd.DataFrame(df_order_list)
    df_promotion = pd.DataFrame(df_promotion_list)

    # Configuration for data quality checking
    # Set up logging configuration to log to a specific file
    logger = logging.getLogger('data_quality_logger')
    logger.setLevel(logging.INFO)

    # Create a file handler to write to 'data_quality.log'
    handler = logging.FileHandler('data_quality.log')
    handler.setLevel(logging.INFO)

    # Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    # Log data quality for the given DataFrames
    log_data_quality(df_menu, df_order, df_promotion)

    # Ensure the logger is flushed and the file is saved
    handler.flush()
    logger.removeHandler(handler)
    handler.close()

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'