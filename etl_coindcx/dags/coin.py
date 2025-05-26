from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import pandas as pd
import os

def extract_crypto_data():
    # GET https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 10,
        'page': 1,
        'sparkline': False
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    df = pd.json_normalize(data)
    df = df[['id', 'symbol', 'current_price', 'market_cap', 'last_updated']]
    df.to_csv('/tmp/crypto_data.csv', index=False)

def transform_crypto_data():
    df = pd.read_csv('/tmp/crypto_data.csv')
    df = df[df['symbol'].isin(['btc', 'eth'])]  # Filter only BTC & ETH
    df['last_updated'] = pd.to_datetime(df['last_updated'])
    df.to_csv('/tmp/filtered_crypto_data.csv', index=False)

def load_crypto_data_to_snowflake():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("PUT file:///tmp/filtered_crypto_data.csv @%crypto_prices auto_compress=true")
        cursor.execute("""
            COPY INTO crypto_prices
            FROM @%crypto_prices/filtered_crypto_data.csv.gz
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = 'CONTINUE'
        """)
    finally:
        cursor.close()

with DAG(
    dag_id='crypto_etl_to_snowflake',
    start_date=datetime(2025, 5, 20),
    schedule_interval='@hourly',
    catchup=False,
    tags=['crypto', 'snowflake', 'etl'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_crypto_data',
        python_callable=extract_crypto_data
    )

    transform_task = PythonOperator(
        task_id='transform_crypto_data',
        python_callable=transform_crypto_data
    )

    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_crypto_data_to_snowflake
    )

    extract_task >> transform_task >> load_task
