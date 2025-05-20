from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Constants
SYMBOL = 'NQ=F'  # Default symbol
POSTGRES_CONN_ID = 'stock_postgres'  # Connection ID set in Airflow UI

@dag(
    dag_id="yahoo_to_postgres_dag",
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market', 'postgres'],
)
def yahoo_to_postgres_dag():

    @task
    def get_yahoo_api_connection():
        """
        Builds the API URL using the Airflow connection.
        """
        conn = BaseHook.get_connection('yahoo_finance_api')
        extra = conn.extra_dejson

        symbol = extra.get("default_symbol", SYMBOL)
        endpoint = extra.get("endpoint", "/v8/finance/chart/")
        period1 = extra.get("default_period1")
        period2 = extra.get("default_period2")
        interval = extra.get("default_interval", "1d")
        headers = extra.get("headers", {})

        if not all([period1, period2]):
            raise ValueError("Missing required parameters: period1 or period2")

        url = f"{conn.host.rstrip('/')}/{endpoint.lstrip('/')}{symbol}"
        params = {
            "period1": period1,
            "period2": period2,
            "interval": interval,
            "events": "history"
        }

        return {
            "url": url,
            "params": params,
            "headers": headers
        }

    @task
    def fetch_historical_data(api_info: dict):
        """
        Fetches historical data from Yahoo Finance API.
        """
        url = api_info["url"]
        params = api_info["params"]
        headers = api_info["headers"]

        logging.info(f"Fetching data from {url} with params {params}")

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()

            data = response.json()

            # Check for chart-level errors
            if data.get("chart", {}).get("error"):
                raise Exception(f"API Error: {data['chart']['error']['description']}")

            result = data["chart"]["result"][0]
            logging.info(f"Fetched data for symbol: {result['meta']['symbol']}")
            return result

        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {e}")

    @task
    def save_data_to_postgres(result):
        """
        Inserts OHLC data into PostgreSQL.
        """
        meta = result["meta"]
        quotes = result["indicators"]["quote"][0]
        timestamps = result["timestamp"]

        rows = []
        for i, ts in enumerate(timestamps):
            row = (
                meta["symbol"],
                datetime.utcfromtimestamp(ts),  # Convert to datetime
                quotes["open"][i],
                quotes["high"][i],
                quotes["low"][i],
                quotes["close"][i],
                quotes["volume"][i]
            )
            rows.append(row)

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        pg_hook.insert_rows(
            table="stock_ohlc_data",
            rows=rows,
            target_fields=['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
        )

        logging.info(f"✅ Inserted {len(rows)} records into PostgreSQL table 'stock_ohlc_data'")

    # Task Flow
    api_info = get_yahoo_api_connection()
    result = fetch_historical_data(api_info)
    save_data_to_postgres(result)

# Instantiate the DAG
yahoo_to_postgres_dag()