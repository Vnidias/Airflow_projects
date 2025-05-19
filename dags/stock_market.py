from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import requests
import logging
import csv

# Set up logging
logging.basicConfig(level=logging.INFO)

SYMBOL = 'NQ=F'

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
)
def yahoo_to_csv_dag():

    @task
    def get_yahoo_api_connection():
        """
        Build the API URL using Airflow connection.
        """
        conn = BaseHook.get_connection('yahoo_finance_api')
        extra = conn.extra_dejson

        symbol = extra.get("default_symbol", SYMBOL)
        endpoint = extra.get("endpoint", "/v8/finance/chart/")
        period1 = extra.get("default_period1")
        period2 = extra.get("default_period2")
        interval = extra.get("default_interval", "1d")

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
            "headers": extra.get("headers")
        }

    @task
    def fetch_historical_data(api_info: dict):
        """
        Make the actual request to Yahoo Finance API.
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
    def save_data_to_csv(result):
        """
        Save OHLC data to a CSV file
        """
        meta = result["meta"]
        quotes = result["indicators"]["quote"][0]
        timestamps = result["timestamp"]

        filename = "/usr/local/airflow/data/yahoo_ohlc.csv"

        # Ensure directory exists
        import os
        os.makedirs("/usr/local/airflow/data", exist_ok=True)

        # Write to CSV
        with open(filename, mode="w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                "symbol", "timestamp", "open", "high", "low", "close", "volume"
            ])

            for i, ts in enumerate(timestamps):
                writer.writerow([
                    meta["symbol"],
                    ts,
                    quotes["open"][i],
                    quotes["high"][i],
                    quotes["low"][i],
                    quotes["close"][i],
                    quotes["volume"][i]
                ])

        logging.info(f"✅ Saved {len(timestamps)} rows to {filename}")

    # Task Flow
    api_info = get_yahoo_api_connection()
    result = fetch_historical_data(api_info)
    save_data_to_csv(result)

yahoo_to_csv_dag()