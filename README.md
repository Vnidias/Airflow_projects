# Airflow Data Projects

## Project 1

A simple, self-contained Airflow DAG demonstrating a mock extract → transform workflow using local data.

Perfect for learning how to build pipelines with Apache Airflow.

## 📦 Features

- Uses `PythonOperator`
- Fully offline (mocked API data)
- Works with official Docker Compose setup
- No external dependencies
- Easy to demo or extend

## 🛠️ How to Run

1. Make sure you're using the official Airflow Docker Compose setup
2. Place your DAG in the `dags/` folder
3. Start Airflow: `docker-compose up -d`
4. Open UI at [http://localhost:8080](http://localhost:8080)
5. Trigger the DAG manually

## 🎯 Head Topics of this

- Modular DAG design
- Task communication via XCom
- Airflow Operators and task dependencies
- Running locally with Docker


## Project 2 - Airflow Yahoo Finance Pipeline

A simple, self-contained DAG demonstrating how to fetch historical stock data from Yahoo Finance and export it as a CSV file using Apache Airflow.

## 📦 Features

- Uses the **Yahoo Finance API** to fetch real OHLC (Open/High/Low/Close) stock data
- Exports results directly to **CSV**
- Works with Astro CLI or Docker Compose
- No external services required (e.g., PostgreSQL, MinIO)
- Easily extendable for further processing or visualization

## 🛠️ How to Run

1. Make sure you're using the official Astro CLI or Docker Compose setup
2. Place your DAG in the `dags/` folder
3. Start Airflow:
   ```bash
   astro dev start


last update 12/05/2025
