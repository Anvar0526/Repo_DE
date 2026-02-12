from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import os
from sqlalchemy import create_engine, text
from airflow.models import Variable
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'plugins'))

from custom_func import download_moex_stocks, download_moex_stocks_postgres


with DAG(
    dag_id='moex_stocks_loader',
    description='Загрузка акций MOEX (JSON + PostgreSQL)',
    schedule='00 06 * * 1-5',
    start_date=datetime(2026, 2, 3),
    catchup=False,
    tags=['moex', 'api', 'postgres']
) as dag:

    start = EmptyOperator(task_id="start")

    download_api = PythonOperator(
        task_id='download_moex_stocks',
        python_callable=download_moex_stocks
    )

    download_pg = PythonOperator(
        task_id='download_moex_stocks_postgres',
        python_callable=download_moex_stocks_postgres
    )

    end = EmptyOperator(task_id="end")

    start >> [download_api, download_pg] >>  end
