from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import os


def download_moex_stocks(**context):
    dags_dir = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
    data_dir = os.path.join(dags_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    
    date_str = context['yesterday_ds']
    
    config_path = os.path.join(dags_dir, "config.json")
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print(f"✅ Загружен config.json: {len(config)} тикеров")
    except FileNotFoundError:
        raise FileNotFoundError(f"❌ config.json не найден: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"❌ Некорректный JSON в {config_path}: {e}")
    
    existing_files = {f for f in os.listdir(data_dir) if f.endswith('.json')}
    print(f"📂 Существующих файлов: {len(existing_files)}")
    
    success_count = 0
    new_count = 0
    error_count = 0
    
    for ticker, info in config.items():
        filename = f"{ticker}_{date_str}.json"
        
        if filename in existing_files:
            print(f"⏭️ {filename} уже существует")
            success_count += 1
            continue
        
        print(f"📊 Загружаю {info['name']} ({ticker})")
        
        url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/{ticker}.json?iss.meta=off&iss.only=history&from={date_str}"
        
        try:
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'history' in data and 'data' in data['history'] and len(data['history']['data']) > 0:
                    df = pd.DataFrame(
                        data['history']['data'], 
                        columns=data['history']['columns']
                    )
                    
                    tmp_filename = f"{data_dir}/{ticker}_{date_str}.tmp"
                    df.to_json(tmp_filename, orient='records', date_format='iso', indent=2)
                    os.rename(tmp_filename, f"{data_dir}/{filename}")
                    
                    print(f"✅ {ticker}: {len(df)} записей сохранено")
                    success_count += 1
                    new_count += 1
                else:
                    print(f"⚠️ Нет исторических данных для {ticker} за {date_str}")
                    error_count += 1
            else:
                print(f"❌ HTTP {response.status_code} для {ticker}")
                error_count += 1
                
        except Exception as e:
            print(f"❌ Ошибка {ticker}: {str(e)}")
            error_count += 1
    
    result = f"Дата: {date_str}, Всего: {len(config)}, новых: {new_count}, пропущено: {success_count-new_count}, ошибок: {error_count}"
    print(f"🎉 Запуск {context['ds']} → Данные за {date_str}: {result}")
    return result


with DAG(
    dag_id='moex_stocks_loader',
    description='Загрузка акций MOEX через API',
    schedule='00 06 * * 1-5',  # ✅ 22:00 MSK — после появления данных
    start_date=datetime(2026, 2, 3),  # ✅ Начало с торгового дня
    catchup=False,
    tags=['moex', 'api']
) as dag:

    start = EmptyOperator(task_id="start")

    download_task = PythonOperator(
        task_id='download_moex_stocks',
        python_callable=download_moex_stocks
    )

    end = EmptyOperator(task_id="end")

    start >> download_task >> end
