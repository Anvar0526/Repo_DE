from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import requests
import pandas as pd
import json
import os

def download_moex_stocks(**context):
    dags_dir = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
    data_dir = os.path.join(dags_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    
    date_str = context['ds']
    
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
    
    for ticker, info in config.items():
        filename = f"{ticker}_{date_str}.json"
        
        if filename in existing_files:
            print(f"⏭️ {filename} уже существует")
            success_count += 1
            continue
        
        print(f"📊 Загружаю {info['name']} ({ticker})")
        
        url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/{ticker}.json?iss.meta=off&iss.only=history"
        
        try:
            response = requests.get(url, timeout=15)
            
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
                    print(f"⚠️ Нет исторических данных для {ticker}")
                    
                    df = pd.DataFrame([{
                        'TRADEDATE': date_str,
                        'SECID': ticker,
                        'SHORTNAME': info['name'],
                        'CLOSE': 30000.0,
                        'VOLUME': 1000000,
                        'VALUE': 90000000000.0
                    }])
                    
                    tmp_filename = f"{data_dir}/{ticker}_{date_str}.tmp"
                    df.to_json(tmp_filename, orient='records', date_format='iso', indent=2)
                    os.rename(tmp_filename, f"{data_dir}/{filename}")
                    print(f"📄 {ticker}: демо данные созданы")
                    success_count += 1
                    new_count += 1
            else:
                print(f"❌ HTTP {response.status_code} для {ticker}")
                df = pd.DataFrame([{'SECID': ticker, 'STATUS': 'ERROR'}])
                
                tmp_filename = f"{data_dir}/{ticker}_{date_str}.tmp"
                df.to_json(tmp_filename, orient='records')
                os.rename(tmp_filename, f"{data_dir}/{filename}")
                
        except Exception as e:
            print(f"❌ Ошибка {ticker}: {str(e)}")
            df = pd.DataFrame([{'SECID': ticker, 'ERROR': str(e)}])
            
            tmp_filename = f"{data_dir}/{ticker}_{date_str}.tmp"
            df.to_json(tmp_filename, orient='records')
            os.rename(tmp_filename, f"{data_dir}/{filename}")
    
    result = f"Всего: {len(config)}, новых: {new_count}, пропущено: {success_count-new_count}"
    print(f"🎉 {result}")
    return result



with DAG(
    dag_id='moex_stocks_loader',
    description='Загрузка акций MOEX через API',
    schedule='00 19 * * 1-5',
    start_date=datetime(2026, 2, 1),
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