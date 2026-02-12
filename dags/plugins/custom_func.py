import os
import sys
import json
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from airflow.models import Variable


def download_moex_stocks(**context):
    """
    Загружает исторические данные акций MOEX за вчерашний день в JSON файлы.
    
    Логика:
    1. Читает список тикеров из /opt/airflow/dags/config.json
    2. Создает папку /opt/airflow/dags/data/ если нет
    3. Проверяет существующие JSON файлы (идемпотентность)
    4. Скачивает данные с MOEX API для новых файлов
    5. Сохраняет pandas DataFrame в JSON с временной защитой от повреждений
    
    Используется в: JSON task параллельно с PostgreSQL
    
    Args:
        context: Airflow context с yesterday_ds (дата за вчера)
    
    Returns:
        str: Статистика загрузки "Дата: YYYY-MM-DD, Всего: N, новых: N, пропущено: N, ошибок: N"
    """
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


def download_moex_stocks_postgres(**context):
    """
    Загружает исторические данные акций MOEX за вчерашний день в PostgreSQL.
    
    Логика:
    1. Читает список тикеров из /opt/airflow/dags/config.json  
    2. Подключается к PostgreSQL через Airflow Variables (postgres_host, etc.)
    3. Создает таблицу moex_stock_history (если не существует)
    4. Для каждого тикера:
       - Скачивает данные с MOEX API (TQBR, дата=вчера)
       - Преобразует DataFrame (добавляет ticker/board, переименовывает колонки)
       - Вставляет через pandas.to_sql() с PRIMARY KEY защитой от дублей
    5. Возвращает статистику загрузки
    
    Таблица: moex_stock_history(trade_date, ticker, board, close, open, high, low, volume, value)
    
    Args:
        context: Airflow context с yesterday_ds (дата за вчера)
    
    Returns:
        str: "[PG] YYYY-MM-DD: N тикеров, N строк"
    """
    dags_dir = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
    config_path = os.path.join(dags_dir, "config.json")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print(f"✅ [PG] Загружен config.json: {len(config)} тикеров")
    except FileNotFoundError:
        raise FileNotFoundError(f"❌ [PG] config.json не найден: {config_path}")
    
    db_host = Variable.get("postgres_host", default_var="localhost")
    db_port = Variable.get("postgres_port", default_var="5432")
    db_name = Variable.get("postgres_db", default_var="airflow")
    db_user = Variable.get("postgres_user", default_var="airflow")
    db_password = Variable.get("postgres_password", default_var="airflow")
    
    connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)
    
    
    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS moex_stock_history (
        trade_date DATE,
        ticker VARCHAR(20),
        board VARCHAR(10),
        close DECIMAL(15,2),
        open DECIMAL(15,2),
        high DECIMAL(15,2),
        low DECIMAL(15,2),
        volume BIGINT,
        value DECIMAL(20,2),
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, ticker, board)
    );
    CREATE INDEX IF NOT EXISTS idx_ticker_date ON moex_stock_history(ticker, trade_date);
    """
    
    with engine.connect() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
    
    date_str = context['yesterday_ds']
    success_count = 0
    inserted_rows = 0
    
    for ticker, info in config.items():
        print(f"[PG] 📊 {info['name']} ({ticker})")
        
        url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/{ticker}.json?iss.meta=off&iss.only=history&from={date_str}"
        
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if 'history' in data and data['history']['data']:
                    df = pd.DataFrame(data['history']['data'], columns=data['history']['columns'])
                    
                    if len(df) > 0:
                        df['ticker'] = ticker
                        df['board'] = 'TQBR'
                        df = df.rename(columns={
                            'CLOSE': 'close', 'OPEN': 'open', 'HIGH': 'high', 'LOW': 'low',
                            'VOLUME': 'volume', 'VALUE': 'value', 'TRADEDATE': 'trade_date'
                        })
                        df = df[['trade_date', 'ticker', 'board', 'close', 'open', 'high', 'low', 'volume', 'value']]
                        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                        
                        rows = df.to_sql('moex_stock_history', engine, if_exists='append', index=False, method='multi')
                        print(f"[PG] ✅ {ticker}: {len(df)} строк")
                        inserted_rows += len(df)
                    success_count += 1
        except Exception as e:
            print(f"[PG] ❌ {ticker}: {e}")
    
    result = f"[PG] {date_str}: {success_count} тикеров, {inserted_rows} строк"
    print(f"🎉 {result}")
    return result