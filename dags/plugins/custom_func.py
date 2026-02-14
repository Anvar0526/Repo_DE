import os
import json
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from airflow.models import Variable

MOEX_HISTORY_URL = (
    "https://iss.moex.com/iss/history/engines/stock/markets/shares/"
    "boards/TQBR/securities/{ticker}.json"
    "?iss.meta=off&iss.only=history&from={date}"
)

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
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    print(f"Загружен config.json: {len(config)} тикеров")
    
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
    
    date_str = str(context["yesterday_ds"])
    date_value = pd.to_datetime(date_str).date()
    success_count = 0
    inserted_rows = 0
    
    for ticker, info in config.items():
        print(f"[PG] {info['name']} ({ticker})")
        
        url = MOEX_HISTORY_URL.format(ticker=ticker, date=date_str)
        
        response = requests.get(url, timeout=30)
        data = response.json()
        df = pd.DataFrame(data['history']['data'], columns=data['history']['columns'])
        
        df['ticker'] = ticker
        df['board'] = 'TQBR'
        df = df.rename(columns={
            'CLOSE': 'close', 'OPEN': 'open', 'HIGH': 'high', 'LOW': 'low',
            'VOLUME': 'volume', 'VALUE': 'value', 'TRADEDATE': 'trade_date'
        })
        df = df[['trade_date', 'ticker', 'board', 'close', 'open', 'high', 'low', 'volume', 'value']]
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

        with engine.begin() as conn:
            conn.execute(
                text("""
                    DELETE FROM moex_stock_history
                    WHERE trade_date = :trade_date
                      AND ticker = :ticker
                      AND board = :board
                """),
                {
                    "trade_date": date_value,
                    "ticker": ticker,
                    "board": "TQBR",
                },
            )
            df.to_sql(
                "moex_stock_history",
                conn,
                if_exists="append",
                index=False,
                method="multi",
            )

        print(f"[PG] {ticker}: {len(df)} строк")
        inserted_rows += len(df)
        success_count += 1
    
    result = f"[PG] {date_str}: {success_count} тикеров, {inserted_rows} строк"
    print(result)
    return result
