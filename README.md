# Airflow ETL для загрузки данных MOEX

Проект для автоматической загрузки исторических данных акций Московской биржи в JSON файлы и PostgreSQL.

## Что делает

- Загружает данные акций с MOEX API
- Сохраняет в JSON файлы (`dags/data/`)
- Записывает в PostgreSQL таблицу `moex_stock_history`
- Запускается ежедневно в 06:00 по будням

## Быстрый старт

1. Запустить Airflow:
   ```bash
   docker-compose up -d
   ```

2. Открыть Web UI: http://localhost:8080
   - Логин: `airflow`
   - Пароль: `airflow`

3. Настроить переменные PostgreSQL (Admin → Variables):
   - `postgres_host`, `postgres_port`, `postgres_db`, `postgres_user`, `postgres_password`
   - Или использовать значения по умолчанию

4. Запустить DAG `moex_stocks_loader` вручную или дождаться автоматического запуска

## Структура

- `dags/ticker_dag.py` - основной DAG
- `dags/plugins/custom_func.py` - функции загрузки данных
- `dags/config.json` - список тикеров для загрузки
- `dags/data/` - JSON файлы с данными

## Настройка тикеров

Отредактируй `dags/config.json` и добавь нужные тикеры в формате:
```json
{
  "SBER": {"name": "Сбербанк"},
  "GAZP": {"name": "Газпром"}
}
```
