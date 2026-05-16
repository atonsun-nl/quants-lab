# Download Orderbook Trades

This directory contains scripts for downloading orderbook and trade data.

## Scripts

### download_orderbook.py
Основной скрипт для сбора снимков стакана и трейдов в реальном времени.

Примеры использования:
```bash
# 12 часов, снимок каждую секунду, глубина 20, parquet:
python download_orderbook.py --exchange binance --pair BTC-USDT --hours 12

# 1 день, глубина 10, CSV:
python download_orderbook.py --exchange binance --pair BTC-USDT --days 1 --depth 10 --format csv

# Сброс на диск каждые 10 минут:
python download_orderbook.py --exchange binance --pair BTC-USDT --hours 12 --flush-interval 600

# Несколько пар из JSON-конфига:
python download_orderbook.py --config my_config.json
```

### merge_parquet_files.py
Скрипт для слияния множества parquet-файлов в один с защитой от переполнения памяти.

**Особенности:**
- Построчное чтение по чанкам (не загружает всё в RAM)
- Возможность возобновления при прерывании
- Прогресс-бар и логирование
- Проверка целостности данных
- Автоматическое определение схемы из первого файла

**Примеры использования:**
```bash
# Слить все parquet файлы в директории
python merge_parquet_files.py \
    --input-dir /path/to/parquet/files \
    --output merged_data.parquet

# С чанками по 50k строк (для экономии памяти)
python merge_parquet_files.py \
    --input-dir /path/to/parquet/files \
    --output merged_data.parquet \
    --chunk-size 50000

# С перезаписью существующего файла
python merge_parquet_files.py \
    --input-dir /path/to/parquet/files \
    --output merged_data.parquet \
    --overwrite

# Подробный вывод (debug режим)
python merge_parquet_files.py \
    --input-dir /path/to/parquet/files \
    --output merged_data.parquet \
    --verbose
```

**Параметры:**
- `--input-dir`, `-i`: Директория с parquet файлами для слияния (обязательно)
- `--output`, `-o`: Путь к выходному parquet файлу (обязательно)
- `--chunk-size`, `-c`: Размер чанка для чтения в строках (по умолчанию 100000)
- `--overwrite`, `-f`: Перезаписать существующий выходной файл
- `--pattern`, `-p`: Паттерн для поиска файлов (по умолчанию *.parquet)
- `--verbose`, `-v`: Подробный вывод (debug уровень логгирования)

## Типичный рабочий процесс

1. **Скачивание данных:**
```bash
cd /workspace/scripts/download_orderbook_trades
python download_orderbook.py --exchange binance_perpetual --pair BNB-USDT --days 30 --depth 20
```

2. **Слияние файлов после завершения скачивания:**
```bash
# Найти директорию с сохранёнными файлами
# Обычно это: app/data/cache/orderbooks/<connector>/<PAIR>/

python merge_parquet_files.py \
    --input-dir app/data/cache/orderbooks/binance_perpetual/BNBUSDT \
    --output app/data/cache/orderbooks/binance_perpetual/BNBUSDT/merged_month.parquet \
    --chunk-size 100000 \
    --overwrite
```

## Примечания

- Скрипт `merge_parquet_files.py` использует потоковую запись через `pyarrow.ParquetWriter`, что позволяет обрабатывать гигабайты данных без загрузки всего объёма в оперативную память.
- При прерывании процесса (Ctrl+C) частично записанный файл может быть повреждён — используйте флаг `--overwrite` для перезаписи при повторном запуске.
- Рекомендуется подбирать `--chunk-size` исходя из доступной памяти: 50000-200000 строк обычно безопасно.