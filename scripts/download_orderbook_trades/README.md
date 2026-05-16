# Download Orderbook Trades

This directory contains scripts for downloading orderbook and trade data from cryptocurrency exchanges.

## Scripts

### download_orderbook.py

Основной скрипт для сбора снимков стакана (orderbook) и данных о трейдах в реальном времени. Данные сохраняются в wide-format (ML-ready), где каждая строка представляет один момент времени со всеми необходимыми признаками.

**Формат выходных данных:**

Каждая строка содержит:
- `bid_price_0..N`, `bid_amount_0..N` — цены и объёмы на покупку
- `ask_price_0..N`, `ask_amount_0..N` — цены и объёмы на продажу
- `spread`, `mid_price` — спред и средняя цена
- `bid_ask_imbalance` — дисбаланс спроса/предложения
- `trades_buy_volume`, `trades_sell_volume` — объёмы покупок/продаж
- `trades_buy_count`, `trades_sell_count` — количество сделок
- `trades_vwap_buy`, `trades_vwap_sell` — средневзвешенные цены
- `trades_total_volume`, `trades_imbalance` — общий объём и дисбаланс трейдов

**Параметры командной строки:**

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `--config` | Путь к JSON-файлу со списком заданий | - |
| `--exchange` | Коннектор (напр. `binance`, `binance_perpetual`) | - |
| `--pair` | Торговая пара (напр. `BTC-USDT`) | - |
| `--hours` | Длительность сбора в часах | - |
| `--days` | Длительность сбора в днях | - |
| `--interval-seconds` | Интервал между снимками (сек) | `1.0` |
| `--depth` | Глубина стакана (уровней на сторону) | `20` |
| `--format` | Формат файлов: `parquet` или `csv` | `parquet` |
| `--csv` | Флаг для сохранения в CSV (эквивалент `--format csv`) | - |
| `--flush-interval` | Интервал сброса буфера на диск (сек) | `300` |
| `--single-file` | Один итоговый файл с дозаписью | - |
| `--progress-log-interval` | Интервал вывода прогресса (сек), `0` — отключить | `60.0` |
| `--save-dir` | Корневая папка для файлов | `app/data/cache/orderbooks/` |
| `--summary` | Вывести сводку после каждой пары | - |
| `--list-exchanges` | Показать список доступных коннекторов | - |

**Примеры использования:**

```bash
# 12 часов, снимок каждую секунду, глубина 20, parquet:
python download_orderbook.py --exchange binance --pair BTC-USDT --hours 12

# 1 день, глубина 10, CSV:
python download_orderbook.py --exchange binance --pair BTC-USDT --days 1 \
    --depth 10 --format csv

# Сброс на диск каждые 10 минут:
python download_orderbook.py --exchange binance --pair BTC-USDT --hours 12 \
    --flush-interval 600

# Несколько пар из JSON-конфига:
python download_orderbook.py --config my_config.json

# Список доступных коннекторов:
python download_orderbook.py --list-exchanges
```

**Формат JSON-конфигурации:**

```json
[
  {
    "exchange": "binance",
    "pair": "BTC-USDT",
    "hours": 12,
    "interval_seconds": 1,
    "depth": 20
  },
  {
    "exchange": "binance",
    "pair": "ETH-USDT",
    "days": 1,
    "interval_seconds": 0.5,
    "depth": 10
  }
]
```

**Выходные данные:**

Файлы сохраняются в директорию:
```
app/data/cache/orderbooks/<connector>/<SYMBOL>/
```

Где `<SYMBOL>` — название пары без дефиса (напр. `BTCUSDT`).

При использовании `--flush-interval` данные записываются частями (part-файлы), которые затем можно объединить с помощью `merge_parquet_files.py`.

---

### merge_parquet_files.py

Скрипт для слияния множества parquet-файлов в один с защитой от переполнения памяти.

**Особенности:**
- Построчное чтение по чанкам (не загружает всё в RAM)
- Возможность возобновления при прерывании
- Прогресс-бар и логирование
- Проверка целостности данных
- Автоматическое определение схемы из первого файла
- Потоковая запись через `pyarrow.ParquetWriter`

**Параметры командной строки:**

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `--input-dir`, `-i` | Директория с parquet файлами | (обязательно) |
| `--output`, `-o` | Путь к выходному файлу | (обязательно) |
| `--chunk-size`, `-c` | Размер чанка для чтения (строк) | `100000` |
| `--overwrite`, `-f` | Перезаписать существующий файл | - |
| `--pattern`, `-p` | Паттерн для поиска файлов | `*.parquet` |
| `--verbose`, `-v` | Подробный вывод (debug режим) | - |

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

---

## Типичный рабочий процесс

### 1. Скачивание данных

Запуск сбора данных на указанный период:

```bash
cd /workspace/scripts/download_orderbook_trades

# Сбор данных за 30 дней с глубиной 20 уровней
python download_orderbook.py \
    --exchange binance_perpetual \
    --pair BNB-USDT \
    --days 30 \
    --depth 20 \
    --flush-interval 600
```

Скрипт создаст part-файлы в директории:
```
app/data/cache/orderbooks/binance_perpetual/BNBUSDT/
```

### 2. Слияние файлов после завершения скачивания

После завершения сбора данных объедините part-файлы в один:

```bash
python merge_parquet_files.py \
    --input-dir app/data/cache/orderbooks/binance_perpetual/BNBUSDT \
    --output app/data/cache/orderbooks/binance_perpetual/BNBUSDT/merged_month.parquet \
    --chunk-size 100000 \
    --overwrite
```

### 3. Проверка результата

Убедитесь, что файл создан и содержит ожидаемое количество строк:

```bash
python -c "import pyarrow.parquet as pq; f=pq.ParquetFile('app/data/cache/orderbooks/binance_perpetual/BNBUSDT/merged_month.parquet'); print(f'Строк: {f.metadata.num_rows:,}')"
```

---

## Примечания

- **Режимы записи:**
  - Без `--single-file`: создаётся новый файл каждые `--flush-interval` секунд (рекомендуется для длительных сессий)
  - С `--single-file`: один файл с периодической дозаписью
  
- **Память:** Скрипт `merge_parquet_files.py` использует потоковую запись, что позволяет обрабатывать гигабайты данных без загрузки всего объёма в оперативную память.

- **Прерывание:** При прерывании процесса (Ctrl+C) частично записанный файл может быть повреждён — используйте флаг `--overwrite` для перезаписи при повторном запуске.

- **Chunk size:** Рекомендуется подбирать `--chunk-size` исходя из доступной памяти: `50000-200000` строк обычно безопасно.

- **Формат данных:** Wide-format оптимизирован для машинного обучения — каждая строка содержит все признаки для одного момента времени.