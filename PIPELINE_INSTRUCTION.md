# 🚀 Pipeline: Cointegration Grid Trading с GridStrike

Полная пошаговая инструкция для запуска pipeline поиска высококоррелированных крипто-пар и автоматической торговли сетками ордеров.

---

## 📋 Содержание

1. [Обзор архитектуры](#обзор-архитектуры)
2. [Предварительные требования](#предварительные-требования)
3. [Установка и настройка](#установка-и-настройка)
4. [Запуск pipeline](#запуск-pipeline)
5. [Мониторинг и управление](#мониторинг-и-управление)
6. [Troubleshooting](#troubleshooting)

---

## 🏗 Обзор архитектуры

### Почему GridStrike вместо StatArb?

| Характеристика | GridStrike | StatArb |
|---------------|------------|---------|
| **Стратегия** | Сетки ордеров в диапазоне | Торговля спредом между парами |
| **Сложность** | Проще | Сложнее |
| **Рынок** | Боковое движение | Mean reversion |
| **Риск-менеджмент** | Проще | Сложнее |
| **Надежность** | Выше | Ниже |
| **Для крипто** | ✅ Лучше | ⚠️ Сложнее |

**GridStrike выбран потому что:**
- Проще в настройке и управлении
- Надежнее в волатильных крипторынках
- Лучший риск-менеджмент с Triple Barrier Config
- Автоматическая адаптация к диапазонам цен

### Поток выполнения (5 задач)

```
┌─────────────────────────┐
│ 1. candles_downloader   │ ──▶ Скачивает свечи (15m, 1h)
│    (каждые 6 часов)     │     для топ-25 пар по объему
└─────────────────────────┘
           │
           ▼
┌─────────────────────────┐
│ 2. funding_rates        │ ──▶ Скачивает funding rates
│    downloader           │     с Binance Perpetual
│    (каждые 4 часа)      │
└─────────────────────────┘
           │
           ▼
┌─────────────────────────┐
│ 3. cointegration_task   │ ──▶ Анализ на коинтеграцию:
│    (каждые 12 часов)    │     • Engle-Granger тест
│                         │     • Cross-correlation
│                         │     • Granger causality
└─────────────────────────┘
           │
           ▼
┌─────────────────────────┐
│ 4. grid_strike_config   │ ──▶ Генерация конфигураций
│    _generator           │     для GridStrike контроллера
│    (каждые 12 часов)    │
└─────────────────────────┘
           │
           ▼
┌─────────────────────────┐
│ 5. grid_strike          │ ──▶ Деплой Hummingbot ботов
│    _deployment          │     с готовыми конфигами
│    (каждые 30 минут)    │
└─────────────────────────┘
           │
           ▼
    🤖 LIVE TRADING BOT
```

---

## 🔧 Предварительные требования

### 1. Docker и Docker Compose
```bash
docker --version
docker-compose --version
```

### 2. MongoDB
Pipeline использует MongoDB для хранения:
- Свечных данных
- Funding rates
- Результатов коинтеграции
- Конфигураций контроллеров

### 3. Переменные окружения

Создайте файл `.env` в корне проекта:

```bash
# MongoDB
MONGO_URI=mongodb://admin:admin@localhost:27017/quants_lab?authSource=admin&retryWrites=true&w=majority

# Binance API (опционально, для live trading)
BINANCE_API_KEY=your_api_key
BINANCE_SECRET_KEY=your_secret_key

# Hummingbot Backend API
BACKEND_API_SERVER=localhost
```

---

## 📥 Установка и настройка

### Шаг 1: Клонирование репозитория

```bash
cd /workspace
git status  # Проверка состояния
```

### Шаг 2: Установка зависимостей

```bash
pip install -r requirements.txt
```

### Шаг 3: Запуск MongoDB (если не запущен)

```bash
docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=admin \
  -v mongo_data:/data/db \
  mongo:latest
```

### Шаг 4: Проверка созданных файлов

```bash
# Проверка новых задач
ls -la app/tasks/quantitative_methods/cointegration/grid_strike_config_generator_task.py
ls -la app/tasks/deployment/implementation/grid_strike_deployment_task.py

# Проверка конфигурации pipeline
ls -la config/template_5_cointegration_grid_pipeline.yml
```

---

## 🚀 Запуск pipeline

### Вариант 1: Пошаговый запуск (рекомендуется для первого раза)

#### Шаг 1: Валидация конфигурации

```bash
cd /workspace
python cli.py validate-config --config template_5_cointegration_grid_pipeline.yml
```

**Ожидаемый результат:**
```
✓ Configuration is valid
✓ All task classes found
✓ All dependencies resolved
```

#### Шаг 2: Просмотр списка задач

```bash
python cli.py list-tasks --config template_5_cointegration_grid_pipeline.yml
```

**Ожидаемый результат:**
```
Task: candles_downloader
  Class: CandlesDownloaderTask
  Schedule: Every 6 hours
  Status: Enabled

Task: funding_rates_downloader
  Class: FundingRatesDownloaderTask
  Schedule: Every 4 hours
  Status: Enabled

Task: cointegration_analysis
  Class: CointegrationTaskV2
  Schedule: Every 12 hours
  Status: Enabled

Task: generate_grid_configs
  Class: GridStrikeConfigGeneratorTask
  Schedule: Every 12 hours
  Status: Enabled

Task: deploy_grid_bots
  Class: GridStrikeDeploymentTask
  Schedule: Every 30 minutes
  Status: Enabled
```

#### Шаг 3: Загрузка данных (свечи)

```bash
python cli.py run-task --config template_5_cointegration_grid_pipeline.yml --task-name candles_downloader
```

**Что происходит:**
- Скачиваются свечи 15m и 1h для 25 топ-пар
- Данные сохраняются в MongoDB `candles_binance_perpetual`
- Время выполнения: ~5-10 минут

#### Шаг 4: Загрузка funding rates

```bash
python cli.py run-task --config template_5_cointegration_grid_pipeline.yml --task-name funding_rates_downloader
```

**Что происходит:**
- Скачиваются funding rates с Binance Perpetual
- Данные сохраняются в MongoDB `funding_rates_processed`
- Время выполнения: ~1-2 минуты

#### Шаг 5: Анализ коинтеграции

```bash
python cli.py run-task --config template_5_cointegration_grid_pipeline.yml --task-name cointegration_analysis
```

**Что происходит:**
- Загружаются свечи из MongoDB
- Пары анализируются на коинтеграцию (Engle-Granger, cross-correlation)
- Результаты сохраняются в `cointegration_results`
- Фильтрация по p-value < 0.05 и correlation > 0.7
- Время выполнения: ~10-30 минут

#### Шаг 6: Генерация конфигураций GridStrike

```bash
python cli.py run-task --config template_5_cointegration_grid_pipeline.yml --task-name generate_grid_configs
```

**Что происходит:**
- Загружаются результаты коинтеграции
- Для каждой подходящей пары создаются LONG и SHORT сетки
- Конфигурации сохраняются в `controller_configs`
- Время выполнения: ~1-2 минуты

#### Шаг 7: Деплой торговых ботов

```bash
python cli.py run-task --config template_5_cointegration_grid_pipeline.yml --task-name deploy_grid_bots
```

**Что происходит:**
- Загружаются конфигурации GridStrike
- Фильтрация кандидатов по текущим ценам
- Создание Hummingbot инстансов с конфигами
- Запуск ботов
- Время выполнения: ~2-5 минут

### Вариант 2: Автоматический запуск (production)

```bash
# Запуск всех задач по расписанию
python cli.py start
```

**Мониторинг:**
```bash
# Просмотр логов
tail -f logs/pipeline.log

# Статус задач
python cli.py status
```

---

## 📊 Мониторинг и управление

### Проверка результатов в MongoDB

```python
from pymongo import MongoClient

client = MongoClient("mongodb://admin:admin@localhost:27017/")
db = client["quants_lab"]

# Проверка результатов коинтеграции
coint_results = db.cointegration_results.find().limit(5)
for result in coint_results:
    print(f"Pair: {result['base']} / {result['quote']}")
    print(f"Coint Value: {result['coint_value']}")
    print(f"Correlation: {result.get('correlation', 'N/A')}")
    print("---")

# Проверка конфигураций GridStrike
configs = db.controller_configs.find({"config.controller_name": "grid_strike"}).limit(5)
for config in configs:
    print(f"Trading Pair: {config['config']['trading_pair']}")
    print(f"Grid Ranges: {len(config['config']['grid_ranges'])}")
    print(f"Total Amount: {config['config']['total_amount_quote']} USDT")
    print("---")
```

### Управление ботами

```bash
# Остановить конкретного бота
python cli.py stop-bot --bot-id <bot_id>

# Перезапустить все боты
python cli.py restart-all-bots

# Получить статус ботов
python cli.py bot-status
```

### Логи и метрики

```bash
# Логи pipeline
tail -f logs/pipeline.log

# Логи Hummingbot
docker logs hummingbot-bot-1 -f

# Метрики производительности
python cli.py get-metrics --last-hours 24
```

---

## 🔧 Troubleshooting

### Проблема 1: Ошибка подключения к MongoDB

**Симптомы:**
```
pymongo.errors.ServerSelectionTimeoutError
```

**Решение:**
```bash
# Проверка статуса MongoDB
docker ps | grep mongodb

# Перезапуск MongoDB
docker restart mongodb

# Проверка логов
docker logs mongodb
```

### Проблема 2: Нет результатов коинтеграции

**Симптомы:**
```
No cointegration results found in MongoDB
```

**Причины:**
- Недостаточно исторических данных
- Слишком строгие фильтры (p-value, correlation)
- Низкая волатильность рынка

**Решение:**
```bash
# Увеличить lookback_days в конфиге
# Изменить пороги фильтрации:
# - p_value_threshold: 0.05 → 0.1
# - min_correlation: 0.7 → 0.6
```

### Проблема 3: Боты не запускаются

**Симптомы:**
```
Failed to deploy bot: Insufficient balance
```

**Решение:**
```bash
# Проверка баланса
python cli.py check-balance

# Уменьшить total_amount_quote в конфиге
# Увеличить leverage (осторожно!)
```

### Проблема 4: Ошибка валидации конфигурации

**Симптомы:**
```
ValidationError: grid_ranges must contain at least one active range
```

**Решение:**
```bash
# Проверить логи генератора конфигов
tail -f logs/grid_strike_config_generator.log

# Отрегулировать параметры сетки:
# - Увеличить total_amount_quote
# - Уменьшить min_order_amount
# - Расширить grid_ranges
```

---

## 📈 Настройка параметров

### Конфигурация GridStrike (template_5_cointegration_grid_pipeline.yml)

```yaml
generate_grid_configs:
  config:
    base_config:
      total_amount_quote: 1000      # Capital per bot (USDT)
      leverage: 20                   # Leverage (谨慎!)
      time_limit: 604800             # 7 days
      activation_bounds: 0.02        # 2% activation bounds
      min_spread_between_orders: 0.001  # 0.1% min spread
      max_open_orders: 5             # Max concurrent orders

deploy_grid_bots:
  config:
    filter_candidate_params:
      max_step: 0.01                 # Max 1% grid step
      min_grid_range_ratio: 0.3      # Min grid range ratio
      max_entry_price_distance: 0.5  # Max distance from price
      max_notional_size: 100.0       # Max order size
    
    config_adjustment_params:
      total_amount_quote: 1000.0
      leverage: 20
      stop_loss: 0.05                # 5% stop loss
      take_profit: 0.002             # 0.2% take profit
      time_limit: 604800             # 7 days
```

### Рекомендации по настройке

| Параметр | Консервативный | Агрессивный | Комментарий |
|----------|---------------|-------------|-------------|
| `total_amount_quote` | 500 | 2000 | Начните с малого |
| `leverage` | 10 | 50 | Осторожно с кредитным плечом! |
| `stop_loss` | 0.03 | 0.1 | Защита от больших потерь |
| `take_profit` | 0.001 | 0.005 | Фиксация прибыли |
| `max_open_orders` | 3 | 10 | Контроль риска |

---

## 🎯 Следующие шаги

1. **Мониторинг первых 24 часов**
   - Проверьте логи ботов
   - Отследьте первые сделки
   - Оцените PnL

2. **Оптимизация параметров**
   - Анализируйте эффективность сеток
   - Настройте диапазоны цен
   - Скорректируйте размеры ордеров

3. **Масштабирование**
   - Добавьте больше торговых пар
   - Увеличьте капитал (постепенно!)
   - Рассмотрите несколько инстансов

4. **Production deployment**
   - Настройте systemd сервисы
   - Добавьте алерты (Telegram, Discord)
   - Реализуйте backup стратегию

---

## 📞 Поддержка

В случае проблем:
1. Проверьте логи (`logs/pipeline.log`)
2. Изучите troubleshooting секцию
3. Проверьте статус MongoDB и Hummingbot
4. Убедитесь что API ключи действительны

**Важно:** Начинайте с небольших сумм и тестируйте стратегию на демо-счете перед реальной торговлей!
