#!/usr/bin/env python3
"""
Сбор снимков стакана + трейдов в реальном времени (wide-format, ML-ready).

Каждая строка = один момент времени:
  bid_price_0..N, bid_amount_0..N, ask_price_0..N, ask_amount_0..N,
  spread, mid_price, bid_ask_imbalance,
  trades_buy_volume, trades_sell_volume, trades_buy_count, trades_sell_count,
  trades_vwap_buy, trades_vwap_sell, trades_total_volume, trades_imbalance

Данные сохраняются в:
  app/data/cache/orderbooks/<connector>/<SYMBOL>/

Примеры запуска:
  # 12 часов, снимок каждую секунду, глубина 20, parquet:
  python download_orderbook.py --exchange binance --pair BTC-USDT --hours 12

  # 1 день, глубина 10, CSV:
  python download_orderbook.py --exchange binance --pair BTC-USDT --days 1 \\
      --depth 10 --format csv

  # Сброс на диск каждые 10 минут:
  python download_orderbook.py --exchange binance --pair BTC-USDT --hours 12 \\
      --flush-interval 600

  # Несколько пар из JSON-конфига:
  python download_orderbook.py --config my_config.json

JSON-конфиг (массив объектов), поддерживает hours или days:
  [
    {"exchange": "binance", "pair": "BTC-USDT", "hours": 12,
     "interval_seconds": 1, "depth": 20},
    {"exchange": "binance", "pair": "ETH-USDT", "hours": 6,
     "interval_seconds": 1, "depth": 20}
  ]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

warnings.filterwarnings("ignore")

root_path = os.getcwd()
if root_path not in sys.path:
    sys.path.append(root_path)

PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), os.pardir, os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

for _noisy in (
    "root",
    "BinancePerpetualBase",
    "hummingbot.connector.exchange.binance.binance_utils",
    "hummingbot.core.api_throttler.async_throttler",
    "asyncio",
):
    logging.getLogger(_noisy).setLevel(logging.ERROR)

# Сообщения сборщика стакана (длинные прогоны)
logging.getLogger("core.data_sources.clob").setLevel(logging.INFO)

# (connector, pair, duration_seconds, interval_seconds, depth)
DEFAULT_CONFIG: List[Tuple[str, str, int, float, int]] = [
    ("binance_perpetual", "SOL-USDT", 12 * 3600, 1.0, 20),
]


def load_config(config_path: Optional[str]) -> List[Tuple[str, str, int, float, int]]:
    if not config_path:
        logger.info("Используется конфигурация по умолчанию (12 ч, шаг 1 с, depth=20)")
        return DEFAULT_CONFIG
    try:
        with open(config_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        if not isinstance(raw, list):
            raise ValueError("Ожидается JSON-массив")
        result: List[Tuple[str, str, int, float, int]] = []
        for i, item in enumerate(raw):
            if not isinstance(item, dict):
                continue
            need = {"exchange", "pair", "interval_seconds", "depth"}
            missing = need - item.keys()
            if missing:
                logger.warning(f"Запись #{i} пропущена, нет полей: {missing}")
                continue
            if "hours" in item:
                duration = int(item["hours"]) * 3600
            elif "days" in item:
                duration = int(item["days"]) * 86400
            else:
                logger.warning(f"Запись #{i} пропущена: нужно поле 'hours' или 'days'")
                continue
            result.append((
                str(item["exchange"]),
                str(item["pair"]),
                duration,
                float(item["interval_seconds"]),
                int(item["depth"]),
            ))
        logger.info(f"Конфиг из {config_path!r}: {len(result)} записей")
        return result
    except Exception as exc:
        logger.error(f"Ошибка конфигурации: {exc}")
        return DEFAULT_CONFIG


def resolve_output_format(csv_flag: bool, format_arg: str) -> str:
    fmt = format_arg.strip().lower()
    if fmt not in ("parquet", "csv"):
        raise ValueError("--format должен быть parquet или csv")
    return "csv" if csv_flag else fmt


def fmt_duration(seconds: int) -> str:
    h, rem = divmod(seconds, 3600)
    m = rem // 60
    if h and m:
        return f"{h}ч {m}м"
    if h:
        return f"{h}ч"
    return f"{m}м"


async def run_collection(
    config_list: List[Tuple[str, str, int, float, int]],
    output_format: str,
    save_dir: Optional[Path],
    flush_interval: int,
    single_file: bool,
    print_summary: bool,
    progress_log_interval: float,
) -> None:
    from core.data_sources.clob import CLOBDataSource

    clob = CLOBDataSource()

    mode_str = f"один файл, дозапись каждые {flush_interval}s" if single_file else f"новый файл каждые {flush_interval}s"
    print("\n" + "=" * 72)
    print("  СБОР СТАКАНА + ТРЕЙДОВ (wide-format, ML-ready)")
    print(f"  Формат: {output_format}  |  Режим: {mode_str}")
    if progress_log_interval > 0:
        print(f"  Прогресс: каждые {progress_log_interval:g} с → лог + stdout (строки, буфер, ETA, mid)")
    else:
        print("  Прогресс: только flush/ошибки (--progress-log-interval 0)")
    print("=" * 72)

    for connector_name, pair, duration_seconds, interval_s, depth in config_list:
        logger.info(
            f"▶ {connector_name} {pair} | {fmt_duration(duration_seconds)} "
            f"({duration_seconds}с) | каждые {interval_s}s | depth={depth}"
        )

        ob_save_dir: Optional[str] = None
        if save_dir is not None:
            ob_sub = save_dir / "orderbooks" / connector_name / pair.replace("-", "")
            ob_sub.mkdir(parents=True, exist_ok=True)
            ob_save_dir = str(ob_sub)

        try:
            result = await clob.collect_orderbook_snapshots(
                connector_name=connector_name,
                trading_pair=pair,
                duration_seconds=duration_seconds,
                interval_seconds=interval_s,
                depth=depth,
                save_dir=ob_save_dir,
                output_format=output_format,
                flush_interval_seconds=flush_interval,
                single_file=single_file,
                progress_log_interval_seconds=progress_log_interval,
            )
            logger.info(
                f"✅ {pair}: строк={result.get('snapshots_collected')} | "
                f"файлов={result.get('files_saved')} | "
                f"ошибок={result.get('errors_count')} | "
                f"каталог: {result.get('save_directory')}"
            )
        except Exception as exc:
            logger.error(f"❌ {connector_name} {pair}: {exc}")
            continue

        if print_summary:
            summary = clob.get_orderbook_collection_summary(connector_name, pair)
            logger.info(
                f"Сводка: {json.dumps(summary, default=str, ensure_ascii=False)}"
            )

    print("\n" + "=" * 72)
    print("✅ Сбор завершён")
    print("=" * 72 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Сбор стакана + трейдов в реальном времени (wide-format, ML-ready)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # --- источник конфига ---
    parser.add_argument(
        "--config", type=str, default=None,
        help="Путь к JSON-файлу со списком заданий"
    )
    parser.add_argument(
        "--exchange", type=str, default=None,
        help="Коннектор, напр. binance или binance_perpetual"
    )
    parser.add_argument(
        "--pair", type=str, default=None,
        help="Торговая пара, напр. BTC-USDT"
    )

    # --- длительность (hours или days, взаимоисключающие) ---
    duration_group = parser.add_mutually_exclusive_group()
    duration_group.add_argument(
        "--hours", type=float, default=None,
        metavar="N",
        help="Сколько часов собирать данные (напр. 12)"
    )
    duration_group.add_argument(
        "--days", type=float, default=None,
        metavar="N",
        help="Сколько дней собирать данные (напр. 1)"
    )

    # --- параметры снимка ---
    parser.add_argument(
        "--interval-seconds", type=float, default=1.0,
        metavar="SEC",
        help="Интервал между снимками в секундах (default: 1.0)"
    )
    parser.add_argument(
        "--depth", type=int, default=20,
        metavar="N",
        help="Глубина стакана — уровней на каждую сторону (default: 20)"
    )

    # --- формат ---
    parser.add_argument(
        "--format", type=str, default="parquet",
        choices=("parquet", "csv"),
        help="Формат файлов (default: parquet)"
    )
    parser.add_argument(
        "--csv", action="store_true",
        help="Сохранять CSV (эквивалент --format csv)"
    )

    # --- flush ---
    parser.add_argument(
        "--flush-interval", type=int, default=300,
        metavar="SEC",
        help="Как часто сбрасывать буфер на диск, секунд (default: 300 = 5 мин)"
    )
    parser.add_argument(
        "--single-file", action="store_true",
        help="Один итоговый файл + дозапись/part по --flush-interval"
    )
    parser.add_argument(
        "--progress-log-interval",
        type=float,
        default=60.0,
        metavar="SEC",
        help="Как часто печатать прогресс (секунды стенных часов). 0 — отключить (default: 60)",
    )

    # --- прочее ---
    parser.add_argument(
        "--save-dir", type=str, default=None,
        help="Корневая папка для файлов; иначе app/data/cache/orderbooks/..."
    )
    parser.add_argument(
        "--summary", action="store_true",
        help="После каждой пары вывести сводку по сохранённым файлам"
    )
    parser.add_argument(
        "--list-exchanges", action="store_true",
        help="Вывести список доступных коннекторов и выйти"
    )

    args = parser.parse_args()

    if args.list_exchanges:
        try:
            from core.data_sources.clob import CLOBDataSource
            clob = CLOBDataSource()
            print("\nДоступные коннекторы:")
            for i, ex in enumerate(sorted(clob.connectors.keys()), 1):
                print(f"  {i:3}. {ex}")
        except Exception as exc:
            logger.error("%s", exc)
        return

    try:
        out_fmt = resolve_output_format(args.csv, args.format)
    except ValueError as e:
        logger.error("%s", e)
        sys.exit(1)

    # --- Собрать конфиг ---
    if args.config:
        config_list = load_config(args.config)
    elif args.exchange and args.pair:
        if args.hours is not None:
            duration = int(args.hours * 3600)
        elif args.days is not None:
            duration = int(args.days * 86400)
        else:
            duration = 12 * 3600  # default: 12 часов
        config_list = [(args.exchange, args.pair, duration, args.interval_seconds, args.depth)]
    else:
        # Нет --exchange/--pair — применяем CLI-параметры к дефолтной паре
        if args.hours is not None:
            duration = int(args.hours * 3600)
        elif args.days is not None:
            duration = int(args.days * 86400)
        else:
            duration = DEFAULT_CONFIG[0][2]
        config_list = [
            (DEFAULT_CONFIG[0][0], DEFAULT_CONFIG[0][1],
             duration, args.interval_seconds, args.depth)
        ]

    save_path = Path(args.save_dir) if args.save_dir else None
    flush_interval = args.flush_interval

    # --- Сводная таблица перед стартом ---
    print("\n" + "=" * 72)
    print(f"  {'#':>3}  {'Коннектор':<22}  {'Пара':<12}  {'Длит.':<8}  {'Интервал':>9}  {'Глубина':>8}")
    print("  " + "-" * 68)
    for i, (ex, pair, dur, iv, dep) in enumerate(config_list, 1):
        rows_est = int(dur / iv)
        print(
            f"  {i:3}. {ex:<22}  {pair:<12}  {fmt_duration(dur):<8}  "
            f"{iv:>8.1f}s  {dep:>7}  (~{rows_est:,} строк)"
        )
    print("=" * 72)
    mode_label = f"один файл, дозапись каждые {flush_interval}s" if args.single_file else f"новый файл каждые {flush_interval}s"
    print(f"  Формат: {out_fmt}  |  Режим: {mode_label}")
    if args.progress_log_interval > 0:
        print(f"  Прогресс в лог: каждые {args.progress_log_interval:g} с")
    print("=" * 72 + "\n")

    try:
        asyncio.run(
            run_collection(
                config_list=config_list,
                output_format=out_fmt,
                save_dir=save_path,
                flush_interval=flush_interval,
                single_file=args.single_file,
                print_summary=args.summary,
                progress_log_interval=args.progress_log_interval,
            )
        )
    except KeyboardInterrupt:
        logger.warning("Прервано пользователем (Ctrl+C)")
    except Exception as exc:
        import traceback
        logger.error("%s\n%s", exc, traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
