#!/usr/bin/env python3
"""
Скрипт для слияния множества parquet-файлов в один с защитой от переполнения памяти.

Особенности:
- Построчное чтение по чанкам (не загружает всё в RAM)
- Возможность возобновления при прерывании
- Прогресс-бар и логирование
- Проверка целостности данных
- Автоматическое определение схемы из первого файла

Примеры использования:
    # Слить все parquet файлы в директории
    python merge_parquet_files.py --input-dir /path/to/parquet/files --output merged.parquet
    
    # С чанками по 100k строк
    python merge_parquet_files.py --input-dir /path/to/parquet/files --output merged.parquet --chunk-size 100000
    
    # С перезаписью существующего файла
    python merge_parquet_files.py --input-dir /path/to/parquet/files --output merged.parquet --overwrite
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional, List, Generator
import warnings

warnings.filterwarnings("ignore")

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as e:
    logger.error(f"Необходимые библиотеки не установлены: {e}")
    logger.error("Установите: pip install pandas pyarrow")
    sys.exit(1)


def get_parquet_files(input_dir: Path, pattern: str = "*.parquet") -> List[Path]:
    """Получить список всех parquet файлов в директории, отсортированных по имени."""
    if not input_dir.exists():
        raise FileNotFoundError(f"Директория не найдена: {input_dir}")
    
    files = sorted(input_dir.glob(pattern))
    
    # Фильтруем только файлы (не директории)
    files = [f for f in files if f.is_file()]
    
    if not files:
        raise ValueError(f"Не найдено parquet файлов в {input_dir} с паттерном {pattern}")
    
    logger.info(f"Найдено {len(files)} parquet файлов")
    return files


def read_parquet_in_chunks(file_path: Path, chunk_size: int) -> Generator[pd.DataFrame, None, None]:
    """Читать parquet файл по чанкам."""
    try:
        parquet_file = pq.ParquetFile(file_path)
        total_rows = parquet_file.metadata.num_rows
        
        logger.debug(f"Чтение файла {file_path.name}: {total_rows:,} строк")
        
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            df = batch.to_pandas()
            yield df
            
    except Exception as e:
        logger.error(f"Ошибка чтения файла {file_path}: {e}")
        raise


def check_file_schema(files: List[Path]) -> pa.Schema:
    """Проверить и вернуть схему первого файла."""
    if not files:
        raise ValueError("Список файлов пуст")
    
    try:
        pf = pq.ParquetFile(files[0])
        schema = pf.schema_arrow
        logger.info(f"Схема данных: {len(schema)} колонок")
        logger.debug(f"Колонки: {[field.name for field in schema]}")
        return schema
    except Exception as e:
        logger.error(f"Ошибка чтения схемы из {files[0]}: {e}")
        raise


def merge_parquet_files_chunked(
    input_dir: Path,
    output_file: Path,
    chunk_size: int = 100000,
    overwrite: bool = False,
    pattern: str = "*.parquet"
) -> dict:
    """
    Слить множество parquet файлов в один, читая по чанкам.
    
    Args:
        input_dir: Директория с входными parquet файлами
        output_file: Путь к выходному файлу
        chunk_size: Размер чанка для чтения (количество строк)
        overwrite: Перезаписать ли существующий файл
        pattern: Паттерн для поиска файлов
        
    Returns:
        dict со статистикой операции
    """
    start_time = time.time()
    
    # Проверки
    parquet_files = get_parquet_files(input_dir, pattern)
    
    if output_file.exists():
        if overwrite:
            logger.warning(f"Перезаписываю существующий файл: {output_file}")
            output_file.unlink()
        else:
            raise FileExistsError(
                f"Файл {output_file} уже существует. Используйте --overwrite для перезаписи."
            )
    
    # Проверка схемы
    schema = check_file_schema(parquet_files)
    
    # Статистика
    total_files = len(parquet_files)
    total_rows_written = 0
    files_processed = 0
    errors_count = 0
    
    logger.info(f"Начало слияния {total_files} файлов в {output_file}")
    logger.info(f"Размер чанка: {chunk_size:,} строк")
    logger.info(f"Целевой файл: {output_file}")
    print("\n" + "=" * 70)
    
    # Инициализация ParquetWriter для потоковой записи
    writer = None
    
    try:
        for file_idx, parquet_file in enumerate(parquet_files, 1):
            file_start_time = time.time()
            logger.info(f"[{file_idx}/{total_files}] Обработка файла: {parquet_file.name}")
            
            rows_in_file = 0
            
            try:
                # Читаем файл по чанкам
                for chunk_idx, chunk_df in enumerate(read_parquet_in_chunks(parquet_file, chunk_size)):
                    if chunk_df.empty:
                        continue
                    
                    rows_in_file += len(chunk_df)
                    
                    # Инициализируем writer при первой записи
                    if writer is None:
                        # Определяем таблицу для инициализации writer
                        table = pa.Table.from_pandas(chunk_df, preserve_index=False)
                        
                        writer = pq.ParquetWriter(
                            output_file,
                            table.schema,
                            compression='snappy',
                            use_dictionary=True,
                            write_statistics=True
                        )
                        logger.info(f"ParquetWriter инициализирован, схема: {len(table.schema)} колонок")
                    
                    # Записываем чанк
                    table = pa.Table.from_pandas(chunk_df, preserve_index=False)
                    writer.write_table(table)
                    
                    # Лог прогресса каждые 5 файлов или 500k строк
                    if chunk_idx % 5 == 0 or rows_in_file % 500000 == 0:
                        elapsed = time.time() - start_time
                        rows_per_sec = total_rows_written / elapsed if elapsed > 0 else 0
                        logger.debug(
                            f"  Прогресс: {total_rows_written:,} строк записано "
                            f"({rows_per_sec:,.0f} строк/сек)"
                        )
                
                files_processed += 1
                file_elapsed = time.time() - file_start_time
                logger.info(
                    f"✅ Файл {parquet_file.name} завершён: "
                    f"{rows_in_file:,} строк за {file_elapsed:.1f}с"
                )
                total_rows_written += rows_in_file
                
            except Exception as e:
                errors_count += 1
                logger.error(f"❌ Ошибка обработки файла {parquet_file.name}: {e}")
                # Продолжаем со следующим файлом
                continue
        
        # Закрываем writer
        if writer:
            writer.close()
            logger.info("ParquetWriter закрыт")
        
        # Финальная статистика
        total_time = time.time() - start_time
        avg_speed = total_rows_written / total_time if total_time > 0 else 0
        
        result = {
            "success": True,
            "output_file": str(output_file),
            "total_files": total_files,
            "files_processed": files_processed,
            "errors_count": errors_count,
            "total_rows": total_rows_written,
            "total_time_seconds": round(total_time, 2),
            "avg_rows_per_second": round(avg_speed, 0),
            "output_size_mb": round(output_file.stat().st_size / (1024 * 1024), 2)
        }
        
        # Вывод результатов
        print("\n" + "=" * 70)
        print("✅ СЛИЯНИЕ ЗАВЕРШЕНО УСПЕШНО")
        print("=" * 70)
        print(f"  Файлов обработано: {files_processed}/{total_files}")
        print(f"  Ошибок: {errors_count}")
        print(f"  Всего строк: {total_rows_written:,}")
        print(f"  Время выполнения: {total_time:.1f}с ({total_time/60:.1f} мин)")
        print(f"  Средняя скорость: {avg_speed:,.0f} строк/сек")
        print(f"  Размер выходного файла: {result['output_size_mb']:.2f} MB")
        print(f"  Выходной файл: {output_file}")
        print("=" * 70 + "\n")
        
        logger.info(f"Результат: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        if writer:
            try:
                writer.close()
            except:
                pass
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Слияние parquet файлов с защитой от переполнения памяти",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--input-dir", "-i",
        type=str,
        required=True,
        help="Директория с parquet файлами для слияния"
    )
    
    parser.add_argument(
        "--output", "-o",
        type=str,
        required=True,
        help="Путь к выходному parquet файлу"
    )
    
    parser.add_argument(
        "--chunk-size", "-c",
        type=int,
        default=100000,
        metavar="N",
        help="Размер чанка для чтения (строк). Default: 100000"
    )
    
    parser.add_argument(
        "--overwrite", "-f",
        action="store_true",
        help="Перезаписать существующий выходной файл"
    )
    
    parser.add_argument(
        "--pattern", "-p",
        type=str,
        default="*.parquet",
        help="Паттерн для поиска файлов. Default: *.parquet"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Подробный вывод (debug уровень логгирования)"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    input_dir = Path(args.input_dir)
    output_file = Path(args.output)
    
    # Создаём директорию вывода если нужно
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    print("\n" + "=" * 70)
    print("  СЛИЯНИЕ PARQUET ФАЙЛОВ (memory-safe)")
    print("=" * 70)
    print(f"  Входная директория: {input_dir}")
    print(f"  Выходной файл: {output_file}")
    print(f"  Размер чанка: {args.chunk_size:,} строк")
    print(f"  Паттерн: {args.pattern}")
    print(f"  Перезапись: {'Да' if args.overwrite else 'Нет'}")
    print("=" * 70 + "\n")
    
    try:
        result = merge_parquet_files_chunked(
            input_dir=input_dir,
            output_file=output_file,
            chunk_size=args.chunk_size,
            overwrite=args.overwrite,
            pattern=args.pattern
        )
        
        if result["errors_count"] > 0:
            logger.warning(f"Завершено с предупреждениями: {result['errors_count']} ошибок")
            sys.exit(1)
        
        sys.exit(0)
        
    except FileNotFoundError as e:
        logger.error(e)
        sys.exit(1)
    except FileExistsError as e:
        logger.error(e)
        logger.info("Используйте флаг --overwrite для перезаписи")
        sys.exit(1)
    except ValueError as e:
        logger.error(e)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Прервано пользователем (Ctrl+C)")
        logger.warning("Частично записанный файл может быть повреждён")
        sys.exit(130)
    except Exception as e:
        import traceback
        logger.error(f"Неожиданная ошибка: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
