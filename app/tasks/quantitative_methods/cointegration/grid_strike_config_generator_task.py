import asyncio
import logging
import os
import time
from datetime import timedelta
from decimal import Decimal
import pandas as pd
from dotenv import load_dotenv
from hummingbot.core.data_type.common import TradeType

from core.task_base import BaseTask


class GridStrikeConfigGeneratorTask(BaseTask):
    """
    Генератор конфигураций для GridStrike контроллера на основе результатов коинтеграционного анализа.
    
    В отличие от stat_arb, который торгует спредом между парами, GridStrike создает сетки ордеров
    для каждой пары отдельно, что проще и надежнее в боковом движении.
    """
    
    def __init__(self, name: str, frequency: str, config: dict):
        super().__init__(name, frequency, config)

    @property
    def base_config(self):
        return {
            "total_amount_quote": 1000,
            "connector_name": "binance_perpetual",
            "leverage": 20,
            "min_order_amount": 5,
            "max_open_orders": 5,
            "time_limit": 60 * 60 * 24 * 7,  # 7 дней
            "activation_bounds": 0.02,  # 2% активационные границы
            "min_spread_between_orders": 0.001,  # 0.1% минимальный спред
            "open_order_type": 3,  # LIMIT_MAKER
            "take_profit_order_type": 1,  # LIMIT
        }

    def get_grid_ranges(self, start_price: float, end_price: float, mid_price: float) -> list:
        """
        Создает диапазоны сетки для LONG и SHORT позиций.
        Для рыночно-нейтральной стратегии создаем две сетки:
        - BUY сетка ниже текущей цены
        - SELL сетка выше текущей цены
        """
        grid_ranges = []
        
        # Рассчитываем процентные границы
        price_range_pct = (end_price - start_price) / start_price
        
        # LONG сетка (покупка на падении)
        long_start = start_price
        long_end = mid_price * (1 + price_range_pct * 0.5)
        
        if long_start < mid_price:
            grid_ranges.append({
                "id": "LONG",
                "start_price": float(Decimal(str(long_start)).quantize(Decimal('0.000001'))),
                "end_price": float(Decimal(str(long_end)).quantize(Decimal('0.000001'))),
                "total_amount_pct": Decimal("0.5"),  # 50% капитала
                "side": "BUY",
                "open_order_type": self.base_config["open_order_type"],
                "take_profit_order_type": self.base_config["take_profit_order_type"],
                "active": True
            })
        
        # SHORT сетка (продажа на росте)
        short_start = mid_price * (1 - price_range_pct * 0.5)
        short_end = end_price
        
        if short_end > mid_price:
            grid_ranges.append({
                "id": "SHORT",
                "start_price": float(Decimal(str(short_start)).quantize(Decimal('0.000001'))),
                "end_price": float(Decimal(str(short_end)).quantize(Decimal('0.000001'))),
                "total_amount_pct": Decimal("0.5"),  # 50% капитала
                "side": "SELL",
                "open_order_type": self.base_config["open_order_type"],
                "take_profit_order_type": self.base_config["take_profit_order_type"],
                "active": True
            })
        
        return grid_ranges

    def get_config_dict(
            self,
            trading_pair: str,
            start_price: float,
            end_price: float,
            mid_price: float,
            coint_value: float,
            p_value: float,
            correlation: float,
    ) -> dict:
        """
        Создает конфигурацию для GridStrike контроллера.
        """
        grid_ranges = self.get_grid_ranges(start_price, end_price, mid_price)
        
        if not grid_ranges:
            logging.warning(f"No valid grid ranges for {trading_pair}, skipping...")
            return None
        
        config_dict = {
            "id": f"grid_{trading_pair.replace('-', '')}_config",
            "controller_name": "grid_strike",
            "controller_type": "generic",
            "connector_name": self.base_config["connector_name"],
            "trading_pair": trading_pair,
            "total_amount_quote": self.base_config["total_amount_quote"],
            "grid_ranges": grid_ranges,
            "position_mode": "HEDGE",
            "leverage": self.base_config["leverage"],
            "time_limit": self.base_config["time_limit"],
            "activation_bounds": self.base_config["activation_bounds"],
            "min_spread_between_orders": self.base_config["min_spread_between_orders"],
            "max_open_orders": self.base_config["max_open_orders"],
            "min_order_amount": self.base_config["min_order_amount"],
            "manual_kill_switch": None,
        }
        
        return config_dict

    async def execute(self):
        """
        1) Читает результаты коинтеграционного анализа из MongoDB
        2) Фильтрует пары по качеству коинтеграции
        3) Генерирует конфигурации GridStrike для каждой подходящей пары
        4) Сохраняет конфигурации в MongoDB
        """
        try:
            logging.info("Starting GridStrike config generation...")
            
            # Чтение результатов коинтеграции
            coint_results = await self.mongodb_client.get_documents("cointegration_results")
            
            if not coint_results:
                logging.warning("No cointegration results found in MongoDB")
                return
            
            coint_results_df = pd.DataFrame(coint_results)
            logging.info(f"Loaded {len(coint_results_df)} cointegration results")
            
            # Фильтрация по качеству коинтеграции
            # p-value < 0.05 (статистическая значимость)
            # correlation > 0.7 (сильная корреляция)
            filtered_df = coint_results_df[
                (coint_results_df["coint_value"] < 0.05) & 
                (coint_results_df.get("correlation", 0) > 0.7)
            ]
            
            logging.info(f"Filtered to {len(filtered_df)} high-quality pairs")
            
            # Генерация конфигураций
            all_configs = []
            for _, row in filtered_df.iterrows():
                # Определяем лучшую пару для торговли (с более сильным сигналом)
                if row.get("base_signal_strength", 0) > row.get("quote_signal_strength", 0):
                    trading_pair = row["base"]
                    start_price = row["grid_base"]["start_price"]
                    end_price = row["grid_base"]["end_price"]
                    mid_price = (start_price + end_price) / 2
                    signal_strength = row.get("base_signal_strength", 0)
                else:
                    trading_pair = row["quote"]
                    start_price = row["grid_quote"]["start_price"]
                    end_price = row["grid_quote"]["end_price"]
                    mid_price = (start_price + end_price) / 2
                    signal_strength = row.get("quote_signal_strength", 0)
                
                config = self.get_config_dict(
                    trading_pair=trading_pair,
                    start_price=start_price,
                    end_price=end_price,
                    mid_price=mid_price,
                    coint_value=row["coint_value"],
                    p_value=row.get("base_p_value", row.get("quote_p_value", 1.0)),
                    correlation=row.get("correlation", 0),
                )
                
                if config:
                    record = {
                        "config": config,
                        "extra_info": {
                            "coint_value": row["coint_value"],
                            "p_value": row.get("base_p_value", row.get("quote_p_value", 1.0)),
                            "correlation": row.get("correlation", 0),
                            "signal_strength": signal_strength,
                            "source_pair_base": row["base"],
                            "source_pair_quote": row["quote"],
                        },
                        "timestamp": time.time()
                    }
                    all_configs.append(record)
            
            if not all_configs:
                logging.warning("No valid configs generated")
                return
            
            # Сохранение в MongoDB
            await self.mongodb_client.insert_documents(
                collection_name="controller_configs",
                documents=all_configs,
                index=[("controller_name", 1), ("controller_type", 1), ("connector_name", 1)]
            )
            
            logging.info(f"Successfully stored {len(all_configs)} GridStrike configs")

        except Exception as e:
            logging.error(f"Error executing GridStrike config generator: {str(e)}")
            raise


async def main():
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "")
    config = {
        "mongo_uri": mongo_uri
    }
    task = GridStrikeConfigGeneratorTask(name="grid_strike_config_generator", frequency=timedelta(hours=12), config=config)
    await task.execute()

if __name__ == "__main__":
    asyncio.run(main())
