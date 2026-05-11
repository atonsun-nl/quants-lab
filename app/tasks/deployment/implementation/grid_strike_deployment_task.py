import asyncio
import os
import time
from datetime import timedelta
from typing import List, Dict, Any
from app.tasks.deployment.deployment_base_task import DeploymentBaseTask, ConfigCandidate


class GridStrikeDeploymentTask(DeploymentBaseTask):
    """
    Задача для деплоя GridStrike контроллеров.
    
    В отличие от stat_arb, который работает с парами, GridStrike работает с отдельными торговыми парами.
    """

    async def _fetch_controller_configs(self) -> List[ConfigCandidate]:
        min_timestamp = time.time() - self.config.get("min_config_timestamp", 24 * 60 * 60)
        controller_configs_query = {
            "timestamp": {"$gt": min_timestamp},
            "config.controller_name": "grid_strike"
        }
        controller_configs_data = await self.mongo_client.get_documents(
            collection_name="controller_configs",
            query=controller_configs_query
        )
        return [ConfigCandidate.from_mongo(config_data) for config_data in controller_configs_data]

    def _extract_trading_pairs(self, config_candidates: List[ConfigCandidate]):
        """Извлекает торговые пары из конфигураций GridStrike."""
        return {
            candidate.config["trading_pair"] for candidate in config_candidates
        }

    def _filter_configs_by_trading_pair(self, all_config_candidates: List[ConfigCandidate], trading_pairs: List[str]):
        """Фильтрует конфигурации по торговым парам."""
        return [
            candidate for candidate in all_config_candidates
            if candidate.config["trading_pair"] in trading_pairs
        ]

    async def _is_candidate_valid(self, candidate: ConfigCandidate, filter_params: Dict[str, Any]):
        """
        Проверяет валидность кандидата на деплой.
        
        Для GridStrike проверяем:
        - Наличие текущих цен
        - Цена находится внутри диапазона сетки
        - Достаточный размер сетки
        - Соответствие минимальным требованиям по объему
        """
        # Извлекаем параметры фильтрации
        max_step = filter_params.get("max_step", 0.01)  # 1% максимальный шаг
        min_grid_range_ratio = filter_params.get("min_grid_range_ratio", 0.5)
        max_grid_range_ratio = filter_params.get("max_grid_range_ratio", 2.0)
        max_entry_price_distance = filter_params.get("max_entry_price_distance", 0.4)
        max_notional_size = filter_params.get("max_notional_size", 20.0)

        trading_pair = candidate.config["trading_pair"]
        
        # Проверяем наличие цены
        if trading_pair not in self.last_prices:
            return False
        
        current_price = self.last_prices[trading_pair]
        
        # Проверяем диапазоны сетки
        grid_ranges = candidate.config.get("grid_ranges", [])
        if not grid_ranges:
            return False
        
        total_amount_quote = candidate.config.get("total_amount_quote", 1000)
        num_orders = sum(len(range_data.get("prices", [])) for range_data in grid_ranges)
        if num_orders == 0:
            # Оцениваем количество ордеров по диапазонам
            num_orders = len(grid_ranges) * 5  # Примерно 5 ордеров на диапазон
        
        level_amount_quote = total_amount_quote / max(num_orders, 1)
        
        # Проверяем каждый диапазон сетки
        valid_ranges = 0
        for grid_range in grid_ranges:
            start_price = grid_range.get("start_price", 0)
            end_price = grid_range.get("end_price", 0)
            
            if start_price <= 0 or end_price <= 0:
                continue
            
            grid_range_pct = (end_price - start_price) / start_price
            
            # Проверяем что цена внутри диапазона (с учетом активационных границ)
            activation_bounds = candidate.config.get("activation_bounds", 0.02)
            inside_grid = start_price * (1 - activation_bounds) < current_price < end_price * (1 + activation_bounds)
            
            # Проверяем размер сетки
            grid_range_valid = min_grid_range_ratio < grid_range_pct < max_grid_range_ratio
            
            # Проверяем расстояние от текущей цены до границ
            entry_price_distance = abs(current_price - start_price) / (end_price - start_price) if end_price != start_price else 1
            distance_valid = entry_price_distance < max_entry_price_distance
            
            if inside_grid and grid_range_valid and distance_valid:
                valid_ranges += 1
        
        # Проверяем размер ордера
        notional_size_valid = level_amount_quote <= max_notional_size
        
        return valid_ranges > 0 and notional_size_valid

    def _adjust_config_candidates(self, config_candidates: List[ConfigCandidate]):
        """
        Корректирует конфигурации кандидатов перед деплоем.
        """
        params = self.config["config_adjustment_params"]
        
        for candidate in config_candidates:
            year, iso_week = self.get_year_and_isoweek()
            time_formatted = f"{year}||isoweek{iso_week}"
            
            connector_name = candidate.config["connector_name"]
            trading_pair = candidate.config["trading_pair"]
            controller_id = f"{connector_name.replace('_', '-')}||{trading_pair}||{time_formatted}"
            
            tag = self.get_rounded_time()
            candidate.config["id"] = f"{controller_id}_{tag}"
            
            # Применяем параметры из конфигурации задачи
            candidate.config["total_amount_quote"] = params["total_amount_quote"]
            candidate.config["leverage"] = params["leverage"]
            candidate.config["connector_name"] = self.config["connector_name"]
            candidate.config["min_spread_between_orders"] = params["min_spread_between_orders"]
            candidate.config["max_open_orders"] = params["max_open_orders"]
            
            # Настраиваем Triple Barrier Config
            candidate.config["triple_barrier_config"] = {
                'stop_loss': params["stop_loss"],
                'take_profit': params["take_profit"],
                'time_limit': params["time_limit"],
                'trailing_stop': {
                    'activation_price': params["activation_price"],
                    'trailing_delta': params["trailing_delta"]
                }
            }
            
            # Обновляем минимальные размеры ордеров для каждого диапазона
            min_notional = float(self.min_notionals_dict.get(trading_pair, 5)) * 1.5
            for grid_range in candidate.config.get("grid_ranges", []):
                grid_range["min_order_amount"] = max(
                    grid_range.get("min_order_amount", 0),
                    min_notional
                )
        
        return config_candidates


async def main():
    connector_name = "binance_perpetual"
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://admin:admin@localhost:27017/quants_lab?authSource=admin&retryWrites=true&w=majority')
    
    task_config = {
        "connector_name": connector_name,
        "mongo_uri": mongo_uri,
        "backend_api_server": os.getenv("BACKEND_API_SERVER", "localhost"),
        "min_config_timestamp": 1.5 * 24 * 60 * 60,
        
        "filter_candidate_params": {
            "max_step": 0.01,
            "min_grid_range_ratio": 0.3,
            "max_grid_range_ratio": 3.0,
            "max_entry_price_distance": 0.5,
            "max_notional_size": 100.0
        },
        
        "config_adjustment_params": {
            "total_amount_quote": 1000.0,
            "min_spread_between_orders": 0.001,
            "max_open_orders": 5,
            "leverage": 20,
            "time_limit": 604800,  # 7 дней
            "stop_loss": 0.05,
            "trailing_delta": 0.005,
            "take_profit": 0.002,
            "activation_price": 0.03,
        },
        
        "deploy_params": {
            "max_bots": 1,
            "max_controller_configs": 2,
            "script_name": "v2_with_controllers.py",
            "image_name": "hummingbot/hummingbot:latest",
            "credentials": "master_account",
            "time_to_cash_out": 2 * 24 * 60 * 60,
        },
        
        "control_params": {
            "controller_max_drawdown": 0.01,
            "controller_max_pnl": 0.01,
            "global_time_limit": 24 * 60 * 60,
            "partial_drawdown": 0.1,
            "partial_profit": 0.1,
            "min_early_stop_time": 6 * 60 * 60,
            "max_early_stop_time": 24 * 60 * 60
        }
    }
    
    task = GridStrikeDeploymentTask(
        name="grid_strike_deployment",
        frequency=timedelta(minutes=20),
        config=task_config
    )
    await task.execute()


if __name__ == "__main__":
    asyncio.run(main())
