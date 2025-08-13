from __future__ import annotations

import json
import logging

from auto_reviews_parser import AutoReviewsParser, Config

from .queue_service import QueueService
from .export_service import ExportService
from ..utils.cache import RedisCache


logger = logging.getLogger(__name__)


class ParserService:
    """Сервис-оркестратор для работы парсеров"""

    def __init__(
        self, db_path: str = Config.DB_PATH, cache: RedisCache | None = None
    ):
        self.queue_service = QueueService(db_path, Config.TARGET_BRANDS)
        self.export_service = ExportService(db_path)
        self.parser = AutoReviewsParser(db_path, queue_service=self.queue_service)
        self.cache = cache

    def get_status_data(self) -> dict:
        """Получить статистику из базы с использованием кэша."""
        cache_key = "parser_status"
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached:
                return json.loads(cached)
        stats = self.parser.db.get_parsing_stats()
        queue_stats = self.queue_service.get_queue_stats()
        data = {"stats": stats, "queue_stats": queue_stats}
        if self.cache:
            self.cache.set(cache_key, json.dumps(data))
        return data

    def show_status(self) -> None:
        """Показать статус базы данных и очереди"""
        data = self.get_status_data()
        stats = data["stats"]
        logger.info("\n📊 СТАТУС БАЗЫ ДАННЫХ")
        logger.info("=" * 50)
        logger.info(f"Всего отзывов: {stats['total_reviews']:,}")
        logger.info(f"Уникальных брендов: {stats['unique_brands']}")
        logger.info(f"Уникальных моделей: {stats['unique_models']}")

        if stats["by_source"]:
            logger.info("\nПо источникам:")
            for source, count in stats["by_source"].items():
                logger.info(f"  {source}: {count:,}")

        if stats["by_type"]:
            logger.info("\nПо типам:")
            for type_name, count in stats["by_type"].items():
                logger.info(f"  {type_name}: {count:,}")

        queue_stats = data["queue_stats"]
        logger.info("\n📋 СТАТУС ОЧЕРЕДИ")
        logger.info("=" * 50)
        total_sources = sum(queue_stats.values())
        for status, count in queue_stats.items():
            percentage = (count / total_sources * 100) if total_sources > 0 else 0
            logger.info(f"{status}: {count} ({percentage:.1f}%)")
        logger.info(f"Всего источников: {total_sources}")

    def reset_queue(self) -> None:
        """Сброс очереди парсинга"""
        logger.info("🔄 Сброс очереди парсинга...")
        self.queue_service.initialize_queue()

    def export_data(self, output_format: str = "xlsx") -> None:
        """Экспорт данных из базы"""
        self.export_service.export_data(output_format)
