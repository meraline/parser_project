from __future__ import annotations

from auto_reviews_parser import AutoReviewsParser
from config.settings import settings, TARGET_BRANDS

from .queue_service import QueueService
from .export_service import ExportService


class ParserService:
    """Сервис-оркестратор для работы парсеров"""

    def __init__(self, db_path: str = settings.db_path):
        self.queue_service = QueueService(db_path, TARGET_BRANDS)
        self.export_service = ExportService(db_path)
        self.parser = AutoReviewsParser(db_path, queue_service=self.queue_service)

    def show_status(self) -> None:
        """Показать статус базы данных и очереди"""
        stats = self.parser.db.get_parsing_stats()
        print("\n📊 СТАТУС БАЗЫ ДАННЫХ")
        print("=" * 50)
        print(f"Всего отзывов: {stats['total_reviews']:,}")
        print(f"Уникальных брендов: {stats['unique_brands']}")
        print(f"Уникальных моделей: {stats['unique_models']}")

        if stats["by_source"]:
            print("\nПо источникам:")
            for source, count in stats["by_source"].items():
                print(f"  {source}: {count:,}")

        if stats["by_type"]:
            print("\nПо типам:")
            for type_name, count in stats["by_type"].items():
                print(f"  {type_name}: {count:,}")

        queue_stats = self.queue_service.get_queue_stats()
        print("\n📋 СТАТУС ОЧЕРЕДИ")
        print("=" * 50)
        total_sources = sum(queue_stats.values())
        for status, count in queue_stats.items():
            percentage = (count / total_sources * 100) if total_sources > 0 else 0
            print(f"{status}: {count} ({percentage:.1f}%)")
        print(f"Всего источников: {total_sources}")

    def reset_queue(self) -> None:
        """Сброс очереди парсинга"""
        print("🔄 Сброс очереди парсинга...")
        self.queue_service.initialize_queue()

    def export_data(self, output_format: str = "xlsx") -> None:
        """Экспорт данных из базы"""
        self.export_service.export_data(output_format)
