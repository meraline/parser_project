from __future__ import annotations
from typing import Optional, Dict

import time

from ..database.repositories.cache_repository import CacheRepository
from ..database.repositories.review_repository import ReviewRepository
from ..models import Review
from ..parsers import BaseParser
from ..utils.logger import get_logger


logger = get_logger(__name__)


class ParserMetrics:
    """Сбор и анализ метрик парсеров."""

    def __init__(self):
        self._parse_times: Dict[str, list[float]] = {}
        self._reviews_count: Dict[str, list[int]] = {}
        self._saved_count: Dict[str, list[int]] = {}
        self._error_count: Dict[str, int] = {}
        self._start_time = time.time()

    def record_parse_time(self, duration: float, parser: str) -> None:
        """Записать время парсинга."""
        if parser not in self._parse_times:
            self._parse_times[parser] = []
        self._parse_times[parser].append(duration)

    def record_reviews_count(self, total: int, saved: int, parser: str) -> None:
        """Записать количество отзывов."""
        if parser not in self._reviews_count:
            self._reviews_count[parser] = []
            self._saved_count[parser] = []
        self._reviews_count[parser].append(total)
        self._saved_count[parser].append(saved)

    def record_error(self, parser: str) -> None:
        """Записать ошибку парсера."""
        self._error_count[parser] = self._error_count.get(parser, 0) + 1

    def get_metrics(self) -> Dict:
        """Получить собранные метрики."""
        metrics = {
            "uptime": time.time() - self._start_time,
            "error_count": sum(self._error_count.values()),
        }

        for parser in self._parse_times:
            times = self._parse_times[parser]
            found = sum(self._reviews_count.get(parser, []))
            saved = sum(self._saved_count.get(parser, []))

            metrics[parser] = {
                "avg_parse_time": (sum(times) / len(times) if times else 0),
                "reviews_found": found,
                "reviews_saved": saved,
                "save_success_rate": (saved / found if found > 0 else 0),
                "errors": self._error_count.get(parser, 0),
            }

        return metrics


class ParserService:
    """Сервис для управления парсингом данных."""

    def __init__(
        self,
        review_repo: ReviewRepository,
        cache_repo: Optional[CacheRepository] = None,
        metrics: Optional[ParserMetrics] = None,
    ):
        self.review_repo = review_repo
        self.cache_repo = cache_repo
        self.metrics = metrics
        self._parsers: dict[str, BaseParser] = {}

    def register_parser(self, name: str, parser: BaseParser) -> None:
        """Регистрация парсера.

        Args:
            name: Имя парсера
            parser: Экземпляр парсера
        """
        self._parsers[name] = parser

    def get_parser(self, name: str) -> Optional[BaseParser]:
        """Получение парсера по имени.

        Args:
            name: Имя парсера

        Returns:
            Optional[BaseParser]: Парсер или None
        """
        return self._parsers.get(name)

    def parse_reviews(
        self, parser_name: str, brand: str, model: str, max_pages: int = 10
    ) -> list[Review]:
        """Парсинг отзывов.

        Args:
            parser_name: Имя парсера
            brand: Марка автомобиля
            model: Модель автомобиля
            max_pages: Максимальное количество страниц

        Returns:
            list[Review]: Список отзывов
        """
        parser = self.get_parser(parser_name)
        if not parser:
            logger.error(f"Парсер {parser_name} не найден")
            return []

        start_time = time.time()
        try:
            reviews = parser.parse_brand_model_reviews(
                {"brand": brand, "model": model, "max_pages": max_pages}
            )

            # Сохраняем пакетом
            saved = self.review_repo.save_batch(reviews)

            # Обновляем метрики
            if self.metrics:
                self.metrics.record_parse_time(time.time() - start_time, parser_name)
                self.metrics.record_reviews_count(len(reviews), saved, parser_name)

            # Инвалидируем кэш
            if self.cache_repo:
                self.cache_repo.delete("parser_stats")

            return reviews

        except Exception as e:
            logger.error(
                f"Ошибка парсинга {brand} {model} " f"парсером {parser_name}: {e}"
            )
            if self.metrics:
                self.metrics.record_error(parser_name)
            return []

    def get_stats(self, use_cache: bool = True) -> dict:
        """Получение статистики парсинга.

        Args:
            use_cache: Использовать кэш

        Returns:
            dict: Статистика
        """
        if use_cache and self.cache_repo:
            cached = self.cache_repo.get("parser_stats")
            if cached:
                return cached

        stats = self.review_repo.get_parsing_stats()

        if self.metrics:
            stats["metrics"] = self.metrics.get_metrics()

        if self.cache_repo:
            self.cache_repo.set("parser_stats", stats)

        return stats

    def show_stats(self) -> None:
        """Вывод статистики в лог."""
        stats = self.get_stats()

        logger.info("\n📊 СТАТИСТИКА ПАРСИНГА")
        logger.info("=" * 50)
        logger.info("📝 Общая информация")
        logger.info("-" * 30)
        logger.info(f"▫️ Всего отзывов: {stats['total_reviews']:,}")
        logger.info(f"▫️ Уникальных брендов: {stats['unique_brands']}")
        logger.info(f"▫️ Уникальных моделей: {stats['unique_models']}")

        logger.info("\n🔍 По источникам")
        logger.info("-" * 30)
        for source, count in stats["by_source"].items():
            logger.info(f"▫️ {source}: {count:,}")

        logger.info("\n📑 По типам")
        logger.info("-" * 30)
        for type_, count in stats["by_type"].items():
            logger.info(f"▫️ {type_}: {count:,}")

        if "metrics" in stats:
            metrics = stats["metrics"]
            logger.info("\n📈 МЕТРИКИ ПРОИЗВОДИТЕЛЬНОСТИ")
            logger.info("=" * 50)

            # Общая информация
            logger.info("⏱ Общая информация")
            logger.info("-" * 30)
            logger.info(f"▫️ Время работы: {metrics['uptime']:.1f}s")
            logger.info(f"▫️ Всего ошибок: {metrics['error_count']}")

            # Информация по парсерам
            logger.info("\n🤖 Детализация по парсерам")
            logger.info("-" * 30)
            for parser, parser_metrics in metrics.items():
                if parser not in ("uptime", "error_count"):
                    logger.info(f"\n▪️ {parser}")
                    logger.info(
                        f"  ⌛️ Среднее время парсинга: "
                        f"{parser_metrics['avg_parse_time']:.2f}s"
                    )
                    logger.info(
                        f"  📥 Найдено отзывов: " f"{parser_metrics['reviews_found']:,}"
                    )
                    logger.info(
                        f"  💾 Сохранено отзывов: "
                        f"{parser_metrics['reviews_saved']:,}"
                    )
                    logger.info(
                        f"  📊 Успешность сохранения: "
                        f"{parser_metrics['save_success_rate']:.1%}"
                    )
                    logger.info(
                        f"  ⚠️ Количество ошибок: " f"{parser_metrics['errors']}"
                    )
