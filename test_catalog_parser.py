"""Тестовый запуск парсера каталога в щадящем режиме."""

import time
import random
from src.auto_reviews_parser.parsers.drom import DromParser
from src.auto_reviews_parser.utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """Основная функция запуска парсера."""
    # Список брендов для тестирования
    brands = [
        ("ac", 1),  # AC - 1 отзыв
        ("aito", 85),  # AITO - 85 отзывов
    ]

    # Создаем инстанс парсера
    with DromParser(
        db_path="auto_reviews.db", proxy=None  # Добавьте прокси если нужно
    ) as parser:
        # Проходим по каждому бренду
        for brand, expected_reviews in brands:
            try:
                logger.info(
                    f"Начинаем парсинг бренда {brand.upper()} "
                    f"(ожидается {expected_reviews} отзывов)"
                )

                # Запускаем парсинг с ограничениями:
                # - не более 10 отзывов за раз
                # - не более 2 страниц на модель
                parser.parse_catalog(brand=brand, max_reviews=10, max_pages_per_model=2)

                # Делаем большую паузу между брендами
                pause = random.uniform(5, 10)
                logger.info(f"Пауза {pause:.1f} сек перед следующим брендом")
                time.sleep(pause)

            except Exception as e:
                logger.error(f"Ошибка при парсинге {brand}: {e}")
                continue


if __name__ == "__main__":
    main()
