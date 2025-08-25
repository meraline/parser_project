"""Тестирование парсера каталога отзывов Drom.ru"""

import logging
import sys
from pathlib import Path

from src.auto_reviews_parser.parsers.drom import DromParser
from src.auto_reviews_parser.utils.logger import get_logger

# Настраиваем логирование
logger = get_logger(__name__)
logger.setLevel(logging.INFO)

# Создаем директорию для логов если её нет
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# Добавляем вывод в файл
file_handler = logging.FileHandler("logs/parser_catalog.log")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
)
logger.addHandler(file_handler)

# Добавляем вывод в консоль
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(console_handler)


def test_catalog_parser():
    """Тестирование парсера каталога"""
    parser = DromParser("auto_reviews.db")

    # Тестируем на Toyota, так как это один из популярных брендов
    brand_url = "https://www.drom.ru/reviews/toyota/"

    # Получаем список моделей
    models = parser.get_brand_models_urls(brand_url)
    logger.info(f"Найдено моделей Toyota: {len(models)}")

    # Выводим первые 5 моделей
    for model in models[:5]:
        logger.info(
            f"Модель: {model['name']}, "
            f"Отзывов: {model['count']}, "
            f"URL: {model['url']}"
        )

    # Тестируем получение отзывов для первой модели
    if models:
        first_model = models[0]
        model_url = first_model["url"]
        model_name = first_model["name"]

        # Получаем первые 10 отзывов
        review_urls = parser.get_review_urls(model_url, max_reviews=10)
        logger.info(f"Получено {len(review_urls)} URL отзывов для {model_name}")

        # Парсим эти отзывы
        reviews = []
        for url in review_urls:
            if page_reviews := parser.parse_page(url):
                reviews.extend(page_reviews)

        logger.info(f"Собрано {len(reviews)} отзывов для {model_name}")


if __name__ == "__main__":
    test_catalog_parser()
