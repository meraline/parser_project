import sys
import os
import logging
from pathlib import Path
from src.auto_reviews_parser.parsers.drom_parser import DromParser
from src.auto_reviews_parser.utils.logger import get_logger

# Добавляем путь к корневой директории проекта
project_root = Path(__file__).parent
sys.path.append(str(project_root))

logger = get_logger(__name__, level=logging.DEBUG)


def main():
    # Создаем экземпляр парсера
    parser = DromParser()

    # URL для тестирования
    test_urls = [
        "https://www.drom.ru/reviews/mitsubishi/outlander/1432586/",
    ]

    try:
        # Запускаем парсинг
        logger.info("Начинаем парсинг отзывов...")
        reviews = parser.parse_reviews(test_urls)

        # Выводим результаты
        logger.info(f"Собрано отзывов: {len(reviews)}")

        # Проверяем наличие ошибок
        errors = parser.get_errors()
        if errors:
            logger.warning(f"Обнаружено ошибок: {len(errors)}")
            for error in errors:
                logger.error(f"URL: {error['url']}")
                logger.error(f"Ошибка: {error['error']}")

        # Выводим несколько первых отзывов для проверки
        if reviews:
            logger.info("\nПримеры собранных отзывов:")
            for i, review in enumerate(reviews[:3], 1):
                logger.info(f"\nОтзыв #{i}:")
                logger.info(f"Автор: {review.author}")
                logger.info(f"Дата: {review.publish_date}")
                logger.info(f"Рейтинг: {review.rating}")
                logger.info(f"URL: {review.url}")
                logger.info(f"Текст: {review.content[:200]}...")

    except Exception as e:
        logger.error(f"Ошибка при тестировании парсера: {e}")


if __name__ == "__main__":
    main()
