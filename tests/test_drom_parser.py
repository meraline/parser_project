import unittest
import os
import sys
from pathlib import Path

# Настраиваем пути для импорта
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

from src.auto_reviews_parser.parsers.drom_botasaurus_bot import DromParserBot


class TestDromParser(unittest.TestCase):
    def setUp(self):
        self.parser = DromParserBot()
        # URL страницы с отзывами для тестирования
        # Используем конкретную страницу с отзывами
        self.test_url = "https://www.drom.ru/reviews/toyota/camry/2018_g/"
        self.test_timeout = 60  # Увеличенный таймаут для тестов

    def tearDown(self):
        # Освобождаем ресурсы
        self.parser.close_driver()

    def test_extract_brand_model(self):
        """Тест извлечения марки и модели из URL"""
        brand, model = self.parser.extract_brand_model(self.test_url)
        self.assertEqual(brand, "toyota")
        self.assertEqual(model, "camry")

    def test_parse_page(self):
        """Тест парсинга одной страницы с отзывами"""
        reviews = self.parser.parse_page(self.test_url)

        # Проверяем, что получили хотя бы один отзыв
        self.assertGreater(len(reviews), 0)

        # Проверяем структуру первого отзыва
        review = reviews[0]
        self.assertEqual(review.source, "drom.ru")
        self.assertEqual(review.type, "review")
        self.assertEqual(review.brand, "toyota")
        self.assertEqual(review.model, "camry")
        self.assertIsNotNone(review.content)
        self.assertTrue(len(review.content) > 0)

        # Проверяем наличие URL
        self.assertTrue(review.url.startswith("https://www.drom.ru"))

        # Выводим информацию о собранных отзывах
        print("\n=== Результаты парсинга ===")
        print(f"Всего собрано отзывов: {len(reviews)}")
        print("\nПример первого отзыва:")
        print("-" * 50)
        print(f"Марка/модель: {review.brand}/{review.model}")
        print(f"Автор: {review.author}")
        print(f"Рейтинг: {review.rating or 'не указан'}")
        print(f"Дата: {review.publish_date or 'не указана'}")
        print(f"Текст: {review.content[:100]}...")
        print("-" * 50)

    def test_error_handling(self):
        """Тест обработки ошибок"""
        # Тестируем неправильный URL
        reviews = self.parser.parse_page("https://www.drom.ru/invalid_url")
        self.assertEqual(len(reviews), 0)
        self.assertGreater(len(self.parser.get_errors()), 0)


if __name__ == "__main__":
    unittest.main()
