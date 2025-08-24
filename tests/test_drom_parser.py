"""Тесты для парсера отзывов с Drom.ru."""

from pathlib import Path
import os
import sys
import unittest

# Добавляем путь к корню проекта
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

# Импорты из тестовой инфраструктуры
from unittest.mock import Mock, patch, MagicMock

# Импорт Playwright
from playwright.sync_api import Page, ElementHandle, Browser, sync_playwright

# Импорт тестируемых компонентов
from src.auto_reviews_parser.parsers.drom import DromParser


class TestDromParser(unittest.TestCase):
    """Тесты для DromParser."""

    def setUp(self):
        """Настройка тестового окружения."""
        self.parser = DromParser()
        self.sample_url = "https://www.drom.ru/reviews/toyota/camry/"
        self.sample_review = {
            "url": f"{self.sample_url}123456/",
            "brand": "toyota",
            "model": "camry",
        }

    def test_extract_brand_model(self):
        """Тест извлечения марки и модели из URL."""
        # Тест корректного URL отзыва
        url = self.sample_review["url"]
        brand, model = self.parser._extract_brand_model(url)
        self.assertEqual(brand, self.sample_review["brand"])
        self.assertEqual(model, self.sample_review["model"])

        # Тест некорректного URL
        brand, model = self.parser._extract_brand_model("https://www.drom.ru/")
        self.assertEqual(brand, "")
        self.assertEqual(model, "")

    def test_go_to_page(self):
        """Тест перехода на страницу."""
        mock_page = MagicMock(spec=Page)
        url = "https://www.drom.ru/test"

        self.parser._go_to_page(mock_page, url)

        # Проверяем, что были вызваны нужные методы
        mock_page.goto.assert_called_once_with(url)
        mock_page.wait_for_load_state.assert_called_once_with("networkidle")

    @patch("playwright.sync_api.sync_playwright")
    def test_parse_catalog(self, mock_playwright):
        """Тест парсинга каталога с моками."""
        # Подготовка моков
        mock_browser = Mock(spec=Browser)
        mock_page = Mock(spec=Page)
        mock_block = Mock(spec=ElementHandle)
        mock_link = Mock(spec=ElementHandle)

        # Настройка моков
        mock_pl = mock_playwright.return_value.__enter__.return_value
        mock_pl.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        mock_page.query_selector_all.return_value = [mock_block]
        mock_block.query_selector.return_value = mock_link
        mock_link.get_attribute.return_value = self.sample_review["url"]
        mock_page.content.return_value = "<html>Test content</html>"

        # Запуск парсинга
        brand = self.sample_review["brand"]
        reviews = self.parser.parse_catalog(brand, max_reviews=1)

        # Проверки
        self.assertEqual(len(reviews), 1)
        self.assertEqual(reviews[0].source, "drom.ru")
        self.assertEqual(reviews[0].url, self.sample_review["url"])
        self.assertEqual(reviews[0].brand, self.sample_review["brand"])
        self.assertEqual(reviews[0].model, self.sample_review["model"])
        self.assertEqual(reviews[0].content, "<html>Test content</html>")

    @patch("playwright.sync_api.sync_playwright")
    def test_parse_reviews(self, mock_playwright):
        """Тест основного метода парсинга отзывов."""
        # Подготовка моков
        mock_browser = Mock(spec=Browser)
        mock_page = Mock(spec=Page)
        mock_block = Mock(spec=ElementHandle)
        mock_link = Mock(spec=ElementHandle)

        # Настройка моков
        mock_pl = mock_playwright.return_value.__enter__.return_value
        mock_pl.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        mock_page.query_selector_all.return_value = [mock_block]
        mock_block.query_selector.return_value = mock_link
        mock_link.get_attribute.return_value = self.sample_review["url"]
        mock_page.content.return_value = "<html>Test content</html>"

        # Запуск парсинга
        reviews = self.parser.parse_reviews(
            self.sample_review["brand"],
            self.sample_review["model"]
        )

        # Проверки
        self.assertEqual(len(reviews), 1)
        review = reviews[0]
        self.assertEqual(review.source, "drom.ru")
        self.assertEqual(review.type, "review")
        self.assertEqual(review.url, self.sample_review["url"])
        self.assertEqual(review.brand, self.sample_review["brand"])
        self.assertEqual(review.model, self.sample_review["model"])
        self.assertEqual(review.content, "<html>Test content</html>")

    @unittest.skipUnless(
        os.getenv("RUN_INTEGRATION_TESTS"),
        "Интеграционные тесты отключены"
    )
    def test_integration_parse_reviews(self):
        """Интеграционный тест парсинга отзывов.
        
        Для запуска установите переменную окружения RUN_INTEGRATION_TESTS=1
        """
        reviews = self.parser.parse_reviews(
            self.sample_review["brand"],
            self.sample_review["model"]
        )
        
        self.assertTrue(len(reviews) > 0)
        for review in reviews:
            self.assertEqual(review.source, "drom.ru")
            self.assertEqual(review.type, "review")
            self.assertTrue(review.url.startswith(
                "https://www.drom.ru/reviews/"
            ))
            self.assertEqual(review.brand.lower(), self.sample_review["brand"])
            self.assertEqual(review.model.lower(), self.sample_review["model"])
            self.assertTrue(len(review.content) > 0)


if __name__ == "__main__":
    unittest.main()
