#!/usr/bin/env python3
"""Простой тест парсера Drom."""

import sys
import logging
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_parser():
    """Тестирование базового функционала парсера."""
    url = "https://www.drom.ru/reviews/toyota/"

    try:
        with sync_playwright() as p:
            # Запускаем браузер
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
                ),
            )

            # Создаем страницу
            page = context.new_page()
            logger.info("Загружаем страницу %s", url)

            # Переходим на страницу
            page.goto(url)
            page.wait_for_load_state("networkidle")

            # Ждем появления элементов
            review_selector = "div[data-ftid='component_review-item']"
            page.wait_for_selector(review_selector)

            # Получаем список отзывов
            reviews = page.query_selector_all(review_selector)
            logger.info("Найдено отзывов: %d", len(reviews))

            # Анализируем первый отзыв
            if reviews:
                first_review = reviews[0]
                html = first_review.inner_html()
                soup = BeautifulSoup(html, "html.parser")

                # Извлекаем данные
                title = soup.select_one("div[data-ftid='component_review-item-title']")
                author = soup.select_one(
                    "div[data-ftid='component_review-item-author']"
                )

                if title and author:
                    logger.info("Заголовок: %s", title.text.strip())
                    logger.info("Автор: %s", author.text.strip())

            # Закрываем браузер
            browser.close()
            logger.info("Тест завершен успешно")

    except Exception as e:
        logger.error("Ошибка при тестировании: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    test_parser()
