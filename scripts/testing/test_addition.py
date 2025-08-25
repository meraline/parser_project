#!/usr/bin/env python3
"""Проверка парсинга конкретного дополнения к отзыву."""

import sys
from pathlib import Path


def test_addition_parsing():
    """Тестируем парсинг дополнения."""
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))

    from src.auto_reviews_parser.parsers.drom import DromParser
    from playwright.sync_api import sync_playwright
    import os

    print("=== ТЕСТ ПАРСИНГА ДОПОЛНЕНИЯ ===")
    print("URL: https://www.drom.ru/reviews/toyota/camry/1428758/187642/")
    print()

    parser = DromParser()

    with sync_playwright() as p:
        # Используем локальный браузер если он доступен
        if os.path.exists(parser.chrome_path):
            browser = p.chromium.launch(
                executable_path=parser.chrome_path, headless=True
            )
        else:
            browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            url = "https://www.drom.ru/reviews/toyota/camry/1428758/187642/"

            # Переходим на страницу
            parser._go_to_page(page, url)
            content = page.content()

            # Извлекаем структурированные данные
            structured_data = parser._extract_review_data(content, url)

            print("=== ИЗВЛЕЧЕННЫЕ ДАННЫЕ ===")
            for key, value in structured_data.items():
                if key == "text" and value:
                    print(f"{key}: {value[:200]}...")
                else:
                    print(f"{key}: {value}")
            print()

            # Проверяем, почему не извлекается текст
            print("=== HTML АНАЛИЗ ===")
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(content, "html.parser")

            # Ищем текст отзыва разными способами
            review_body = soup.find("div", {"itemprop": "reviewBody"})
            print(f"reviewBody найден: {review_body is not None}")

            if review_body:
                text = review_body.get_text(strip=True)
                print(f"Текст из reviewBody: {text[:200]}...")

            # Попробуем другие селекторы
            editable_area = soup.find("div", class_="b-editable-area")
            print(f"b-editable-area найден: {editable_area is not None}")

            if editable_area:
                text = editable_area.get_text(strip=True)
                print(f"Текст из editable-area: {text[:200]}...")

            # Ищем любой текст в странице
            title = soup.find("title")
            if title:
                print(f"Заголовок страницы: {title.text}")

        finally:
            browser.close()


if __name__ == "__main__":
    test_addition_parsing()
