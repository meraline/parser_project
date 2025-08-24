#!/usr/bin/env python3
"""Тест извлечения данных для дополнения."""

import sys
from pathlib import Path


def test_data_extraction():
    """Тестируем извлечение данных."""
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))

    from src.auto_reviews_parser.parsers.drom import DromParser
    from playwright.sync_api import sync_playwright
    import os

    parser = DromParser()

    with sync_playwright() as p:
        if os.path.exists(parser.chrome_path):
            browser = p.chromium.launch(
                executable_path=parser.chrome_path, headless=True
            )
        else:
            browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            url = "https://www.drom.ru/reviews/toyota/camry/1428758/187642/"
            parser._go_to_page(page, url)
            content = page.content()

            structured_data = parser._extract_review_data(content, url)

            print("=== ДАННЫЕ ИЗ _extract_review_data ===")
            for key, value in structured_data.items():
                if key == "text":
                    print(
                        f"{key}: '{value[:100]}...'"
                        if value
                        else f"{key}: пустая строка"
                    )
                else:
                    print(f"{key}: {value}")

        finally:
            browser.close()


if __name__ == "__main__":
    test_data_extraction()
