#!/usr/bin/env python3
"""Отладка парсинга характеристик из основного отзыва."""

import sys
from pathlib import Path


def debug_characteristics():
    """Отлаживаем извлечение характеристик."""
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))

    from src.auto_reviews_parser.parsers.drom import DromParser
    from playwright.sync_api import sync_playwright
    import os

    parser = DromParser()

    # Основной отзыв с характеристиками
    url = "https://www.drom.ru/reviews/toyota/camry/1428758/"

    print("=== ОТЛАДКА ХАРАКТЕРИСТИК ===")
    print(f"URL: {url}")
    print()

    with sync_playwright() as p:
        if os.path.exists(parser.chrome_path):
            browser = p.chromium.launch(
                executable_path=parser.chrome_path, headless=True
            )
        else:
            browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            parser._go_to_page(page, url)
            content = page.content()

            # Извлекаем структурированные данные
            structured_data = parser._extract_review_data(content, url)

            print("=== ИЗВЛЕЧЕННЫЕ ДАННЫЕ ===")
            print(f"Характеристики: {structured_data.get('characteristics', {})}")
            print(f"Спецификации автомобиля: {structured_data.get('car_specs', {})}")
            print()

            # Детальный анализ HTML
            print("=== HTML АНАЛИЗ ===")
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(content, "html.parser")

            # Ищем таблицы с характеристиками
            tables = soup.find_all("table", class_="drom-table")
            print(f"Найдено таблиц drom-table: {len(tables)}")

            for i, table in enumerate(tables):
                print(f"\nТаблица {i+1}:")
                rows = table.find_all("tr")
                print(f"  Строк в таблице: {len(rows)}")

                for j, row in enumerate(rows[:5]):  # Показываем первые 5 строк
                    cells = row.find_all("td")
                    if len(cells) == 2:
                        key = cells[0].get_text(strip=True).rstrip(":")
                        value = cells[1].get_text(strip=True)
                        print(f"    {key}: {value}")

            # Дополнительно ищем другие возможные селекторы
            print("\n=== АЛЬТЕРНАТИВНЫЕ СЕЛЕКТОРЫ ===")

            # Ищем таблицы без класса
            all_tables = soup.find_all("table")
            print(f"Всего таблиц: {len(all_tables)}")

            # Ищем span с годом
            year_spans = soup.find_all(
                "td", string=lambda text: text and "Год выпуска" in text
            )
            print(f"Найдено 'Год выпуска': {len(year_spans)}")
            if year_spans:
                for span in year_spans[:2]:
                    next_td = span.find_next_sibling("td")
                    if next_td:
                        print(f"  Год: {next_td.get_text(strip=True)}")

        finally:
            browser.close()


if __name__ == "__main__":
    debug_characteristics()
