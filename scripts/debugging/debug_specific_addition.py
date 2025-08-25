#!/usr/bin/env python3
"""
Отладка извлечения характеристик для конкретного дополнения
"""
import sys
import os

# Добавляем путь к исходному коду
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser
from playwright.sync_api import sync_playwright


def debug_specific_addition():
    print("🔧 Отладка извлечения характеристик для конкретного дополнения...")

    url = "https://www.drom.ru/reviews/toyota/camry/1428758/187642/"
    parser = DromParser()

    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path=parser.chrome_path, headless=True)

        try:
            page = browser.new_page()
            page.goto(url, wait_until="networkidle")

            # Получаем HTML
            content = page.content()

            # Используем метод парсера для извлечения данных
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(content, "html.parser")

            # Ищем таблицы характеристик
            print("🔍 Поиск таблиц характеристик...")

            # Проверяем разные селекторы
            tables = soup.find_all("table", class_="drom-table")
            print(f"Найдено таблиц drom-table: {len(tables)}")

            for i, table in enumerate(tables):
                print(f"\n📋 Таблица {i+1}:")
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        print(f"  {key}: {value}")

            # Проверяем контент страницы
            print(f"\n📄 Длина HTML: {len(content)}")

            # Ищем любые упоминания характеристик
            if "Год выпуска" in content:
                print("✅ Найдено 'Год выпуска' в HTML")
            else:
                print("❌ 'Год выпуска' не найдено в HTML")

            # Используем метод парсера
            structured_data = parser._extract_review_data(soup, url)
            print(f"\n🎯 Результат _extract_review_data:")
            print(f"  car_specs: {structured_data.get('car_specs', {})}")

        finally:
            browser.close()


if __name__ == "__main__":
    debug_specific_addition()
