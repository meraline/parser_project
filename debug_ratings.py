#!/usr/bin/env python3
"""
Отладка извлечения оценок - смотрим все таблицы на странице
"""
import sys
import os
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Добавляем путь к исходному коду
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser


def debug_ratings_extraction():
    print("🔧 Отладка извлечения оценок...")

    parser = DromParser()

    # Тестируем на конкретном URL с оценками
    test_url = "https://www.drom.ru/reviews/toyota/camry/1428758/"

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
            parser._go_to_page(page, test_url)
            html_content = page.content()
            browser.close()

            # Парсим HTML
            soup = BeautifulSoup(html_content, "html.parser")

            print(f"🔍 Анализируем страницу: {test_url}")

            # 1. Ищем все таблицы
            print(f"\n📊 Все таблицы на странице:")
            tables = soup.find_all("table")
            for i, table in enumerate(tables):
                print(f"\n  Таблица #{i+1}:")
                print(f"    Классы: {table.get('class', 'нет')}")

                rows = table.find_all("tr")
                for j, row in enumerate(rows[:5]):  # первые 5 строк
                    cells = row.find_all(["td", "th"])
                    if cells:
                        cell_texts = [cell.text.strip() for cell in cells]
                        print(f"    Строка {j+1}: {cell_texts}")

            # 2. Ищем конкретно drom-table
            print(f"\n🎯 Таблицы drom-table:")
            drom_tables = soup.find_all("table", class_="drom-table")
            for i, table in enumerate(drom_tables):
                print(f"\n  drom-table #{i+1}:")
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) == 2:
                        key = cells[0].text.strip().rstrip(":")
                        value = cells[1].text.strip()
                        print(f"    {key}: {value}")

            # 3. Ищем по тексту "Внешний вид", "Салон" и т.д.
            print(f"\n🔍 Поиск оценочных терминов:")
            rating_terms = ["Внешний вид", "Салон", "Двигатель", "Ходовые качества"]

            for term in rating_terms:
                elements = soup.find_all(text=lambda text: text and term in text)
                for elem in elements[:2]:  # первые 2 вхождения
                    parent = elem.parent
                    if parent:
                        # Ищем соседние элементы с числами
                        siblings = parent.find_next_siblings()
                        for sibling in siblings[:3]:
                            text = sibling.text.strip()
                            if text and text.isdigit():
                                print(f"    {term}: найдена оценка {text}")
                                break

                        # Проверяем следующий элемент после родителя
                        next_elem = parent.find_next()
                        if next_elem:
                            next_text = next_elem.text.strip()
                            if next_text and (next_text.isdigit() or "/" in next_text):
                                print(f"    {term}: в следующем элементе {next_text}")

            # 4. Ищем элементы с числами от 1 до 10 (возможные оценки)
            print(f"\n🔢 Поиск числовых оценок:")
            for i in range(1, 11):
                number_elements = soup.find_all(text=str(i))
                rating_context_count = 0
                for elem in number_elements:
                    parent_text = ""
                    if elem.parent:
                        parent_text = elem.parent.text.lower()

                    # Ищем контекст оценок
                    if any(term.lower() in parent_text for term in rating_terms):
                        rating_context_count += 1
                        if rating_context_count <= 2:  # показываем первые 2
                            print(f"    Оценка {i}: контекст '{parent_text[:100]}'")

        except Exception as e:
            print(f"❌ Ошибка: {e}")
            browser.close()


if __name__ == "__main__":
    debug_ratings_extraction()
