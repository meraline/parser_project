#!/usr/bin/env python3
"""
Тестируем извлечение автора и города
"""
import sys
import os
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Добавляем путь к исходному коду
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser


def test_author_and_city():
    print("🔧 Тестируем извлечение автора и города...")

    parser = DromParser()

    # Тестируем на конкретном URL
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

            print(f"🔍 Ищем автора и город на странице: {test_url}")

            # 1. Проверяем title
            title = soup.find("title")
            if title:
                print(f"📄 Title: {title.text.strip()}")

            # 2. Ищем автора
            print("\n👤 Поиск автора:")

            # Вариант 1: itemprop="name"
            author_elem = soup.find("span", {"itemprop": "name"})
            if author_elem:
                print(f"  📍 [itemprop=name]: {author_elem.text.strip()}")

            # Вариант 2: в мета-данных
            author_meta = soup.find("meta", {"name": "author"})
            if author_meta:
                print(f"  📍 [meta author]: {author_meta.get('content', '')}")

            # Вариант 3: в заголовках h1, h2
            for tag in ["h1", "h2", "h3"]:
                headers = soup.find_all(tag)
                for header in headers:
                    text = header.text.strip()
                    if "автор" in text.lower() or len(text.split()) <= 3:
                        print(f"  📍 [{tag}]: {text}")

            # Вариант 4: в блоках с классами содержащими user, author, name
            for class_pattern in ["user", "author", "name", "reviewer"]:
                elements = soup.find_all(
                    class_=lambda x: x and class_pattern in x.lower()
                )
                for elem in elements[:3]:  # показываем первые 3
                    text = elem.text.strip()
                    if text and len(text) < 100:
                        print(f"  📍 [.{class_pattern}*]: {text[:50]}")

            # 3. Ищем город
            print("\n🏙️ Поиск города:")

            # Вариант 1: в мета-данных
            location_meta = soup.find("meta", {"name": "geo.placename"})
            if location_meta:
                print(f"  📍 [meta geo]: {location_meta.get('content', '')}")

            # Вариант 2: в классах с city, location, geo
            for class_pattern in ["city", "location", "geo", "place"]:
                elements = soup.find_all(
                    class_=lambda x: x and class_pattern in x.lower()
                )
                for elem in elements[:3]:  # показываем первые 3
                    text = elem.text.strip()
                    if text and len(text) < 100:
                        print(f"  📍 [.{class_pattern}*]: {text[:50]}")

            # Вариант 3: поиск по тексту "город", "Кемерово"
            text_elements = soup.find_all(
                text=lambda text: text
                and (
                    "город" in text.lower()
                    or "кемерово" in text.lower()
                    or "москва" in text.lower()
                )
            )
            for text_elem in text_elements[:3]:
                parent = text_elem.parent
                if parent:
                    print(f"  📍 [text search]: {parent.text.strip()[:100]}")

            # 4. Проверим блоки информации об отзыве
            print("\n📊 Блоки информации:")
            info_blocks = soup.find_all(
                class_=lambda x: x
                and (
                    "info" in x.lower() or "details" in x.lower() or "meta" in x.lower()
                )
            )
            for block in info_blocks[:5]:
                text = block.text.strip()
                if text and len(text) < 200:
                    print(f"  📦 {text[:100]}")

        except Exception as e:
            print(f"❌ Ошибка: {e}")
            browser.close()


if __name__ == "__main__":
    test_author_and_city()
