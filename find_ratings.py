#!/usr/bin/env python3
"""
Детальный поиск рейтингов на странице отзыва
"""

import asyncio
from playwright.async_api import async_playwright


async def find_ratings():
    """Ищем рейтинги на странице"""

    url = "https://www.drom.ru/reviews/toyota/camry/1428758/"

    async with async_playwright() as p:
        # Используем локальный Chromium
        browser = await p.chromium.launch(
            headless=False,
            executable_path="/home/analityk/Документы/projects/parser_project/chrome-linux/chrome",
        )
        page = await browser.new_page()

        try:
            await page.goto(url, wait_until="networkidle")

            print("🔍 Поиск всех возможных рейтингов...")

            # 1. Ищем общий рейтинг (уже найден)
            rating_element = await page.query_selector('[itemprop="ratingValue"]')
            if rating_element:
                rating_text = await rating_element.text_content()
                print(f"    ✅ Общий рейтинг: {rating_text}")

            # 2. Ищем все элементы содержащие числа от 1 до 10
            print("\n🔢 Поиск элементов с числами 1-10:")
            for i in range(1, 11):
                elements = await page.query_selector_all(f'text="{i}"')
                if elements:
                    print(f"    Найдено элементов с числом {i}: {len(elements)}")

            # 3. Ищем все элементы с текстом "Внешний вид", "Салон", "Двигатель", "Ходовые качества"
            print("\n🏷️ Поиск категорий рейтингов:")
            categories = ["Внешний вид", "Салон", "Двигатель", "Ходовые качества"]
            for category in categories:
                elements = await page.query_selector_all(f'text="{category}"')
                print(f"    {category}: {len(elements)} элементов")

            # 4. Ищем все таблицы на странице
            print("\n📊 Все таблицы на странице:")
            tables = await page.query_selector_all("table")
            print(f"    Найдено таблиц: {len(tables)}")

            for i, table in enumerate(tables):
                print(f"\n    === Таблица #{i+1} ===")
                rows = await table.query_selector_all("tr")
                for j, row in enumerate(rows[:5]):  # Показываем первые 5 строк
                    cells = await row.query_selector_all("td, th")
                    if cells:
                        cell_texts = []
                        for cell in cells:
                            text = await cell.text_content()
                            cell_texts.append(text.strip())
                        print(f"        Строка {j+1}: {' | '.join(cell_texts)}")

            # 5. Ищем все div с классами содержащими "rating", "score", "grade"
            print("\n⭐ Поиск элементов с рейтингами:")
            selectors = [
                '[class*="rating"]',
                '[class*="score"]',
                '[class*="grade"]',
                '[class*="rate"]',
                ".drom-table",
                "[target-status]",
            ]

            for selector in selectors:
                elements = await page.query_selector_all(selector)
                if elements:
                    print(f"    {selector}: {len(elements)} элементов")

            # 6. Проверяем все скрытые блоки
            print("\n👁️ Скрытые блоки:")
            hidden_elements = await page.query_selector_all('[style*="display: none"]')
            print(f"    Скрытых элементов: {len(hidden_elements)}")

            # 7. Поиск по специфическим классам Drom
            print("\n🎯 Специфические классы Drom:")
            drom_selectors = [
                ".rating",
                ".grade",
                ".score",
                ".review-rating",
                ".owner-rating",
                ".car-rating",
            ]

            for selector in drom_selectors:
                elements = await page.query_selector_all(selector)
                if elements:
                    print(f"    {selector}: {len(elements)} элементов")

            # Ждем для визуального осмотра
            print("\n⏰ Ждем 15 секунд для визуальной проверки...")
            await page.wait_for_timeout(15000)

        except Exception as e:
            print(f"Ошибка: {e}")
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(find_ratings())
