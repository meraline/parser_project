#!/usr/bin/env python3
"""
Отладка скрытых блоков с рейтингами
"""

import asyncio
from playwright.async_api import async_playwright


async def debug_hidden_blocks():
    """Отладка скрытых блоков с рейтингами"""

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

            print("🔧 Отладка скрытых блоков...")

            # Проверяем наличие блоков до клика
            print("\n📋 Блоки ДО клика:")
            blocks_before = await page.query_selector_all(".drom-table")
            print(f"    Найдено блоков drom-table: {len(blocks_before)}")

            # Ищем блоки с рейтингами
            hidden_blocks = await page.query_selector_all('[target-status="close"]')
            print(f"    Найдено скрытых блоков: {len(hidden_blocks)}")

            # Ищем кнопки для разворачивания
            buttons = await page.query_selector_all(
                'button[data-toggle="preview-dropdown"]'
            )
            print(f"    Найдено кнопок для разворачивания: {len(buttons)}")

            # Кликаем на кнопки
            for i, button in enumerate(buttons):
                print(f"    Кликаем на кнопку #{i+1}")
                await button.click()
                await page.wait_for_timeout(1000)  # Ждем 1 секунду

            # Проверяем блоки после клика
            print("\n📋 Блоки ПОСЛЕ клика:")
            blocks_after = await page.query_selector_all(".drom-table")
            print(f"    Найдено блоков drom-table: {len(blocks_after)}")

            # Ищем блоки с рейтингами после клика
            rating_blocks = await page.query_selector_all('[target-status="open"]')
            print(f"    Найдено открытых блоков: {len(rating_blocks)}")

            # Показываем содержимое всех таблиц
            print("\n📊 Содержимое всех таблиц:")
            for i, table in enumerate(blocks_after):
                print(f"\n=== Таблица #{i+1} ===")
                rows = await table.query_selector_all("tr")
                for j, row in enumerate(rows):
                    cells = await row.query_selector_all("td")
                    if len(cells) >= 2:
                        key_elem = cells[0]
                        value_elem = cells[1]
                        key = await key_elem.text_content()
                        value = await value_elem.text_content()
                        print(f"    {key}: {value}")

            # Ждем 10 секунд для визуальной проверки
            print("\n⏰ Ждем 10 секунд для визуальной проверки...")
            await page.wait_for_timeout(10000)

        except Exception as e:
            print(f"Ошибка: {e}")
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(debug_hidden_blocks())
