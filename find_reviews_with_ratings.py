#!/usr/bin/env python3
"""
Поиск отзывов с категорийными рейтингами
"""

import asyncio
from playwright.async_api import async_playwright


async def find_reviews_with_ratings():
    """Ищем отзывы с категорийными рейтингами"""

    # Попробуем несколько разных отзывов Toyota Camry
    urls = [
        "https://www.drom.ru/reviews/toyota/camry/1428758/",  # Текущий
        "https://www.drom.ru/reviews/toyota/camry/",  # Список отзывов
    ]

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            executable_path="/home/analityk/Документы/projects/parser_project/chrome-linux/chrome",
        )

        for url in urls:
            print(f"\n🔍 Проверяем: {url}")

            page = await browser.new_page()
            try:
                await page.goto(url, wait_until="networkidle")

                if "/reviews/" in url and url.count("/") >= 6:
                    # Это конкретный отзыв
                    await check_review_page(page, url)
                else:
                    # Это список отзывов
                    await find_reviews_from_list(page)

            except Exception as e:
                print(f"    ❌ Ошибка загрузки {url}: {e}")
            finally:
                await page.close()

        await browser.close()


async def check_review_page(page, url):
    """Проверяем отдельную страницу отзыва"""

    print(f"    📄 Анализируем отзыв...")

    # Ищем общий рейтинг
    rating_element = await page.query_selector('[itemprop="ratingValue"]')
    if rating_element:
        rating_text = await rating_element.text_content()
        print(f"    ⭐ Общий рейтинг: {rating_text}")

    # Ищем категории рейтингов
    categories = [
        "Внешний вид",
        "Салон",
        "Двигатель",
        "Ходовые качества",
        "Комфорт",
        "Безопасность",
    ]
    found_categories = []

    for category in categories:
        elements = await page.query_selector_all(f'text="{category}"')
        if elements:
            found_categories.append(category)

    if found_categories:
        print(f"    🏷️ Найденные категории: {', '.join(found_categories)}")

        # Кликаем на кнопки разворачивания
        buttons = await page.query_selector_all(
            'button[data-toggle="preview-dropdown"]'
        )
        print(f"    🔘 Кнопок разворачивания: {len(buttons)}")

        for button in buttons:
            await button.click()
            await page.wait_for_timeout(500)

        # Проверяем появились ли рейтинги
        await page.wait_for_timeout(1000)
        tables = await page.query_selector_all(".drom-table")
        print(f"    📊 Таблиц после клика: {len(tables)}")

        for i, table in enumerate(tables):
            print(f"\n    === Таблица #{i+1} ===")
            rows = await table.query_selector_all("tr")
            for row in rows[:10]:  # Первые 10 строк
                cells = await row.query_selector_all("td")
                if len(cells) >= 2:
                    key_elem = cells[0]
                    value_elem = cells[1]
                    key = await key_elem.text_content()
                    value = await value_elem.text_content()

                    # Проверяем, похоже ли на рейтинг
                    if (
                        key in categories
                        and value
                        and value.strip().replace(".", "").isdigit()
                    ):
                        print(f"        🎯 РЕЙТИНГ: {key}: {value}")
                    else:
                        print(f"        📝 {key}: {value}")
    else:
        print(f"    ❌ Категории рейтингов не найдены")


async def find_reviews_from_list(page):
    """Ищем отзывы в списке"""

    print(f"    📋 Ищем отзывы в списке...")

    # Ищем ссылки на отзывы
    review_links = await page.query_selector_all('a[href*="/reviews/"]')
    print(f"    🔗 Найдено ссылок на отзывы: {len(review_links)}")

    # Берем первые 3 отзыва для проверки
    for i, link in enumerate(review_links[:3]):
        href = await link.get_attribute("href")
        if href and href.count("/") >= 6:  # Полная ссылка на отзыв
            full_url = f"https://www.drom.ru{href}" if href.startswith("/") else href
            print(f"\n    🔍 Проверяем отзыв #{i+1}: {full_url}")

            # Открываем отзыв в новой вкладке
            new_page = await page.context.new_page()
            try:
                await new_page.goto(full_url, wait_until="networkidle")
                await check_review_page(new_page, full_url)
                await new_page.wait_for_timeout(2000)
            except Exception as e:
                print(f"        ❌ Ошибка: {e}")
            finally:
                await new_page.close()


if __name__ == "__main__":
    asyncio.run(find_reviews_with_ratings())
