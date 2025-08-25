"""Анализ HTML структуры страницы каталога Drom.ru"""

import logging
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Настраиваем логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/page_analysis.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def analyze_page_structure(url: str) -> None:
    """Анализирует HTML структуру страницы.

    Args:
        url: URL страницы для анализа
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto(url)
            html = page.content()

            # Сохраняем HTML для анализа
            with open("debug_page.html", "w", encoding="utf-8") as f:
                f.write(html)

            # Парсим HTML
            soup = BeautifulSoup(html, "html.parser")

            # Ищем все возможные селекторы для списка моделей
            logger.info("Анализ структуры страницы...")

            # Проверяем разные возможные контейнеры
            containers = [
                (".css-", "CSS классы начинающиеся с css-"),
                (".b-", "Классы начинающиеся с b-"),
                ("div[class*='list']", "Div элементы содержащие 'list'"),
                ("a", "Все ссылки на странице"),
            ]

            for selector, desc in containers:
                elements = soup.select(selector)
                logger.info(f"{desc}: найдено {len(elements)} элементов")

                # Выводим первые 3 элемента для анализа
                for i, el in enumerate(elements[:3]):
                    logger.info(f"Элемент {i + 1}:")
                    logger.info(f"Классы: {el.get('class', [])}")
                    logger.info(f"Текст: {el.text.strip()[:100]}")
                    logger.info("---")

        except Exception as e:
            logger.error(f"Ошибка при анализе страницы: {e}")
        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    brand_url = "https://www.drom.ru/reviews/toyota/"
    analyze_page_structure(brand_url)
