#!/usr/bin/env python3
"""
🗂️ ПРОСТОЙ ИНИЦИАЛИЗАТОР КАТАЛОГА DROM.RU
=====================================

Упрощенный скрипт для инициализации базы данных брендами и моделями.

Автор: AI Assistant
Дата: 2024
"""

import re
import logging
from typing import List, Dict
from bs4 import BeautifulSoup
from database_schema import DatabaseManager


class SimpleCatalogInitializer:
    """Простой инициализатор каталога"""

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.logger = logging.getLogger(__name__)

    def parse_brands_from_html(self, html_content: str) -> List[Dict]:
        """Извлечение брендов из HTML блока каталога"""
        brands = []

        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # Поиск всех элементов брендов
            brand_items = soup.find_all("div", class_="frg44i0")

            for item in brand_items:
                try:
                    # Название бренда
                    name_elem = item.find(
                        "span", {"data-ftid": "component_cars-list-item_name"}
                    )
                    if not name_elem:
                        continue

                    brand_name = name_elem.get_text(strip=True)

                    # Количество отзывов
                    counter_elem = item.find(
                        "span", {"data-ftid": "component_cars-list-item_counter"}
                    )
                    review_count = 0
                    if counter_elem:
                        counter_text = counter_elem.get_text(strip=True)
                        # Убираем пробелы из чисел
                        counter_clean = re.sub(r"\s+", "", counter_text)
                        try:
                            review_count = int(counter_clean)
                        except ValueError:
                            review_count = 0

                    # Ссылка на бренд
                    link_elem = item.find(
                        "a", {"data-ftid": "component_cars-list-item_hidden-link"}
                    )
                    if not link_elem:
                        continue

                    brand_url = link_elem.get("href")
                    if not brand_url:
                        continue

                    # Извлечение slug из URL
                    slug_match = re.search(r"/reviews/([^/]+)/?$", brand_url)
                    if not slug_match:
                        continue

                    brand_slug = slug_match.group(1)

                    brands.append(
                        {
                            "slug": brand_slug,
                            "name": brand_name,
                            "review_count": review_count,
                            "url": brand_url,
                            "logo_url": "",
                        }
                    )

                    self.logger.debug(f"Бренд: {brand_name} ({brand_slug})")

                except Exception as e:
                    self.logger.error(f"Ошибка обработки бренда: {e}")
                    continue

            self.logger.info(f"Извлечено {len(brands)} брендов из HTML")
            return brands

        except Exception as e:
            self.logger.error(f"Ошибка парсинга HTML каталога: {e}")
            return []

    def save_brands(self, brands: List[Dict]) -> int:
        """Сохранение брендов в базу данных"""
        saved_count = 0

        for brand in brands:
            try:
                result = self.db_manager.add_brand(
                    slug=brand["slug"],
                    name=brand["name"],
                    review_count=brand["review_count"],
                    url=brand["url"],
                    logo_url=brand.get("logo_url", ""),
                )

                if result:
                    saved_count += 1

            except Exception as e:
                self.logger.error(f"Ошибка сохранения бренда {brand['slug']}: {e}")

        return saved_count

    def save_models(self, brand_slug: str, models: List[Dict]) -> int:
        """Сохранение моделей в базу данных"""
        saved_count = 0

        for model in models:
            try:
                result = self.db_manager.add_model(
                    brand_slug=brand_slug,
                    model_slug=model["slug"],
                    model_name=model["name"],
                    review_count=model["review_count"],
                    url=model["url"],
                )

                if result:
                    saved_count += 1

            except Exception as e:
                self.logger.error(f"Ошибка сохранения модели {model['slug']}: {e}")

        return saved_count


# Для обратной совместимости
DromCatalogInitializer = SimpleCatalogInitializer


def main():
    """Демонстрация инициализации каталога"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    print("🗂️ ПРОСТАЯ ИНИЦИАЛИЗАЦИЯ КАТАЛОГА DROM.RU")
    print("=" * 50)

    initializer = SimpleCatalogInitializer()

    # Создание базы данных
    print("📋 Создание базы данных...")
    initializer.db_manager.create_database()

    # Тестовые данные
    test_html = """
    <div class="frg44i0">
        <div class="frg44i1 frg44i2">
            <span data-ftid="component_cars-list-item_name">Toyota</span>
            <span data-ftid="component_cars-list-item_counter">282196</span>
        </div>
        <a data-ftid="component_cars-list-item_hidden-link" 
           href="https://www.drom.ru/reviews/toyota/">Toyota</a>
    </div>
    """

    brands = initializer.parse_brands_from_html(test_html)
    saved = initializer.save_brands(brands)

    print(f"✅ Сохранено {saved} брендов")


if __name__ == "__main__":
    main()
