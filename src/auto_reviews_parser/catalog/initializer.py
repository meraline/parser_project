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
from ..database.schema import DatabaseManager


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
                    reviews_count = 0
                    if counter_elem:
                        counter_text = counter_elem.get_text(strip=True)
                        # Извлекаем число из текста
                        numbers = re.findall(r"\d+", counter_text.replace(" ", ""))
                        if numbers:
                            reviews_count = int(numbers[0])

                    # URL ссылка
                    link_elem = item.find(
                        "a", {"data-ftid": "component_cars-list-item_hidden-link"}
                    )
                    if not link_elem:
                        continue

                    href = link_elem.get("href", "")
                    if not href:
                        continue

                    # Извлечение URL имени из href
                    url_match = re.search(r"/reviews/([^/]+)/?", href)
                    if not url_match:
                        continue

                    url_name = url_match.group(1)

                    brands.append(
                        {
                            "name": brand_name,
                            "url_name": url_name,
                            "full_url": href,
                            "reviews_count": reviews_count,
                        }
                    )

                except Exception as e:
                    self.logger.warning(f"Ошибка обработки бренда: {e}")
                    continue

            self.logger.info(f"✅ Извлечено {len(brands)} брендов из HTML")
            return brands

        except Exception as e:
            self.logger.error(f"❌ Ошибка парсинга HTML: {e}")
            return []

    def save_brands(self, brands: List[Dict]) -> bool:
        """Сохранение брендов в базу данных"""
        if not brands:
            self.logger.warning("⚠️ Нет брендов для сохранения")
            return False

        saved_count = 0
        for brand in brands:
            brand_id = self.db_manager.add_brand(
                name=brand["name"],
                url_name=brand["url_name"],
                full_url=brand["full_url"],
                reviews_count=brand["reviews_count"],
            )
            if brand_id:
                saved_count += 1

        self.logger.info(f"✅ Сохранено {saved_count}/{len(brands)} брендов")
        return saved_count > 0

    def save_models(self, brand_url_name: str, models: List[Dict]) -> bool:
        """Сохранение моделей для бренда"""
        if not models:
            self.logger.warning(f"⚠️ Нет моделей для бренда {brand_url_name}")
            return False

        # Получаем бренд из базы
        brand = self.db_manager.get_brand_by_url_name(brand_url_name)
        if not brand:
            self.logger.error(f"❌ Бренд {brand_url_name} не найден в базе")
            return False

        brand_id = brand["id"]
        saved_count = 0

        for model in models:
            model_id = self.db_manager.add_model(
                brand_id=brand_id,
                name=model["name"],
                url_name=model["url_name"],
                full_url=model["full_url"],
                reviews_count=model["reviews_count"],
            )
            if model_id:
                saved_count += 1

        self.logger.info(
            f"✅ Сохранено {saved_count}/{len(models)} "
            f"моделей для бренда {brand['name']}"
        )
        return saved_count > 0

    def initialize_from_html_file(self, html_file_path: str) -> bool:
        """Инициализация каталога из HTML файла"""
        try:
            with open(html_file_path, "r", encoding="utf-8") as f:
                html_content = f.read()

            # Парсим бренды
            brands = self.parse_brands_from_html(html_content)
            if not brands:
                self.logger.error("❌ Не удалось извлечь бренды из HTML")
                return False

            # Создаем базу данных если её нет
            if not self.db_manager.create_database():
                self.logger.error("❌ Не удалось создать базу данных")
                return False

            # Сохраняем бренды
            return self.save_brands(brands)

        except FileNotFoundError:
            self.logger.error(f"❌ Файл {html_file_path} не найден")
            return False
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации: {e}")
            return False


if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    initializer = SimpleCatalogInitializer()

    # Инициализация из HTML файла
    html_file = "brands_html.txt"
    if initializer.initialize_from_html_file(html_file):
        print("\n🎉 Каталог успешно инициализирован!")

        # Показываем статистику
        stats = initializer.db_manager.get_statistics()
        print(f"\n📊 Статистика базы данных:")
        print(f"   🏢 Брендов: {stats.get('brands_count', 0)}")
        print(f"   🚗 Моделей: {stats.get('models_count', 0)}")
        print(f"   📝 Отзывов всего: {stats.get('total_reviews', 0)}")
    else:
        print("\n❌ Ошибка инициализации каталога!")
