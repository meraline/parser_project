#!/usr/bin/env python3
"""
🗂️ ИНИЦИАЛИЗАЦИЯ КАТАЛОГА БРЕНДОВ И МОДЕЛЕЙ
========================================

Заполняет базу данных каталогом брендов из HTML-структуры drom.ru
и извлекает модели для каждого бренда.

Функции:
- Парсинг HTML-блока с брендами
- Добавление брендов в базу данных
- Извлечение моделей для каждого бренда
- Обновление счетчиков отзывов
"""

import re
import requests
import time
import logging
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from database_schema import DatabaseManager


class DromCatalogInitializer:
    """Инициализатор каталога брендов и моделей Drom.ru"""

    def __init__(self, delay: float = 1.0):
        self.base_url = "https://www.drom.ru"
        self.delay = delay
        self.session = requests.Session()
        self.db_manager = DatabaseManager()

        # Настройка логирования
        self.logger = logging.getLogger(__name__)

        # Headers для имитации браузера
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;" "q=0.9,*/*;q=0.8"
                ),
                "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

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
                        # Убираем пробелы из чисел (например "12 554" -> "12554")
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
                    # URL формат: https://www.drom.ru/reviews/toyota/
                    slug_match = re.search(r"/reviews/([^/]+)/?$", brand_url)
                    if not slug_match:
                        continue

                    brand_slug = slug_match.group(1)

                    # Логотип бренда
                    logo_elem = item.find("img")
                    logo_url = None
                    if logo_elem:
                        logo_url = logo_elem.get("src")

                    brands.append(
                        {
                            "slug": brand_slug,
                            "name": brand_name,
                            "review_count": review_count,
                            "url": brand_url,
                            "logo_url": logo_url,
                        }
                    )

                    self.logger.debug(
                        f"✅ Бренд: {brand_name} ({brand_slug}) - {review_count} отзывов"
                    )

                except Exception as e:
                    self.logger.error(f"❌ Ошибка обработки элемента бренда: {e}")
                    continue

            self.logger.info(f"📋 Извлечено {len(brands)} брендов из HTML")
            return brands

        except Exception as e:
            self.logger.error(f"❌ Ошибка парсинга HTML каталога: {e}")
            return []

    def parse_brand_models(self, brand_slug: str) -> List[Dict]:
        """Получение моделей бренда через парсинг страницы"""
        pass  # Заглушка - будет реализовано в следующей версии


def main():
    """Демонстрация инициализации каталога"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # HTML блок брендов (предоставлен пользователем)
    brands_html = """<полный HTML блок брендов>"""

    print("🗂️ ИНИЦИАЛИЗАЦИЯ КАТАЛОГА DROM.RU")
    print("=" * 50)

    initializer = DromCatalogInitializer()

    # Тестирование извлечения брендов
    brands = initializer.parse_brands_from_html(brands_html)

    print(f"✅ Извлечено {len(brands)} брендов")

    # Показ первых 5 брендов
    for i, brand in enumerate(brands[:5], 1):
        print(
            f"{i}. {brand['name']} ({brand['slug']}) - {brand['review_count']} отзывов"
        )


if __name__ == "__main__":
    main()

import logging
import time
import re
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

from database_schema import DatabaseManager


@dataclass
class BrandData:
    """Данные бренда из HTML"""

    name: str
    slug: str
    review_count: int
    url: str


@dataclass
class ModelData:
    """Данные модели"""

    name: str
    slug: str
    url: str


class CatalogInitializer:
    """Инициализатор каталога брендов и моделей"""

    def __init__(
        self,
        browser_path: str = "/home/analityk/Документы/projects/parser_project/chrome-linux/chrome",
        db_path: str = "auto_reviews.db",
    ):
        self.browser_path = browser_path
        self.db_manager = DatabaseManager(db_path)
        self.base_url = "https://www.drom.ru/reviews/"

        # Настройка логирования
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def extract_brands_from_html(self, html_content: str) -> List[BrandData]:
        """Извлечение брендов из HTML-блока"""
        brands = []
        soup = BeautifulSoup(html_content, "html.parser")

        # Ищем все элементы брендов
        brand_elements = soup.find_all("div", class_="frg44i0")

        for element in brand_elements:
            try:
                # Название бренда
                name_elem = element.find(
                    "span", {"data-ftid": "component_cars-list-item_name"}
                )
                if not name_elem:
                    continue

                brand_name = name_elem.get_text(strip=True)

                # Количество отзывов
                counter_elem = element.find(
                    "span", {"data-ftid": "component_cars-list-item_counter"}
                )
                review_count = 0
                if counter_elem:
                    count_text = counter_elem.get_text(strip=True)
                    count_match = re.search(r"(\d+)", count_text)
                    if count_match:
                        review_count = int(count_match.group(1))

                # URL бренда
                link_elem = element.find(
                    "a", {"data-ftid": "component_cars-list-item_hidden-link"}
                )
                if not link_elem:
                    continue

                brand_url = link_elem.get("href", "")

                # Извлекаем slug из URL
                url_match = re.search(r"/reviews/([^/]+)/", brand_url)
                if not url_match:
                    continue

                brand_slug = url_match.group(1)

                brands.append(
                    BrandData(
                        name=brand_name,
                        slug=brand_slug,
                        review_count=review_count,
                        url=brand_url,
                    )
                )

            except Exception as e:
                self.logger.warning(f"Ошибка обработки элемента бренда: {e}")
                continue

        return brands

    def load_brands_from_file(
        self, html_file: str = "brands_html.txt"
    ) -> List[BrandData]:
        """Загрузка брендов из HTML-файла"""
        html_path = Path(html_file)

        if not html_path.exists():
            self.logger.error(f"HTML-файл не найден: {html_file}")
            return []

        try:
            with open(html_path, "r", encoding="utf-8") as f:
                html_content = f.read()

            brands = self.extract_brands_from_html(html_content)
            self.logger.info(f"Извлечено {len(brands)} брендов из HTML")

            return brands

        except Exception as e:
            self.logger.error(f"Ошибка чтения HTML-файла: {e}")
            return []

    def extract_models_for_brand(
        self, brand_slug: str, max_pages: int = 5
    ) -> List[ModelData]:
        """Извлечение моделей для бренда"""
        models = []

    def save_brands(self, brands: List[Dict]) -> int:
        """Сохранение брендов в базу данных"""
        saved_count = 0
        
        for brand in brands:
            try:
                result = self.db_manager.add_brand(
                    slug=brand['slug'],
                    name=brand['name'],
                    review_count=brand['review_count'],
                    url=brand['url'],
                    logo_url=brand.get('logo_url')
                )
                
                if result:
                    saved_count += 1
                    
            except Exception as e:
                self.logger.error(f"❌ Ошибка сохранения бренда {brand['slug']}: {e}")
        
        return saved_count

    def save_models(self, brand_slug: str, models: List[Dict]) -> int:
        """Сохранение моделей в базу данных"""
        saved_count = 0
        
        for model in models:
            try:
                result = self.db_manager.add_model(
                    brand_slug=brand_slug,
                    model_slug=model['slug'],
                    model_name=model['name'],
                    review_count=model['review_count'],
                    url=model['url']
                )
                
                if result:
                    saved_count += 1
                    
            except Exception as e:
                self.logger.error(f"❌ Ошибка сохранения модели {model['slug']}: {e}")
        
        return saved_count

                page = browser.new_page()
                brand_url = f"{self.base_url}{brand_slug}/"

                self.logger.info(f"Извлекаем модели для {brand_slug}: {brand_url}")

                page.goto(brand_url, wait_until="networkidle")
                time.sleep(2)

                # Ищем блок с моделями
                content = page.content()
                soup = BeautifulSoup(content, "html.parser")

                # Различные варианты селекторов для моделей
                model_selectors = [
                    'a[href*="/reviews/' + brand_slug + '/"]',
                    ".models-list a",
                    ".model-item a",
                    'a[href*="/' + brand_slug + '/"]',
                ]

                for selector in model_selectors:
                    model_links = soup.select(selector)

                    if model_links:
                        self.logger.info(
                            f"Найдено {len(model_links)} ссылок через селектор: {selector}"
                        )

                        for link in model_links:
                            href = link.get("href", "")

                            # Проверяем, что это ссылка на модель
                            pattern = rf"/reviews/{re.escape(brand_slug)}/([^/]+)/?$"
                            match = re.search(pattern, href)

                            if match:
                                model_slug = match.group(1)
                                model_name = link.get_text(strip=True)

                                if model_name and model_slug:
                                    models.append(
                                        ModelData(
                                            name=model_name, slug=model_slug, url=href
                                        )
                                    )

                        if models:
                            break

                browser.close()

        except Exception as e:
            self.logger.error(f"Ошибка извлечения моделей для {brand_slug}: {e}")

        # Удаляем дубликаты
        unique_models = {}
        for model in models:
            if model.slug not in unique_models:
                unique_models[model.slug] = model

        result = list(unique_models.values())
        self.logger.info(f"Извлечено {len(result)} уникальных моделей для {brand_slug}")

        return result

    def populate_brands(self, brands: List[BrandData]) -> int:
        """Заполнение таблицы брендов"""
        added_count = 0

        for brand in brands:
            brand_id = self.db_manager.add_brand(
                slug=brand.slug,
                name=brand.name,
                review_count=brand.review_count,
                url=brand.url,
            )

            if brand_id:
                added_count += 1
                self.logger.info(f"✅ Добавлен бренд: {brand.name} ({brand.slug})")
            else:
                self.logger.error(f"❌ Ошибка добавления бренда: {brand.name}")

        return added_count

    def populate_models_for_brand(self, brand_slug: str) -> int:
        """Заполнение моделей для бренда"""
        models = self.extract_models_for_brand(brand_slug)
        added_count = 0

        for model in models:
            model_id = self.db_manager.add_model(
                brand_slug=brand_slug,
                model_slug=model.slug,
                model_name=model.name,
                url=model.url,
            )

            if model_id:
                added_count += 1
                self.logger.info(f"✅ Добавлена модель: {model.name} ({model.slug})")
            else:
                self.logger.error(f"❌ Ошибка добавления модели: {model.name}")

        return added_count

    def initialize_full_catalog(
        self, html_file: str = "brands_html.txt", max_brands: int = None
    ) -> Dict[str, int]:
        """Полная инициализация каталога"""
        self.logger.info("🏭 НАЧАЛО ИНИЦИАЛИЗАЦИИ КАТАЛОГА")

        # Загружаем бренды
        brands = self.load_brands_from_file(html_file)
        if not brands:
            return {"brands": 0, "models": 0, "errors": 1}

        if max_brands:
            brands = brands[:max_brands]
            self.logger.info(f"Ограничение: обрабатываем первые {max_brands} брендов")

        # Добавляем бренды
        brands_added = self.populate_brands(brands)

        # Добавляем модели для каждого бренда
        models_added = 0
        errors = 0

        for i, brand in enumerate(brands, 1):
            try:
                self.logger.info(
                    f"[{i}/{len(brands)}] Обрабатываем модели для {brand.name}"
                )

                count = self.populate_models_for_brand(brand.slug)
                models_added += count

                self.logger.info(f"Добавлено {count} моделей для {brand.name}")

                # Пауза между брендами
                if i < len(brands):
                    time.sleep(2)

            except Exception as e:
                self.logger.error(f"Ошибка обработки бренда {brand.name}: {e}")
                errors += 1

        return {"brands": brands_added, "models": models_added, "errors": errors}


def create_sample_brands_html():
    """Создает образец HTML-файла с брендами"""
    sample_html = """
    <div class="frg44i0">
        <div class="frg44i1 frg44i2">
            <span data-ftid="component_cars-list-item_name" class="css-1tc5ro3 e162wx9x0">Toyota</span>
            <span class="frg44i5" data-ftid="component_cars-list-item_counter"> <!-- -->282196</span>
        </div>
        <a class="frg44i6" data-ftid="component_cars-list-item_hidden-link" href="https://www.drom.ru/reviews/toyota/">Toyota</a>
    </div>
    """

    with open("brands_html_sample.txt", "w", encoding="utf-8") as f:
        f.write(sample_html)

    print("📄 Создан образец HTML-файла: brands_html_sample.txt")


def main():
    """Основная функция инициализации"""
    print("🏭 ИНИЦИАЛИЗАЦИЯ КАТАЛОГА БРЕНДОВ И МОДЕЛЕЙ")
    print("=" * 50)

    # Проверяем существование базы данных
    db_manager = DatabaseManager()
    if not Path(db_manager.db_path).exists():
        print("📊 Создаем базу данных...")
        if not db_manager.create_database():
            print("❌ Ошибка создания базы данных!")
            return 1

    # Инициализатор
    initializer = CatalogInitializer()

    # Проверяем наличие HTML-файла
    if not Path("brands_html.txt").exists():
        print("📄 HTML-файл с брендами не найден!")
        print("Создайте файл brands_html.txt с HTML-блоком брендов из drom.ru")
        print("Или используйте brands_html_sample.txt как образец")
        create_sample_brands_html()
        return 1

    # Запускаем инициализацию
    print("🚀 Начинаем инициализацию...")

    # Для тестирования - ограничиваем количество брендов
    results = initializer.initialize_full_catalog(max_brands=5)

    print(f"\n📊 РЕЗУЛЬТАТЫ ИНИЦИАЛИЗАЦИИ:")
    print(f"Брендов добавлено: {results['brands']}")
    print(f"Моделей добавлено: {results['models']}")
    print(f"Ошибок: {results['errors']}")

    # Финальная статистика
    stats = db_manager.get_database_stats()
    print(f"\n📈 СТАТИСТИКА БАЗЫ ДАННЫХ:")
    print(f"Всего брендов: {stats.get('brands', 0)}")
    print(f"Всего моделей: {stats.get('models', 0)}")
    print(f"Всего отзывов: {stats.get('reviews', 0)}")

    return 0


if __name__ == "__main__":
    exit(main())
