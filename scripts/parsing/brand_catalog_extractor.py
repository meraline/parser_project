#!/usr/bin/env python3
"""
🔍 ЭКСТРАКТОР КАТАЛОГА БРЕНДОВ DROM.RU

Извлекает полный каталог брендов и моделей с сайта drom.ru
для последующего использования в парсере отзывов.

Структура URL: https://www.drom.ru/reviews/toyota/4runner/1425079/
- Протокол: https://
- Домен: www.drom.ru
- Раздел: /reviews/
- Бренд: /toyota/
- Модель: /4runner/
- ID отзыва: /1425079/
"""

import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, asdict
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


@dataclass
class BrandInfo:
    """Информация о бренде автомобилей"""

    name: str
    slug: str
    url: str
    review_count: int
    models: List[str] = None

    def __post_init__(self):
        if self.models is None:
            self.models = []


@dataclass
class CatalogData:
    """Полные данные каталога"""

    extraction_date: str
    total_brands: int
    brands: Dict[str, BrandInfo]

    @classmethod
    def from_dict(cls, data: dict):
        """Создание из словаря"""
        brands = {}
        for slug, brand_data in data.get("brands", {}).items():
            brands[slug] = BrandInfo(**brand_data)

        return cls(
            extraction_date=data.get("extraction_date", ""),
            total_brands=data.get("total_brands", 0),
            brands=brands,
        )

    def to_dict(self) -> dict:
        """Преобразование в словарь для JSON"""
        return {
            "extraction_date": self.extraction_date,
            "total_brands": self.total_brands,
            "brands": {slug: asdict(brand) for slug, brand in self.brands.items()},
        }


class BrandCatalogExtractor:
    """Экстрактор каталога брендов с Drom.ru"""

    def __init__(
        self,
        catalog_file: str = "data/brand_catalog.json",
        browser_path: str = "/home/analityk/Документы/projects/parser_project/chrome-linux/chrome",
    ):
        self.catalog_file = Path(catalog_file)
        self.browser_path = browser_path
        self.base_url = "https://www.drom.ru/reviews/"

        # Создаем директорию для данных
        self.catalog_file.parent.mkdir(parents=True, exist_ok=True)

        # Настройка логирования
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("logs/brand_extractor.log"),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def extract_brands_from_html(self, html_content: str) -> Dict[str, BrandInfo]:
        """Извлечение брендов из HTML-блока каталога"""
        soup = BeautifulSoup(html_content, "html.parser")
        brands = {}

        # Ищем блок с брендами
        brands_container = soup.find("div", {"data-ftid": "component_cars-list"})
        if not brands_container:
            self.logger.error("Не найден контейнер с брендами")
            return brands

        # Извлекаем все элементы брендов
        brand_items = brands_container.find_all("div", class_="frg44i0")

        for item in brand_items:
            try:
                # Название бренда
                name_elem = item.find(
                    "span", {"data-ftid": "component_cars-list-item_name"}
                )
                if not name_elem:
                    continue

                name = name_elem.get_text(strip=True)

                # Количество отзывов
                count_elem = item.find(
                    "span", {"data-ftid": "component_cars-list-item_counter"}
                )
                review_count = 0
                if count_elem:
                    count_text = count_elem.get_text(strip=True)
                    try:
                        review_count = int(count_text.replace(" ", "").replace(",", ""))
                    except (ValueError, AttributeError):
                        review_count = 0

                # Ссылка на бренд
                link_elem = item.find(
                    "a", {"data-ftid": "component_cars-list-item_hidden-link"}
                )
                if not link_elem:
                    continue

                url = link_elem.get("href", "")
                if not url:
                    continue

                # Извлекаем slug из URL (например, /reviews/toyota/ -> toyota)
                slug = url.strip("/").split("/")[-1]

                brands[slug] = BrandInfo(
                    name=name, slug=slug, url=url, review_count=review_count
                )

                self.logger.info(
                    f"Извлечен бренд: {name} ({slug}) - {review_count} отзывов"
                )

            except Exception as e:
                self.logger.error(f"Ошибка при извлечении бренда: {e}")
                continue

        return brands

    def extract_models_for_brand(self, brand_slug: str) -> List[str]:
        """Извлечение моделей для конкретного бренда"""
        models = []

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    executable_path=self.browser_path,
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )

                page = browser.new_page()

                # Переходим на страницу бренда
                brand_url = f"{self.base_url}{brand_slug}/"
                self.logger.info(f"Извлекаем модели для {brand_slug}: {brand_url}")

                page.goto(brand_url, wait_until="networkidle")
                time.sleep(2)

                # Ищем ссылки на модели (структура может отличаться)
                model_links = page.query_selector_all(
                    'a[href*="/reviews/' + brand_slug + '/"]'
                )

                for link in model_links:
                    href = link.get_attribute("href")
                    if href and href.count("/") >= 4:  # /reviews/brand/model/
                        model_slug = href.strip("/").split("/")[-1]
                        if model_slug not in models and model_slug != brand_slug:
                            models.append(model_slug)

                browser.close()

        except Exception as e:
            self.logger.error(f"Ошибка при извлечении моделей для {brand_slug}: {e}")

        self.logger.info(
            f"Найдено {len(models)} моделей для {brand_slug}: {models[:5]}..."
        )
        return models

    def extract_full_catalog(self) -> CatalogData:
        """Извлечение полного каталога брендов"""
        self.logger.info("Начинаем извлечение каталога брендов...")

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    executable_path=self.browser_path,
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )

                page = browser.new_page()
                page.goto(self.base_url, wait_until="networkidle")
                time.sleep(3)

                # Получаем HTML содержимое
                html_content = page.content()
                browser.close()

                # Извлекаем бренды
                brands = self.extract_brands_from_html(html_content)

                # Создаем каталог
                catalog = CatalogData(
                    extraction_date=datetime.now().isoformat(),
                    total_brands=len(brands),
                    brands=brands,
                )

                self.logger.info(f"Извлечено {len(brands)} брендов")
                return catalog

        except Exception as e:
            self.logger.error(f"Ошибка при извлечении каталога: {e}")
            return CatalogData(
                extraction_date=datetime.now().isoformat(), total_brands=0, brands={}
            )

    def extract_models_for_all_brands(
        self, catalog: CatalogData, max_brands: Optional[int] = None
    ) -> CatalogData:
        """Извлечение моделей для всех брендов"""
        self.logger.info("Начинаем извлечение моделей для всех брендов...")

        brand_list = list(catalog.brands.items())
        if max_brands:
            brand_list = brand_list[:max_brands]

        for i, (slug, brand) in enumerate(brand_list, 1):
            self.logger.info(
                f"[{i}/{len(brand_list)}] Извлекаем модели для {brand.name}"
            )

            try:
                models = self.extract_models_for_brand(slug)
                brand.models = models

                # Небольшая пауза между запросами
                time.sleep(1)

            except Exception as e:
                self.logger.error(f"Ошибка при извлечении моделей для {slug}: {e}")
                brand.models = []

        return catalog

    def save_catalog(self, catalog: CatalogData):
        """Сохранение каталога в JSON файл"""
        try:
            with open(self.catalog_file, "w", encoding="utf-8") as f:
                json.dump(catalog.to_dict(), f, ensure_ascii=False, indent=2)

            self.logger.info(f"Каталог сохранен в {self.catalog_file}")

        except Exception as e:
            self.logger.error(f"Ошибка при сохранении каталога: {e}")

    def load_catalog(self) -> Optional[CatalogData]:
        """Загрузка каталога из файла"""
        if not self.catalog_file.exists():
            return None

        try:
            with open(self.catalog_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            catalog = CatalogData.from_dict(data)
            self.logger.info(f"Каталог загружен из {self.catalog_file}")
            return catalog

        except Exception as e:
            self.logger.error(f"Ошибка при загрузке каталога: {e}")
            return None

    def update_catalog(self) -> CatalogData:
        """Обновление каталога (проверка новых брендов)"""
        self.logger.info("Проверяем обновления каталога...")

        # Загружаем существующий каталог
        old_catalog = self.load_catalog()

        # Извлекаем новый каталог
        new_catalog = self.extract_full_catalog()

        if old_catalog is None:
            self.logger.info("Старый каталог не найден, создаем новый")
            return new_catalog

        # Сравниваем каталоги
        old_brands = set(old_catalog.brands.keys())
        new_brands = set(new_catalog.brands.keys())

        added_brands = new_brands - old_brands
        removed_brands = old_brands - new_brands

        if added_brands:
            self.logger.info(f"Новые бренды: {list(added_brands)}")

        if removed_brands:
            self.logger.info(f"Удаленные бренды: {list(removed_brands)}")

        if not added_brands and not removed_brands:
            self.logger.info("Изменений в каталоге не обнаружено")
            return old_catalog

        return new_catalog

    def get_brand_stats(self, catalog: CatalogData) -> dict:
        """Статистика по каталогу"""
        if not catalog.brands:
            return {}

        total_reviews = sum(brand.review_count for brand in catalog.brands.values())
        brands_with_models = sum(1 for brand in catalog.brands.values() if brand.models)
        total_models = sum(len(brand.models) for brand in catalog.brands.values())

        # Топ брендов по количеству отзывов
        top_brands = sorted(
            catalog.brands.values(), key=lambda x: x.review_count, reverse=True
        )[:10]

        return {
            "total_brands": catalog.total_brands,
            "total_reviews": total_reviews,
            "brands_with_models": brands_with_models,
            "total_models": total_models,
            "extraction_date": catalog.extraction_date,
            "top_brands": [
                {
                    "name": brand.name,
                    "slug": brand.slug,
                    "review_count": brand.review_count,
                    "models_count": len(brand.models) if brand.models else 0,
                }
                for brand in top_brands
            ],
        }


def main():
    """Основная функция для тестирования"""
    import argparse

    parser = argparse.ArgumentParser(description="Экстрактор каталога брендов Drom.ru")
    parser.add_argument("--extract", action="store_true", help="Извлечь новый каталог")
    parser.add_argument(
        "--models", action="store_true", help="Извлечь модели для брендов"
    )
    parser.add_argument("--update", action="store_true", help="Обновить каталог")
    parser.add_argument("--stats", action="store_true", help="Показать статистику")
    parser.add_argument(
        "--max-brands", type=int, help="Максимум брендов для извлечения моделей"
    )

    args = parser.parse_args()

    extractor = BrandCatalogExtractor()

    if args.extract:
        print("🔍 Извлекаем каталог брендов...")
        catalog = extractor.extract_full_catalog()
        extractor.save_catalog(catalog)

        if args.models:
            print("🚗 Извлекаем модели для брендов...")
            catalog = extractor.extract_models_for_all_brands(catalog, args.max_brands)
            extractor.save_catalog(catalog)

    elif args.update:
        print("🔄 Обновляем каталог...")
        catalog = extractor.update_catalog()
        extractor.save_catalog(catalog)

    elif args.stats:
        catalog = extractor.load_catalog()
        if catalog:
            stats = extractor.get_brand_stats(catalog)
            print("\n📊 СТАТИСТИКА КАТАЛОГА:")
            print(f"Всего брендов: {stats['total_brands']}")
            print(f"Всего отзывов: {stats['total_reviews']:,}")
            print(f"Брендов с моделями: {stats['brands_with_models']}")
            print(f"Всего моделей: {stats['total_models']}")
            print(f"Дата извлечения: {stats['extraction_date']}")

            print("\n🏆 ТОП-10 БРЕНДОВ ПО ОТЗЫВАМ:")
            for i, brand in enumerate(stats["top_brands"], 1):
                print(
                    f"{i:2d}. {brand['name']:15} - {brand['review_count']:6,} отзывов, {brand['models_count']:3d} моделей"
                )

    else:
        print("Используйте --extract, --update или --stats")


if __name__ == "__main__":
    main()
