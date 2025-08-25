#!/usr/bin/env python3
"""
Модуль для получения списка всех брендов и моделей с drom.ru.
Используется для построения полного каталога отзывов.
"""

import requests
import time
import re
from typing import List, Dict, Set
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import logging


class DromCatalogExtractor:
    """Извлекатель каталога брендов и моделей с Drom.ru."""

    def __init__(self, delay: float = 2.0):
        self.base_url = "https://www.drom.ru"
        self.reviews_url = "https://www.drom.ru/reviews/"
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
        )

        # Настройка логирования
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_all_brands(self) -> List[str]:
        """Получает список всех брендов с главной страницы отзывов."""
        try:
            response = self.session.get(self.reviews_url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            brands = []

            # Ищем ссылки на бренды в разделе отзывов
            # Обычно они имеют вид /reviews/{brand}/
            brand_links = soup.find_all("a", href=re.compile(r"/reviews/[a-z0-9\-]+/$"))

            for link in brand_links:
                href = link.get("href")
                if href:
                    # Извлекаем название бренда из URL
                    match = re.search(r"/reviews/([a-z0-9\-]+)/", href)
                    if match:
                        brand = match.group(1)
                        if (
                            brand not in brands and len(brand) > 1
                        ):  # Исключаем короткие случайные совпадения
                            brands.append(brand)

            # Если не нашли бренды таким способом, пробуем альтернативный метод
            if not brands:
                brands = self._get_brands_alternative_method(soup)

            # Сортируем по алфавиту
            brands.sort()

            self.logger.info(f"Найдено {len(brands)} брендов")
            return brands

        except Exception as e:
            self.logger.error(f"Ошибка получения списка брендов: {e}")
            # Возвращаем базовый список известных брендов
            return self._get_fallback_brands()

    def _get_brands_alternative_method(self, soup: BeautifulSoup) -> List[str]:
        """Альтернативный метод поиска брендов на странице."""
        brands = []

        # Ищем все ссылки, содержащие названия брендов
        all_links = soup.find_all("a", href=True)

        for link in all_links:
            href = link.get("href", "")
            text = link.get_text(strip=True).lower()

            # Проверяем, является ли это ссылкой на бренд
            if "/reviews/" in href and text:
                # Извлекаем бренд из URL или текста
                brand_match = re.search(r"/reviews/([a-z0-9\-]+)", href)
                if brand_match:
                    brand = brand_match.group(1)
                    if brand not in brands and self._is_valid_brand_name(brand):
                        brands.append(brand)

        return brands

    def _is_valid_brand_name(self, brand: str) -> bool:
        """Проверяет, является ли строка валидным названием бренда."""
        # Исключаем служебные страницы и слишком короткие названия
        invalid_names = {
            "new",
            "used",
            "catalog",
            "search",
            "reviews",
            "news",
            "auto",
            "cars",
            "sale",
            "buy",
            "sell",
            "price",
            "photo",
        }

        return (
            len(brand) >= 3
            and brand not in invalid_names
            and not brand.isdigit()
            and re.match(r"^[a-z0-9\-]+$", brand)
        )

    def _get_fallback_brands(self) -> List[str]:
        """Возвращает базовый список известных автомобильных брендов."""
        return [
            "acura",
            "alfa-romeo",
            "aston-martin",
            "audi",
            "bentley",
            "bmw",
            "bugatti",
            "buick",
            "cadillac",
            "changan",
            "chery",
            "chevrolet",
            "chrysler",
            "citroen",
            "daewoo",
            "datsun",
            "dodge",
            "faw",
            "ferrari",
            "fiat",
            "ford",
            "geely",
            "genesis",
            "great-wall",
            "honda",
            "hyundai",
            "infiniti",
            "isuzu",
            "jaguar",
            "jeep",
            "kia",
            "lada",
            "lamborghini",
            "land-rover",
            "lexus",
            "lifan",
            "maserati",
            "mazda",
            "mercedes",
            "mini",
            "mitsubishi",
            "nissan",
            "opel",
            "peugeot",
            "porsche",
            "renault",
            "rolls-royce",
            "seat",
            "skoda",
            "smart",
            "ssangyong",
            "subaru",
            "suzuki",
            "toyota",
            "volkswagen",
            "volvo",
            "zaz",
        ]

    def get_brand_models(self, brand: str) -> List[str]:
        """Получает список всех моделей для конкретного бренда."""
        try:
            brand_url = f"{self.reviews_url}{brand}/"

            self.logger.info(f"Получение моделей для бренда: {brand}")
            response = self.session.get(brand_url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            models = []

            # Ищем ссылки на модели
            # Обычно они имеют вид /reviews/{brand}/{model}/
            model_links = soup.find_all(
                "a", href=re.compile(rf"/reviews/{re.escape(brand)}/[a-z0-9\-_]+/$")
            )

            for link in model_links:
                href = link.get("href")
                if href:
                    # Извлекаем название модели из URL
                    match = re.search(
                        rf"/reviews/{re.escape(brand)}/([a-z0-9\-_]+)/", href
                    )
                    if match:
                        model = match.group(1)
                        if model not in models and self._is_valid_model_name(model):
                            models.append(model)

            # Сортируем по алфавиту
            models.sort()

            self.logger.info(f"Найдено {len(models)} моделей для бренда {brand}")
            time.sleep(self.delay)

            return models

        except Exception as e:
            self.logger.error(f"Ошибка получения моделей для бренда {brand}: {e}")
            return []

    def _is_valid_model_name(self, model: str) -> bool:
        """Проверяет, является ли строка валидным названием модели."""
        # Исключаем служебные страницы
        invalid_names = {"reviews", "catalog", "all", "new", "used", "search"}

        return (
            len(model) >= 1
            and model not in invalid_names
            and re.match(r"^[a-z0-9\-_]+$", model)
        )

    def get_model_review_urls(
        self, brand: str, model: str, max_pages: int = 100
    ) -> List[str]:
        """Получает все URL отзывов для конкретной модели."""
        try:
            model_url = f"{self.reviews_url}{brand}/{model}/"

            self.logger.info(f"Получение отзывов для {brand} {model}")

            all_review_urls = []
            page = 1

            while page <= max_pages:
                if page == 1:
                    page_url = model_url
                else:
                    page_url = f"{model_url}?page={page}"

                response = self.session.get(page_url, timeout=10)
                response.raise_for_status()

                soup = BeautifulSoup(response.content, "html.parser")

                # Ищем ссылки на отдельные отзывы
                # Обычно они имеют вид /reviews/{brand}/{model}/{review_id}/
                review_links = soup.find_all(
                    "a",
                    href=re.compile(
                        rf"/reviews/{re.escape(brand)}/{re.escape(model)}/\d+/"
                    ),
                )

                page_review_urls = []
                for link in review_links:
                    href = link.get("href")
                    if href:
                        full_url = urljoin(self.base_url, href)
                        if full_url not in all_review_urls:
                            page_review_urls.append(full_url)
                            all_review_urls.append(full_url)

                self.logger.info(
                    f"Страница {page}: найдено {len(page_review_urls)} отзывов"
                )

                # Если на странице нет отзывов, значит мы дошли до конца
                if not page_review_urls:
                    break

                page += 1
                time.sleep(self.delay)

            self.logger.info(
                f"Всего найдено {len(all_review_urls)} отзывов для {brand} {model}"
            )
            return all_review_urls

        except Exception as e:
            self.logger.error(f"Ошибка получения отзывов для {brand} {model}: {e}")
            return []

    def get_full_catalog(self) -> Dict[str, Dict[str, List[str]]]:
        """Получает полный каталог: бренды -> модели -> отзывы."""
        catalog = {}

        # Получаем все бренды
        brands = self.get_all_brands()

        for brand in brands:
            self.logger.info(f"Обработка бренда: {brand}")
            catalog[brand] = {}

            # Получаем модели бренда
            models = self.get_brand_models(brand)

            for model in models:
                # Получаем отзывы модели
                review_urls = self.get_model_review_urls(brand, model)
                catalog[brand][model] = review_urls

                # Небольшая пауза между моделями
                time.sleep(self.delay)

            # Пауза между брендами
            time.sleep(self.delay * 2)

        return catalog

    def save_catalog_to_file(self, catalog: Dict, filename: str = "drom_catalog.txt"):
        """Сохраняет каталог в текстовый файл."""
        with open(filename, "w", encoding="utf-8") as f:
            for brand, models in catalog.items():
                f.write(f"BRAND: {brand}\n")
                for model, reviews in models.items():
                    f.write(f"  MODEL: {model} ({len(reviews)} отзывов)\n")
                    for review_url in reviews:
                        f.write(f"    {review_url}\n")
                f.write("\n")

        self.logger.info(f"Каталог сохранен в файл: {filename}")


def main():
    """Демонстрация работы экстрактора каталога."""
    extractor = DromCatalogExtractor(delay=1.0)

    print("🔍 Получение списка брендов...")
    brands = extractor.get_all_brands()
    print(f"Найдено {len(brands)} брендов: {', '.join(brands[:10])}...")

    if brands:
        # Тестируем на первом бренде
        test_brand = brands[0]
        print(f"\n🚗 Получение моделей для бренда: {test_brand}")
        models = extractor.get_brand_models(test_brand)
        print(f"Найдено {len(models)} моделей: {', '.join(models[:5])}...")

        if models:
            # Тестируем на первой модели
            test_model = models[0]
            print(f"\n📝 Получение отзывов для: {test_brand} {test_model}")
            reviews = extractor.get_model_review_urls(
                test_brand, test_model, max_pages=2
            )
            print(f"Найдено {len(reviews)} отзывов")

            if reviews:
                print("Примеры URL:")
                for url in reviews[:3]:
                    print(f"  - {url}")


if __name__ == "__main__":
    main()
