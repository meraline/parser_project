#!/usr/bin/env python3
"""
🚀 КАТАЛОЖНЫЙ ПАРСЕР ОТЗЫВОВ DROM.RU

Парсер, использующий предварительно извлеченный каталог брендов
для последовательного и эффективного парсинга отзывов.

Два основных режима:
1. Полный парсинг всех брендов по каталогу
2. Парсинг конкретного бренда с его моделями

Особенности:
- Использует готовый каталог брендов и моделей
- Последовательная обработка (не случайная)
- Проверка дубликатов и перезапись неполных данных
- Детальная статистика и логирование
"""

import json
import logging
import time
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

from playwright.sync_api import sync_playwright

# Импортируем наш нормализованный парсер
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.parsing.normalized_parser import NormalizedDatabaseParser
from scripts.parsing.brand_catalog_extractor import (
    BrandCatalogExtractor,
    CatalogData,
    BrandInfo,
)


@dataclass
class ParseResult:
    """Результат парсинга"""

    brand_slug: str
    brand_name: str
    total_models: int
    processed_models: int
    total_reviews: int
    new_reviews: int
    skipped_reviews: int
    error_reviews: int
    duration_seconds: float
    errors: List[str]


class CatalogBasedParser:
    """Парсер на основе каталога брендов"""

    def __init__(
        self,
        catalog_file: str = "data/brand_catalog.json",
        database_path: str = "нормализованная_бд_v3.db",
        browser_path: str = "/home/analityk/Документы/projects/parser_project/chrome-linux/chrome",
    ):

        self.catalog_file = Path(catalog_file)
        self.database_path = database_path
        self.browser_path = browser_path
        self.base_url = "https://www.drom.ru/reviews/"

        # Инициализируем компоненты
        self.catalog_extractor = BrandCatalogExtractor(catalog_file, browser_path)
        self.db_parser = NormalizedDatabaseParser(database_path)

        # Настройка логирования
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_dir / "catalog_parser.log"),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger(__name__)

        # Загружаем каталог
        self.catalog = self.load_catalog()

    def load_catalog(self) -> Optional[CatalogData]:
        """Загрузка каталога брендов"""
        if not self.catalog_file.exists():
            self.logger.error(f"Каталог не найден: {self.catalog_file}")
            self.logger.info(
                "Запустите brand_catalog_extractor.py --extract для создания каталога"
            )
            return None

        catalog = self.catalog_extractor.load_catalog()
        if catalog:
            self.logger.info(f"Загружен каталог с {catalog.total_brands} брендами")

        return catalog

    def get_brand_models(self, brand_slug: str) -> List[str]:
        """Получение моделей бренда"""
        if not self.catalog or brand_slug not in self.catalog.brands:
            return []

        brand = self.catalog.brands[brand_slug]
        return brand.models if brand.models else []

    def extract_review_urls_from_model_page(
        self, brand_slug: str, model_slug: str, max_pages: int = 5
    ) -> List[str]:
        """Извлечение URL отзывов со страницы модели"""
        review_urls = []

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    executable_path=self.browser_path,
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )

                page = browser.new_page()

                # Страница модели
                model_url = f"{self.base_url}{brand_slug}/{model_slug}/"
                self.logger.info(f"Извлекаем отзывы: {model_url}")

                for page_num in range(1, max_pages + 1):
                    try:
                        # Переходим на страницу
                        if page_num == 1:
                            page.goto(model_url, wait_until="networkidle")
                        else:
                            # Для пагинации (если есть)
                            page_url = f"{model_url}?page={page_num}"
                            page.goto(page_url, wait_until="networkidle")

                        time.sleep(2)

                        # Ищем ссылки на отзывы
                        # Стандартные селекторы для Drom
                        review_selectors = [
                            f'a[href*="/reviews/{brand_slug}/{model_slug}/"]',
                            'a[href*="/reviews/"][href*="/"]',
                            ".review-item a",
                            ".reviews-list a",
                        ]

                        found_reviews = False

                        for selector in review_selectors:
                            try:
                                links = page.query_selector_all(selector)

                                for link in links:
                                    href = link.get_attribute("href")
                                    if (
                                        href
                                        and f"/reviews/{brand_slug}/{model_slug}/"
                                        in href
                                        and href.count("/") >= 5
                                    ):  # /reviews/brand/model/id/

                                        full_url = (
                                            href
                                            if href.startswith("http")
                                            else f"https://www.drom.ru{href}"
                                        )

                                        if full_url not in review_urls:
                                            review_urls.append(full_url)
                                            found_reviews = True

                                if found_reviews:
                                    break

                            except Exception as e:
                                self.logger.debug(f"Ошибка селектора {selector}: {e}")
                                continue

                        # Если отзывы не найдены на первой странице, прекращаем
                        if page_num == 1 and not found_reviews:
                            self.logger.warning(
                                f"Отзывы не найдены для {brand_slug}/{model_slug}"
                            )
                            break

                        # Если на текущей странице ничего не найдено, прекращаем
                        if not found_reviews:
                            break

                    except Exception as e:
                        self.logger.error(f"Ошибка на странице {page_num}: {e}")
                        break

                browser.close()

        except Exception as e:
            self.logger.error(
                f"Ошибка при извлечении URL для {brand_slug}/{model_slug}: {e}"
            )

        self.logger.info(
            f"Найдено {len(review_urls)} отзывов для {brand_slug}/{model_slug}"
        )
        return review_urls

    def parse_brand_sequential(
        self,
        brand_slug: str,
        max_models: Optional[int] = None,
        max_reviews_per_model: int = 50,
    ) -> ParseResult:
        """Последовательный парсинг бренда"""
        start_time = time.time()

        if not self.catalog or brand_slug not in self.catalog.brands:
            raise ValueError(f"Бренд {brand_slug} не найден в каталоге")

        brand = self.catalog.brands[brand_slug]
        models = self.get_brand_models(brand_slug)

        if not models:
            self.logger.warning(f"Модели для бренда {brand_slug} не найдены")
            return ParseResult(
                brand_slug=brand_slug,
                brand_name=brand.name,
                total_models=0,
                processed_models=0,
                total_reviews=0,
                new_reviews=0,
                skipped_reviews=0,
                error_reviews=0,
                duration_seconds=time.time() - start_time,
                errors=["Модели не найдены"],
            )

        if max_models:
            models = models[:max_models]

        self.logger.info(f"🚗 Начинаем парсинг бренда {brand.name} ({brand_slug})")
        self.logger.info(f"📋 Моделей к обработке: {len(models)}")

        result = ParseResult(
            brand_slug=brand_slug,
            brand_name=brand.name,
            total_models=len(models),
            processed_models=0,
            total_reviews=0,
            new_reviews=0,
            skipped_reviews=0,
            error_reviews=0,
            duration_seconds=0,
            errors=[],
        )

        for i, model_slug in enumerate(models, 1):
            self.logger.info(f"[{i}/{len(models)}] Обрабатываем модель: {model_slug}")

            try:
                # Извлекаем URL отзывов для модели
                review_urls = self.extract_review_urls_from_model_page(
                    brand_slug, model_slug, max_pages=3
                )

                if not review_urls:
                    self.logger.warning(f"Отзывы для {model_slug} не найдены")
                    continue

                # Ограничиваем количество отзывов
                if len(review_urls) > max_reviews_per_model:
                    review_urls = review_urls[:max_reviews_per_model]

                result.total_reviews += len(review_urls)

                # Парсим каждый отзыв
                for j, review_url in enumerate(review_urls, 1):
                    try:
                        self.logger.info(
                            f"  [{j}/{len(review_urls)}] Парсим: {review_url}"
                        )

                        # Используем существующий парсер
                        success = self.db_parser.parse_single_review(review_url)

                        if success:
                            result.new_reviews += 1
                        else:
                            result.skipped_reviews += 1

                        # Небольшая пауза между отзывами
                        time.sleep(1)

                    except Exception as e:
                        self.logger.error(f"Ошибка парсинга отзыва {review_url}: {e}")
                        result.error_reviews += 1
                        result.errors.append(f"{review_url}: {str(e)}")

                result.processed_models += 1

                # Пауза между моделями
                time.sleep(2)

            except Exception as e:
                self.logger.error(f"Ошибка обработки модели {model_slug}: {e}")
                result.errors.append(f"Модель {model_slug}: {str(e)}")

        result.duration_seconds = time.time() - start_time

        self.logger.info(f"✅ Парсинг {brand.name} завершен:")
        self.logger.info(
            f"   Обработано моделей: {result.processed_models}/{result.total_models}"
        )
        self.logger.info(f"   Новых отзывов: {result.new_reviews}")
        self.logger.info(f"   Пропущено: {result.skipped_reviews}")
        self.logger.info(f"   Ошибок: {result.error_reviews}")
        self.logger.info(f"   Время: {result.duration_seconds:.1f} сек")

        return result

    def parse_all_brands_sequential(
        self,
        max_brands: Optional[int] = None,
        max_models_per_brand: int = 10,
        max_reviews_per_model: int = 20,
    ) -> List[ParseResult]:
        """Последовательный парсинг всех брендов"""
        if not self.catalog:
            raise ValueError("Каталог не загружен")

        # Сортируем бренды по количеству отзывов (от большего к меньшему)
        brands_sorted = sorted(
            self.catalog.brands.values(), key=lambda x: x.review_count, reverse=True
        )

        if max_brands:
            brands_sorted = brands_sorted[:max_brands]

        self.logger.info(f"🌟 Начинаем полный парсинг {len(brands_sorted)} брендов")

        results = []

        for i, brand in enumerate(brands_sorted, 1):
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"БРЕНД {i}/{len(brands_sorted)}: {brand.name}")
            self.logger.info(f"{'='*60}")

            try:
                result = self.parse_brand_sequential(
                    brand.slug,
                    max_models=max_models_per_brand,
                    max_reviews_per_model=max_reviews_per_model,
                )
                results.append(result)

                # Пауза между брендами
                time.sleep(5)

            except Exception as e:
                self.logger.error(f"Критическая ошибка при парсинге {brand.name}: {e}")

                error_result = ParseResult(
                    brand_slug=brand.slug,
                    brand_name=brand.name,
                    total_models=0,
                    processed_models=0,
                    total_reviews=0,
                    new_reviews=0,
                    skipped_reviews=0,
                    error_reviews=0,
                    duration_seconds=0,
                    errors=[str(e)],
                )
                results.append(error_result)

        # Итоговая статистика
        self.print_final_stats(results)

        return results

    def print_final_stats(self, results: List[ParseResult]):
        """Вывод итоговой статистики"""
        total_brands = len(results)
        total_models = sum(r.processed_models for r in results)
        total_reviews = sum(r.new_reviews for r in results)
        total_skipped = sum(r.skipped_reviews for r in results)
        total_errors = sum(r.error_reviews for r in results)
        total_time = sum(r.duration_seconds for r in results)

        self.logger.info(f"\n{'='*60}")
        self.logger.info("🎉 ИТОГОВАЯ СТАТИСТИКА ПАРСИНГА")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Обработано брендов: {total_brands}")
        self.logger.info(f"Обработано моделей: {total_models}")
        self.logger.info(f"Новых отзывов: {total_reviews}")
        self.logger.info(f"Пропущено отзывов: {total_skipped}")
        self.logger.info(f"Ошибок: {total_errors}")
        self.logger.info(f"Общее время: {total_time/60:.1f} мин")

        # Топ брендов по новым отзывам
        top_results = sorted(results, key=lambda x: x.new_reviews, reverse=True)[:10]

        self.logger.info(f"\n🏆 ТОП-10 БРЕНДОВ ПО НОВЫМ ОТЗЫВАМ:")
        for i, result in enumerate(top_results, 1):
            self.logger.info(
                f"{i:2d}. {result.brand_name:15} - {result.new_reviews:4d} отзывов"
            )

    def get_database_stats(self) -> dict:
        """Статистика базы данных"""
        return self.db_parser.get_database_stats()


def main():
    """Основная функция"""
    import argparse

    parser = argparse.ArgumentParser(description="Каталожный парсер отзывов Drom.ru")
    parser.add_argument("--brand", type=str, help="Парсить конкретный бренд")
    parser.add_argument("--all-brands", action="store_true", help="Парсить все бренды")
    parser.add_argument("--max-brands", type=int, default=10, help="Максимум брендов")
    parser.add_argument(
        "--max-models", type=int, default=5, help="Максимум моделей на бренд"
    )
    parser.add_argument(
        "--max-reviews", type=int, default=10, help="Максимум отзывов на модель"
    )
    parser.add_argument("--stats", action="store_true", help="Показать статистику БД")

    args = parser.parse_args()

    # Создаем парсер
    catalog_parser = CatalogBasedParser()

    if not catalog_parser.catalog:
        print("❌ Каталог не загружен!")
        print("Запустите: python brand_catalog_extractor.py --extract")
        return

    if args.stats:
        stats = catalog_parser.get_database_stats()
        print("\n📊 СТАТИСТИКА БАЗЫ ДАННЫХ:")
        for key, value in stats.items():
            print(f"{key}: {value}")

    elif args.brand:
        print(f"🎯 Парсим бренд: {args.brand}")
        try:
            result = catalog_parser.parse_brand_sequential(
                args.brand,
                max_models=args.max_models,
                max_reviews_per_model=args.max_reviews,
            )
            print(f"✅ Результат: {result.new_reviews} новых отзывов")
        except Exception as e:
            print(f"❌ Ошибка: {e}")

    elif args.all_brands:
        print(f"🌟 Парсим все бренды (макс. {args.max_brands})")
        try:
            results = catalog_parser.parse_all_brands_sequential(
                max_brands=args.max_brands,
                max_models_per_brand=args.max_models,
                max_reviews_per_model=args.max_reviews,
            )
            total_reviews = sum(r.new_reviews for r in results)
            print(f"✅ Итого новых отзывов: {total_reviews}")
        except Exception as e:
            print(f"❌ Ошибка: {e}")

    else:
        print("Используйте --brand BRAND, --all-brands или --stats")
        print("\nПримеры:")
        print("python catalog_parser.py --brand toyota --max-models 3 --max-reviews 5")
        print("python catalog_parser.py --all-brands --max-brands 5 --max-models 2")
        print("python catalog_parser.py --stats")


if __name__ == "__main__":
    main()
