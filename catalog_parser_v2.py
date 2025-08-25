#!/usr/bin/env python3
"""
🚀 КАТАЛОЖНЫЙ ПАРСЕР ОТЗЫВОВ DROM.RU (версия 2.0)

Использует нормализованную структуру базы данных с отдельными таблицами:
- brands: каталог брендов
- models: модели для каждого бренда
- reviews: отзывы по моделям

Особенности:
- Работает с каталогом брендов из базы данных
- Последовательная обработка по брендам и моделям
- Проверка дубликатов и перезапись неполных данных
- Детальная статистика и метрики
"""

import logging
import time
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

from database_schema import DatabaseManager


@dataclass
class ReviewData:
    """Данные отзыва"""

    review_id: str
    url: str
    title: str = ""
    content: str = ""
    author: str = ""
    publish_date: str = ""
    rating: float = 0.0
    pros: str = ""
    cons: str = ""
    general_impression: str = ""
    experience_period: str = ""
    car_year: int = 0
    car_modification: str = ""


@dataclass
class ParseResult:
    """Результат парсинга"""

    brand_slug: str
    brand_name: str
    processed_models: int
    total_reviews: int
    new_reviews: int
    skipped_reviews: int
    error_reviews: int
    duration_seconds: float
    errors: List[str]


class CatalogBasedParser:
    """Парсер на основе каталога брендов из базы данных"""

    def __init__(
        self,
        db_path: str = "auto_reviews.db",
        browser_path: str = "/home/analityk/Документы/projects/parser_project/chrome-linux/chrome",
    ):
        self.db_manager = DatabaseManager(db_path)
        self.browser_path = browser_path
        self.base_url = "https://www.drom.ru/reviews/"

        # Настройка логирования
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_dir / "catalog_parser_v2.log"),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger(__name__)

        # Проверяем базу данных
        self.check_database()

    def check_database(self) -> bool:
        """Проверка состояния базы данных"""
        if not Path(self.db_manager.db_path).exists():
            self.logger.error(f"❌ База данных не найдена: {self.db_manager.db_path}")
            self.logger.info("🔧 Создайте ее сначала с помощью database_schema.py")
            return False

        # Проверяем наличие брендов
        brands = self.db_manager.get_all_brands()
        if not brands:
            self.logger.error("❌ В базе данных нет брендов!")
            self.logger.info("🔧 Заполните каталог с помощью catalog_initializer.py")
            return False

        self.logger.info(f"✅ База данных готова: {len(brands)} брендов")
        return True

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
                model_url = f"{self.base_url}{brand_slug}/{model_slug}/"

                self.logger.info(f"Извлекаем отзывы: {model_url}")

                for page_num in range(1, max_pages + 1):
                    try:
                        if page_num == 1:
                            page.goto(model_url, wait_until="networkidle")
                        else:
                            page_url = f"{model_url}?page={page_num}"
                            page.goto(page_url, wait_until="networkidle")

                        time.sleep(2)

                        # Ищем ссылки на отзывы
                        content = page.content()
                        soup = BeautifulSoup(content, "html.parser")

                        # Различные селекторы для поиска отзывов
                        review_selectors = [
                            f'a[href*="/reviews/{brand_slug}/{model_slug}/"]',
                            ".review-item a",
                            ".review-link",
                            'a[href*="/reviews/"]',
                        ]

                        for selector in review_selectors:
                            links = soup.select(selector)

                            for link in links:
                                href = link.get("href", "")

                                # Проверяем, что это ссылка на отзыв
                                pattern = rf"/reviews/{re.escape(brand_slug)}/{re.escape(model_slug)}/(\d+)/?"
                                match = re.search(pattern, href)

                                if match:
                                    full_url = (
                                        href
                                        if href.startswith("http")
                                        else f"https://www.drom.ru{href}"
                                    )
                                    review_urls.append(full_url)

                            if review_urls:
                                break

                        # Если нашли отзывы, проверяем есть ли следующая страница
                        if not review_urls:
                            break

                    except Exception as e:
                        self.logger.warning(f"Ошибка на странице {page_num}: {e}")
                        continue

                browser.close()

        except Exception as e:
            self.logger.error(
                f"Ошибка извлечения отзывов для {brand_slug}/{model_slug}: {e}"
            )

        # Удаляем дубликаты
        unique_urls = list(set(review_urls))
        self.logger.info(
            f"Найдено {len(unique_urls)} уникальных отзывов для {model_slug}"
        )

        return unique_urls

    def parse_review_page(self, review_url: str) -> Optional[ReviewData]:
        """Парсинг страницы отзыва"""
        try:
            # Извлекаем ID отзыва из URL
            review_id_match = re.search(r"/(\d+)/?$", review_url)
            if not review_id_match:
                return None

            review_id = review_id_match.group(1)

            with sync_playwright() as p:
                browser = p.chromium.launch(
                    executable_path=self.browser_path,
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )

                page = browser.new_page()
                page.goto(review_url, wait_until="networkidle")
                time.sleep(2)

                content = page.content()
                soup = BeautifulSoup(content, "html.parser")

                # Извлекаем данные отзыва
                review_data = ReviewData(review_id=review_id, url=review_url)

                # Заголовок
                title_elem = soup.find("h1") or soup.find(".review-title")
                if title_elem:
                    review_data.title = title_elem.get_text(strip=True)

                # Автор
                author_elem = soup.find(".review-author") or soup.find(".author")
                if author_elem:
                    review_data.author = author_elem.get_text(strip=True)

                # Содержание отзыва
                content_elem = soup.find(".review-content") or soup.find(".review-text")
                if content_elem:
                    review_data.content = content_elem.get_text(strip=True)

                # Плюсы
                pros_elem = soup.find(".review-pros") or soup.find(".pros")
                if pros_elem:
                    review_data.pros = pros_elem.get_text(strip=True)

                # Минусы
                cons_elem = soup.find(".review-cons") or soup.find(".cons")
                if cons_elem:
                    review_data.cons = cons_elem.get_text(strip=True)

                # Общее впечатление
                impression_elem = soup.find(".review-impression") or soup.find(
                    ".general-impression"
                )
                if impression_elem:
                    review_data.general_impression = impression_elem.get_text(
                        strip=True
                    )

                browser.close()

                return review_data

        except Exception as e:
            self.logger.error(f"Ошибка парсинга отзыва {review_url}: {e}")
            return None

    def save_review_to_database(
        self, review_data: ReviewData, brand_slug: str, model_slug: str
    ) -> bool:
        """Сохранение отзыва в базу данных"""
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()

            # Получаем ID бренда и модели
            cursor.execute(
                """
                SELECT b.id, m.id 
                FROM brands b
                JOIN models m ON m.brand_id = b.id
                WHERE b.slug = ? AND m.slug = ?
            """,
                (brand_slug, model_slug),
            )

            result = cursor.fetchone()
            if not result:
                self.logger.error(f"Не найден бренд/модель: {brand_slug}/{model_slug}")
                conn.close()
                return False

            brand_id, model_id = result

            # Проверяем, существует ли уже такой отзыв
            cursor.execute(
                "SELECT id FROM reviews WHERE review_id = ?", (review_data.review_id,)
            )
            existing = cursor.fetchone()

            if existing:
                # Обновляем существующий отзыв
                cursor.execute(
                    """
                    UPDATE reviews SET
                        title = ?, content = ?, author = ?, publish_date = ?,
                        rating = ?, pros = ?, cons = ?, general_impression = ?,
                        experience_period = ?, car_year = ?, car_modification = ?,
                        is_complete = ?, parse_attempts = parse_attempts + 1
                    WHERE review_id = ?
                """,
                    (
                        review_data.title,
                        review_data.content,
                        review_data.author,
                        review_data.publish_date,
                        review_data.rating,
                        review_data.pros,
                        review_data.cons,
                        review_data.general_impression,
                        review_data.experience_period,
                        review_data.car_year,
                        review_data.car_modification,
                        1,
                        review_data.review_id,
                    ),
                )
                self.logger.info(f"🔄 Обновлен отзыв: {review_data.review_id}")
            else:
                # Создаем новый отзыв
                cursor.execute(
                    """
                    INSERT INTO reviews (
                        brand_id, model_id, review_id, url, title, content, author,
                        publish_date, rating, pros, cons, general_impression,
                        experience_period, car_year, car_modification, is_complete, parse_attempts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        brand_id,
                        model_id,
                        review_data.review_id,
                        review_data.url,
                        review_data.title,
                        review_data.content,
                        review_data.author,
                        review_data.publish_date,
                        review_data.rating,
                        review_data.pros,
                        review_data.cons,
                        review_data.general_impression,
                        review_data.experience_period,
                        review_data.car_year,
                        review_data.car_modification,
                        1,
                        1,
                    ),
                )
                self.logger.info(f"✅ Добавлен отзыв: {review_data.review_id}")

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            self.logger.error(f"Ошибка сохранения отзыва: {e}")
            return False

    def parse_brand_models(
        self, brand_slug: str, max_models: int = None, max_reviews: int = 10
    ) -> ParseResult:
        """Парсинг всех моделей бренда"""
        start_time = time.time()

        # Получаем информацию о бренде
        brands = [
            b for b in self.db_manager.get_all_brands() if b["slug"] == brand_slug
        ]
        if not brands:
            return ParseResult(
                brand_slug=brand_slug,
                brand_name="Unknown",
                processed_models=0,
                total_reviews=0,
                new_reviews=0,
                skipped_reviews=0,
                error_reviews=0,
                duration_seconds=0,
                errors=[f"Бренд не найден: {brand_slug}"],
            )

        brand_name = brands[0]["name"]

        # Получаем модели бренда
        models = self.db_manager.get_brand_models(brand_slug)
        if max_models:
            models = models[:max_models]

        self.logger.info(f"🚗 Парсинг бренда {brand_name}: {len(models)} моделей")

        # Статистика
        processed_models = 0
        total_reviews = 0
        new_reviews = 0
        skipped_reviews = 0
        error_reviews = 0
        errors = []

        for i, model in enumerate(models, 1):
            model_slug = model["slug"]
            model_name = model["name"]

            self.logger.info(f"[{i}/{len(models)}] Обрабатываем модель: {model_name}")

            try:
                # Извлекаем URL отзывов для модели
                review_urls = self.extract_review_urls_from_model_page(
                    brand_slug, model_slug
                )

                if max_reviews:
                    review_urls = review_urls[:max_reviews]

                total_reviews += len(review_urls)

                # Парсим каждый отзыв
                for j, review_url in enumerate(review_urls, 1):
                    try:
                        self.logger.info(
                            f"  [{j}/{len(review_urls)}] Парсинг отзыва: {review_url}"
                        )

                        review_data = self.parse_review_page(review_url)
                        if review_data:
                            if self.save_review_to_database(
                                review_data, brand_slug, model_slug
                            ):
                                new_reviews += 1
                            else:
                                error_reviews += 1
                        else:
                            error_reviews += 1

                        # Пауза между отзывами
                        time.sleep(1)

                    except Exception as e:
                        error_msg = f"Ошибка парсинга отзыва {review_url}: {e}"
                        self.logger.error(error_msg)
                        errors.append(error_msg)
                        error_reviews += 1

                processed_models += 1

                # Пауза между моделями
                if i < len(models):
                    time.sleep(2)

            except Exception as e:
                error_msg = f"Ошибка обработки модели {model_name}: {e}"
                self.logger.error(error_msg)
                errors.append(error_msg)

        duration = time.time() - start_time

        return ParseResult(
            brand_slug=brand_slug,
            brand_name=brand_name,
            processed_models=processed_models,
            total_reviews=total_reviews,
            new_reviews=new_reviews,
            skipped_reviews=skipped_reviews,
            error_reviews=error_reviews,
            duration_seconds=duration,
            errors=errors,
        )

    def get_parsing_statistics(self) -> Dict:
        """Получение статистики парсинга"""
        return self.db_manager.get_database_stats()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Каталожный парсер отзывов Drom.ru v2.0"
    )
    parser.add_argument("--brand", type=str, help="Slug бренда для парсинга")
    parser.add_argument(
        "--max-models", type=int, default=5, help="Максимум моделей для обработки"
    )
    parser.add_argument(
        "--max-reviews", type=int, default=5, help="Максимум отзывов на модель"
    )
    parser.add_argument(
        "--list-brands", action="store_true", help="Показать доступные бренды"
    )

    args = parser.parse_args()

    print("🚀 КАТАЛОЖНЫЙ ПАРСЕР ОТЗЫВОВ DROM.RU v2.0")
    print("=" * 50)

    # Инициализация парсера
    catalog_parser = CatalogBasedParser()

    if args.list_brands:
        brands = catalog_parser.db_manager.get_all_brands()
        print(f"\n📋 ДОСТУПНЫЕ БРЕНДЫ ({len(brands)}):")
        for brand in brands:
            print(
                f"- {brand['name']} ({brand['slug']}) - {brand['review_count']} отзывов"
            )
        return 0

    if not args.brand:
        print("❌ Укажите бренд для парсинга: --brand SLUG")
        print("Используйте --list-brands для просмотра доступных брендов")
        return 1

    # Запуск парсинга
    print(f"\n🎯 ПАРСИНГ БРЕНДА: {args.brand}")
    print(f"Максимум моделей: {args.max_models}")
    print(f"Максимум отзывов на модель: {args.max_reviews}")

    result = catalog_parser.parse_brand_models(
        brand_slug=args.brand, max_models=args.max_models, max_reviews=args.max_reviews
    )

    # Вывод результатов
    print(f"\n📊 РЕЗУЛЬТАТЫ ПАРСИНГА:")
    print(f"Бренд: {result.brand_name} ({result.brand_slug})")
    print(f"Обработано моделей: {result.processed_models}")
    print(f"Всего отзывов: {result.total_reviews}")
    print(f"Новых отзывов: {result.new_reviews}")
    print(f"Ошибок: {result.error_reviews}")
    print(f"Время выполнения: {result.duration_seconds:.2f} сек")

    if result.errors:
        print(f"\n⚠️ ОШИБКИ ({len(result.errors)}):")
        for error in result.errors[:5]:  # Показываем только первые 5 ошибок
            print(f"- {error}")

    # Финальная статистика
    stats = catalog_parser.get_parsing_statistics()
    print(f"\n📈 ОБЩАЯ СТАТИСТИКА БАЗЫ ДАННЫХ:")
    print(f"Брендов: {stats.get('brands', 0)}")
    print(f"Моделей: {stats.get('models', 0)}")
    print(f"Отзывов: {stats.get('reviews', 0)}")
    print(f"Полных отзывов: {stats.get('complete_reviews', 0)}")
    print(f"Процент завершенности: {stats.get('completion_rate', 0)}%")

    return 0


if __name__ == "__main__":
    exit(main())
