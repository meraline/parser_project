"""Парсер отзывов с Drom.ru."""

from typing import Dict, List, Optional, Union
import json
import time
import random
from datetime import datetime
from urllib.parse import urlparse, unquote
import hashlib

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from ..models.review import Review
from .base import BaseParser
from ..utils.logger import get_logger
from ..database.base import Database
from ..database.repositories.review_repository import ReviewRepository


logger = get_logger(__name__)

HEADERS = {
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9," "image/webp,*/*;q=0.8"
    ),
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
}

USER_AGENTS = [
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/16.5 Safari/605.1.15"
    ),
]


class DromParser:
    """Класс для парсинга отзывов с сайта Drom.ru"""

    def __init__(self, db_path: str = "auto_reviews.db", proxy: str = None):
        """Инициализация парсера.

        Args:
            db_path: Путь к файлу базы данных SQLite
            proxy: Прокси-сервер в формате http://user:pass@host:port
        """
        self.browser = None
        self.context = None
        self.proxy = proxy
        self.reviews = []
        self.errors = []

        # Инициализация базы данных и репозитория
        self.db = Database(db_path)
        self.repository = ReviewRepository(self.db)

    def _calculate_content_hash(self, content: str) -> str:
        """Вычисляет хеш контента отзыва для проверки дубликатов.

        Args:
            content: Текст отзыва

        Returns:
            str: MD5 хеш контента
        """
        return hashlib.md5(content.encode()).hexdigest()

    def _review_exists(self, url: str, content: str) -> bool:
        """Проверяет существование отзыва в базе.

        Args:
            url: URL отзыва
            content: Текст отзыва

        Returns:
            bool: True если отзыв уже существует
        """
        with self.db.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 1 FROM reviews 
                WHERE url = ? OR content_hash = ?
                LIMIT 1
                """,
                (url, self._calculate_content_hash(content)),
            )
            return cursor.fetchone() is not None

    @staticmethod
    def extract_brand_model(url: str) -> tuple[str, str]:
        """Извлекает марку и модель из URL"""
        try:
            clean_url = url.split("?")[0].split("#")[0]
            path = urlparse(clean_url).path
            parts = [p for p in path.split("/") if p]

            for i, part in enumerate(parts):
                if part == "reviews" and i + 2 < len(parts):
                    return unquote(parts[i + 1]), unquote(parts[i + 2])

        except Exception as e:
            logger.error(f"Ошибка извлечения марки/модели из {url}: {e}")
        return "", ""

    @staticmethod
    def extract_review_from_schema(
        schema_data: Dict, url: str, brand: str, model: str
    ) -> Optional[Review]:
        """Извлекает данные отзыва из JSON-LD схемы"""
        try:
            # Сохраняем схему для анализа
            logger.debug(f"Обработка схемы: {json.dumps(schema_data, indent=2)}")

            if not isinstance(schema_data, dict):
                logger.debug("schema_data не является словарем")
                return None

            # Проверяем все возможные поля с контентом
            content = (
                schema_data.get("reviewBody", "")
                or schema_data.get("description", "")
                or schema_data.get("articleBody", "")
            )

            # Сохраняем схему в файл для анализа
            with open(f"error_logs/review_schema_{int(time.time())}.json", "w") as f:
                json.dump(schema_data, f, indent=2, ensure_ascii=False)

            if not content:
                logger.debug("Отсутствует описание отзыва")
                return None

            review_rating = schema_data.get("reviewRating", {})
            rating = None
            if isinstance(review_rating, dict):
                rating_value = review_rating.get("ratingValue")
                if rating_value:
                    try:
                        rating = float(rating_value)
                    except (ValueError, TypeError):
                        pass

            author_data = schema_data.get("author", {})
            author = (
                author_data.get("name") if isinstance(author_data, dict) else "Аноним"
            )

            date_str = schema_data.get("datePublished")
            publish_date = None
            if date_str:
                try:
                    publish_date = datetime.fromisoformat(date_str.split("T")[0])
                except (ValueError, TypeError):
                    pass

            item_reviewed = schema_data.get("itemReviewed", {})
            if isinstance(item_reviewed, dict):
                car_brand = item_reviewed.get("brand", {}).get("name", "")
                car_model = item_reviewed.get("model", "")
                if car_brand and car_model:
                    brand = car_brand
                    model = car_model

            review = Review(
                source="drom.ru",
                type="review",
                brand=brand or "unknown",
                model=model or "unknown",
                content=content,
                rating=rating,
                author=author or "Аноним",
                url=url,
                publish_date=publish_date,
            )

            logger.debug(f"Создан отзыв: {review}")
            return review

        except Exception as e:
            logger.error(f"Ошибка при разборе JSON-LD схемы: {e}")
            return None

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1.5, min=2, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def fetch_review_page(self, url: str) -> Optional[Dict]:
        """Получает данные страницы с отзывом"""
        logger.info(f"Загрузка страницы {url}")
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                executable_path=(
                    "/home/analityk/Документы/projects/parser_project/"
                    "chrome-linux/chrome"
                ),
            )

            context_args = {
                "user_agent": random.choice(USER_AGENTS),
                "extra_http_headers": HEADERS,
                "viewport": {"width": 1366, "height": 768},
            }
            if self.proxy:
                context_args["proxy"] = {"server": self.proxy}

            context = browser.new_context(**context_args)

            try:
                page = context.new_page()
                page.set_default_timeout(30000)
                page.set_default_navigation_timeout(45000)

                time.sleep(random.uniform(1.2, 2.2))
                page.goto(url, wait_until="domcontentloaded")

                h1 = page.locator("h1").first
                h1_text = h1.inner_text().strip()

                jsonld_data = []
                for script in page.locator("script[type='application/ld+json']").all():
                    try:
                        data = json.loads(script.inner_text().strip())
                        if isinstance(data, list):
                            jsonld_data.extend(data)
                        else:
                            jsonld_data.append(data)
                    except Exception:
                        continue

                return {"url": url, "title": h1_text, "schema_data": jsonld_data}

            finally:
                context.close()
                browser.close()

    def parse_page(self, url: str) -> List[Review]:
        """Парсит страницу с отзывом и сохраняет результаты в базу данных.

        Args:
            url: URL страницы с отзывом

        Returns:
            List[Review]: Список обработанных отзывов
        """
        reviews = []
        new_reviews = []

        try:
            logger.info(f"Начинаем парсинг страницы {url}")
            page_data = self.fetch_review_page(url)
            if not page_data:
                logger.error("Не удалось получить данные страницы")
                return []

            brand, model = self.extract_brand_model(url)
            logger.info(f"Извлечены марка/модель: {brand}/{model}")

            logger.info(f"Найдено JSON-LD схем: {len(page_data['schema_data'])}")
            for schema in page_data["schema_data"]:
                logger.info(f"Тип схемы: {schema.get('@type')}")

                if isinstance(schema, dict) and schema.get("@type") == "Review":
                    if review := self.extract_review_from_schema(
                        schema, url, brand, model
                    ):
                        # Проверяем существование отзыва
                        if review.content:
                            exists = self._review_exists(url, review.content)
                            if not exists:
                                # Добавляем хеш контента
                                content_hash = self._calculate_content_hash(
                                    review.content
                                )
                                review.content_hash = content_hash
                                # Сохраняем отзыв
                                if self.repository.save(review):
                                    new_reviews.append(review)
                                    logger.info(
                                        "Добавлен новый отзыв: "
                                        f"{review.brand} {review.model}"
                                    )
                            else:
                                logger.info(
                                    "Отзыв уже существует: "
                                    f"{review.brand} {review.model}"
                                )
                        reviews.append(review)

            logger.info(f"Получено отзывов: {len(reviews)} (новых: {len(new_reviews)})")
            return reviews

        except Exception as e:
            logger.error(f"Ошибка при парсинге {url}: {str(e)}")
            self.errors.append({"url": url, "error": str(e)})
            return []

    def parse_reviews(self, urls: Union[List[str], str]) -> List[Review]:
        """Парсит отзывы со всех указанных страниц"""
        if isinstance(urls, str):
            urls = [urls]

        for url in urls:
            if reviews := self.parse_page(url):
                self.reviews.extend(reviews)

        return self.reviews
