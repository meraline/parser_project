import asyncio
import re

from typing import Any, Dict, List

from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseParser
from ..models import Review
from ..utils.logger import get_logger

logger = get_logger(__name__)


class DromParser(BaseParser):
    """Parser for reviews from drom.ru pages."""

    async def parse_reviews(self, html: str, brand: str, model: str) -> List[Review]:
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select('[data-ftid="component_reviews-item"], .css-1ksh4lf')
        reviews: List[Review] = []
        for card in cards:
            review = Review(source="drom.ru", type="review", brand=brand, model=model)

            title_link = card.select_one(
                'h3 a, a[data-ftid="component_reviews-item-title"]'
            )
            if title_link:
                review.title = self.text_extractor.normalize(title_link.get_text())
                href = title_link.get("href")
                if href:
                    review.url = urljoin("https://www.drom.ru", href)

            rating_elem = card.select_one('.css-kxziuu, [data-ftid="component_rating"]')
            if rating_elem:
                m = re.search(r"(\d+(?:\.\d+)?)", rating_elem.get_text())
                if m:
                    review.rating = float(m.group(1))

            author_elem = card.select_one(
                '.css-username, [data-ftid="component_username"]'
            )
            if author_elem:
                review.author = self.text_extractor.normalize(author_elem.get_text())

            specs_elem = card.select_one(".css-1x4jntm, .css-car-info")
            if specs_elem:
                fields = self.extract_common_fields(specs_elem.get_text())
                for field, value in fields.items():
                    if value:
                        setattr(review, field, value)

            content_elem = card.select_one(".css-1wdvlz0, .review-preview")
            if content_elem:
                review.content = self.text_extractor.normalize(content_elem.get_text())

            date_elem = card.select_one('.css-date, [data-ftid="component_date"]')
            if date_elem:
                review.publish_date = self.text_extractor.parse_date(
                    date_elem.get_text()
                )

            if review.url:
                reviews.append(review)
        await self.random_delay()
        return reviews

    async def parse_brand_model_reviews(self, data: Dict[str, Any]) -> List[Review]:
        """Парсинг отзывов для конкретной марки и модели"""
        brand = data.get("brand", "")
        model = data.get("model", "")
        max_pages = data.get("max_pages", 10)

        reviews = []

        # URL для поиска отзывов на drom.ru
        base_url = f"https://www.drom.ru/reviews/{brand}/{model}/"

        for page in range(1, max_pages + 1):
            try:
                page_url = f"{base_url}?page={page}" if page > 1 else base_url

                # Используем botasaurus для получения HTML
                from botasaurus.request import request

                response = await asyncio.to_thread(
                    request.get,
                    page_url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                    },
                )

                if response.status_code != 200:
                    break

                page_reviews = await self.parse_reviews(response.text, brand, model)
                reviews.extend(page_reviews)

                # Задержка между запросами
                await self.random_delay(3, 7)

            except Exception as e:
                logger.error(f"Ошибка парсинга страницы {page}: {e}")
                break

        return reviews
