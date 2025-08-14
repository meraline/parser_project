import logging
import re

from typing import Any, Dict, List

from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseParser
from ..models import Review
from ..utils.logger import get_logger

logger = get_logger(__name__)


logger = logging.getLogger(__name__)


class Drive2Parser(BaseParser):
    """Parser for reviews and logbook entries from drive2.ru."""

    async def parse_reviews(self, html: str, brand: str, model: str) -> List[Review]:
        soup = BeautifulSoup(html, "html.parser")
        reviews: List[Review] = []

        cards_exp = soup.select(".c-car-card")
        cards_log = soup.select(".c-post-card, .c-logbook-card")

        for card in cards_exp:
            reviews.append(self._parse_card(card, brand, model, "review"))
        for card in cards_log:
            reviews.append(self._parse_card(card, brand, model, "board_journal"))

        reviews = [r for r in reviews if r and r.url]
        await self.random_delay()
        return reviews

    def _parse_card(self, card, brand: str, model: str, review_type: str) -> Review:
        review = Review(source="drive2.ru", type=review_type, brand=brand, model=model)

        title_link = (
            card.select_one("a.c-car-card__caption")
            or card.select_one("a.c-post-card__title")
            or card.select_one("h3 a")
        )
        if title_link:
            review.title = self.text_extractor.normalize(title_link.get_text())
            href = title_link.get("href")
            if href:
                review.url = urljoin("https://www.drive2.ru", href)

        author_elem = card.select_one(".c-username__link, .c-post-card__author")
        if author_elem:
            review.author = self.text_extractor.normalize(author_elem.get_text())

        info_elem = card.select_one(".c-car-card__info, .c-post-card__car-info")
        if info_elem:
            fields = self.extract_common_fields(info_elem.get_text())
            for field, value in fields.items():
                if value:
                    setattr(review, field, value)

        mileage_elem = card.select_one(".c-car-card__param_mileage")
        if mileage_elem:
            mileage = self.text_extractor.extract_mileage(mileage_elem.get_text())
            if mileage:
                review.mileage = mileage

        preview_elem = card.select_one(".c-car-card__preview, .c-post-card__preview")
        if preview_elem:
            review.content = self.text_extractor.normalize(preview_elem.get_text())

        views_elem = card.select_one(".c-post-card__views")
        if views_elem:
            m = re.search(r"(\d+)", views_elem.get_text())
            if m:
                review.views_count = int(m.group(1))

        likes_elem = card.select_one(".c-post-card__likes")
        if likes_elem:
            m = re.search(r"(\d+)", likes_elem.get_text())
            if m:
                review.likes_count = int(m.group(1))

        date_elem = card.select_one(".c-post-card__date, .c-car-card__date")
        if date_elem:
            review.publish_date = self.text_extractor.parse_date(date_elem.get_text())

        return review

    async def parse_brand_model_reviews(self, data: Dict[str, Any]) -> List[Review]:
        """Парсинг отзывов для конкретной марки и модели"""
        brand = data.get("brand", "")
        model = data.get("model", "")
        max_pages = data.get("max_pages", 10)

        reviews = []

        # URL для поиска отзывов на drive2.ru
        base_url = f"https://www.drive2.ru/cars/{brand}/{model}/reviews/"

        for page in range(1, max_pages + 1):
            try:
                page_url = f"{base_url}?page={page}" if page > 1 else base_url

                # Используем botasaurus для получения HTML
                from botasaurus.request import request

                response = request.get(
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
