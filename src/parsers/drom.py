import re
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseParser
from .models import ReviewData


class DromParser(BaseParser):
    """Parser for reviews from drom.ru pages."""

    async def parse_reviews(self, html: str, brand: str, model: str) -> List[ReviewData]:
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select('[data-ftid="component_reviews-item"], .css-1ksh4lf')
        reviews: List[ReviewData] = []
        for card in cards:
            review = ReviewData(source="drom.ru", type="review", brand=brand, model=model)

            title_link = card.select_one('h3 a, a[data-ftid="component_reviews-item-title"]')
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

            author_elem = card.select_one('.css-username, [data-ftid="component_username"]')
            if author_elem:
                review.author = self.text_extractor.normalize(author_elem.get_text())

            specs_elem = card.select_one('.css-1x4jntm, .css-car-info')
            if specs_elem:
                fields = self.extract_common_fields(specs_elem.get_text())
                for field, value in fields.items():
                    if value:
                        setattr(review, field, value)

            content_elem = card.select_one('.css-1wdvlz0, .review-preview')
            if content_elem:
                review.content = self.text_extractor.normalize(content_elem.get_text())

            date_elem = card.select_one('.css-date, [data-ftid="component_date"]')
            if date_elem:
                review.publish_date = self.text_extractor.parse_date(date_elem.get_text())

            if review.url:
                reviews.append(review)
        await self.random_delay()
        return reviews
