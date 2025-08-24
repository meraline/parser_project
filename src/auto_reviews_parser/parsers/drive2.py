import re
import requests
from typing import Any, Dict, List
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from .base import BaseParser, NetworkError, ParseError
from ..models import Review
from ..utils.logger import get_logger

logger = get_logger(__name__)


class Drive2Parser(BaseParser):
    """Parser for reviews and logbook entries from drive2.ru."""

    def parse_reviews(self, html: str, brand: str, model: str) -> List[Review]:
        """Parse reviews from HTML content."""
        try:
            soup = BeautifulSoup(html, "html.parser")
            reviews: List[Review] = []

            # Парсим обзоры
            cards_exp = soup.select(".c-car-card")
            for card in cards_exp:
                review = self._parse_card(card, brand, model, "review")
                if review and review.url:
                    reviews.append(review)

            # Парсим записи бортжурнала
            cards_log = soup.select(".c-post-card, .c-logbook-card")
            for card in cards_log:
                review = self._parse_card(card, brand, model, "board_journal")
                if review and review.url:
                    reviews.append(review)

            # Добавляем задержку между запросами
            self.delay_manager.apply_delay()
            return reviews

        except Exception as e:
            logger.error(f"Ошибка при парсинге HTML: {e}")
            raise ParseError(f"Ошибка парсинга HTML: {e}")

    def _parse_card(self, card, brand: str, model: str, review_type: str) -> Review:
        """Parse a single review card."""
        review = Review(source="drive2.ru", type=review_type, brand=brand, model=model)

        # Извлекаем заголовок и URL
        title_link = (
            card.select_one("a.c-car-card__caption")
            or card.select_one("a.c-post-card__title")
            or card.select_one("h3 a")
        )
        if title_link:
            title = title_link.get_text()
            review.title = self.text_extractor.normalize(title)

            href = title_link.get("href")
            if href:
                review.url = urljoin("https://www.drive2.ru", str(href))

        # Извлекаем автора
        author_selector = ".c-username__link, " ".c-post-card__author"
        author_elem = card.select_one(author_selector)
        if author_elem:
            author = author_elem.get_text()
            review.author = self.text_extractor.normalize(author)

        # Извлекаем информацию об автомобиле
        info_selector = ".c-car-card__info, " ".c-post-card__car-info"
        info_elem = card.select_one(info_selector)
        if info_elem:
            text = info_elem.get_text()
            fields = self.extract_common_fields(text)
            for field, value in fields.items():
                if value:
                    setattr(review, field, value)

        # Извлекаем пробег
        mileage_elem = card.select_one(".c-car-card__param_mileage")
        if mileage_elem:
            text = mileage_elem.get_text()
            mileage = self.text_extractor.extract_mileage(text)
            if mileage:
                review.mileage = mileage

        # Извлекаем текст отзыва
        preview_selector = ".c-car-card__preview, " ".c-post-card__preview"
        preview_elem = card.select_one(preview_selector)
        if preview_elem:
            text = preview_elem.get_text()
            review.content = self.text_extractor.normalize(text)

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
            text = date_elem.get_text()
            review.publish_date = self.text_extractor.parse_date(text)

        return review

    def parse_brand_model_reviews(self, data: Dict[str, Any]) -> List[Review]:
        """Парсинг отзывов для конкретной марки и модели."""
        brand = data.get("brand", "")
        model = data.get("model", "")
        max_pages = data.get("max_pages", 10)

        reviews = []
        base_url = f"https://www.drive2.ru/cars/{brand}/{model}/reviews/"

        for page in range(1, max_pages + 1):
            try:
                # Формируем URL для текущей страницы
                page_url = f"{base_url}?page={page}" if page > 1 else base_url

                # Задаем User-Agent для запроса
                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36"
                    )
                }

                try:
                    # Выполняем запрос к странице
                    response = requests.get(page_url, headers=headers, timeout=30)
                    response.raise_for_status()
                except requests.RequestException as e:
                    raise NetworkError(f"Ошибка сети при парсинге {brand} {model}: {e}")

                if response.status_code != 200:
                    break

                try:
                    # Парсим отзывы со страницы
                    page_reviews = self.parse_reviews(response.text, brand, model)
                    reviews.extend(page_reviews)
                except ParseError as e:
                    logger.error(f"Ошибка парсинга страницы {page}: {e}")
                    break

                # Задержка между запросами
                self.delay_manager.apply_delay(3, 7)

            except (NetworkError, Exception) as e:
                logger.error(f"Ошибка парсинга страницы {page}: {e}")
                break

        return reviews
