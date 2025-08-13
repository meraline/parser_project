import re
from typing import Dict, List, Optional
from urllib.parse import urljoin

from botasaurus.browser import browser, Driver

from .base_parser import BaseParser
<<<<<<< HEAD:parsers/drom_parser.py
<<<<<<< HEAD
from .models import ReviewData
from src.utils.logger import get_logger
from src.utils.validators import validate_non_empty_string

logger = get_logger(__name__)
=======
from src.models.review import Review
>>>>>>> origin/codex/create-review-model-and-update-parsers
=======
from src.models import ReviewData
>>>>>>> origin/codex/restructure-project-directory-and-update-imports:auto_reviews_parser/src/parsers/drom_parser.py


class DromParser(BaseParser):
    """Парсер отзывов с Drom.ru"""

    @browser(
        block_images=True,
        cache=False,
        reuse_driver=True,
        max_retry=3,
        headless=True,
    )
    def parse_brand_model_reviews(self, driver: Driver, data: Dict) -> List[Review]:
        """Парсинг отзывов для конкретной марки и модели"""
        brand = validate_non_empty_string(data["brand"], "brand")
        model = validate_non_empty_string(data["model"], "model")
        max_pages = data.get("max_pages", 50)

        reviews: List[Review] = []
        base_url = f"https://www.drom.ru/reviews/{brand}/{model}/"

        try:
            print(f"  🔍 Drom.ru: Парсинг отзывов {brand} {model}")
            driver.google_get(base_url, bypass_cloudflare=True)
            self.random_delay(3, 7)

            if driver.select(".error-page") or "404" in driver.title:
                print(f"    ⚠️ Страница не найдена: {base_url}")
                return reviews

            current_page = 1
            while current_page <= max_pages:
                print(f"    📄 Страница {current_page}")

                review_cards = driver.select_all('[data-ftid="component_reviews-item"]')
                if not review_cards:
                    review_cards = driver.select_all(".css-1ksh4lf")
                if not review_cards:
                    print(f"    ⚠️ Отзывы не найдены на странице {current_page}")
                    break

                page_reviews = 0
                for card in review_cards:
                    try:
                        review = self._parse_review_card(card, brand, model, base_url)
                        if review and not self.db.is_url_parsed(review.url):
                            reviews.append(review)
                            page_reviews += 1
                    except Exception as e:
                        self.session_stats["errors"] += 1
                        logger.error(f"Ошибка парсинга карточки отзыва: {e}")

                print(f"    ✓ Найдено {page_reviews} новых отзывов")

                next_link = driver.select('a[rel="next"]')
                if not next_link:
                    print("    📋 Больше страниц нет")
                    break

                next_url = next_link.get_attribute("href")
                if next_url:
                    if not next_url.startswith("http"):
                        next_url = urljoin(base_url, next_url)
                    driver.get_via_this_page(next_url)
                    self.random_delay()
                    current_page += 1
                else:
                    break

            print(f"  ✓ Drom.ru: Собрано {len(reviews)} отзывов для {brand} {model}")
        except Exception as e:
            logger.error(f"Ошибка парсинга Drom.ru {brand} {model}: {e}")
            self.session_stats["errors"] += 1

        return reviews

    def _parse_review_card(
        self, card, brand: str, model: str, base_url: str
    ) -> Optional[Review]:
        """Парсинг одной карточки отзыва"""
        try:
            review = Review(
                source="drom.ru", type="review", brand=brand, model=model
            )

            title_link = card.select("h3 a") or card.select(
                'a[data-ftid="component_reviews-item-title"]'
            )
            if title_link:
                review.title = self.normalize_text(title_link.get_text())
                href = title_link.get_attribute("href")
                if href:
                    review.url = urljoin(base_url, href)

            rating_elem = card.select(".css-kxziuu") or card.select(
                '[data-ftid="component_rating"]'
            )
            if rating_elem:
                rating_text = rating_elem.get_text()
                rating_match = re.search(r"(\d+(?:\.\d+)?)", rating_text)
                if rating_match:
                    review.rating = float(rating_match.group(1))

            author_elem = card.select(".css-username") or card.select(
                '[data-ftid="component_username"]'
            )
            if author_elem:
                review.author = self.normalize_text(author_elem.get_text())

            specs_elem = card.select(".css-1x4jntm") or card.select(".css-car-info")
            if specs_elem:
                specs_text = specs_elem.get_text()
                review.year = self.extract_year(specs_text)
                review.engine_volume = self.extract_engine_volume(specs_text)
                review.mileage = self.extract_mileage(specs_text)
                if "бензин" in specs_text.lower():
                    review.fuel_type = "бензин"
                elif "дизель" in specs_text.lower():
                    review.fuel_type = "дизель"
                elif "гибрид" in specs_text.lower():
                    review.fuel_type = "гибрид"
                if "автомат" in specs_text.lower() or "акпп" in specs_text.lower():
                    review.transmission = "автомат"
                elif "механик" in specs_text.lower() or "мкпп" in specs_text.lower():
                    review.transmission = "механика"
                elif "вариатор" in specs_text.lower():
                    review.transmission = "вариатор"

            content_elem = card.select(".css-1wdvlz0") or card.select(".review-preview")
            if content_elem:
                review.content = self.normalize_text(content_elem.get_text())

            date_elem = card.select(".css-date") or card.select(
                '[data-ftid="component_date"]'
            )
            if date_elem:
                date_text = date_elem.get_text()
                review.publish_date = self._parse_date(date_text)

            return review if review.url else None
        except Exception as e:
            logger.error(f"Ошибка парсинга карточки отзыва Drom: {e}")
            return None
