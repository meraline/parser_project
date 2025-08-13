import re
from typing import Dict, List, Optional
from urllib.parse import urljoin

from botasaurus.browser import browser, Driver

from .base_parser import BaseParser
<<<<<<< HEAD:parsers/drive2_parser.py
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
>>>>>>> origin/codex/restructure-project-directory-and-update-imports:auto_reviews_parser/src/parsers/drive2_parser.py


class Drive2Parser(BaseParser):
    """Парсер отзывов и бортжурналов с Drive2.ru"""

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
        for content_type in ["experience", "logbook"]:
            try:
                type_reviews = self._parse_content_type(
                    driver, brand, model, content_type, max_pages // 2
                )
                reviews.extend(type_reviews)
                self.random_delay(5, 10)
            except Exception as e:
                logger.error(
                    f"Ошибка парсинга {content_type} Drive2.ru {brand} {model}: {e}"
                )
                self.session_stats["errors"] += 1
        return reviews

    def _parse_content_type(
        self, driver: Driver, brand: str, model: str, content_type: str, max_pages: int
    ) -> List[Review]:
        """Парсинг конкретного типа контента"""
        reviews: List[Review] = []
        if content_type == "experience":
            base_url = f"https://www.drive2.ru/experience/{brand}/{model}/"
            review_type = "review"
        else:
            base_url = f"https://www.drive2.ru/cars/{brand}/{model}/logbook/"
            review_type = "board_journal"

        print(f"  🔍 Drive2.ru: Парсинг {review_type} {brand} {model}")

        try:
            driver.google_get(base_url, bypass_cloudflare=True)
            self.random_delay(3, 7)
            if driver.select(".c-error") or "404" in driver.title:
                print(f"    ⚠️ Страница не найдена: {base_url}")
                return reviews

            current_page = 1
            while current_page <= max_pages:
                print(f"    📄 Страница {current_page} ({review_type})")
                if content_type == "experience":
                    cards = driver.select_all(".c-car-card")
                else:
                    cards = driver.select_all(".c-post-card") or driver.select_all(
                        ".c-logbook-card"
                    )

                if not cards:
                    print(f"    ⚠️ Контент не найден на странице {current_page}")
                    break

                page_reviews = 0
                for card in cards:
                    try:
                        review = self._parse_drive2_card(
                            card, brand, model, review_type, base_url
                        )
                        if review and not self.db.is_url_parsed(review.url):
                            reviews.append(review)
                            page_reviews += 1
                    except Exception as e:
                        self.session_stats["errors"] += 1
                        logger.error(f"Ошибка парсинга карточки Drive2: {e}")

                print(f"    ✓ Найдено {page_reviews} новых записей")

                next_link = driver.select(".c-pagination__next") or driver.select(
                    'a[rel="next"]'
                )
                if not next_link or "disabled" in next_link.get_attribute("class", ""):
                    print(f"    📋 Больше страниц нет")
                    break

                next_url = next_link.get_attribute("href")
                if next_url:
                    if not next_url.startswith("http"):
                        next_url = urljoin(base_url, next_url)
                    driver.google_get(next_url, bypass_cloudflare=True)
                    self.random_delay(3, 7)
                    current_page += 1
                else:
                    break
        except Exception as e:
            logger.error(
                f"Ошибка парсинга Drive2.ru {content_type} {brand} {model}: {e}"
            )
            self.session_stats["errors"] += 1

        print(
            f"  ✓ Drive2.ru: Собрано {len(reviews)} записей ({review_type}) для {brand} {model}"
        )
        return reviews

    def _parse_drive2_card(
        self, card, brand: str, model: str, review_type: str, base_url: str
    ) -> Optional[Review]:
        """Парсинг одной карточки Drive2"""
        try:
            review = Review(
                source="drive2.ru", type=review_type, brand=brand, model=model
            )

            title_link = (
                card.select("a.c-car-card__caption")
                or card.select("a.c-post-card__title")
                or card.select("h3 a")
            )
            if title_link:
                review.title = self.normalize_text(title_link.get_text())
                href = title_link.get_attribute("href")
                if href:
                    review.url = urljoin(base_url, href)

            author_elem = card.select(".c-username__link") or card.select(
                ".c-post-card__author"
            )
            if author_elem:
                review.author = self.normalize_text(author_elem.get_text())

            info_elem = card.select(".c-car-card__info") or card.select(
                ".c-post-card__car-info"
            )
            if info_elem:
                info_text = info_elem.get_text()
                review.year = self.extract_year(info_text)
                review.engine_volume = self.extract_engine_volume(info_text)
                review.mileage = self.extract_mileage(info_text)
                if "бензин" in info_text.lower():
                    review.fuel_type = "бензин"
                elif "дизель" in info_text.lower():
                    review.fuel_type = "дизель"
                if "автомат" in info_text.lower():
                    review.transmission = "автомат"
                elif "механик" in info_text.lower():
                    review.transmission = "механика"
                if "полный" in info_text.lower() or "4wd" in info_text.lower():
                    review.drive_type = "полный"
                elif "передний" in info_text.lower() or "fwd" in info_text.lower():
                    review.drive_type = "передний"
                elif "задний" in info_text.lower() or "rwd" in info_text.lower():
                    review.drive_type = "задний"

            mileage_elem = card.select(".c-car-card__param_mileage")
            if mileage_elem:
                mileage_text = mileage_elem.get_text()
                review.mileage = self.extract_mileage(mileage_text)

            preview_elem = card.select(".c-car-card__preview") or card.select(
                ".c-post-card__preview"
            )
            if preview_elem:
                review.content = self.normalize_text(preview_elem.get_text())

            views_elem = card.select(".c-post-card__views")
            if views_elem:
                views_text = views_elem.get_text()
                views_match = re.search(r"(\d+)", views_text)
                if views_match:
                    review.views_count = int(views_match.group(1))

            likes_elem = card.select(".c-post-card__likes")
            if likes_elem:
                likes_text = likes_elem.get_text()
                likes_match = re.search(r"(\d+)", likes_text)
                if likes_match:
                    review.likes_count = int(likes_match.group(1))

            date_elem = card.select(".c-post-card__date") or card.select(
                ".c-car-card__date"
            )
            if date_elem:
                date_text = date_elem.get_text()
                review.publish_date = self._parse_date(date_text)

            return review if review.url else None
        except Exception as e:
            logger.error(f"Ошибка парсинга карточки Drive2: {e}")
            return None
