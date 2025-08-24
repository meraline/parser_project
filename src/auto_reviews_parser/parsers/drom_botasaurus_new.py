from typing import List, Optional
from bs4 import BeautifulSoup
from datetime import datetime
import time
import random

from botasaurus.request import request, Request, NotFoundException
from auto_reviews_parser.models.review import Review
from auto_reviews_parser.parsers.base import BaseParser, ParseError
from auto_reviews_parser.utils.logger import get_logger

logger = get_logger(__name__)


def parse_date(date_text: str) -> Optional[datetime]:
    """Парсит дату из строки."""
    try:
        return datetime.strptime(date_text, "%d %B %Y")
    except ValueError:
        return None


def extract_text(block) -> str:
    """Извлекает текст из HTML блока."""
    return block.get_text(strip=True) if block else ""


@request(
    cache=True, max_retry=3, retry_wait=1, raise_exception=False, create_error_logs=True
)
def scrape_html(url: str) -> str:
    """Получает HTML страницы."""
    response = Request().get(url)
    if response.status_code == 404:
        raise NotFoundException(url)
    response.raise_for_status()
    return response.text


def parse_review_page(html: str, brand: str, model: str) -> Review:
    """Парсит детальную страницу отзыва."""
    soup = BeautifulSoup(html, "lxml")
    try:
        review = Review(source="drom.ru", type="review", brand=brand, model=model)

        # Заголовок
        title = soup.select_one(".b-showcase__title")
        review.title = extract_text(title)

        # Рейтинг
        rating_elem = soup.select_one(".rating-item-overall")
        if rating_elem:
            try:
                review.rating = float(extract_text(rating_elem))
            except (ValueError, TypeError):
                pass

        # Текст отзыва
        text_blocks = soup.select(".b-text")
        if text_blocks:
            texts = [extract_text(block) for block in text_blocks]
            review.content = "\n".join(text for text in texts if text)

        # Достоинства и недостатки
        pros_cons = soup.select(".b-pros-n-cons")
        if len(pros_cons) >= 2:
            review.pros = extract_text(pros_cons[0])
            review.cons = extract_text(pros_cons[1])

        # Дата
        date_elem = soup.select_one(".b-post-meta")
        if date_elem:
            review.publish_date = parse_date(extract_text(date_elem))

        # Автор
        author = soup.select_one(".b-showcase__author a")
        review.author = extract_text(author)

        return review
    except Exception as e:
        logger.error(f"Error parsing review page: {e}")
        raise ParseError(f"Failed to parse review page: {e}")


class DromBotasaurusParser(BaseParser):
    """Parser for drom.ru using Botasaurus."""

    def extract_reviews_from_html(
        self, html: str, brand: str, model: str
    ) -> List[Review]:
        """Extract review links from HTML content."""
        soup = BeautifulSoup(html, "html.parser")
        reviews = []

        # Находим ссылки на отзывы
        for card in soup.select(".b-showcase__item"):
            link = card.select_one(".b-showcase__title a")
            if not link or not link.get("href"):
                continue

            url = f"https://www.drom.ru{link['href']}"
            try:
                review_html = scrape_html(url)
                review = parse_review_page(review_html, brand, model)
                review.url = url
                reviews.append(review)
            except Exception as e:
                logger.error(f"Failed to parse review {url}: {e}")

        return reviews

    def get_review_pages(self, brand: str, model: str) -> List[str]:
        """Get all review pages HTML."""
        pages_html = []
        page = 1

        while True:
            url_template = "https://www.drom.ru/reviews/{}/{}/page{}/"
            url = url_template.format(brand, model, page)

            try:
                html = scrape_html(url)
                if "not found" in html.lower():
                    break

                pages_html.append(html)
                page += 1

                time.sleep(random.uniform(1.0, 2.0))

            except NotFoundException:
                break
            except Exception as e:
                logger.error(f"Failed to get page {page}: {e}")
                break

        return pages_html

    def parse_reviews(self, brand: str, model: str) -> List[Review]:
        """Parse reviews for brand and model."""
        try:
            pages_html = self.get_review_pages(brand, model)

            all_reviews = []
            for html in pages_html:
                reviews = self.extract_reviews_from_html(html, brand, model)
                all_reviews.extend(reviews)

            return all_reviews

        except Exception as e:
            msg = f"Failed to parse reviews for {brand} {model}: {e}"
            logger.error(msg)
            raise ParseError(msg)
