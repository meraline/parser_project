from typing import List, Optional, Dict, Any, Union
from bs4 import BeautifulSoup
from datetime import datetime
import time
import random

from botasaurus.request import request
from auto_reviews_parser.models.review import Review
from auto_reviews_parser.parsers.base import BaseParser, ParseError
from auto_reviews_parser.utils.logger import get_logger

logger = get_logger(__name__)

# Конфигурация запросов
REQUEST_CONFIG = {
    "cache": True,
    "max_retry": 5,
    "retry_wait": [1, 2, 3],
    "parallel": 3,
    "timeout": 30,
}


def parse_date(date_text: str) -> Optional[datetime]:
    """Парсит дату из строки."""
    try:
        return datetime.strptime(date_text, "%d %B %Y")
    except ValueError:
        return None


def extract_text(block) -> str:
    """Извлекает текст из HTML блока."""
    return block.get_text(strip=True) if block else ""


@request(**REQUEST_CONFIG)
def scrape_html(req, url: str) -> Optional[str]:
    """Получает HTML страницы через HTTP-запрос."""
    try:
        response = req.get(url)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"Failed to scrape {url}: {e}")
        return None


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
    """Parser for drom.ru using Botasaurus with advanced features."""

    @request(**REQUEST_CONFIG)
    def extract_reviews_batch(
        self, req, urls: List[str], brand: str, model: str
    ) -> List[Review]:
        """Извлекает отзывы из списка URL."""
        reviews = []
        for url in urls:
            try:
                html = scrape_html(req, url)
                if not html:
                    continue
                review = parse_review_page(html, brand, model)
                review.url = url
                reviews.append(review)
            except Exception as e:
                logger.error(f"Failed to parse review {url}: {e}")
        return reviews

    @request(**REQUEST_CONFIG)
    def extract_reviews_from_html(
        self, req, html: str, brand: str, model: str, batch_size: int = 5
    ) -> List[Review]:
        """Extract review links from HTML content."""
        soup = BeautifulSoup(html, "html.parser")
        reviews = []
        urls = []

        # Собираем URL отзывов
        for card in soup.select(".b-showcase__item"):
            link = card.select_one(".b-showcase__title a")
            if not link or not link.get("href"):
                continue
            urls.append(f"https://www.drom.ru{link['href']}")

        # Обрабатываем URL пакетами
        for i in range(0, len(urls), batch_size):
            batch = urls[i : i + batch_size]
            batch_reviews = self.extract_reviews_batch(
                req=req, urls=batch, brand=brand, model=model
            )
            reviews.extend(batch_reviews)

        return reviews

    @request(**REQUEST_CONFIG)
    def get_review_pages(
        self, req, brand: str, model: str, max_pages: int = 10
    ) -> List[str]:
        """Get all review pages HTML."""
        pages_html = []
        page = 1

        while page <= max_pages:
            url = (
                f"https://www.drom.ru/reviews/{brand}/{model}/"
                f"{'page' + str(page) if page > 1 else ''}"
            )

            try:
                html = scrape_html(req, url)
                if not html or "not found" in html.lower():
                    break

                pages_html.append(html)
                page += 1
                time.sleep(random.uniform(1.0, 2.0))

            except Exception as e:
                logger.error(f"Failed to get page {page}: {e}")
                break

        return pages_html

    def parse_reviews(self, brand: str, model: str) -> List[Review]:
        """Parse reviews for brand and model."""
        try:
            all_reviews = []

            @request(**REQUEST_CONFIG)
            def process_pages(req):
                nonlocal all_reviews
                pages_html = self.get_review_pages(req=req, brand=brand, model=model)

                for html in pages_html:
                    reviews = self.extract_reviews_from_html(
                        req=req, html=html, brand=brand, model=model
                    )
                    all_reviews.extend(reviews)

            process_pages()
            return all_reviews

        except Exception as e:
            msg = f"Failed to parse reviews for {brand} {model}: {e}"
            logger.error(msg)
            raise ParseError(msg)

    def parse_brand_model_reviews(self, data: Dict[str, Any]) -> List[Review]:
        """Parse reviews with parallel processing."""
        brand = data.get("brand", "")
        model = data.get("model", "")
        max_pages = data.get("max_pages", 10)
        all_reviews = []

        @request(**REQUEST_CONFIG)
        def process_reviews(req):
            nonlocal all_reviews
            pages_html = self.get_review_pages(
                req=req, brand=brand, model=model, max_pages=max_pages
            )

            for content in pages_html:
                if not content:
                    continue

                reviews = self.extract_reviews_from_html(
                    req=req, html=content, brand=brand, model=model
                )
                all_reviews.extend(reviews)

        process_reviews()
        return all_reviews
