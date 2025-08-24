from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup
from datetime import datetime
import time
import random

from botasaurus.browser import browser, Browser
from botasaurus.request import request, Request, NotFoundException
from auto_reviews_parser.models.review import Review
from auto_reviews_parser.parsers.base import BaseParser, ParseError
from auto_reviews_parser.utils.logger import get_logger
from auto_reviews_parser.utils.retry_decorator import retry

logger = get_logger(__name__)

# Конфигурация браузера
BROWSER_CONFIG = {
    "headless": True,
    "block_images": True,
    "block_resources": True,
}

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
def scrape_html_with_request(request: Request, url: str) -> str:
    """Получает HTML страницы через HTTP-запрос."""
    response = request.get(url)
    if response.status_code == 404:
        raise NotFoundException(url)
    response.raise_for_status()
    return response.text


@browser(**BROWSER_CONFIG)
def scrape_html_with_browser(browser, url: str) -> str:
    """Получает HTML страницы через браузер."""
    return browser.get(url)


@retry(max_attempts=3, delay=1)
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

    def __init__(self, use_browser: bool = False):
        """
        Инициализация парсера.

        Args:
            use_browser: Использовать браузер вместо HTTP-запросов
        """
        super().__init__()
        self.use_browser = use_browser
        self.scrape_html = (
            scrape_html_with_browser if use_browser else scrape_html_with_request
        )

    @request(**REQUEST_CONFIG)
    def extract_reviews_from_html(
        self, request: Request, html: str, brand: str, model: str
    ) -> List[Review]:
        """Extract review links from HTML content with parallel processing."""
        soup = BeautifulSoup(html, "html.parser")
        reviews = []
        tasks = []

        # Собираем все URL для параллельной обработки
        for card in soup.select(".b-showcase__item"):
            link = card.select_one(".b-showcase__title a")
            if not link or not link.get("href"):
                continue

            url = f"https://www.drom.ru{link['href']}"
            tasks.append({"url": url})

        # Параллельный парсинг отзывов
        results = request.get_many(tasks)
        for url, html in zip([t["url"] for t in tasks], results):
            try:
                review = parse_review_page(html, brand, model)
                review.url = url
                reviews.append(review)
            except Exception as e:
                logger.error(f"Failed to parse review {url}: {e}")

        return reviews

    @request(**REQUEST_CONFIG)
    def get_review_pages(self, request: Request, brand: str, model: str) -> List[str]:
        """Get all review pages HTML with parallel processing."""
        pages_html = []
        page = 1
        tasks = []

        # Сначала определяем количество страниц
        while True:
            url_template = "https://www.drom.ru/reviews/{}/{}/page{}/"
            url = url_template.format(brand, model, page)

            try:
                response = request.get(url)
                if response.status_code == 404 or "not found" in response.text.lower():
                    break

                tasks.append({"url": url})
                page += 1

            except Exception as e:
                logger.error(f"Failed to get page {page}: {e}")
                break

        # Параллельный сбор HTML страниц
        if tasks:
            pages_html = request.get_many(tasks)

        return [html for html in pages_html if html]

    def parse_reviews(self, brand: str, model: str) -> List[Review]:
        """Parse reviews for brand and model."""
        try:
            pages_html = self.get_review_pages({"brand": brand, "model": model})

            all_reviews = []
            for html in pages_html:
                reviews = self.extract_reviews_from_html(html, brand, model)
                all_reviews.extend(reviews)

            return all_reviews

        except Exception as e:
            msg = f"Failed to parse reviews for {brand} {model}: {e}"
            logger.error(msg)
            raise ParseError(msg)

    def parse_brand_model_reviews(self, data: Dict[str, Any]) -> List[Review]:
        """Parse reviews with browser support and parallel processing."""
        brand = data.get("brand", "")
        model = data.get("model", "")
        max_pages = data.get("max_pages", 10)

        all_reviews = []
        base_url = f"https://www.drom.ru/reviews/{brand}/{model}/"

        # Подготавливаем список URL для параллельного парсинга
        urls = []
        for page_num in range(1, max_pages + 1):
            url = f"{base_url}?page={page_num}" if page_num > 1 else base_url
            urls.append(url)

        # Параллельный парсинг страниц
        if self.use_browser:
            with bt.Browser(**BROWSER_CONFIG) as browser:
                for url in urls:
                    try:
                        page_content = browser.get(url)
                        if not page_content or "not found" in page_content.lower():
                            break

                        reviews = self.extract_reviews_from_html(
                            page_content, brand, model
                        )
                        if not reviews:
                            break

                        all_reviews.extend(reviews)
                        time.sleep(random.uniform(1, 2))

                    except Exception as e:
                        logger.error(f"Error on page {url}: {e}")
                        break
        else:
            # Используем параллельные HTTP-запросы
            pages_content = self.get_review_pages({"brand": brand, "model": model})
            for content in pages_content:
                if not content:
                    continue
                reviews = self.extract_reviews_from_html(content, brand, model)
                all_reviews.extend(reviews)

        return all_reviews
