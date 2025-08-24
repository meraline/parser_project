from typing import List, Optional, Any
import time
from random import uniform
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, unquote
from src.auto_reviews_parser.models.review import Review
from src.auto_reviews_parser.utils.logger import get_logger

logger = get_logger(__name__)


class DromBotasaurusParser:
    def __init__(self):
        self.reviews: List[Review] = []
        self.errors = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def _get_page(self, url: str) -> Optional[str]:
        """
        Получает HTML-страницу с задержкой и повторными попытками.
        
        Args:
            url: URL страницы
            
        Returns:
            str: HTML-код страницы или None в случае ошибки
        """
        for attempt in range(3):  # 3 попытки
            try:
                # Случайная задержка от 2 до 5 секунд
                time.sleep(uniform(2, 5))
                
                response = self.session.get(url, timeout=30, verify=False)
                response.raise_for_status()
                return response.text
                
            except Exception as e:
                logger.error(f"Ошибка при загрузке страницы {url} (попытка {attempt + 1}): {str(e)}")
                if attempt == 2:  # Последняя попытка
                    self.errors.append({
                        "url": url,
                        "error": str(e)
                    })
                    return None
                    
        return None

    def parse_page(self, url: str) -> Optional[List[Review]]:
        """
        Парсит страницу с отзывами.
        
        Args:
            url: URL страницы с отзывами
            
        Returns:
            List[Review]: Список отзывов или None в случае ошибки
        """
        html = self._get_page(url)
        if not html:
            return None
            
        try:
            soup = BeautifulSoup(html, 'lxml')
            reviews_data = []
            
            # Находим все блоки отзывов
            review_blocks = soup.find_all('div', {'class': 'b-comments-item'})
            
            for block in review_blocks:
                try:
                    # Извлекаем текст отзыва
                    text_block = block.select_one('div.b-comments-item__text')
                    if not text_block:
                        continue
                    text = text_block.get_text(strip=True)
                    
                    # Извлекаем рейтинг
                    rating_block = block.select_one('div.b-comments-item__grade')
                    try:
                        rating = float(rating_block.get_text(strip=True)) if rating_block else None
                    except (ValueError, AttributeError):
                        rating = None
                    
                    # Извлекаем дату
                    date_block = block.select_one('time.b-comments-item__time')
                    date_str = date_block.get('datetime') if date_block else None
                    try:
                        date = datetime.fromisoformat(str(date_str)) if date_str else None
                    except ValueError:
                        date = None
                    
                    # Извлекаем автора
                    author_block = block.select_one('a.b-comments-item__author')
                    author = author_block.get_text(strip=True) if author_block else ""
                    
                    # Извлекаем марку и модель из URL
                    brand, model = self.extract_brand_model(url)
                    
                    # Создаем объект отзыва
                    review = Review(
                        source="drom.ru",
                        type="review",
                        brand=brand,
                        model=model,
                        content=text,
                        rating=rating,
                        author=author,
                        url=url,
                        publish_date=date
                    )
                    reviews_data.append(review)
                    
                except Exception as e:
                    logger.error(f"Ошибка при парсинге блока отзыва: {str(e)}")
                    self.errors.append({
                        "url": url,
                        "error": str(e),
                        "block": str(block)
                    })
                    continue
                    
            return reviews_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге страницы {url}: {str(e)}")
            self.errors.append({
                "url": url,
                "error": str(e)
            })
            return None
        parallel=True, max_retry=3, cache=True, delay=(2, 5), verify=False, timeout=30
    )
    def parse_review_page(self, url: str) -> Optional[List[Review]]:
        """
        Парсит страницу с отзывами.

        Args:
            url: URL страницы с отзывами

        Returns:
            List[Review]: Список отзывов или None в случае ошибки
        """
        try:
            soup = BeautifulSoup(self.html, "lxml")

            reviews_data = []
            review_blocks = soup.find_all("div", class_="b-comments-item")

            for block in review_blocks:
                try:
                    # Извлекаем текст отзыва
                    text_block = block.find("div", class_="b-comments-item__text")
                    if not text_block:
                        continue
                    text = text_block.get_text(strip=True)

                    # Извлекаем рейтинг
                    rating_block = block.find("div", class_="b-comments-item__grade")
                    rating = (
                        int(rating_block.get_text(strip=True)) if rating_block else None
                    )

                    # Извлекаем дату
                    date_block = block.find("time", class_="b-comments-item__time")
                    date = date_block.get("datetime") if date_block else None

                    # Извлекаем автора
                    author_block = block.find("a", class_="b-comments-item__author")
                    author = author_block.get_text(strip=True) if author_block else None

                    # Извлекаем марку и модель из URL
                    brand, model = self.extract_brand_model(url)

                    # Создаем объект отзыва
                    review = Review(
                        source="drom.ru",
                        type="review",
                        brand=brand,
                        model=model,
                        content=text,
                        rating=float(rating) if rating else None,
                        author=author or "",  # Пустая строка вместо None
                        url=url,
                        publish_date=datetime.fromisoformat(date) if date else None,
                    )
                    reviews_data.append(review)

                except Exception as e:
                    logger.error(f"Ошибка при парсинге блока отзыва: {str(e)}")
                    self.errors.append(
                        {"url": url, "error": str(e), "block": str(block)}
                    )
                    continue

            return reviews_data

        except Exception as e:
            logger.error(f"Ошибка при парсинге страницы {url}: {str(e)}")
            self.errors.append({"url": url, "error": str(e)})
            return None

    def extract_brand_model(self, url: str) -> tuple[str, str]:
        """
        Извлекает марку и модель из URL.

        Args:
            url: URL страницы отзыва

        Returns:
            tuple[str, str]: (марка, модель)
        """
        path = urlparse(url).path
        parts = path.strip("/").split("/")
        if len(parts) >= 4 and parts[1] == "reviews":
            return unquote(parts[2]), unquote(parts[3])
        return "", ""

    def parse_reviews(self, urls: List[str]) -> List[Review]:
        """
        Парсит отзывы с нескольких страниц параллельно.

        Args:
            urls: Список URL страниц с отзывами

        Returns:
            List[Review]: Список всех собранных отзывов
        """
        for url in urls:
            result = self.parse_review_page(url)
            if result:
                self.reviews.extend(result)

        return self.reviews

    def get_errors(self) -> List[dict]:
        """
        Возвращает список ошибок, возникших при парсинге.

        Returns:
            List[dict]: Список ошибок с информацией о URL и деталях ошибки
        """
        return self.errors
