from typing import List, Optional, cast
from bs4 import BeautifulSoup, Tag
from datetime import datetime
from urllib.parse import urlparse, unquote
import httpx
import time
import random
import logging
import json
from src.auto_reviews_parser.models.review import Review
from src.auto_reviews_parser.utils.logger import get_logger

logger = get_logger(__name__, level=logging.DEBUG)


class DromParser:
    def __init__(self):
        self.reviews: List[Review] = []
        self.errors: List[dict] = []
        self.last_request_time = 0.0
        self.client = httpx.Client(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 "
                    "Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;"
                    "q=0.9,*/*;q=0.8"
                ),
                "Accept-Language": "ru,en-US;q=0.7,en;q=0.3",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
            },
            follow_redirects=True,
            timeout=30.0,
            http2=True,
        )

    def _get_soup(self, url: str) -> BeautifulSoup:
        """
        Получает BeautifulSoup объект для страницы.

        Args:
            url: URL страницы

        Returns:
            BeautifulSoup: объект для парсинга HTML
        """
        # Добавляем случайную задержку между запросами
        time_since_last = time.time() - self.last_request_time
        if time_since_last < 2:
            delay = 2 + random.uniform(0.5, 2.0) - time_since_last
            time.sleep(delay)
        
        try:
            headers = self.client.headers.copy()
            headers["Referer"] = "https://www.drom.ru/"
            response = self.client.get(url, headers=headers)
            response.raise_for_status()
            
            # Определяем кодировку из заголовка Content-Type или meta тегов
            encoding = response.encoding or 'windows-1251'
            
            # Декодируем контент с учетом кодировки
            html = response.content.decode(encoding, errors='ignore')
            soup = BeautifulSoup(html, "html.parser")
            
            logger.debug(f"HTML получен и декодирован с использованием {encoding}")
            return soup
            
        except Exception as e:
            logger.error(f"Ошибка при получении страницы {url}: {str(e)}")
            raise
        finally:
            self.last_request_time = time.time()

    def _get_page_html(self, url: str) -> str:
        """
        Получает HTML страницы с учетом задержек и повторных попыток.

        Args:
            url: URL страницы

        Returns:
            str: HTML контент страницы
        """
        time_since_last = time.time() - self.last_request_time
        if time_since_last < 2:
            delay = 2 + random.uniform(0.5, 2.0) - time_since_last
            time.sleep(delay)

        try:
            headers = self.client.headers.copy()
            headers["Referer"] = "https://www.drom.ru/"
            response = self.client.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        finally:
            self.last_request_time = time.time()    from typing import List, Optional, cast
from bs4 import BeautifulSoup, Tag
from datetime import datetime
import httpx
import time
import random
import logging
import json
from urllib.parse import urlparse, unquote
from src.auto_reviews_parser.models.review import Review
from src.auto_reviews_parser.utils.logger import get_logger

logger = get_logger(__name__, level=logging.DEBUG)

class DromParser:
    def __init__(self):
        self.reviews: List[Review] = []
        self.errors: List[dict] = []
        self.last_request_time = 0.0
        self.client = httpx.Client(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 "
                    "Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;"
                    "q=0.9,*/*;q=0.8"
                ),
                "Accept-Language": "ru,en-US;q=0.7,en;q=0.3",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
            },
            follow_redirects=True,
            timeout=30.0,
            http2=True,
        )

    def _get_soup(self, url: str) -> BeautifulSoup:
        """
        Получает BeautifulSoup объект для страницы.

        Args:
            url: URL страницы

        Returns:
            BeautifulSoup: объект для парсинга HTML
        """
        time_since_last = time.time() - self.last_request_time
        if time_since_last < 2:
            delay = 2 + random.uniform(0.5, 2.0) - time_since_last
            time.sleep(delay)
        
        try:
            headers = self.client.headers.copy()
            headers["Referer"] = "https://www.drom.ru/"
            response = self.client.get(url, headers=headers)
            response.raise_for_status()
            
            encoding = response.encoding or 'windows-1251'
            html = response.content.decode(encoding, errors='ignore')
            self.soup = BeautifulSoup(html, "html.parser")
            
            logger.debug(f"HTML получен и декодирован: {encoding}")
            return self.soup
            
        except Exception as e:
            logger.error(f"Ошибка при получении страницы {url}: {str(e)}")
            raise
        finally:
            self.last_request_time = time.time()

    def _parse_block(self, block: Tag, url: str) -> Optional[Review]:
        """
        Парсит блок отзыва.

        Args:
            block: HTML-блок с отзывом
            url: URL страницы

        Returns:
            Optional[Review]: Объект отзыва или None в случае ошибки
        """
        try:
            text = []
            rating = None
            author = ""
            publish_date = None

            # Поиск основного текста
            main_content = block.find(class_="b-text")
            if main_content:
                text.append(main_content.get_text(strip=True))

            # Поиск достоинств и недостатков
            for key in ["Достоинства", "Недостатки", "Поломки"]:
                elem = block.find("div", {"data-paragraph": key})
                if elem:
                    elem_text = elem.get_text(strip=True)
                    if elem_text:
                        text.append(f"{key}: {elem_text}")

            if not text:
                logger.debug("Текст отзыва не найден")
                return None

            # Поиск рейтинга
            rating_elem = block.find(class_="rating")
            if rating_elem:
                rating_text = rating_elem.get_text(strip=True)
                try:
                    if "/" in rating_text:
                        rating = float(rating_text.split("/")[0].replace(",", "."))
                    else:
                        rating = float(rating_text.replace(",", "."))
                except (ValueError, IndexError):
                    pass

            # Поиск автора
            author_elem = block.find(class_="u-login")
            if author_elem:
                author = author_elem.get_text(strip=True)

            # Поиск даты
            date_elem = block.find("time")
            if date_elem and isinstance(date_elem, Tag):
                try:
                    if datetime_str := str(date_elem.attrs.get("datetime", "")):
                        if "T" in datetime_str:
                            publish_date = datetime.fromisoformat(datetime_str.split("T")[0])
                except (ValueError, AttributeError, KeyError):
                    pass

            # Извлечение марки и модели из URL
            brand = model = "unknown"
            try:
                url_parts = urlparse(url).path.strip("/").split("/")
                reviews_index = url_parts.index("reviews")
                if len(url_parts) > reviews_index + 2:
                    brand = unquote(url_parts[reviews_index + 1])
                    model = unquote(url_parts[reviews_index + 2])
            except (ValueError, IndexError) as e:
                logger.debug(f"Не удалось извлечь марку/модель из URL: {str(e)}")

            return Review(
                source="drom.ru",
                type="review",
                brand=brand,
                model=model,
                content="\n".join(text).strip(),
                rating=rating,
                author=author or "Аноним",
                url=url,
                publish_date=publish_date
            )

        except Exception as e:
            logger.error(f"Ошибка при парсинге блока: {str(e)}")
            self.errors.append({"url": url, "error": str(e)})
            return None

    def parse_page(self, url: str) -> Optional[Review]:
        """
        Парсит страницу с отзывом.

        Args:
            url: URL страницы с отзывом

        Returns:
            Optional[Review]: Объект отзыва или None в случае ошибки
        """
        try:
            self.soup = self._get_soup(url)

            # Найдем блок с отзывом на странице
            for block_class in ["reviews-list", "c-car-forum", "forum-message"]:
                content_block = self.soup.find("div", class_=block_class)
                if content_block:
                    break

            if not content_block:
                logger.error(f"Не найден блок с отзывом на странице: {url}")
                self.errors.append({
                    "url": url,
                    "error": "Не найден блок с отзывом"
                })
                return None

            # Проверим наличие основных блоков
            main_content = content_block.find("div", class_="b-content") or content_block

            # Парсим отзыв
            if review := self._parse_block(main_content, url):
                logger.info(f"Успешно спарсен отзыв с URL: {url}")
                return review

            logger.error(f"Не удалось спарсить отзыв с URL: {url}")
            return None

        except Exception as e:
            logger.error(f"Ошибка при парсинге страницы {url}: {str(e)}")
            self.errors.append({"url": url, "error": str(e)})
            return None

    def parse_reviews(self, urls: List[str] | str) -> List[Review]:
        """
        Парсит отзывы по списку URL.

        Args:
            urls: URL или список URL-ов страниц с отзывами

        Returns:
            List[Review]: Список объектов отзывов
        """
        if isinstance(urls, str):
            urls = [urls]

        self.reviews = []
        for url in urls:
            if review := self.parse_page(url):
                self.reviews.append(review)

        return self.reviews

    def get_errors(self) -> List[dict]:
        """
        Возвращает список ошибок, возникших при парсинге.

        Returns:
            List[dict]: Список ошибок
        """
        return self.errors

    def _parse_json_review(self, review_data: dict, url: str) -> Optional[Review]:
        """
        Парсит отзыв из JSON данных.

        Args:
            review_data: Словарь с данными отзыва
            url: URL страницы

        Returns:
            Optional[Review]: Объект отзыва или None в случае ошибки
        """
        try:
            brand, model = self.extract_brand_model(url)
            if not brand or not model:
                return None

            review = Review(
                source="drom.ru", type="review", brand=brand, model=model, url=url
            )

            review.content = review_data.get("text", "")
            if rating_str := review_data.get("rating"):
                try:
                    review.rating = float(rating_str)
                except (ValueError, TypeError):
                    pass

            review.author = review_data.get("author", "")
            if date_str := review_data.get("date"):
                try:
                    review.publish_date = datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    pass

            return review
        except Exception as e:
            logger.error(f"Ошибка при парсинге JSON данных: {str(e)}")
            return None

    def _parse_block(self, block: Tag, url: str) -> Optional[Review]:
        """
        Парсит блок отзыва.

        Args:
            block: HTML-блок с отзывом
            url: URL страницы

        Returns:
            Optional[Review]: Объект отзыва или None в случае ошибки
        """
        try:
            text = []
            rating = None
            author = ""
            publish_date = None

            # Поиск основного текста
            main_content = block.find(class_="b-text")
            if main_content:
                text.append(main_content.get_text(strip=True))

            # Поиск достоинств и недостатков
            for key in ["Достоинства", "Недостатки", "Поломки"]:
                elem = block.find("div", {"data-paragraph": key})
                if elem:
                    elem_text = elem.get_text(strip=True)
                    if elem_text:
                        text.append(f"{key}: {elem_text}")

            if not text:
                logger.debug("Текст отзыва не найден")
                return None

            # Поиск рейтинга
            rating_elem = block.find(class_="rating")
            if rating_elem:
                rating_text = rating_elem.get_text(strip=True)
                try:
                    if "/" in rating_text:
                        rating = float(rating_text.split("/")[0].replace(",", "."))
                    else:
                        rating = float(rating_text.replace(",", "."))
                except (ValueError, IndexError):
                    pass

            # Поиск автора
            author_elem = block.find(class_="u-login")
            if author_elem:
                author = author_elem.get_text(strip=True)

            # Поиск даты
            date_elem = block.find("time")
            if date_elem and isinstance(date_elem, Tag):
                try:
                    if datetime_str := str(date_elem.attrs.get("datetime", "")):
                        if "T" in datetime_str:
                            publish_date = datetime.fromisoformat(datetime_str.split("T")[0])
                except (ValueError, AttributeError, KeyError):
                    pass

            # Извлечение марки и модели из URL
            brand = model = "unknown"
            try:
                url_parts = url.split("/")
                if len(url_parts) >= 2:
                    brand = url_parts[-2]
                    if len(url_parts) >= 3:
                        model = url_parts[-3]
            except Exception as e:
                logger.error(f"Ошибка при извлечении марки/модели из URL: {str(e)}")

            return Review(
                source="drom.ru",
                type="review",
                brand=brand,
                model=model,
                content="\n".join(text).strip(),
                rating=rating,
                author=author or "Аноним",
                url=url,
                publish_date=publish_date
            )

        except Exception as e:
            logger.error(f"Ошибка при парсинге блока: {str(e)}")
            self.errors.append({"url": url, "error": str(e)})
            return None

    def parse_page(self, url: str) -> Review | None:
        """
        Парсит страницу с отзывом.

        Args:
            url: URL страницы с отзывом

        Returns:
            Review | None: объект отзыва или None в случае ошибки
        """
        try:
            soup = self._get_soup(url)
            logger.debug(f"Получен HTML для {url}")

            brand, model = self.extract_brand_model(url)
            logger.debug(f"Извлечены марка {brand} и модель {model}")

            if not brand or not model:
                logger.warning("Не удалось извлечь марку и модель из URL")
                return None

            # Ищем основной контент отзыва
            main_content = soup.select_one(".b-master") or soup.select_one(".b-comments") or soup.select_one(".card")
            if not main_content:
                logger.debug("Не найден основной блок контента")
                return None
            
            logger.debug(f"Найден блок контента с классами: {main_content.get('class', [])}")

            review = self._parse_block(main_content, url)
            if review:
                review.brand = brand
                review.model = model
                review.url = url
                return review

            # Если основной контент не найден, пробуем искать в JSON
            json_data = None
            for script in soup.find_all("script"):
                script_text = getattr(script, "string", "") or script.text
                if script_text and "window.__starting_data" in script_text:
                    try:
                        json_text = (
                            script_text.split("window.__starting_data =")[1]
                            .split(";")[0]
                            .strip()
                        )
                        json_data = json.loads(json_text)
                        break
                    except Exception as e:
                        logger.error(f"Ошибка при парсинге JSON: {str(e)}")

            if json_data:
                review = self._parse_json_review(json_data, url)
                if review:
                    review.brand = brand
                    review.model = model
                    review.url = url
                    return review

            logger.warning("Не найдено отзывов на странице")
            return None

        except Exception as e:
            logger.error(f"Ошибка при парсинге страницы: {str(e)}", exc_info=True)
            return None

    def parse_reviews(self, urls: List[str] | str) -> List[Review]:
        """
        Парсит отзывы с нескольких страниц.

        Args:
            urls: URL страницы или список URL страниц с отзывами

        Returns:
            List[Review]: Список всех собранных отзывов
        """
        if isinstance(urls, str):
            urls = [urls]

        for url in urls:
            result = self.parse_page(url)
            if result:
                self.reviews.append(result)

        return self.reviews

    def get_errors(self) -> List[dict]:
        """
        Возвращает список ошибок, возникших при парсинге.

        Returns:
            List[dict]: Список ошибок с информацией о URL и деталях ошибки
        """
        return self.errors
