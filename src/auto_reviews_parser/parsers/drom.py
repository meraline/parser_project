"""Парсер отзывов с Drom.ru."""

from typing import Dict, List, Optional, Union
import json
import time
import random
from datetime import datetime
from urllib.parse import urlparse, unquote
import hashlib

from playwright.sync_api import sync_playwright, Page, ProxySettings
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
       def parse_catalog(
        self, brand: str, max_reviews: int = 1000, max_pages: int = 5
    ) -> None:
        """Парсит отзывы для указанного бренда из каталога.

        Args:
            brand: Название бренда
            max_reviews: Максимальное количество отзывов
            max_pages: Максимальное количество страниц
        """
        url = f"{self.base_url}/reviews/{brand.lower()}/"
        self.total_reviews = 0

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    **self._get_browser_context_args()
                )
                page = context.new_page()
                
                # Загружаем страницу
                logger.info("Загружаем страницу каталога...")
                page.goto(url)
                page.wait_for_load_state("networkidle")
                
                # Получаем список отзывов
                reviews = []
                current_page = 1
                
                while current_page <= max_pages:
                    logger.info("Обработка страницы %d", current_page)
                    
                    # Ждем загрузки отзывов
                    page.wait_for_selector(self.selectors["review_block"])
                    
                    # Получаем все блоки отзывов
                    blocks = page.query_selector_all(
                        self.selectors["review_block"]
                    )
                    
                    for block in blocks:
                        if len(reviews) >= max_reviews:
                            break
                            
                        # Извлекаем данные отзыва
                        link = block.query_selector(
                            self.selectors["review_link"]
                        )
                        if not link:
                            continue
                            
                        href = link.get_attribute("href")
                        if not href:
                            continue
                            
                        review_url = (
                            href if href.startswith("http")
                            else f"{self.base_url}{href}"
                        )
                        
                        # Получаем детали отзыва
                        review_data = self._parse_review_page(
                            page, review_url
                        )
                        if review_data:
                            reviews.append(review_data)
                    
                    if len(reviews) >= max_reviews:
                        break
                        
                    # Проверяем следующую страницу
                    next_link = page.query_selector(
                        self.selectors["next_page"]
                    )
                    if not next_link:
                        break
                        
                    current_page += 1
                    page.click(self.selectors["next_page"])
                    page.wait_for_load_state("networkidle")
                
                # Сохраняем результаты
                for review_data in reviews:
                    if not self._review_exists(
                        review_data["url"],
                        review_data["content"]
                    ):
                        review = Review(**review_data)
                        self.repository.add(review)
                        self.total_reviews += 1
                
                logger.info(
                    "Завершен парсинг %s: собрано %d отзывов",
                    brand,
                    self.total_reviews
                )tion_type,
)

from ..models.review import Review
from ..utils.logger import get_logger
from ..database.base import Database
from ..database.repositories.review_repository import ReviewRepository


logger = get_logger(__name__)

HEADERS = {
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9," "image/webp,*/*;q=0.8"
    ),
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
}

USER_AGENTS = [
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"
    ),
]


class DromParser:
    """Класс для парсинга отзывов с сайта Drom.ru"""

    def __init__(
        self, db_path: str = "auto_reviews.db", proxy: Optional[ProxySettings] = None
    ):
        """Инициализация парсера.

        Args:
            db_path: Путь к файлу базы данных SQLite
            proxy: Настройки прокси в формате {"server": "http://host:port",
                   "username": "user", "password": "pass"}
        """
        self.proxy = proxy
        self.db_path = db_path
        self.total_reviews = 0  # Счетчик обработанных отзывов
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.base_url = "https://www.drom.ru"
        self.reviews: List[Review] = []
        self.errors: List[Dict] = []

        # Селекторы
        self.selectors = {
            "review_link": "a.vd61sn0",  # Ссылки на отзывы
            "review_author": "._1ngifes0",  # Автор отзыва
            "review_text": ".hxiweg0",  # Текст отзыва
            "review_rating": ".vfprma1",  # Рейтинг отзыва
            "paginator": "a._1bpxqn0",  # Ссылки пагинации
        }

        # Инициализация базы данных и репозитория
        self.db = Database(db_path)
        self.repository = ReviewRepository(self.db)

    def _get_browser_context_args(self) -> dict:
        """Получение аргументов для создания контекста браузера."""
        viewport_sizes = [
            {"width": 1920, "height": 1080},
            {"width": 1366, "height": 768},
            {"width": 1440, "height": 900},
        ]

        return {
            "viewport": random.choice(viewport_sizes),
            "user_agent": random.choice(USER_AGENTS),
            "proxy": {"server": self.proxy} if self.proxy else None,
            "extra_http_headers": {
                **HEADERS,
                "Sec-Ch-Ua": '"Chromium";v="112", "Not_A Brand";v="24"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Linux"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
            },
        }

    def __enter__(self):
        """Инициализация браузера при входе в контекст."""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        )
        self.context = self.browser.new_context(**self._get_browser_context_args())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Закрытие браузера при выходе из контекста."""
        try:
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
        except Exception as e:
            logger.error(f"Ошибка при закрытии браузера: {e}")

        return False  # Не подавляем исключения
        self.reviews: List[Review] = []
        self.errors: List[Dict] = []
        self.base_url = "https://www.drom.ru"

        # Инициализация базы данных и репозитория
        self.db = Database(db_path)
        self.repository = ReviewRepository(self.db)

        # Селекторы для различных элементов страницы
        self.selectors = {
            # Основные селекторы для списка отзывов
            "review_block": "div[data-ftid='component_review-item']",
            "review_link": "a[data-ftid='component_review-item-link']",
            "review_title": "div[data-ftid='component_review-item-title']",
            "review_author": "div[data-ftid='component_review-item-author']",
            "review_date": "div[data-ftid='component_review-item-date']",
            # Селекторы для страницы отзыва
            "review_content": "div[data-ftid='review-text']",
            "review_rating": "div[data-ftid='rating-value']",
            "review_pros": "div[data-ftid='review-pros']",
            "review_cons": "div[data-ftid='review-cons']",
            "review_year": "div[data-ftid='review-year']",
            # Навигация
            "paginator": "a[data-ftid='component_pagination-item']",
            "next_page": "a[data-ftid='component_pagination-next']",
            # Мета-информация
            "car_info": "div[data-ftid='component_review-vehicle-info']",
            "modification": "div[data-ftid='component_review-modification']",
        }

    def _wait_for_element(self, selector: str, timeout: int = 10000) -> Optional[str]:
        """Ожидает появления элемента и возвращает его текст.

        Args:
            selector: CSS селектор элемента
            timeout: Таймаут ожидания в миллисекундах

        Returns:
            Optional[str]: Текст элемента или None, если элемент не найден
        """
        try:
            element = self.page.wait_for_selector(
                selector, timeout=timeout, state="visible"
            )
            if element:
                return element.text_content()
            return None
        except Exception as e:
            logger.debug(f"Элемент не найден ({selector}): {e}")
            return None

    def _extract_review_data(self, review_block: BeautifulSoup) -> Optional[Dict]:
        """Извлекает данные отзыва из блока.

        Args:
            review_block: HTML блок с отзывом

        Returns:
            Optional[Dict]: Словарь с данными отзыва или None при ошибке
        """
        try:
            # Получаем базовые элементы
            link = review_block.select_one(self.selectors["review_link"])
            title = review_block.select_one(self.selectors["review_title"])
            author = review_block.select_one(self.selectors["review_author"])
            date = review_block.select_one(self.selectors["review_date"])

            if not all([link, title, author, date]):
                logger.warning("Не найдены обязательные элементы отзыва")
                return None

            # Извлекаем данные
            url = link.get("href", "")
            if not url.startswith("http"):
                url = f"{self.base_url}{url}"

            # Парсим дату
            try:
                date_str = date.text.strip()
                review_date = datetime.strptime(date_str, "%d.%m.%Y").strftime(
                    "%Y-%m-%d"
                )
            except ValueError:
                review_date = None
                logger.warning(f"Не удалось распарсить дату: {date_str}")

            return {
                "url": url,
                "title": title.text.strip(),
                "author": author.text.strip(),
                "date": review_date,
                "brand": "",  # Будет заполнено позже
                "model": "",  # Будет заполнено позже
            }
        except Exception as e:
            logger.error(f"Ошибка при извлечении данных отзыва: {e}")
            return None

    def _extract_rating(self, element) -> Optional[float]:
        """Извлекает рейтинг из элемента страницы.
        
        Args:
            element: Элемент с рейтингом
            
        Returns:
            Optional[float]: Рейтинг или None
        """
        try:
            if element:
                text = element.text_content().strip()
                return float(text)
            return None
        except (ValueError, AttributeError):
            return None
            
    def _get_optional_content(
        self, page, selector: str
    ) -> str:
        """Получает опциональный контент по селектору.
        
        Args:
            page: Объект страницы
            selector: CSS селектор
            
        Returns:
            str: Найденный текст или пустая строка
        """
        try:
            element = page.query_selector(selector)
            return element.text_content().strip() if element else ""
        except Exception:
            return ""
            
    def _calculate_content_hash(self, content: str) -> str:
        """Вычисляет хеш контента отзыва для проверки дубликатов.

        Args:
            content: Текст отзыва

        Returns:
            str: MD5 хеш контента
        """
        return hashlib.md5(content.encode()).hexdigest()

    def _parse_review_page(self, url: str, base_data: Dict) -> Optional[Dict]:
        """Парсит страницу отдельного отзыва.

        Args:
            url: URL страницы отзыва
            base_data: Базовые данные отзыва из списка

        Returns:
            Optional[Dict]: Полные данные отзыва или None при ошибке
        """
        try:
            # Переходим на страницу отзыва
            self.page.goto(url)
            time.sleep(random.uniform(2, 4))  # Случайная задержка

            # Получаем основной контент
            content = self._wait_for_element(self.selectors["review_content"])
            rating = self._wait_for_element(self.selectors["review_rating"])
            pros = self._wait_for_element(self.selectors["review_pros"])
            cons = self._wait_for_element(self.selectors["review_cons"])

            # Получаем информацию об автомобиле
            car_info = self._wait_for_element(self.selectors["car_info"])
            modification = self._wait_for_element(self.selectors["modification"])

            # Обновляем базовые данные
            review_data = {
                **base_data,
                "content": content.strip() if content else "",
                "rating": float(rating) if rating else None,
                "pros": pros.strip() if pros else "",
                "cons": cons.strip() if cons else "",
                "car_info": car_info.strip() if car_info else "",
                "modification": modification.strip() if modification else "",
            }

            return review_data

        except Exception as e:
            logger.error(f"Ошибка при парсинге страницы отзыва {url}: {e}")
            return None

    def _review_exists(self, url: str, content: str) -> bool:
        """Проверяет существование отзыва в базе.

        Args:
            url: URL отзыва
            content: Текст отзыва

        Returns:
            bool: True если отзыв уже существует
        """
        with self.db.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 1 FROM reviews
                WHERE url = ? OR content_hash = ?
                LIMIT 1
                """,
                (url, self._calculate_content_hash(content)),
            )
            return cursor.fetchone() is not None

    @staticmethod
    def _parse_reviews_list(self, url: str, max_pages: int = 5) -> List[Dict]:
        """Парсит список отзывов с пагинацией.
        
        Args:
            url: URL страницы со списком отзывов
            max_pages: Максимальное количество страниц для парсинга
            
        Returns:
            List[Dict]: Список отзывов
        """
        self.page = self.context.new_page()  # Создаем новую страницу
        reviews = []
        page = 1

        try:
            while page <= max_pages:
                page_url = f"{url}?page={page}" if page > 1 else url
                self.page.goto(page_url)
                time.sleep(random.uniform(2, 4))  # Случайная задержка

                # Ждем загрузки блоков с отзывами
                review_blocks = self.page.query_selector_all(
                    self.selectors["review_block"]
                )

                if not review_blocks:
                    logger.warning(f"Не найдены блоки отзывов на странице {page_url}")
                    break

                # Обрабатываем каждый блок
                for block in review_blocks:
                    html = block.inner_html()
                    soup = BeautifulSoup(html, "html.parser")

                    # Извлекаем базовые данные
                    review_data = self._extract_review_data(soup)
                    if review_data:
                        # Получаем полные данные отзыва
                        full_data = self._parse_review_page(
                            review_data["url"], review_data
                        )
                        if full_data:
                            reviews.append(full_data)

                # Проверяем наличие следующей страницы
                next_btn = self.page.query_selector(self.selectors["next_page"])
                if not next_btn:
                    break

                page += 1
                time.sleep(random.uniform(1, 2))  # Задержка перед следующей

        except Exception as e:
            logger.error(f"Ошибка при парсинге списка отзывов: {e}")

        return reviews

    @staticmethod
    def extract_brand_model(url: str) -> tuple[str, str]:
        """Извлекает марку и модель из URL"""
        try:
            clean_url = url.split("?")[0].split("#")[0]
            path = urlparse(clean_url).path
            parts = [p for p in path.split("/") if p]

            for i, part in enumerate(parts):
                if part == "reviews" and i + 2 < len(parts):
                    return unquote(parts[i + 1]), unquote(parts[i + 2])

        except Exception as e:
            logger.error(f"Ошибка извлечения марки/модели из {url}: {e}")
        return "", ""

    def parse_catalog(
        self, brand: str, max_reviews: int = 1000, max_pages: int = 5
    ) -> None:
        """Парсит отзывы для указанного бренда из каталога.

        Args:
            brand: Название бренда
            max_reviews: Максимальное количество отзывов
            max_pages: Максимальное количество страниц
        """
        brand_url = f"{self.base_url}/reviews/{brand.lower()}/"
        total_reviews = 0

        try:
            with self:
                reviews = self._parse_reviews_list(brand_url, max_pages=max_pages)

                for data in reviews:
                    if total_reviews >= max_reviews:
                        logger.info("Достигнут лимит отзывов: %d", total_reviews)
                        break

                    brand_name, model_name = self.extract_brand_model(data["url"])

                    review = Review(
                        url=data["url"],
                        brand=brand_name or brand,
                        model=model_name,
                        author=data["author"],
                        rating=data["rating"],
                        content=data["content"],
                        pros=data.get("pros", ""),
                        cons=data.get("cons", ""),
                        date=data["date"],
                        car_info=data.get("car_info", ""),
                        modification=data.get("modification", ""),
                    )

                    if not self._review_exists(review.url, review.content):
                        self.repository.add(review)
                        total_reviews += 1
                        logger.info("Добавлен отзыв: %s %s", review.brand, review.model)
                    break

                try:
                    # Преобразуем данные в объект Review
                    review_obj = Review(
                        source="drom.ru",
                        type="review",
                        brand=brand,
                        model=review.get("title", "").split(",")[0],
                        content=review.get("text", ""),
                        rating=float(review["rating"]) if "rating" in review else None,
                        author=review.get("author", "Аноним"),
                        url=review.get("url", ""),
                        publish_date=None,  # Дата будет получена при парсинге
                    )

                    # Проверяем на дубликаты
                    if not self._review_exists(review_obj.url, review_obj.content):
                        # Сохраняем в базу
                        self.repository.add(review_obj)
                        logger.info(
                            "Сохранен новый отзыв: %s",
                            review_obj.url
                        )

                except Exception as e:
                    logger.error("Ошибка при обработке отзыва: %s", e)

            logger.info(
                "Сохранено %d новых отзывов для %s",
                self.total_reviews,
                brand
            )

        except Exception as e:
            logger.error(
                "Ошибка при парсинге каталога %s: %s",
                brand,
                e
            )

    def get_reviews_from_catalog(
        self, brand_url: str, max_pages: int = 5
    ) -> List[Dict[str, str]]:
        """Получает список отзывов из каталога.

        Args:
            brand_url: URL каталога бренда
            max_pages: Максимальное количество страниц для обработки

        Returns:
            List[Dict[str, str]]: Список отзывов с их данными
        """
        reviews = []
        page_num = 1
        current_url = brand_url

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                proxy=self.proxy if self.proxy else None,
            )

            with context.new_page() as page:
                while page_num <= max_pages:
                    try:
                        # Загружаем страницу
                        page.goto(str(current_url))
                        page.wait_for_selector(self.selectors["review_link"])

                        content = page.content()
                        soup = BeautifulSoup(content, "html.parser")

                        # Обрабатываем отзывы на странице
                        blocks = soup.select(".ahgfoy0")  # Блоки отзывов
                        for block in blocks:
                            review = {}

                            # Заголовок и URL
                            link = block.select_one(self.selectors["review_link"])
                            if link:
                                href = link.get("href", "")
                                if isinstance(href, str) and href:
                                    if not href.startswith("http"):
                                        href = f"{self.base_url}{href}"
                                    review["url"] = href
                                    review["title"] = link.text.strip()

                            # Автор
                            author = block.select_one(self.selectors["review_author"])
                            if author:
                                review["author"] = author.text.strip()

                            # Текст
                            text = block.select_one(self.selectors["review_text"])
                            if text:
                                review["text"] = text.text.strip()

                            # Рейтинг
                            rating = block.select_one(self.selectors["review_rating"])
                            if rating:
                                try:
                                    value = float(rating.text.strip())
                                    review["rating"] = value
                                except ValueError:
                                    review["rating"] = None

                            if review:
                                reviews.append(review)

                        # Проверяем следующую страницу
                        next_page = soup.select(self.selectors["paginator"])[-1]
                        if not next_page or "следующая" not in next_page.text.lower():
                            break

                        next_url = next_page.get("href", "")
                        if next_url and not str(next_url).startswith("http"):
                            next_url = f"{self.base_url}{next_url}"
                        current_url = next_url
                        page_num += 1

                        # Небольшая задержка между страницами
                        time.sleep(random.uniform(1, 3))

                    except Exception as e:
                        logger.error(f"Ошибка при обработке страницы {page_num}: {e}")
                        break

        return reviews

    @staticmethod
    def extract_review_from_schema(
        schema_data: Dict, url: str, brand: str, model: str
    ) -> Optional[Review]:
        """Извлекает данные отзыва из JSON-LD схемы"""
        try:
            # Сохраняем схему для анализа
            schema_str = json.dumps(schema_data, indent=2)
            logger.debug(f"Обработка схемы: {schema_str}")

            if not isinstance(schema_data, dict):
                logger.debug("schema_data не является словарем")
                return None

            # Проверяем все возможные поля с контентом
            content = (
                schema_data.get("reviewBody", "")
                or schema_data.get("description", "")
                or schema_data.get("articleBody", "")
            )

            # Сохраняем схему в файл для анализа
            filename = f"error_logs/review_schema_{int(time.time())}.json"
            with open(filename, "w") as f:
                json.dump(schema_data, f, indent=2, ensure_ascii=False)
            if not content:
                logger.debug("Отсутствует описание отзыва")
                return None

            review_rating = schema_data.get("reviewRating", {})
            rating = None
            if isinstance(review_rating, dict):
                rating_value = review_rating.get("ratingValue")
                if rating_value:
                    try:
                        rating = float(rating_value)
                    except (ValueError, TypeError):
                        pass

            author = "Аноним"
            author_data = schema_data.get("author", {})
            if isinstance(author_data, dict):
                author = author_data.get("name", author)

            date_str = schema_data.get("datePublished")
            publish_date = None
            if date_str:
                try:
                    pub_date = date_str.split("T")[0]
                    publish_date = datetime.fromisoformat(pub_date)
                except (ValueError, TypeError):
                    pass

            item_reviewed = schema_data.get("itemReviewed", {})
            if isinstance(item_reviewed, dict):
                brand_data = item_reviewed.get("brand", {})
                if isinstance(brand_data, dict):
                    brand = brand_data.get("name", brand)
                model = item_reviewed.get("model", model)

            review = Review(
                source="drom.ru",
                type="review",
                brand=brand or "unknown",
                model=model or "unknown",
                content=content,
                rating=rating,
                author=author or "Аноним",
                url=url,
                publish_date=publish_date,
            )

            logger.debug(f"Создан отзыв: {review}")
            return review

        except Exception as e:
            logger.error(f"Ошибка при разборе JSON-LD схемы: {e}")
            return None

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1.5, min=2, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def fetch_review_page(self, url: str) -> Optional[Dict]:
        """Получает данные страницы с отзывом"""
        logger.info(f"Загрузка страницы {url}")
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                executable_path=(
                    "/home/analityk/Документы/projects/parser_project/"
                    "chrome-linux/chrome"
                ),
            )

            context_args = {
                "user_agent": random.choice(USER_AGENTS),
                "extra_http_headers": HEADERS,
                "viewport": {"width": 1366, "height": 768},
            }
            if self.proxy:
                context_args["proxy"] = {"server": self.proxy}

            context = browser.new_context(**context_args)

            try:
                page = context.new_page()
                page.set_default_timeout(30000)
                page.set_default_navigation_timeout(45000)

                time.sleep(random.uniform(1.2, 2.2))
                page.goto(url, wait_until="domcontentloaded")

                html = page.content()

                h1 = page.locator("h1").first
                h1_text = h1.inner_text().strip()

                jsonld_data = []
                scripts = page.locator("script[type='application/ld+json']").all()
                for script in scripts:
                    try:
                        data = json.loads(script.inner_text().strip())
                        if isinstance(data, list):
                            jsonld_data.extend(data)
                        else:
                            jsonld_data.append(data)
                    except Exception:
                        continue

                return {
                    "url": url,
                    "title": h1_text,
                    "schema_data": jsonld_data,
                    "content": html,
                }

            finally:
                context.close()
                browser.close()

    def parse_page(self, url: str) -> List[Review]:
        """Парсит страницу с отзывом и сохраняет результаты в базу данных.

        Args:
            url: URL страницы с отзывом

        Returns:
            List[Review]: Список обработанных отзывов
        """
        reviews = []
        new_reviews = []

        try:
            page_data = self.fetch_review_page(url)
            if not page_data:
                logger.error(f"Не удалось загрузить страницу {url}")
                return []

            # Извлекаем марку/модель из URL
            brand, model = self.extract_brand_model(url)
            logger.info(f"Извлечены марка/модель: {brand}/{model}")

            schema_count = len(page_data["schema_data"])
            logger.info(f"Найдено схем JSON-LD: {schema_count}")

            for schema in page_data["schema_data"]:
                if schema.get("@type") == "Review":
                    logger.info("Тип схемы: Review")
                    try:
                        review = self.extract_review_from_schema(
                            schema, url, brand, model
                        )
                        if not review:
                            logger.warning("Не удалось извлечь отзыв из схемы")
                            continue

                        if not review.content:
                            continue

                        exists = self._review_exists(url, review.content)
                        if not exists:
                            # Добавляем хеш контента
                            content_hash = self._calculate_content_hash(review.content)
                            review.content_hash = content_hash

                            # Сохраняем отзыв
                            if self.repository.save(review):
                                new_reviews.append(review)
                                brand_model = f"{review.brand} {review.model}"
                                logger.info(f"Добавлен новый отзыв: {brand_model}")
                        else:
                            brand_model = f"{review.brand} {review.model}"
                            logger.info(f"Отзыв уже существует: {brand_model}")
                        reviews.append(review)

                    except Exception as e:
                        logger.error(f"Ошибка при обработке схемы: {e}")
                else:
                    logger.info(f"Тип схемы: {schema.get('@type')}")

            new_count = len(new_reviews)
            total = len(reviews)
            logger.info(f"Получено отзывов: {total} (новых: {new_count})")
            return reviews

        except Exception as e:
            logger.error(f"Ошибка при парсинге страницы {url}: {e}")
            return []
        try:
            logger.info(f"Начинаем парсинг страницы {url}")
            page_data = self.fetch_review_page(url)
            if not page_data:
                logger.error("Не удалось получить данные страницы")
                return []

            brand, model = self.extract_brand_model(url)
            logger.info(f"Извлечены марка/модель: {brand}/{model}")
            reviews = []

            schema_count = len(page_data["schema_data"])
            logger.info(f"Найдено схем JSON-LD: {schema_count}")

            for schema in page_data["schema_data"]:
                schema_type = schema.get("@type", "Unknown")
                logger.info(f"Тип схемы: {schema_type}")

                is_review = isinstance(schema, dict) and schema.get("@type") in {
                    "Review",
                    "UserReview",
                }
                if is_review:
                    if review := self.extract_review_from_schema(
                        schema, url, brand, model
                    ):
                        reviews.append(review)
                        brand_model = f"{review.brand} {review.model}"
                        logger.info(f"Добавлен отзыв: {brand_model}")

            return reviews

        except Exception as e:
            logger.error(f"Ошибка при парсинге {url}: {str(e)}")
            self.errors.append({"url": url, "error": str(e)})
            return []

    def get_brand_models_urls(self, brand_url: str) -> List[Dict[str, str]]:
        """Получает список моделей бренда и количество отзывов.

        Args:
            brand_url: URL страницы бренда (например, drom.ru/reviews/toyota/)

        Returns:
            List[Dict[str, str]]: Список моделей с url и кол-вом отзывов
        """
        logger.info(f"Получение списка моделей для {brand_url}")
        models = []
        try:
            page_data = self.fetch_review_page(brand_url)
            if not page_data:
                return []

            # Находим список моделей
            soup = BeautifulSoup(page_data["content"], "html.parser")

            # Каждая модель представлена в формате "Model Name count"
            models_list = soup.select(".b-mainAutoList a")
            for model in models_list:
                url = model["href"]
                name = model.text.strip()

                # Извлекаем количество отзывов из текста
                parts = name.split("(")
                if len(parts) == 2:
                    name = parts[0].strip()
                    count = parts[1].strip(")").strip()
                    models.append({"name": name, "url": url, "count": count})

            return models
        except Exception as e:
            logger.error(f"Ошибка при получении моделей {brand_url}: {e}")
            return []

    def get_review_urls(
        self, model_url: str, max_reviews: Optional[int] = None
    ) -> List[str]:
        """Получает список URL всех отзывов для модели.

        Args:
            model_url: URL страницы модели
            max_reviews: Максимальное количество отзывов для сбора

        Returns:
            List[str]: Список URL отзывов
        """
        urls = []
        page = 1

        while True:
            if max_reviews and len(urls) >= max_reviews:
                logger.info(f"Достигнут лимит отзывов: {len(urls)}/{max_reviews}")
                break

            try:
                url = f"{model_url}page{page}/"
                page_data = self.fetch_review_page(url)
                if not page_data:
                    break

                # Находим все ссылки на отзывы
                soup = BeautifulSoup(page_data["content"], "html.parser")
                reviews = soup.select(".b-modelLine a")
                if not reviews:
                    break

                for review in reviews:
                    urls.append(review["href"])
                    if max_reviews and len(urls) >= max_reviews:
                        break

                # Проверяем наличие следующей страницы
                next_page = soup.select_one(".b-pagination a.next")
                if not next_page:
                    break

                page += 1
                logger.info(f"Обработана страница {page}, найдено {len(urls)}")

            except Exception as e:
                logger.error(f"Ошибка при получении отзывов {model_url}: {e}")
                break

        return urls

    def parse_brand(
        self, brand_url: str, max_reviews_per_model: Optional[int] = None
    ) -> List[Review]:
        """Парсит отзывы для бренда.

        Args:
            brand_url: URL страницы бренда
            max_reviews_per_model: Ограничение отзывов для каждой модели

        Returns:
            List[Review]: Список собранных отзывов
        """
        all_reviews = []

        # Получаем список моделей
        models = self.get_brand_models_urls(brand_url)
        logger.info(f"Найдено {len(models)} моделей")

        for model_info in models:
            model_name = model_info["name"]
            model_url = model_info["url"]
            model_count = int(model_info["count"])

            logger.info(
                f"Обработка модели {model_name}\n" f"(всего отзывов: {model_count})"
            )

            # Получаем URL отзывов с ограничением
            review_urls = self.get_review_urls(
                model_url, max_reviews=max_reviews_per_model
            )

            logger.info(f"Собрано {len(review_urls)} URL для {model_name}")

            # Парсим каждый отзыв
            for url in review_urls:
                try:
                    if reviews := self.parse_page(url):
                        all_reviews.extend(reviews)
                except Exception as e:
                    logger.error(f"Ошибка при парсинге {url}: {str(e)}")
                    continue

            logger.info(f"Обработано {len(review_urls)} отзывов для {model_name}")

        return all_reviews

    def parse_reviews(self, urls: Union[List[str], str]) -> List[Review]:
        """Парсит отзывы со всех указанных страниц"""
        if isinstance(urls, str):
            urls = [urls]

        for url in urls:
            if reviews := self.parse_page(url):
                self.reviews.extend(reviews)

        return self.reviews
