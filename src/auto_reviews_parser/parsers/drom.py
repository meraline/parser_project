"""Парсер отзывов с Drom.ru с использованием Playwright."""

from typing import Dict, List, Optional, Tuple
import logging
import os
import re
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright, Page
from bs4 import BeautifulSoup

from .base import BaseParser
from ..models.review import Review
from ..models.extended_review import ExtendedReview

logger = logging.getLogger(__name__)


class DromParser(BaseParser):
    """Парсер для сайта Drom.ru."""

    def __init__(self):
        """Инициализация парсера."""
        super().__init__()
        self.base_url = "https://www.drom.ru"
        self.selectors = {
            "review_link": "a[href*='/reviews/']",
            "next_page": ".b-pagination__next",
        }
        # Путь к локальному браузеру
        self.chrome_path = os.path.join(
            os.path.dirname(__file__), "../../../chrome-linux/chrome"
        )

    def parse_reviews(self, brand: str, model: str) -> List[Review]:
        """Parse reviews for given brand and model.

        Args:
            brand: Car brand name
            model: Car model name

        Returns:
            List of parsed reviews

        Raises:
            NetworkError: On network-related errors
            ParseError: On parsing errors
        """
        # Парсим отзывы конкретной модели
        return self.parse_catalog_model(brand, model)

    def _extract_brand_model(self, url: str) -> Tuple[str, str]:
        """Извлекает марку и модель из URL.

        Args:
            url: URL отзыва или каталога

        Returns:
            Кортеж (марка, модель)
        """
        path = urlparse(url).path
        parts = [p for p in path.split("/") if p]

        if len(parts) >= 3 and parts[0] == "reviews":
            return parts[1], parts[2]

        return "", ""

    def _is_review_url(self, url: str) -> bool:
        """Проверяет, является ли URL ссылкой на конкретный отзыв.

        Args:
            url: URL для проверки

        Returns:
            True, если это ссылка на отзыв с ID
        """
        if not url:
            return False

        # Обрабатываем как полные URL, так и относительные пути
        if url.startswith("https://www.drom.ru"):
            path = url[19:]  # убираем "https://www.drom.ru"
        elif url.startswith("/"):
            path = url
        else:
            return False

        if not path.startswith("/reviews/"):
            return False

        # Удаляем начальный /reviews/
        path = path[9:]  # убираем "/reviews/"

        # Разбиваем на части: brand/model/ID/ (может быть еще доп. части)
        parts = [p for p in path.split("/") if p]

        # Должно быть минимум 3 части: brand, model, ID
        if len(parts) < 3:
            return False

        # Третья часть должна быть числом (ID отзыва)
        try:
            int(parts[2])
            return True
        except ValueError:
            return False

    def _go_to_page(self, page: Page, url: str) -> None:
        """Переходит на страницу и ждет ее загрузки.

        Args:
            page: Объект страницы Playwright
            url: URL страницы
        """
        page.goto(url)
        page.wait_for_load_state("networkidle")

    def _extract_review_data(self, html_content: str, url: str) -> Dict:
        """Извлекает структурированные данные из HTML отзыва.

        Args:
            html_content: HTML содержимое страницы
            url: URL отзыва

        Returns:
            Словарь с извлеченными данными
        """
        soup = BeautifulSoup(html_content, "html.parser")
        data = {
            "url": url,
            "text": "",
            "rating": None,
            "owner_rating": None,
            "characteristics": {},
            "car_specs": {},
            "author": "",
            "views": 0,
            "comments": 0,
            "likes": 0,
        }

        try:
            # Извлекаем текст отзыва (основной отзыв)
            review_text = soup.find("div", {"itemprop": "reviewBody"})
            if review_text:
                data["text"] = review_text.get_text(strip=True)
            else:
                # Для дополнений к отзывам используем другой селектор
                editable_area = soup.find("div", class_="b-editable-area")
                if editable_area:
                    # Ищем текст внутри editable-area
                    text_content = editable_area.get_text(strip=True)
                    data["text"] = text_content

                    # Определяем тип контента
                    if "/reviews/" in url and url.count("/") >= 6:  # дополнение
                        data["content_type"] = "addition"
                    else:
                        data["content_type"] = "review"

            # Извлекаем рейтинг отзыва (от пользователей)
            rating_elem = soup.find("span", {"data-rating-mark-avg": True})
            if rating_elem:
                try:
                    data["rating"] = float(rating_elem.text.strip())
                except (ValueError, AttributeError):
                    pass

            # Извлекаем оценку автомобиля владельцем
            owner_rating = soup.find("span", {"itemprop": "ratingValue"})
            if owner_rating:
                try:
                    data["owner_rating"] = float(owner_rating.text.strip())
                except (ValueError, AttributeError):
                    pass

            # Извлекаем автора
            author_elem = soup.find("span", {"itemprop": "name"})
            if author_elem:
                data["author"] = author_elem.text.strip()
            else:
                # Для дополнений автор может быть в другом месте
                title_elem = soup.find("title")
                if title_elem:
                    # Автор часто в заголовке дополнения
                    title_text = title_elem.text.strip()
                    if "Toyota Camry" in title_text:
                        data["author"] = title_text

            # Извлекаем статистику
            stats = soup.find_all("span", class_="b-text-gray")
            if len(stats) >= 3:
                try:
                    data["views"] = int(stats[0].text.strip())
                    data["comments"] = int(stats[1].text.strip())
                    data["likes"] = int(stats[2].text.strip())
                except (ValueError, IndexError):
                    pass

            # Извлекаем характеристики автомобиля
            tables = soup.find_all("table", class_="drom-table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) == 2:
                        key = cells[0].text.strip().rstrip(":")
                        value = cells[1].text.strip()

                        # Разделяем характеристики и оценки
                        if key in [
                            "Внешний вид",
                            "Салон",
                            "Двигатель",
                            "Ходовые качества",
                        ]:
                            try:
                                data["characteristics"][key] = int(value)
                            except ValueError:
                                data["characteristics"][key] = value
                        else:
                            data["car_specs"][key] = value

        except Exception as e:
            logger.error(f"Ошибка при извлечении данных из {url}: {e}")

        return data

    def parse_catalog_model(
        self, brand: str, model: str, max_reviews: int = 1000
    ) -> List[Review]:
        """Парсит отзывы о конкретной модели.

        Args:
            brand: Название бренда
            model: Название модели
            max_reviews: Максимальное количество отзывов

        Returns:
            Список отзывов
        """
        reviews = []
        url = f"{self.base_url}/reviews/{brand.lower()}/{model.lower()}/"

        with sync_playwright() as p:
            # Используем локальный браузер если он доступен
            if os.path.exists(self.chrome_path):
                browser = p.chromium.launch(
                    executable_path=self.chrome_path, headless=True
                )
            else:
                browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            try:
                self._go_to_page(page, url)

                while len(reviews) < max_reviews:
                    # Получаем все ссылки на отзывы и сразу извлекаем href
                    links = page.evaluate(
                        """() => {
                            const links = Array.from(document.querySelectorAll('a[href*="/reviews/"]'));
                            return links.map(link => link.href);
                        }"""
                    )

                    review_urls = []
                    for href in links:
                        if len(review_urls) >= max_reviews:
                            break

                        # Фильтруем только ссылки на конкретные отзывы
                        if not self._is_review_url(href):
                            continue

                        # Дополнительная фильтрация - только отзывы этой модели
                        if f"/{brand.lower()}/{model.lower()}/" not in href:
                            continue

                        review_urls.append(href)

                    # Парсим каждый отзыв
                    for review_url in review_urls:
                        if len(reviews) >= max_reviews:
                            break

                        try:
                            self._go_to_page(page, review_url)
                            content = page.content()

                            # Извлекаем структурированные данные
                            structured_data = self._extract_review_data(
                                content, review_url
                            )

                            review_brand, review_model = self._extract_brand_model(
                                review_url
                            )

                            # Определяем тип контента
                            content_type = "review"
                            if review_url.count("/") >= 6:  # дополнение к отзыву
                                content_type = "addition"

                            review = Review(
                                source="drom.ru",
                                type=content_type,
                                url=review_url,
                                brand=review_brand,
                                model=review_model,
                                content=structured_data.get("text", ""),
                                author=structured_data.get("author", ""),
                                rating=structured_data.get("owner_rating"),
                                views_count=structured_data.get("views", 0),
                                comments_count=structured_data.get("comments", 0),
                                likes_count=structured_data.get("likes", 0),
                            )

                            reviews.append(review)
                            content_label = (
                                "дополнение" if content_type == "addition" else "отзыв"
                            )
                            print(
                                f"Парсим {content_label} {len(reviews)}: {review_url}"
                            )
                            print(f"  Тип: {content_type}")
                            print(f"  Автор: {review.author}")
                            print(f"  Рейтинг: {structured_data.get('rating')}")
                            print(f"  Оценка владельца: {review.rating}")
                            print(f"  Просмотры: {review.views_count}")
                            print(f"  Длина контента: {len(review.content)} символов")

                            # Показываем характеристики если есть
                            chars = structured_data.get("characteristics", {})
                            if chars:
                                print(f"  Характеристики: {chars}")

                        except Exception as e:
                            print(f"Ошибка при парсинге {review_url}: {e}")
                            continue

                    # Если получили нужное количество отзывов, выходим
                    if len(reviews) >= max_reviews:
                        break

                    # Переходим на следующую страницу
                    self._go_to_page(page, url)

                    next_button = page.query_selector(self.selectors["next_page"])
                    if not next_button:
                        break

                    next_button.click()
                    page.wait_for_load_state("networkidle")

            finally:
                browser.close()

        return reviews

    def parse_catalog(self, brand: str, max_reviews: int = 1000) -> List[Review]:
        """Парсит отзывы о бренде.

        Args:
            brand: Название бренда
            max_reviews: Максимальное количество отзывов

        Returns:
            Список отзывов
        """
        reviews = []
        url = f"{self.base_url}/reviews/{brand.lower()}/"

        with sync_playwright() as p:
            # Используем локальный браузер если он доступен
            if os.path.exists(self.chrome_path):
                browser = p.chromium.launch(
                    executable_path=self.chrome_path, headless=True
                )
            else:
                browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            try:
                self._go_to_page(page, url)

                while len(reviews) < max_reviews:
                    # Получаем ссылки на отзывы напрямую
                    links = page.query_selector_all(self.selectors["review_link"])

                    for link in links:
                        if len(reviews) >= max_reviews:
                            break

                        review_url = link.get_attribute("href")
                        if not review_url:
                            continue

                        # Фильтруем только ссылки на конкретные отзывы
                        # Структура: /reviews/brand/model/ID/
                        if not self._is_review_url(review_url):
                            continue

                        if not review_url.startswith("http"):
                            review_url = f"{self.base_url}{review_url}"

                        self._go_to_page(page, review_url)

                        content = page.content()
                        brand, model = self._extract_brand_model(review_url)

                        review = Review(
                            source="drom.ru",
                            type="review",
                            url=review_url,
                            brand=brand,
                            model=model,
                            content=content,
                        )
                        reviews.append(review)

                        if len(reviews) >= max_reviews:
                            break

                        self._go_to_page(page, url)

                    next_button = page.query_selector(self.selectors["next_page"])
                    if not next_button:
                        break

                    next_button.click()
                    page.wait_for_load_state("networkidle")

            finally:
                browser.close()

        return reviews
