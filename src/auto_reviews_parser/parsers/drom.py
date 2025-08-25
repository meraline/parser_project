"""Парсер отзывов с Drom.ru с использованием Playwright."""

from typing import Dict, List, Optional, Tuple
import logging
import os
import re
import time
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

    def __init__(self, gentle_mode=True):
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

        # Настройки щадящего режима
        self.gentle_mode = gentle_mode
        self.request_delay = 2.0 if gentle_mode else 0.5  # задержка между запросами
        self.page_delay = 3.0 if gentle_mode else 1.0  # задержка между страницами

    def parse_single_review(self, url: str) -> List[Review]:
        """Парсит один отзыв по прямому URL.

        Args:
            url: URL конкретного отзыва

        Returns:
            Список с одним отзывом или пустой список
        """
        if not self._is_review_url(url):
            print(f"❌ URL не является ссылкой на отзыв: {url}")
            return []

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

                # Разворачиваем скрытые блоки
                self._expand_hidden_blocks(page)

                # Получаем HTML после всех манипуляций
                html_content = page.content()

                # Извлекаем данные отзыва
                review_data = self._extract_review_data(html_content, url)

                if review_data["type"] == "review":
                    review = Review(
                        source="drom.ru",
                        type="review",
                        brand=review_data["brand"],
                        model=review_data["model"],
                        url=url,
                        content=review_data["content"],
                        author=review_data["author"],
                        rating=review_data.get("rating"),
                        # Новые поля для рейтингов
                        overall_rating=review_data.get("owner_rating"),
                        exterior_rating=review_data.get("exterior_rating"),
                        interior_rating=review_data.get("interior_rating"),
                        engine_rating=review_data.get("engine_rating"),
                        driving_rating=review_data.get("driving_rating"),
                        views_count=review_data.get("views", 0),
                    )
                    return [review]
                else:
                    print(f"❌ URL не содержит отзыв: {url}")
                    return []

            except Exception as e:
                print(f"❌ Ошибка при парсинге отзыва {url}: {e}")
                return []
            finally:
                browser.close()

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

    def _expand_hidden_blocks(self, page: Page) -> None:
        """Разворачивает скрытые блоки с характеристиками и оценками.

        Args:
            page: Объект страницы Playwright
        """
        try:
            # Простой подход - кликаем на все кнопки разворачивания
            buttons = page.query_selector_all('button[data-toggle="preview-dropdown"]')
            print(f"    Найдено кнопок для разворачивания: {len(buttons)}")

            for i, button in enumerate(buttons):
                try:
                    button.click()
                    print(f"    Кликнули на кнопку #{i+1}")
                    page.wait_for_timeout(500)  # небольшая пауза между кликами
                except Exception as e:
                    print(f"    Ошибка клика на кнопку #{i+1}: {e}")

            # Дополнительная пауза для завершения анимации
            page.wait_for_timeout(1000)

        except Exception as e:
            print(f"    Ошибка разворачивания блоков: {e}")

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
            "type": "review",  # Для одиночного отзыва всегда review
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
            # Извлекаем brand и model из URL
            brand, model = self._extract_brand_model(url)
            data["brand"] = brand
            data["model"] = model
            data["date"] = ""  # Будет заполнено ниже если найдено
            data["content"] = ""  # Будет заполнено ниже

            # Извлекаем текст отзыва (основной отзыв)
            review_text = soup.find("div", {"itemprop": "reviewBody"})
            if review_text:
                data["text"] = review_text.get_text(strip=True)
                data["content"] = data["text"]  # Для совместимости
            else:
                # Для дополнений к отзывам используем другой селектор
                editable_area = soup.find("div", class_="b-editable-area")
                if editable_area:
                    # Ищем текст внутри editable-area
                    text_content = editable_area.get_text(strip=True)
                    data["text"] = text_content
                    data["content"] = text_content  # Для совместимости

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
                    rating_value = float(owner_rating.text.strip())
                    data["owner_rating"] = rating_value
                    print(f"    Найдена общая оценка: {rating_value}")
                except (ValueError, AttributeError):
                    print(
                        f"    Ошибка парсинга общей оценки: {owner_rating.text if owner_rating else 'None'}"
                    )
            else:
                print("    Общая оценка не найдена")

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

            # Извлекаем характеристики автомобиля и оценки
            # Ищем все таблицы с классом drom-table (включая подклассы)
            all_tables = soup.find_all("table")
            tables = []
            for table in all_tables:
                table_classes = table.get("class", [])
                if any("drom-table" in str(cls) for cls in table_classes):
                    tables.append(table)

            print(f"    Найдено {len(tables)} таблиц drom-table")

            for i, table in enumerate(tables):
                print(f"    Обрабатываем таблицу #{i+1}")
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) == 2:
                        key = cells[0].text.strip().rstrip(":")
                        value = cells[1].text.strip()

                        # Оценки по категориям (числовые значения 1-10)
                        rating_keys = [
                            "Внешний вид",
                            "Салон",
                            "Двигатель",
                            "Ходовые качества",
                        ]
                        if key in rating_keys:
                            try:
                                # Проверяем, является ли это числовой оценкой
                                if value.isdigit() or (
                                    value.replace(".", "").isdigit()
                                ):
                                    numeric_value = int(float(value))
                                    data["characteristics"][key] = numeric_value
                                    print(f"    Найдена оценка {key}: {numeric_value}")
                                else:
                                    # Это техническая характеристика с тем же названием
                                    data["car_specs"][key] = value
                                    print(
                                        f"    Найдена тех. характеристика {key}: {value}"
                                    )
                            except ValueError:
                                # Если не число, сохраняем как техническую характеристику
                                data["car_specs"][key] = value
                                print(f"    Найдена характеристика {key}: {value}")
                        else:
                            # Технические характеристики автомобиля
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
                            # Щадящая задержка между отзывами
                            if self.gentle_mode:
                                time.sleep(self.request_delay)

                            self._go_to_page(page, review_url)

                            # Разворачиваем скрытые блоки с оценками
                            self._expand_hidden_blocks(page)

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
                            # Дополнение имеет доп. сегмент в URL после основного ID
                            url_parts = review_url.rstrip("/").split("/")
                            # Дополнение: .../camry/ID/ADDITION_ID/
                            # Основной: .../camry/ID/
                            if len(url_parts) > 7:
                                content_type = "addition"

                            # Извлекаем характеристики из car_specs
                            car_specs = structured_data.get("car_specs", {})
                            characteristics = structured_data.get("characteristics", {})

                            # Год выпуска
                            year = None
                            if "Год выпуска" in car_specs:
                                try:
                                    year = int(car_specs["Год выпуска"])
                                except (ValueError, TypeError):
                                    year = None

                            # Объем двигателя
                            engine_volume = None
                            if "Объем двигателя" in car_specs:
                                try:
                                    engine_str = (
                                        car_specs["Объем двигателя"]
                                        .replace("л", "")
                                        .strip()
                                    )
                                    engine_volume = float(engine_str)
                                except (ValueError, TypeError):
                                    engine_volume = None
                            elif "Двигатель" in characteristics:
                                # Извлекаем из "бензин, 2000 куб.см, 150 л.с."
                                engine_info = characteristics["Двигатель"]
                                if (
                                    isinstance(engine_info, str)
                                    and "куб.см" in engine_info
                                ):
                                    try:
                                        parts = engine_info.split(", ")
                                        for part in parts:
                                            if "куб.см" in part:
                                                volume_str = part.replace(
                                                    "куб.см", ""
                                                ).strip()
                                                engine_volume = (
                                                    float(volume_str) / 1000
                                                )  # переводим в литры
                                                break
                                    except (ValueError, TypeError):
                                        engine_volume = None

                            # Мощность двигателя
                            engine_power = None
                            if "Двигатель" in characteristics:
                                engine_info = characteristics["Двигатель"]
                                if (
                                    isinstance(engine_info, str)
                                    and "л.с." in engine_info
                                ):
                                    try:
                                        # Извлекаем "150 л.с." из "бензин, 2000 куб.см, 150 л.с."
                                        parts = engine_info.split(", ")
                                        for part in parts:
                                            if "л.с." in part:
                                                power_str = part.replace(
                                                    "л.с.", ""
                                                ).strip()
                                                engine_power = int(power_str)
                                                break
                                    except (ValueError, TypeError):
                                        engine_power = None

                            # Расход топлива
                            fuel_consumption_city = None
                            if "Расход топлива по городу" in car_specs:
                                try:
                                    city_str = car_specs["Расход топлива по городу"]
                                    city_str = city_str.replace("л/100км", "").strip()
                                    fuel_consumption_city = float(city_str)
                                except (ValueError, TypeError):
                                    fuel_consumption_city = None

                            fuel_consumption_highway = None
                            if "Расход топлива по трассе" in car_specs:
                                try:
                                    highway_str = car_specs["Расход топлива по трассе"]
                                    highway_str = highway_str.replace(
                                        "л/100км", ""
                                    ).strip()
                                    fuel_consumption_highway = float(highway_str)
                                except (ValueError, TypeError):
                                    fuel_consumption_highway = None

                            # Год приобретения
                            year_purchased = None
                            if "Год приобретения" in car_specs:
                                try:
                                    year_purchased = int(car_specs["Год приобретения"])
                                except (ValueError, TypeError):
                                    year_purchased = None

                            # Пробег
                            mileage = None
                            if "Пробег" in car_specs:
                                try:
                                    mileage_str = (
                                        car_specs["Пробег"]
                                        .replace("км", "")
                                        .replace(" ", "")
                                        .strip()
                                    )
                                    mileage = int(mileage_str)
                                except (ValueError, TypeError):
                                    mileage = None

                            # Извлекаем оценки по категориям
                            characteristics = structured_data.get("characteristics", {})
                            overall_rating = structured_data.get("owner_rating")
                            exterior_rating = characteristics.get("Внешний вид")
                            interior_rating = characteristics.get("Салон")
                            engine_rating = characteristics.get("Двигатель")
                            driving_rating = characteristics.get("Ходовые качества")

                            review = Review(
                                source="drom.ru",
                                type=content_type,
                                url=review_url,
                                brand=review_brand,
                                model=review_model,
                                content=structured_data.get("text", ""),
                                author=structured_data.get("author", ""),
                                rating=structured_data.get("owner_rating"),
                                overall_rating=overall_rating,
                                exterior_rating=exterior_rating,
                                interior_rating=interior_rating,
                                engine_rating=engine_rating,
                                driving_rating=driving_rating,
                                views_count=structured_data.get("views", 0),
                                comments_count=structured_data.get("comments", 0),
                                likes_count=structured_data.get("likes", 0),
                                year=year,
                                engine_volume=engine_volume,
                                engine_power=engine_power,
                                fuel_type=car_specs.get("Тип топлива", ""),
                                fuel_consumption_city=fuel_consumption_city,
                                fuel_consumption_highway=fuel_consumption_highway,
                                transmission=car_specs.get("Трансмиссия", ""),
                                body_type=car_specs.get("Тип кузова", ""),
                                drive_type=car_specs.get("Привод", ""),
                                steering_wheel=car_specs.get("Руль", ""),
                                year_purchased=year_purchased,
                                mileage=mileage,
                                generation=car_specs.get("Поколение", ""),
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

                            # Показываем car_specs если есть
                            car_specs = structured_data.get("car_specs", {})
                            if car_specs:
                                print(f"  Технические характеристики: {car_specs}")

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

                    # Щадящая задержка между страницами
                    if self.gentle_mode:
                        time.sleep(self.page_delay)

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

                        # Разворачиваем скрытые блоки с оценками
                        self._expand_hidden_blocks(page)

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

                    # Щадящая задержка между страницами
                    if self.gentle_mode:
                        time.sleep(self.page_delay)

            finally:
                browser.close()

        return reviews
