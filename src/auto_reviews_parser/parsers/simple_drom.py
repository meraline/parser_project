"""Синхронная версия парсера для тестирования."""

import random
import time
from datetime import datetime
from typing import Dict, Optional
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page

from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository
from auto_reviews_parser.models.review import Review
from auto_reviews_parser.utils.logger import get_logger


logger = get_logger(__name__)

USER_AGENTS = [
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
]


class SimpleDromParser:
    """Упрощенный парсер для тестирования."""

    def __init__(self, db_path="auto_reviews.db"):
        """Инициализация парсера."""
        self.db_path = db_path
        self.db = Database(db_path)
        self.repository = ReviewRepository(self.db)
        self.base_url = "https://www.drom.ru"

        # Список User-Agent для эмуляции разных браузеров
        self.user_agents = [
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/128.0 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Firefox/122.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36",
        ]

        # Селекторы для каталога
        self.catalog_selectors = {
            "review_link": "a.vd61sn0",  # Ссылки на отзывы
            "review_author": "._1ngifes0",  # Автор отзыва
            "review_rating": ".vfprma1",  # Рейтинг отзыва
            "paginator": "a._1bpxqn0",  # Ссылки пагинации
        }

        # Селекторы для страницы отзыва
        self.review_selectors = {
            "full_text": ".x2bjsf0",  # Полный текст отзыва
            "author": ".j6kee50",  # Автор
            "rating": ".e1ei1k23",  # Рейтинг
            "pros": "._2iifd0",  # Плюсы
            "cons": "._2iifd0",  # Минусы
            "publish_date": ".e1ei1k21",  # Дата публикации
        }

    def __enter__(self):
        """Инициализация браузера с расширенными настройками эмуляции."""
        self.playwright = sync_playwright().start()

        # Настройка размера viewport
        self.browser = self.playwright.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"]
        )

        # Создание контекста с расширенными настройками
        self.context = self.browser.new_context(
            viewport={
                "width": random.randint(1280, 1920),
                "height": random.randint(800, 1080),
            },
            user_agent=random.choice(self.user_agents),
            java_script_enabled=True,
            accept_downloads=False,
            ignore_https_errors=True,
            locale="ru-RU",
        )

        # Установка cookies
        self.context.add_cookies(
            [
                {
                    "name": "viewed_reviews",
                    "value": "1",
                    "domain": ".drom.ru",
                    "path": "/",
                }
            ]
        )

        # Создание страницы
        self.page = self.context.new_page()

        # Установка дополнительных заголовков
        self.page.set_extra_http_headers(
            {
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "DNT": "1",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Закрытие браузера."""
        try:
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
        except Exception as e:
            logger.error(f"Ошибка при закрытии браузера: {e}")
        return False

    def get_full_review_content(self, url: str, page: Page) -> Dict:
        """Получает полное содержимое отзыва со страницы отзыва.

        Args:
            url: URL отзыва
            page: Объект страницы браузера

        Returns:
            dict: Данные отзыва или пустой словарь в случае ошибки
        """
        try:
            logger.info(f"Загрузка полного отзыва: {url}")

            # Пробуем загрузить страницу с таймаутом
            try:
                response = page.goto(url, timeout=30000)
                if not response:
                    logger.error(f"Не получен ответ от {url}")
                    return {}
                if not response.ok:
                    logger.error(
                        f"Ошибка загрузки {url}: "
                        f"{response.status} {response.status_text}"
                    )
                    return {}
            except Exception as e:
                logger.error(f"Ошибка при переходе на {url}: {e}")
                return {}

            # Эмулируем человеческое поведение
            logger.info("Эмуляция поведения пользователя...")

            # Случайная задержка перед действиями
            time.sleep(random.uniform(1.5, 3.0))

            # Сначала прокручиваем немного вниз
            page.mouse.wheel(0, 300)
            time.sleep(random.uniform(0.8, 1.5))

            # Ждем загрузки страницы
            logger.info("Ожидание загрузки страницы...")
            page.wait_for_load_state("networkidle", timeout=30000)
            page.wait_for_load_state("domcontentloaded", timeout=30000)

            # Делаем еще несколько прокруток с паузами
            for _ in range(3):
                page.mouse.wheel(0, random.randint(100, 300))
                time.sleep(random.uniform(0.5, 1.2))

            # Сделаем скриншот страницы для отладки
            logger.info("Создание скриншота страницы...")
            page.screenshot(path="debug_screenshot.png")

            # Финальная прокрутка для уверенности
            logger.info("Финальная прокрутка страницы...")
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(random.uniform(1.0, 2.0))

            # Ищем контент по разным селекторам
            selectors = [
                "div[data-ftid='component_review']",
                ".review-page",
                ".x2bjsf0",
                ".e130jyea0",
                ".o1y6ur0",
            ]
            content_loaded = False

            logger.info("Поиск контента отзыва...")
            for selector in selectors:
                try:
                    element = page.wait_for_selector(
                        selector, timeout=15000, state="visible"
                    )
                    if element:
                        logger.info(f"Найден селектор: {selector}")
                        content_loaded = True
                        break
                except Exception as e:
                    logger.debug(f"Селектор {selector} не найден")

            if not content_loaded:
                logger.error(
                    f"Не удалось найти текст отзыва на странице {url} "
                    f"по селекторам {', '.join(selectors)}"
                )
                return {}

            # Даем дополнительное время для полной загрузки страницы
            time.sleep(2)

            content = page.content()
            if not content:
                logger.error(f"Пустой контент страницы {url}")
                return {}

            soup = BeautifulSoup(content, "html.parser")

            # Основной текст отзыва (проверяем разные селекторы)
            review_text = (
                soup.select_one(".e130jyea0")  # Новый дизайн
                or soup.select_one(".o1y6ur0")  # Старый дизайн
                or soup.select_one(".c-text-block")  # Самый старый дизайн
            )
            full_text = review_text.text.strip() if review_text else ""

            if not full_text:
                logger.error("Не найден текст отзыва")
                return {}

            # Достоинства (проверяем разные селекторы)
            pros = soup.select_one(".e1wpcexe2") or soup.select_one(
                ".c-site-review-plus"
            )
            pros_text = pros.text.strip() if pros else ""

            # Недостатки (проверяем разные селекторы)
            cons = soup.select_one(".e1wpcexe3") or soup.select_one(
                ".c-site-review-minus"
            )
            cons_text = cons.text.strip() if cons else ""

            # Пробег (проверяем разные селекторы)
            mileage_el = soup.select_one(".e1wpcexe7") or soup.select_one(".mileage")
            mileage = None
            if mileage_el:
                try:
                    mileage_text = mileage_el.text.strip()
                    mileage = int("".join(filter(str.isdigit, mileage_text)))
                except (ValueError, AttributeError):
                    pass

            # Двигатель и характеристики
            engine_info = {}
            specs = soup.select(".e1wpcexe8") or soup.select(".c-car-specs dt")
            for spec in specs:
                text = spec.text.strip().lower()
                if "двигатель" in text:
                    engine_info["engine"] = text
                elif "коробка" in text or "кпп" in text:
                    engine_info["transmission"] = text
                elif "привод" in text:
                    engine_info["drive_type"] = text
                elif "кузов" in text or "тип кузова" in text:
                    engine_info["body_type"] = text

            # Год выпуска
            year = None
            title = soup.select_one("h1")
            if title:
                try:
                    year_str = title.text.split(",")[-1].strip()
                    year = int("".join(filter(str.isdigit, year_str)))
                except (IndexError, ValueError):
                    pass

            return {
                "content": full_text,
                "pros": pros_text,
                "cons": cons_text,
                "mileage": mileage,
                "year": year,
                **engine_info,
            }

        except Exception as e:
            logger.error(f"Ошибка при загрузке отзыва {url}: {e}")
            return {}

    def parse_catalog(
        self, brand: str, max_reviews: int = 5, max_pages_per_model: int = 2
    ) -> None:
        """Парсит отзывы для указанного бренда.

        Args:
            brand: Название бренда
            max_reviews: Максимальное количество отзывов
            max_pages_per_model: Максимальное количество страниц
        """
        brand_url = f"{self.base_url}/reviews/{brand.lower()}/"
        page = self.context.new_page()
        total = 0

        try:
            for page_num in range(1, max_pages_per_model + 1):
                if page_num > 1:
                    current_url = f"{brand_url}page{page_num}/"
                else:
                    current_url = brand_url

                logger.info(f"Обработка страницы каталога {page_num}")
                page.goto(current_url)
                page.wait_for_selector(self.catalog_selectors["review_link"])

                content = page.content()
                soup = BeautifulSoup(content, "html.parser")

                # Собираем отзывы
                for block in soup.select(".ahgfoy0"):
                    if total >= max_reviews:
                        return

                    try:
                        # Ссылка и заголовок
                        link = block.select_one(self.catalog_selectors["review_link"])
                        if not link:
                            continue

                        url = link.get("href", "")
                        if not isinstance(url, str) or not url:
                            continue

                        if not url.startswith("http"):
                            url = f"{self.base_url}{url}"

                        # Предварительные данные
                        title = link.text.strip()
                        model = title.split(",")[0].strip()

                        author = block.select_one(
                            self.catalog_selectors["review_author"]
                        )
                        author = author.text.strip() if author else "Аноним"

                        rating = block.select_one(
                            self.catalog_selectors["review_rating"]
                        )
                        try:
                            rating = float(rating.text.strip()) if rating else None
                        except (ValueError, AttributeError):
                            rating = None

                        # Получаем полные данные отзыва
                        full_data = self.get_full_review_content(url, page)
                        if not full_data:
                            logger.error(
                                "Не удалось загрузить отзыв. "
                                "Прерывание парсинга для безопасности."
                            )
                            return  # Прерываем весь парсинг

                        # Создаем объект отзыва
                        review = Review(
                            source="drom.ru",
                            type="review",
                            brand=brand,
                            model=model,
                            url=url,
                            author=author,
                            rating=rating,
                            content=full_data["content"],
                            pros=full_data.get("pros", ""),
                            cons=full_data.get("cons", ""),
                            year=full_data.get("year"),
                            mileage=full_data.get("mileage"),
                            engine_volume=full_data.get("engine", ""),
                            transmission=full_data.get("transmission", ""),
                            body_type=full_data.get("body_type", ""),
                            drive_type=full_data.get("drive_type", ""),
                            parsed_at=datetime.now(),
                        )

                        # Сохраняем в базу
                        self.repository.add(review)
                        total += 1
                        logger.info(
                            f"Добавлен отзыв {total}/{max_reviews} "
                            f"для {brand} - {model}"
                        )

                        # Небольшая задержка между отзывами
                        time.sleep(random.uniform(1, 2))

                    except Exception as e:
                        logger.error(f"Ошибка при обработке отзыва: {e}")
                        continue

                time.sleep(random.uniform(2, 3))

        except Exception as e:
            logger.error(f"Ошибка при парсинге каталога {brand}: {e}")
