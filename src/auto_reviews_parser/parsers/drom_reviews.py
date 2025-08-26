#!/usr/bin/env python3
"""
🚗 ПАРСЕР ОТЗЫВОВ DROM.RU - ОБНОВЛЕННАЯ ВЕРСИЯ
============================================

Парсер для извлечения отзывов с сайта drom.ru с поддержкой:
- Длинных отзывов (подробные обзоры)
- Коротких отзывов (краткие мнения)
- Нормализованной базы данных
- Каталогизации брендов и моделей

Автор: AI Assistant
Дата: 2024
"""

import requests
import json
import time
import logging
import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from ..database.schema import DatabaseManager


class DromReviewsParser:
    """Парсер отзывов с drom.ru с поддержкой двух типов отзывов"""

    def __init__(self, delay: float = 1.0):
        self.base_url = "https://www.drom.ru"
        self.delay = delay
        self.session = requests.Session()
        self.db_manager = DatabaseManager()

        # Настройка логирования
        self.logger = logging.getLogger(__name__)

        # Отключаем прокси для избежания SOCKS ошибок
        self.session.proxies = {}
        
        # Headers для имитации браузера
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

    def _make_request(self, url: str) -> Optional[BeautifulSoup]:
        """Выполнение HTTP запроса с обработкой ошибок"""
        try:
            self.logger.info(f"Запрос к {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            time.sleep(self.delay)
            return soup

        except requests.RequestException as e:
            self.logger.error(f"Ошибка запроса к {url}: {e}")
            return None

    def parse_long_reviews(
        self, brand_url_name: str, model_url_name: str, max_pages: int = 10
    ) -> List[Dict]:
        """
        Парсинг длинных отзывов (подробные обзоры)
        URL формат: https://www.drom.ru/reviews/toyota/4runner/
        """
        reviews = []
        page = 1

        while page <= max_pages:
            url = (
                f"{self.base_url}/reviews/{brand_url_name}/"
                f"{model_url_name}/?page={page}"
            )
            soup = self._make_request(url)

            if not soup:
                break

            # Поиск контейнеров отзывов
            review_items = soup.find_all("div", {"data-ftid": "review-item"})

            if not review_items:
                self.logger.info(f"Нет отзывов на странице {page}")
                break

            self.logger.info(
                f"Найдено {len(review_items)} " f"длинных отзывов на странице {page}"
            )

            for item in review_items:
                try:
                    review_data = self._extract_long_review_data(item)
                    if review_data:
                        reviews.append(review_data)
                except Exception as e:
                    self.logger.warning(f"Ошибка извлечения отзыва: {e}")
                    continue

            page += 1

        self.logger.info(f"Всего извлечено {len(reviews)} длинных отзывов")
        return reviews

    def parse_short_reviews(
        self, brand_url_name: str, model_url_name: str, max_pages: int = 10
    ) -> List[Dict]:
        """
        Парсинг коротких отзывов
        URL формат: https://www.drom.ru/reviews/toyota/4runner/5kopeek/
        """
        reviews = []
        page = 1

        while page <= max_pages:
            url = (
                f"{self.base_url}/reviews/{brand_url_name}/"
                f"{model_url_name}/5kopeek/?page={page}"
            )
            soup = self._make_request(url)

            if not soup:
                break

            # Поиск контейнеров коротких отзывов
            review_items = soup.find_all("div", {"data-ftid": "short-review-item"})

            if not review_items:
                self.logger.info(f"Нет коротких отзывов на странице {page}")
                break

            self.logger.info(
                f"Найдено {len(review_items)} " f"коротких отзывов на странице {page}"
            )

            for item in review_items:
                try:
                    review_data = self._extract_short_review_data(item)
                    if review_data:
                        reviews.append(review_data)
                except Exception as e:
                    self.logger.warning(f"Ошибка извлечения короткого отзыва: {e}")
                    continue

            page += 1

        self.logger.info(f"Всего извлечено {len(reviews)} коротких отзывов")
        return reviews

    def _extract_long_review_data(self, item) -> Optional[Dict]:
        """Извлечение данных из длинного отзыва"""
        try:
            review_data = {}

            # ID отзыва
            review_id = item.get("id")
            if review_id:
                review_data["review_id"] = review_id

            # Заголовок
            title_elem = item.find("h3")
            if title_elem:
                review_data["title"] = title_elem.get_text(strip=True)

            # Основной контент
            content_parts = []

            # Ищем секции с текстом
            content_sections = item.find_all("div", class_="css-6hj46s")
            for section in content_sections:
                text = section.get_text(strip=True)
                if text:
                    content_parts.append(text)

            if content_parts:
                review_data["content"] = "\n".join(content_parts)

            # Плюсы
            positive_elem = item.find("div", {"data-ftid": "review-content__positive"})
            if positive_elem:
                review_data["positive_text"] = positive_elem.get_text(strip=True)

            # Минусы
            negative_elem = item.find("div", {"data-ftid": "review-content__negative"})
            if negative_elem:
                review_data["negative_text"] = negative_elem.get_text(strip=True)

            # Поломки
            breakages_elem = item.find(
                "div", {"data-ftid": "review-content__breakages"}
            )
            if breakages_elem:
                review_data["breakages_text"] = breakages_elem.get_text(strip=True)

            # Информация об авторе и дате
            author_info = self._extract_author_info(item)
            if author_info:
                review_data.update(author_info)

            # Характеристики автомобиля
            car_info = self._extract_car_info(item)
            if car_info:
                review_data.update(car_info)

            # Оценки
            ratings = self._extract_ratings(item)
            if ratings:
                review_data.update(ratings)

            # Количество фотографий
            photos = item.find_all("img")
            review_data["photos_count"] = len(photos) if photos else 0

            return review_data if review_data else None

        except Exception as e:
            self.logger.error(f"Ошибка извлечения длинного отзыва: {e}")
            return None

    def _extract_short_review_data(self, item) -> Optional[Dict]:
        """Извлечение данных из короткого отзыва"""
        try:
            review_data = {}

            # ID отзыва
            review_id = item.get("id")
            if review_id:
                review_data["review_id"] = review_id

            # Информация об авторе
            author_elem = item.find("span", class_="css-1u4ddp")
            if author_elem:
                review_data["author_name"] = author_elem.get_text(strip=True)

            # Город
            city_elem = item.find("span", {"data-ftid": "short-review-city"})
            if city_elem:
                review_data["author_city"] = city_elem.get_text(strip=True)

            # Год и характеристики автомобиля
            year_elem = item.find("span", {"data-ftid": "short-review-item__year"})
            if year_elem:
                try:
                    review_data["car_year"] = int(year_elem.get_text(strip=True))
                except ValueError:
                    pass

            volume_elem = item.find("span", {"data-ftid": "short-review-item__volume"})
            if volume_elem:
                try:
                    review_data["car_engine_volume"] = float(
                        volume_elem.get_text(strip=True)
                    )
                except ValueError:
                    pass

            # Плюсы
            positive_elem = item.find(
                "div", {"data-ftid": "short-review-content__positive"}
            )
            if positive_elem:
                review_data["positive_text"] = positive_elem.get_text(strip=True)

            # Минусы
            negative_elem = item.find(
                "div", {"data-ftid": "short-review-content__negative"}
            )
            if negative_elem:
                review_data["negative_text"] = negative_elem.get_text(strip=True)

            # Поломки
            breakages_elem = item.find(
                "div", {"data-ftid": "short-review-content__breakages"}
            )
            if breakages_elem:
                review_data["breakages_text"] = breakages_elem.get_text(strip=True)

            # Количество фотографий
            photos = item.find_all("img", class_="css-1e2elm8")
            review_data["photos_count"] = len(photos) if photos else 0

            return review_data if review_data else None

        except Exception as e:
            self.logger.error(f"Ошибка извлечения короткого отзыва: {e}")
            return None

    def _extract_author_info(self, item) -> Dict:
        """Извлечение информации об авторе"""
        author_info = {}

        # Поиск имени автора
        author_elem = item.find("span", class_="css-1u4ddp")
        if author_elem:
            author_info["author_name"] = author_elem.get_text(strip=True)

        # Дата отзыва
        date_elem = item.find("time")
        if date_elem:
            author_info["review_date"] = date_elem.get(
                "datetime"
            ) or date_elem.get_text(strip=True)

        # Город (может быть в разных местах)
        city_selectors = [
            {"data-ftid": "review-city"},
            {"class": "css-city"},
        ]

        for selector in city_selectors:
            city_elem = item.find("span", selector)
            if city_elem:
                author_info["author_city"] = city_elem.get_text(strip=True)
                break

        return author_info

    def _extract_car_info(self, item) -> Dict:
        """Извлечение характеристик автомобиля"""
        car_info = {}

        # Поиск блока с характеристиками
        specs_text = item.get_text()

        # Год выпуска
        year_match = re.search(r"(\d{4})\s*год", specs_text)
        if year_match:
            try:
                car_info["car_year"] = int(year_match.group(1))
            except ValueError:
                pass

        # Объем двигателя
        volume_match = re.search(r"(\d+\.?\d*)\s*л", specs_text)
        if volume_match:
            try:
                car_info["car_engine_volume"] = float(volume_match.group(1))
            except ValueError:
                pass

        # Тип топлива
        fuel_types = ["бензин", "дизель", "гибрид", "электро"]
        for fuel_type in fuel_types:
            if fuel_type in specs_text.lower():
                car_info["car_fuel_type"] = fuel_type
                break

        # Коробка передач
        if "автомат" in specs_text.lower():
            car_info["car_transmission"] = "автомат"
        elif "механика" in specs_text.lower():
            car_info["car_transmission"] = "механика"

        # Привод
        if "передний" in specs_text.lower():
            car_info["car_drive_type"] = "передний"
        elif "задний" in specs_text.lower():
            car_info["car_drive_type"] = "задний"
        elif "полный" in specs_text.lower():
            car_info["car_drive_type"] = "полный"

        return car_info

    def _extract_ratings(self, item) -> Dict:
        """Извлечение оценок"""
        ratings = {}

        # Ищем элементы с оценками
        rating_elements = item.find_all("div", class_=re.compile(r"rating"))

        for elem in rating_elements:
            text = elem.get_text(strip=True)
            # Извлекаем числовые оценки
            rating_match = re.search(r"(\d+\.?\d*)", text)
            if rating_match:
                try:
                    rating_value = float(rating_match.group(1))
                    # Определяем тип оценки по тексту
                    text_lower = text.lower()
                    if "общая" in text_lower or "общий" in text_lower:
                        ratings["overall_rating"] = rating_value
                    elif "комфорт" in text_lower:
                        ratings["comfort_rating"] = rating_value
                    elif "надежность" in text_lower:
                        ratings["reliability_rating"] = rating_value
                    elif "расход" in text_lower:
                        ratings["fuel_consumption_rating"] = rating_value
                    elif "вождение" in text_lower:
                        ratings["driving_rating"] = rating_value
                    elif "внешний" in text_lower:
                        ratings["appearance_rating"] = rating_value
                except ValueError:
                    pass

        return ratings

    def save_reviews_to_db(
        self,
        brand_url_name: str,
        model_url_name: str,
        long_reviews: List[Dict],
        short_reviews: List[Dict],
    ) -> bool:
        """Сохранение отзывов в базу данных"""
        try:
            # Получаем модель из базы
            brand = self.db_manager.get_brand_by_url_name(brand_url_name)
            if not brand:
                self.logger.error(f"Бренд {brand_url_name} не найден в базе")
                return False

            model = self.db_manager.get_model_by_url_name(brand["id"], model_url_name)
            if not model:
                self.logger.error(
                    f"Модель {model_url_name} "
                    f"не найдена для бренда {brand_url_name}"
                )
                return False

            model_id = model["id"]
            saved_count = 0

            # Сохраняем длинные отзывы
            for review in long_reviews:
                review_id = self.db_manager.add_review(
                    model_id=model_id, review_type="long", **review
                )
                if review_id:
                    saved_count += 1

            # Сохраняем короткие отзывы
            for review in short_reviews:
                review_id = self.db_manager.add_review(
                    model_id=model_id, review_type="short", **review
                )
                if review_id:
                    saved_count += 1

            self.logger.info(
                f"✅ Сохранено {saved_count} отзывов " f"для модели {model['name']}"
            )

            # Обновляем счетчики
            self.db_manager.update_reviews_count()

            return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения отзывов: {e}")
            return False

    def parse_model_reviews(
        self,
        brand_url_name: str,
        model_url_name: str,
        max_pages_long: int = 5,
        max_pages_short: int = 10,
    ) -> bool:
        """Комплексный парсинг отзывов модели (длинные + короткие)"""
        self.logger.info(
            f"🚀 Начинаем парсинг отзывов для {brand_url_name}/{model_url_name}"
        )

        # Парсим длинные отзывы
        long_reviews = self.parse_long_reviews(
            brand_url_name, model_url_name, max_pages_long
        )

        # Парсим короткие отзывы
        short_reviews = self.parse_short_reviews(
            brand_url_name, model_url_name, max_pages_short
        )

        # Сохраняем в базу данных
        if long_reviews or short_reviews:
            return self.save_reviews_to_db(
                brand_url_name, model_url_name, long_reviews, short_reviews
            )
        else:
            self.logger.warning("Не найдено отзывов для сохранения")
            return False


if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Создание парсера
    parser = DromReviewsParser(delay=1.0)

    # Инициализация базы данных
    if not parser.db_manager.create_database():
        print("❌ Ошибка создания базы данных!")
        exit(1)

    # Пример парсинга отзывов для Toyota 4Runner
    success = parser.parse_model_reviews(
        brand_url_name="toyota",
        model_url_name="4runner",
        max_pages_long=2,
        max_pages_short=3,
    )

    if success:
        print("\n🎉 Парсинг завершен успешно!")

        # Показываем статистику
        stats = parser.db_manager.get_statistics()
        print(f"\n📊 Статистика базы данных:")
        print(f"   🏢 Брендов: {stats.get('brands_count', 0)}")
        print(f"   🚗 Моделей: {stats.get('models_count', 0)}")
        print(f"   📝 Отзывов всего: {stats.get('total_reviews', 0)}")
        print(f"   📄 Длинных отзывов: {stats.get('long_reviews', 0)}")
        print(f"   📋 Коротких отзывов: {stats.get('short_reviews', 0)}")
    else:
        print("\n❌ Ошибка парсинга отзывов!")
