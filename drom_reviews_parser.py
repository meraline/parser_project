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
from database_schema import DatabaseManager


class DromReviewsParser:
    """Парсер отзывов с drom.ru с поддержкой двух типов отзывов"""

    def __init__(self, delay: float = 1.0):
        self.base_url = "https://www.drom.ru"
        self.delay = delay
        self.session = requests.Session()
        self.db_manager = DatabaseManager()

        # Настройка логирования
        self.logger = logging.getLogger(__name__)

        # Headers для имитации браузера
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Получение и парсинг страницы"""
        try:
            self.logger.info(f"📄 Загружаем страницу: {url}")

            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            time.sleep(self.delay)

            soup = BeautifulSoup(response.content, "html.parser")
            return soup

        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки {url}: {e}")
            return None

    def parse_model_page(self, brand_slug: str, model_slug: str) -> Dict[str, int]:
        """Парсинг страницы модели для получения количества отзывов"""
        url = f"{self.base_url}/reviews/{brand_slug}/{model_slug}/"
        soup = self.get_page(url)

        if not soup:
            return {"long": 0, "short": 0}

        try:
            # Поиск блока с табами отзывов
            tabs_block = soup.find("div", {"class": "_65ykvx0"})

            if not tabs_block:
                self.logger.warning(
                    f"⚠️ Не найден блок табов для {brand_slug}/{model_slug}"
                )
                return {"long": 0, "short": 0}

            long_count = 0
            short_count = 0

            # Парсинг длинных отзывов
            long_tab = tabs_block.find(
                "a", {"data-ftid": "reviews_tab_button_long_reviews"}
            )
            if long_tab:
                long_text = long_tab.get_text()
                long_match = re.search(r"(\d+)\s*отзыв", long_text)
                if long_match:
                    long_count = int(long_match.group(1))

            # Парсинг коротких отзывов
            short_tab = tabs_block.find(
                "a", {"data-ftid": "reviews_tab_button_short_reviews"}
            )
            if short_tab:
                short_text = short_tab.get_text()
                short_match = re.search(r"(\d+(?:\s+\d+)*)\s*короток", short_text)
                if short_match:
                    # Убираем пробелы из числа (например "2 525" -> "2525")
                    short_count_str = short_match.group(1).replace(" ", "")
                    short_count = int(short_count_str)

            self.logger.info(
                f"📊 {brand_slug}/{model_slug}: "
                f"длинных {long_count}, коротких {short_count}"
            )

            return {"long": long_count, "short": short_count}

        except Exception as e:
            self.logger.error(
                f"❌ Ошибка парсинга модели {brand_slug}/{model_slug}: {e}"
            )
            return {"long": 0, "short": 0}

    def parse_long_reviews(
        self, brand_slug: str, model_slug: str, limit: int = None
    ) -> List[Dict]:
        """Парсинг длинных отзывов модели"""
        url = f"{self.base_url}/reviews/{brand_slug}/{model_slug}/"

        reviews = []
        page = 1

        while True:
            page_url = f"{url}?p={page}" if page > 1 else url
            soup = self.get_page(page_url)

            if not soup:
                break

            # Поиск блоков отзывов
            review_blocks = soup.find_all("article", {"data-ftid": "review-item"})

            if not review_blocks:
                self.logger.info(f"📄 Страница {page}: отзывы не найдены")
                break

            for block in review_blocks:
                try:
                    review_data = self.extract_long_review(
                        block, brand_slug, model_slug
                    )
                    if review_data:
                        reviews.append(review_data)

                        # Проверка лимита
                        if limit and len(reviews) >= limit:
                            return reviews

                except Exception as e:
                    self.logger.error(f"❌ Ошибка извлечения отзыва: {e}")
                    continue

            self.logger.info(
                f"📄 Страница {page}: найдено {len(review_blocks)} отзывов"
            )
            page += 1

            # Защита от бесконечного цикла
            if page > 100:
                break

        return reviews

    def extract_long_review(
        self, block: BeautifulSoup, brand_slug: str, model_slug: str
    ) -> Optional[Dict]:
        """Извлечение данных из блока длинного отзыва"""
        try:
            # ID отзыва из атрибута
            review_id = block.get("id")
            if not review_id:
                return None

            # URL отзыва
            url_link = block.find("a", class_="css-1ycyg4y")
            review_url = None
            if url_link:
                review_url = urljoin(self.base_url, url_link.get("href", ""))

            # Заголовок
            title_elem = block.find("h3", class_="css-1eac5kj")
            title = title_elem.get_text(strip=True) if title_elem else None

            # Автор и дата
            author_elem = block.find("span", class_="css-1u4ddp")
            author = author_elem.get_text(strip=True) if author_elem else None

            # Дата публикации
            date_elem = block.find("time")
            publish_date = None
            if date_elem:
                publish_date = date_elem.get("datetime") or date_elem.get_text(
                    strip=True
                )

            # Рейтинг
            rating_elem = block.find("div", {"data-ftid": "component_rating"})
            rating = None
            if rating_elem:
                rating_text = rating_elem.get_text()
                rating_match = re.search(r"(\d+(?:\.\d+)?)", rating_text)
                if rating_match:
                    rating = float(rating_match.group(1))

            # Контент отзыва (плюсы, минусы, впечатления)
            content_parts = []

            # Плюсы
            pros_elem = block.find("div", {"data-ftid": "review-content__positive"})
            pros = pros_elem.get_text(strip=True) if pros_elem else None
            if pros:
                content_parts.append(f"Плюсы: {pros}")

            # Минусы
            cons_elem = block.find("div", {"data-ftid": "review-content__negative"})
            cons = cons_elem.get_text(strip=True) if cons_elem else None
            if cons:
                content_parts.append(f"Минусы: {cons}")

            # Общие впечатления
            impression_elem = block.find(
                "div", {"data-ftid": "review-content__general"}
            )
            general_impression = (
                impression_elem.get_text(strip=True) if impression_elem else None
            )
            if general_impression:
                content_parts.append(f"Общие впечатления: {general_impression}")

            content = "\n\n".join(content_parts) if content_parts else None

            # Информация об автомобиле
            car_info = self.extract_car_info(block)

            return {
                "review_id": review_id,
                "review_type": "long",
                "url": review_url,
                "title": title,
                "content": content,
                "author": author,
                "publish_date": publish_date,
                "rating": rating,
                "pros": pros,
                "cons": cons,
                "general_impression": general_impression,
                **car_info,
            }

        except Exception as e:
            self.logger.error(f"❌ Ошибка извлечения длинного отзыва: {e}")
            return None

    def parse_short_reviews(
        self, brand_slug: str, model_slug: str, limit: int = None
    ) -> List[Dict]:
        """Парсинг коротких отзывов модели"""
        url = f"{self.base_url}/reviews/{brand_slug}/{model_slug}/5kopeek/"

        reviews = []
        page = 1

        while True:
            page_url = f"{url}?p={page}" if page > 1 else url
            soup = self.get_page(page_url)

            if not soup:
                break

            # Поиск блоков коротких отзывов
            review_blocks = soup.find_all("div", {"data-ftid": "short-review-item"})

            if not review_blocks:
                self.logger.info(f"📄 Страница {page}: короткие отзывы не найдены")
                break

            for block in review_blocks:
                try:
                    review_data = self.extract_short_review(
                        block, brand_slug, model_slug
                    )
                    if review_data:
                        reviews.append(review_data)

                        # Проверка лимита
                        if limit and len(reviews) >= limit:
                            return reviews

                except Exception as e:
                    self.logger.error(f"❌ Ошибка извлечения короткого отзыва: {e}")
                    continue

            self.logger.info(
                f"📄 Страница {page}: найдено {len(review_blocks)} коротких отзывов"
            )
            page += 1

            # Защита от бесконечного цикла
            if page > 50:
                break

        return reviews

    def extract_short_review(
        self, block: BeautifulSoup, brand_slug: str, model_slug: str
    ) -> Optional[Dict]:
        """Извлечение данных из блока короткого отзыва"""
        try:
            # ID отзыва из атрибута
            review_id = block.get("id")
            if not review_id:
                return None

            # Автор (номер)
            author_elem = block.find("span", class_="css-1u4ddp")
            author = author_elem.get_text(strip=True) if author_elem else None

            # Город
            city_elem = block.find("span", {"data-ftid": "short-review-city"})
            city = city_elem.get_text(strip=True) if city_elem else None

            # Информация об автомобиле из заголовка
            title_elem = block.find("div", {"data-ftid": "short-review-item__title"})
            car_info = {}

            if title_elem:
                title_text = title_elem.get_text()

                # Год
                year_elem = title_elem.find(
                    "span", {"data-ftid": "short-review-item__year"}
                )
                if year_elem:
                    car_info["car_year"] = int(year_elem.get_text())

                # Объем двигателя
                volume_elem = title_elem.find(
                    "span", {"data-ftid": "short-review-item__volume"}
                )
                if volume_elem:
                    car_info["car_volume"] = float(volume_elem.get_text())

                # Парсинг остальной информации из текста
                # Формат: "2001 год, 1.5 л, бензин, автомат, передний"
                parts = title_text.split(", ")
                if len(parts) >= 5:
                    car_info["car_fuel_type"] = parts[2].strip()
                    car_info["car_transmission"] = parts[3].strip()
                    car_info["car_drive"] = parts[4].strip()

            # Плюсы
            pros_elem = block.find(
                "div", {"data-ftid": "short-review-content__positive"}
            )
            pros = pros_elem.get_text(strip=True) if pros_elem else None

            # Минусы
            cons_elem = block.find(
                "div", {"data-ftid": "short-review-content__negative"}
            )
            cons = cons_elem.get_text(strip=True) if cons_elem else None

            # Поломки
            breakages_elem = block.find(
                "div", {"data-ftid": "short-review-content__breakages"}
            )
            breakages = breakages_elem.get_text(strip=True) if breakages_elem else None

            # Объединяем контент
            content_parts = []
            if pros:
                content_parts.append(f"Плюсы: {pros}")
            if cons:
                content_parts.append(f"Минусы: {cons}")
            if breakages:
                content_parts.append(f"Поломки: {breakages}")

            content = "\n\n".join(content_parts) if content_parts else None

            # Фотографии
            photos = []
            photo_blocks = block.find_all("img", class_="css-1e2elm8")
            for img in photo_blocks:
                src = img.get("src") or img.get("srcset", "").split(" ")[0]
                if src:
                    photos.append(src)

            photos_json = json.dumps(photos) if photos else None

            return {
                "review_id": f"short_{review_id}",
                "review_type": "short",
                "url": f"{self.base_url}/reviews/{brand_slug}/{model_slug}/5kopeek/#{review_id}",
                "title": f"Короткий отзыв {car_info.get('car_year', '')} {car_info.get('car_volume', '')}л",
                "content": content,
                "author": author,
                "city": city,
                "pros": pros,
                "cons": cons,
                "breakages": breakages,
                "photos": photos_json,
                **car_info,
            }

        except Exception as e:
            self.logger.error(f"❌ Ошибка извлечения короткого отзыва: {e}")
            return None

    def extract_car_info(self, block: BeautifulSoup) -> Dict:
        """Извлечение информации об автомобиле из блока отзыва"""
        car_info = {}

        try:
            # Поиск блока с характеристиками автомобиля
            specs_block = block.find("div", class_="css-1ymyv8x")

            if specs_block:
                specs_text = specs_block.get_text()

                # Парсинг года
                year_match = re.search(r"(\d{4})\s*г", specs_text)
                if year_match:
                    car_info["car_year"] = int(year_match.group(1))

                # Парсинг объема двигателя
                volume_match = re.search(r"(\d+(?:\.\d+)?)\s*л", specs_text)
                if volume_match:
                    car_info["car_volume"] = float(volume_match.group(1))

                # Другие характеристики можно добавить по мере необходимости

        except Exception as e:
            self.logger.error(f"❌ Ошибка извлечения информации об автомобиле: {e}")

        return car_info

    def save_reviews(
        self, reviews: List[Dict], brand_slug: str, model_slug: str
    ) -> int:
        """Сохранение отзывов в базу данных"""
        saved_count = 0

        for review in reviews:
            try:
                result = self.db_manager.add_review(
                    brand_slug=brand_slug, model_slug=model_slug, **review
                )

                if result:
                    saved_count += 1

            except Exception as e:
                self.logger.error(
                    f"❌ Ошибка сохранения отзыва {review.get('review_id', 'unknown')}: {e}"
                )

        return saved_count

    def parse_model_reviews(
        self,
        brand_slug: str,
        model_slug: str,
        long_limit: int = None,
        short_limit: int = None,
    ) -> Dict[str, int]:
        """Полный парсинг отзывов модели (длинных и коротких)"""
        results = {"long": 0, "short": 0}

        self.logger.info(f"🚗 Начинаем парсинг отзывов {brand_slug}/{model_slug}")

        # Парсинг длинных отзывов
        if long_limit is None or long_limit > 0:
            self.logger.info("📝 Парсинг длинных отзывов...")
            long_reviews = self.parse_long_reviews(brand_slug, model_slug, long_limit)

            if long_reviews:
                saved_long = self.save_reviews(long_reviews, brand_slug, model_slug)
                results["long"] = saved_long
                self.logger.info(f"✅ Сохранено {saved_long} длинных отзывов")

        # Парсинг коротких отзывов
        if short_limit is None or short_limit > 0:
            self.logger.info("🔸 Парсинг коротких отзывов...")
            short_reviews = self.parse_short_reviews(
                brand_slug, model_slug, short_limit
            )

            if short_reviews:
                saved_short = self.save_reviews(short_reviews, brand_slug, model_slug)
                results["short"] = saved_short
                self.logger.info(f"✅ Сохранено {saved_short} коротких отзывов")

        total_saved = results["long"] + results["short"]
        self.logger.info(f"🎉 Всего сохранено отзывов: {total_saved}")

        return results


def main():
    """Демонстрация работы парсера"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = DromReviewsParser(delay=1.5)

    # Тестирование на примере Toyota 4Runner
    brand_slug = "toyota"
    model_slug = "4runner"

    print(f"🚗 ТЕСТИРОВАНИЕ ПАРСЕРА ОТЗЫВОВ")
    print(f"Модель: {brand_slug}/{model_slug}")
    print("=" * 50)

    # Получение количества отзывов
    counts = parser.parse_model_page(brand_slug, model_slug)
    print(f"📊 Найдено отзывов:")
    print(f"- Длинных: {counts['long']}")
    print(f"- Коротких: {counts['short']}")

    # Парсинг небольшого количества для теста
    results = parser.parse_model_reviews(
        brand_slug, model_slug, long_limit=3, short_limit=5
    )

    print(f"\n✅ РЕЗУЛЬТАТЫ ПАРСИНГА:")
    print(f"- Длинных отзывов сохранено: {results['long']}")
    print(f"- Коротких отзывов сохранено: {results['short']}")


if __name__ == "__main__":
    main()
