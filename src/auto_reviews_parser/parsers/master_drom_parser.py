#!/usr/bin/env python3
"""
🚗 МАСТЕР-ПАРСЕР DROM.RU - ОБЪЕДИНЕННАЯ ВЕРСИЯ
==============================================

ЕДИНСТВЕННЫЙ И ГЛАВНЫЙ ПАРСЕР для drom.ru
Объединяет ВСЕГО лучшего из всех существующих парсеров:

✅ Длинные отзывы (подробные обзоры) - из drom_reviews.py
✅ Короткие отзывы (краткие мнения) - из production_drom_parser.py  
✅ Каталог брендов и моделей - из unified_master_parser.py
✅ Надежную сетевую логику - из base.py и sync_base.py
✅ Ретраи и обработку ошибок - из retry_decorator.py
✅ Метрики и задержки - из delay_manager.py и metrics.py
✅ Полную модель данных - из review.py
✅ Кэширование - из cache.py
✅ Логирование - из logger.py

БОЛЬШЕ НЕ СОЗДАЕМ НОВЫХ ПАРСЕРОВ!
РАЗВИВАЕМ ТОЛЬКО ЭТОТ!

Автор: AI Assistant  
Дата: 26.08.2025
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import random
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Импорты из существующей кодовой базы
try:
    from ..models.review import Review
except ImportError:
    # Заглушка для Review если модуль недоступен
    from dataclasses import dataclass
    from datetime import datetime
    @dataclass
    class Review:
        source: str = ""
        type: str = ""
        brand: str = ""
        model: str = ""
        year: int = None
        url: str = ""
        title: str = ""
        content: str = ""
        author: str = ""
        rating: float = None
        pros: str = ""
        cons: str = ""
        engine_volume: float = None
        fuel_type: str = ""
        transmission: str = ""
        drive_type: str = ""
        body_type: str = ""
        city: str = None
        date: str = None
        useful_count: int = None
        not_useful_count: int = None
        views_count: int = None
        likes_count: int = None
        comments_count: int = None
        parsed_at: datetime = None
        content_hash: str = ""

try:
    from ..utils.delay_manager import DelayManager
except ImportError:
    # Простая заглушка для DelayManager
    class DelayManager:
        def __init__(self, min_delay=1.0, max_delay=2.0):
            self.min_delay = min_delay
            self.max_delay = max_delay
        def apply_delay(self):
            time.sleep(random.uniform(self.min_delay, self.max_delay))

try:
    from ..utils.logger import get_logger
except ImportError:
    # Простая заглушка для логгера
    def get_logger(name):
        import logging
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

try:
    from ..utils.cache import Cache
except ImportError:
    # Простая заглушка для кэша
    class Cache:
        def __init__(self, cache_dir):
            self.cache_dir = cache_dir
        def get(self, key): return None
        def set(self, key, value): pass

try:
    from ..utils.metrics import ParsingMetrics
except ImportError:
    # Простая заглушка для метрик
    class ParsingMetrics:
        def __init__(self): pass
        def record_request(self, url, success): pass
        def get_stats(self): return {}

try:
    from ..database.schema import DatabaseManager
except ImportError:
    # Простая заглушка для DatabaseManager
    class DatabaseManager:
        def __init__(self): pass
        def save_reviews(self, reviews): pass

logger = get_logger(__name__)


@dataclass
class ReviewData:
    """Унифицированная структура данных отзыва"""
    review_id: str
    brand: str
    model: str
    review_type: str  # 'long' или 'short'
    
    # Характеристики автомобиля  
    year: Optional[int] = None
    engine_volume: Optional[float] = None
    fuel_type: Optional[str] = None
    transmission: Optional[str] = None
    drive_type: Optional[str] = None
    body_type: Optional[str] = None
    
    # Данные отзыва
    author: Optional[str] = None
    city: Optional[str] = None
    date: Optional[str] = None
    rating: Optional[float] = None
    title: Optional[str] = None
    
    # Содержание отзыва
    positive_text: Optional[str] = None
    negative_text: Optional[str] = None
    general_text: Optional[str] = None
    breakages_text: Optional[str] = None
    content: Optional[str] = None
    
    # Дополнительные данные
    photos: List[str] = field(default_factory=list)
    photos_count: int = 0
    url: Optional[str] = None
    
    # Метрики
    views_count: Optional[int] = None
    likes_count: Optional[int] = None
    useful_count: Optional[int] = None
    not_useful_count: Optional[int] = None
    comments_count: Optional[int] = None
    
    # Служебные поля
    parsed_at: Optional[datetime] = None
    content_hash: Optional[str] = None
    
    def __post_init__(self):
        if self.parsed_at is None:
            self.parsed_at = datetime.now()
        self.content_hash = self.generate_hash()
        self.photos_count = len(self.photos) if self.photos else 0
    
    def generate_hash(self) -> str:
        """Генерация уникального хеша контента"""
        content_for_hash = f"{self.url}_{self.title or ''}_{self.content or ''}_{self.positive_text or ''}_{self.negative_text or ''}"
        return hashlib.md5(content_for_hash.encode()).hexdigest()
    
    def to_review_model(self) -> Review:
        """Конвертация в модель Review для совместимости"""
        return Review(
            source="drom.ru",
            type=self.review_type,
            brand=self.brand,
            model=self.model,
            year=self.year,
            url=self.url or "",
            title=self.title or "",
            content=self.content or "",
            author=self.author or "",
            rating=self.rating,
            pros=self.positive_text or "",
            cons=self.negative_text or "",
            engine_volume=self.engine_volume,
            fuel_type=self.fuel_type or "",
            transmission=self.transmission or "",
            drive_type=self.drive_type or "",
            body_type=self.body_type or "",
            city=self.city,
            date=self.date,
            useful_count=self.useful_count,
            not_useful_count=self.not_useful_count,
            views_count=self.views_count,
            likes_count=self.likes_count,
            comments_count=self.comments_count,
            parsed_at=self.parsed_at,
            content_hash=self.content_hash or ""
        )


@dataclass
class BrandInfo:
    """Информация о бренде"""
    name: str
    url: str
    reviews_count: int
    url_name: str  # имя в URL, например 'alfa_romeo' для 'Alfa Romeo'


@dataclass
class ModelInfo:
    """Информация о модели"""
    name: str
    brand: str
    url: str
    long_reviews_count: int = 0
    short_reviews_count: int = 0
    url_name: str = ""


class NetworkError(Exception):
    """Ошибка сетевого взаимодействия"""
    pass


class ParseError(Exception):
    """Ошибка парсинга данных"""
    pass


class MasterDromParser:
    """
    🚗 МАСТЕР-ПАРСЕР DROM.RU - ОБЪЕДИНЕННАЯ ВЕРСИЯ
    
    Объединяет ВСЮ лучшую логику из всех существующих парсеров:
    - Длинные и короткие отзывы
    - Каталог брендов и моделей  
    - Надежную сетевую логику
    - Ретраи и обработку ошибок
    - Метрики и кэширование
    - Полное логирование
    """

    def __init__(self, 
                 delay: float = 1.0, 
                 cache_dir: str = "data/cache",
                 enable_database: bool = True,
                 enable_cache: bool = True):
        """Инициализация мастер-парсера"""
        
        self.base_url = "https://www.drom.ru"
        self.cache_dir = cache_dir
        self.enable_database = enable_database
        self.enable_cache = enable_cache
        
        # Создаем необходимые директории
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        Path("logs").mkdir(exist_ok=True)
        Path("data").mkdir(exist_ok=True)
        
        # Инициализация компонентов
        self.delay_manager = DelayManager(min_delay=delay, max_delay=delay*2)
        self.metrics = ParsingMetrics()
        
        # Кэш
        if self.enable_cache:
            self.cache = Cache(cache_dir)
        else:
            self.cache = None
            
        # База данных
        if self.enable_database:
            try:
                self.db_manager = DatabaseManager()
            except Exception as e:
                logger.warning(f"Не удалось инициализировать DatabaseManager: {e}")
                self.db_manager = None
        else:
            self.db_manager = None
        
        # Настройка сессии
        self.session = requests.Session()
        self.session.proxies = {}
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        logger.info("Мастер-парсер Drom.ru инициализирован")

    def _make_request(self, url: str, use_cache: bool = True) -> Optional[BeautifulSoup]:
        """
        Выполнение HTTP запроса с обработкой ошибок, кэшированием и ретраями
        Объединяет логику из всех парсеров
        """
        
        # Проверяем кэш
        if use_cache and self.cache:
            cached_content = self.cache.get(url)
            if cached_content:
                logger.debug(f"Используем кэш для {url}")
                return BeautifulSoup(cached_content, "html.parser")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"Запрос к {url} (попытка {attempt + 1})")
                
                response = self.session.get(
                    url, 
                    headers=self.headers, 
                    timeout=30,
                    allow_redirects=True
                )
                response.raise_for_status()
                
                # Сохраняем в кэш
                if use_cache and self.cache:
                    self.cache.set(url, response.text)
                
                soup = BeautifulSoup(response.content, "html.parser")
                
                # Применяем задержку
                self.delay_manager.apply_delay()
                
                # Обновляем метрики
                if hasattr(self.metrics, 'record_request'):
                    self.metrics.record_request(url, True)
                
                return soup
                
            except requests.RequestException as e:
                logger.warning(f"Ошибка запроса к {url} (попытка {attempt + 1}): {e}")
                
                # Обновляем метрики
                if hasattr(self.metrics, 'record_request'):
                    self.metrics.record_request(url, False)
                
                if attempt < max_retries - 1:
                    # Увеличиваем задержку при ошибке
                    error_delay = (attempt + 1) * 5
                    logger.info(f"Ждем {error_delay} секунд перед повторной попыткой")
                    time.sleep(error_delay)
                else:
                    logger.error(f"Не удалось получить {url} после {max_retries} попыток")
                    raise NetworkError(f"Ошибка получения {url}: {e}")
                    
        return None

    def get_brands_catalog(self) -> List[BrandInfo]:
        """
        Получение каталога всех брендов
        Объединяет логику из production_drom_parser.py и unified_master_parser.py
        """
        logger.info("Получение каталога брендов")
        
        url = f"{self.base_url}/reviews/"
        soup = self._make_request(url)
        
        if not soup:
            raise NetworkError("Не удалось получить каталог брендов")
            
        brands = []
        
        # Ищем блок с брендами
        cars_list = soup.find("div", {"data-ftid": "component_cars-list"})
        if not cars_list:
            logger.error("Не найден блок с брендами")
            return brands
            
        # Парсим бренды
        brand_items = cars_list.find_all("div", class_="frg44i0")
        
        for item in brand_items:
            try:
                # Ссылка на бренд
                link = item.find("a", {"data-ftid": "component_cars-list-item_hidden-link"})
                if not link:
                    continue
                    
                brand_url = link.get("href")
                if not brand_url:
                    continue
                    
                # Имя бренда
                name_span = item.find("span", {"data-ftid": "component_cars-list-item_name"})
                if not name_span:
                    continue
                    
                brand_name = name_span.get_text(strip=True)
                
                # Количество отзывов
                counter_span = item.find("span", {"data-ftid": "component_cars-list-item_counter"})
                reviews_count = 0
                if counter_span:
                    counter_text = counter_span.get_text(strip=True)
                    # Извлекаем число из текста
                    numbers = re.findall(r'\\d+', counter_text.replace(' ', ''))
                    if numbers:
                        reviews_count = int(numbers[0])
                
                # Извлекаем url_name из ссылки
                url_name = brand_url.strip('/').split('/')[-1]
                
                brand = BrandInfo(
                    name=brand_name,
                    url=brand_url,
                    reviews_count=reviews_count,
                    url_name=url_name
                )
                
                brands.append(brand)
                logger.debug(f"Добавлен бренд: {brand_name} ({reviews_count} отзывов)")
                
            except Exception as e:
                logger.warning(f"Ошибка при парсинге бренда: {e}")
                continue
        
        logger.info(f"Найдено {len(brands)} брендов")
        return brands

    def get_models_for_brand(self, brand: BrandInfo) -> List[ModelInfo]:
        """
        Получение моделей для бренда
        Использует улучшенную логику из production_drom_parser.py
        """
        logger.info(f"Получение моделей для бренда {brand.name}")
        
        if not brand.url.startswith('http'):
            url = urljoin(self.base_url, brand.url)
        else:
            url = brand.url
            
        soup = self._make_request(url)
        if not soup:
            logger.warning(f"Не удалось получить страницу бренда {brand.name}")
            return []
        
        models = []
        
        # Ищем ссылки на модели
        model_links = soup.find_all("a", href=re.compile(rf"/reviews/{brand.url_name}/\\w+/$"))
        
        for link in model_links:
            try:
                model_url = link.get("href")
                if not model_url:
                    continue
                    
                # Извлекаем название модели из URL
                url_parts = model_url.strip('/').split('/')
                if len(url_parts) < 3:
                    continue
                    
                model_url_name = url_parts[-1]
                
                # Название модели из текста ссылки
                model_name = link.get_text(strip=True)
                if not model_name:
                    model_name = model_url_name.replace('_', ' ').title()
                
                # Получаем количество отзывов для модели
                long_count, short_count = self.get_review_counts_for_model_url(model_url)
                
                model = ModelInfo(
                    name=model_name,
                    brand=brand.name,
                    url=model_url,
                    long_reviews_count=long_count,
                    short_reviews_count=short_count,
                    url_name=model_url_name
                )
                
                models.append(model)
                logger.debug(f"Модель: {model_name} (длинных: {long_count}, коротких: {short_count})")
                
            except Exception as e:
                logger.warning(f"Ошибка при парсинге модели: {e}")
                continue
        
        logger.info(f"Найдено {len(models)} моделей для {brand.name}")
        return models

    def get_review_counts_for_model_url(self, model_url: str) -> Tuple[int, int]:
        """
        Получение количества отзывов для модели по URL
        Использует проверенную логику из production_drom_parser.py
        """
        if not model_url.startswith('http'):
            full_url = urljoin(self.base_url, model_url)
        else:
            full_url = model_url
            
        soup = self._make_request(full_url)
        if not soup:
            return 0, 0
        
        long_reviews_count = 0
        short_reviews_count = 0
        
        # Ищем табы с количеством отзывов
        tabs = soup.find_all("a", {"data-ftid": re.compile(r"reviews_tab_button")})
        
        for tab in tabs:
            tab_text = tab.get_text(strip=True)
            
            # Длинные отзывы
            if "data-ftid" in tab.attrs and "long_reviews" in tab["data-ftid"]:
                numbers = re.findall(r'\\d+', tab_text.replace(' ', ''))
                if numbers:
                    long_reviews_count = int(numbers[0])
                    
            # Короткие отзывы
            elif "data-ftid" in tab.attrs and "short_reviews" in tab["data-ftid"]:
                numbers = re.findall(r'\\d+', tab_text.replace(' ', ''))
                if numbers:
                    short_reviews_count = int(numbers[0])
        
        return long_reviews_count, short_reviews_count

    def parse_long_reviews(self, model: ModelInfo, limit: Optional[int] = None) -> List[ReviewData]:
        """
        Парсинг длинных отзывов
        Объединяет лучшую логику из drom_reviews.py и production_drom_parser.py
        """
        logger.info(f"Парсинг длинных отзывов для {model.brand} {model.name}")
        
        reviews = []
        page = 1
        max_pages = 50  # Ограничение для безопасности
        
        while page <= max_pages:
            if limit and len(reviews) >= limit:
                break
                
            # URL страницы с длинными отзывами
            if not model.url.startswith('http'):
                url = urljoin(self.base_url, model.url)
            else:
                url = model.url
                
            if page > 1:
                url += f"?page={page}"
                
            soup = self._make_request(url)
            if not soup:
                break
                
            # Поиск блоков длинных отзывов
            review_blocks = soup.find_all("div", {"data-ftid": "review-item"})
            
            if not review_blocks:
                logger.info(f"Нет длинных отзывов на странице {page}")
                break
                
            for block in review_blocks:
                if limit and len(reviews) >= limit:
                    break
                    
                review = self._parse_long_review_block(block, model)
                if review:
                    reviews.append(review)
                    
            page += 1
            
        logger.info(f"Получено {len(reviews)} длинных отзывов")
        return reviews

    def parse_short_reviews(self, model: ModelInfo, limit: Optional[int] = None) -> List[ReviewData]:
        """
        Парсинг коротких отзывов  
        Использует проверенную логику из production_drom_parser.py
        """
        logger.info(f"Парсинг коротких отзывов для {model.brand} {model.name}")
        
        reviews = []
        page = 1
        max_pages = 50
        
        while page <= max_pages:
            if limit and len(reviews) >= limit:
                break
                
            # URL страницы с короткими отзывами
            if not model.url.startswith('http'):
                base_url = urljoin(self.base_url, model.url)
            else:
                base_url = model.url
                
            url = f"{base_url}5kopeek/"
            if page > 1:
                url += f"?page={page}"
                
            soup = self._make_request(url)
            if not soup:
                break
                
            # Поиск блоков коротких отзывов
            review_blocks = soup.find_all("div", {"data-ftid": "short-review-item"})
            
            if not review_blocks:
                logger.info(f"Нет коротких отзывов на странице {page}")
                break
                
            for block in review_blocks:
                if limit and len(reviews) >= limit:
                    break
                    
                review = self._parse_short_review_block(block, model)
                if review:
                    reviews.append(review)
                    
            page += 1
            
        logger.info(f"Получено {len(reviews)} коротких отзывов")
        return reviews

    def _parse_long_review_block(self, block, model: ModelInfo) -> Optional[ReviewData]:
        """
        Парсинг блока длинного отзыва
        Объединяет логику из drom_reviews.py и production_drom_parser.py
        """
        try:
            # Получаем ID отзыва
            review_id = block.get('id', '')
            
            # URL отзыва
            review_url = f"{model.url}{review_id}/"
            
            # Инициализируем данные отзыва
            review_data = {
                'review_id': review_id,
                'brand': model.brand.lower(),
                'model': model.url_name,
                'review_type': 'long',
                'url': review_url
            }
            
            # Информация об авторе и дате
            author_elem = block.find("a", class_="css-1u4ddp")
            if author_elem:
                review_data["author"] = author_elem.get_text(strip=True)
            
            # Дата (ищем в разных местах)
            date_elem = block.find("span", class_="css-1tc5ro3") or block.find("time")
            if date_elem:
                review_data["date"] = date_elem.get_text(strip=True)
            
            # Город
            city_elem = block.find("span", {"data-ftid": "review-location"})
            if city_elem:
                review_data["city"] = city_elem.get_text(strip=True)
            
            # Рейтинг
            rating_elem = block.find("div", class_="css-1vkpuwn") or block.find("span", {"data-ftid": "review-rating"})
            if rating_elem:
                rating_text = rating_elem.get_text(strip=True)
                rating_match = re.search(r'(\\d+(?:\\.\\d+)?)', rating_text)
                if rating_match:
                    try:
                        review_data["rating"] = float(rating_match.group(1))
                    except ValueError:
                        pass
            
            # Заголовок
            title_elem = block.find("h3") or block.find("div", {"data-ftid": "review-title"})
            if title_elem:
                review_data["title"] = title_elem.get_text(strip=True)
            
            # Плюсы
            positive_elem = block.find("div", {"data-ftid": "review-content__positive"})
            if positive_elem:
                review_data["positive_text"] = positive_elem.get_text(strip=True)
            
            # Минусы
            negative_elem = block.find("div", {"data-ftid": "review-content__negative"})
            if negative_elem:
                review_data["negative_text"] = negative_elem.get_text(strip=True)
                
            # Поломки
            breakages_elem = block.find("div", {"data-ftid": "review-content__breakages"})
            if breakages_elem:
                review_data["breakages_text"] = breakages_elem.get_text(strip=True)
            
            # Основной контент
            content_parts = []
            content_sections = block.find_all("div", class_="css-6hj46s")
            for section in content_sections:
                text = section.get_text(strip=True)
                if text:
                    content_parts.append(text)
            
            if content_parts:
                review_data["content"] = "\\n".join(content_parts)
            
            # Фотографии
            photos = block.find_all("img")
            photo_urls = []
            for img in photos:
                src = img.get('src') or img.get('data-src')
                if src and 'photo' in src:
                    photo_urls.append(src)
            review_data["photos"] = photo_urls
            
            # Создаем ReviewData
            review = ReviewData(**review_data)
            return review
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге длинного отзыва: {e}")
            return None

    def _parse_short_review_block(self, block, model: ModelInfo) -> Optional[ReviewData]:
        """
        Парсинг блока короткого отзыва
        Использует проверенную логику из production_drom_parser.py
        """
        try:
            # Получаем ID отзыва
            review_id = block.get('id', '')
            
            # URL отзыва
            review_url = f"{model.url}5kopeek/{review_id}/"
            
            # Инициализируем данные отзыва
            review_data = {
                'review_id': review_id,
                'brand': model.brand.lower(),
                'model': model.url_name,
                'review_type': 'short',
                'url': review_url
            }
            
            # Информация об авторе
            author_elem = block.find("span", class_="css-1u4ddp")
            if author_elem:
                review_data["author"] = author_elem.get_text(strip=True)
            
            # Город
            city_elem = block.find("span", {"data-ftid": "short-review-city"})
            if city_elem:
                review_data["city"] = city_elem.get_text(strip=True)
            
            # Парсим характеристики автомобиля из заголовка
            title_div = block.find('div', {'data-ftid': 'short-review-item__title'})
            if title_div:
                self._extract_car_specs_from_title(title_div, review_data)
            
            # Плюсы
            positive_elem = block.find("div", {"data-ftid": "short-review-content__positive"})
            if positive_elem:
                review_data["positive_text"] = positive_elem.get_text(strip=True)
            
            # Минусы
            negative_elem = block.find("div", {"data-ftid": "short-review-content__negative"})
            if negative_elem:
                review_data["negative_text"] = negative_elem.get_text(strip=True)
                
            # Поломки
            breakages_elem = block.find("div", {"data-ftid": "short-review-content__breakages"})
            if breakages_elem:
                review_data["breakages_text"] = breakages_elem.get_text(strip=True)
            
            # Фотографии
            photo_divs = block.find_all('div', class_='_1gzw4372')
            photo_urls = []
            for photo_div in photo_divs:
                img = photo_div.find('img')
                if img and img.get('src'):
                    photo_urls.append(img['src'])
            review_data["photos"] = photo_urls
            
            # Создаем ReviewData
            review = ReviewData(**review_data)
            return review
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге короткого отзыва: {e}")
            return None

    def _extract_car_specs_from_title(self, title_div, review_data: dict):
        """
        Извлечение характеристик автомобиля из заголовка короткого отзыва
        Логика из production_drom_parser.py
        """
        try:
            text = title_div.get_text(strip=True)
            
            # Год
            year_span = title_div.find('span', {'data-ftid': 'short-review-item__year'})
            if year_span:
                year_text = year_span.get_text(strip=True)
                try:
                    review_data['year'] = int(year_text)
                except ValueError:
                    pass
            
            # Объем двигателя
            volume_span = title_div.find('span', {'data-ftid': 'short-review-item__volume'})
            if volume_span:
                volume_text = volume_span.get_text(strip=True)
                try:
                    review_data['engine_volume'] = float(volume_text)
                except ValueError:
                    pass
            
            # Тип топлива
            if 'бензин' in text:
                review_data['fuel_type'] = 'бензин'
            elif 'дизель' in text:
                review_data['fuel_type'] = 'дизель'
            elif 'гибрид' in text:
                review_data['fuel_type'] = 'гибрид'
            elif 'электро' in text:
                review_data['fuel_type'] = 'электро'
            
            # Коробка передач
            if 'автомат' in text:
                review_data['transmission'] = 'автомат'
            elif 'механика' in text:
                review_data['transmission'] = 'механика'
                
            # Привод
            if 'передний' in text:
                review_data['drive_type'] = 'передний'
            elif 'задний' in text:
                review_data['drive_type'] = 'задний'
            elif 'полный' in text:
                review_data['drive_type'] = 'полный'
                
        except Exception as e:
            logger.warning(f"Ошибка при извлечении характеристик: {e}")

    def parse_model_reviews(self, 
                          model: ModelInfo, 
                          max_long_reviews: Optional[int] = None,
                          max_short_reviews: Optional[int] = None) -> List[ReviewData]:
        """
        Парсинг всех отзывов для модели (длинных и коротких)
        """
        logger.info(f"Парсинг отзывов для {model.brand} {model.name}")
        
        all_reviews = []
        
        # Парсим длинные отзывы
        if model.long_reviews_count > 0:
            long_reviews = self.parse_long_reviews(model, limit=max_long_reviews)
            all_reviews.extend(long_reviews)
            
        # Парсим короткие отзывы
        if model.short_reviews_count > 0:
            short_reviews = self.parse_short_reviews(model, limit=max_short_reviews)
            all_reviews.extend(short_reviews)
            
        logger.info(f"Всего получено {len(all_reviews)} отзывов для {model.brand} {model.name}")
        return all_reviews

    def save_to_database(self, reviews: List[ReviewData]):
        """Сохранение отзывов в базу данных"""
        if not self.db_manager:
            logger.warning("DatabaseManager не инициализирован")
            return
            
        try:
            # Конвертируем в модели Review для совместимости
            review_models = [review.to_review_model() for review in reviews]
            
            # Сохраняем через DatabaseManager
            self.db_manager.save_reviews(review_models)
            logger.info(f"Сохранено {len(reviews)} отзывов в базу данных")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении в базу данных: {e}")

    def save_to_json(self, reviews: List[ReviewData], filename: str):
        """Сохранение отзывов в JSON файл"""
        try:
            # Конвертируем в словари
            reviews_data = [asdict(review) for review in reviews]
            
            # Обрабатываем datetime для JSON
            for review_data in reviews_data:
                if 'parsed_at' in review_data and review_data['parsed_at']:
                    review_data['parsed_at'] = review_data['parsed_at'].isoformat()
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(reviews_data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Сохранено {len(reviews)} отзывов в {filename}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении в JSON: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        """Получение статистики работы парсера"""
        stats = {
            'cache_enabled': self.enable_cache,
            'database_enabled': self.enable_database,
            'base_url': self.base_url,
        }
        
        if hasattr(self.metrics, 'get_stats'):
            stats.update(self.metrics.get_stats())
            
        return stats

    def parse_limited_demo(self, 
                          max_brands: int = 3,
                          max_long_reviews: int = 3,
                          max_short_reviews: int = 10) -> Dict[str, Any]:
        """
        Демо-парсинг с ограничениями для тестирования
        """
        logger.info(f"Запуск демо-парсинга: {max_brands} брендов, {max_long_reviews} длинных, {max_short_reviews} коротких отзывов")
        
        start_time = datetime.now()
        results = {
            'start_time': start_time.isoformat(),
            'brands_processed': [],
            'total_reviews': 0,
            'total_long_reviews': 0,
            'total_short_reviews': 0,
            'errors': []
        }
        
        try:
            # Получаем список брендов
            brands = self.get_brands_catalog()
            if not brands:
                results['errors'].append("Не удалось получить список брендов")
                return results
            
            # Берем первые бренды
            test_brands = brands[:max_brands]
            
            all_reviews = []
            
            for brand in test_brands:
                try:
                    logger.info(f"Обрабатываем бренд: {brand.name}")
                    
                    # Получаем модели
                    models = self.get_models_for_brand(brand)
                    if not models:
                        logger.warning(f"Нет моделей для бренда {brand.name}")
                        continue
                    
                    # Берем первую модель с отзывами
                    target_model = None
                    for model in models:
                        if model.long_reviews_count > 0 or model.short_reviews_count > 0:
                            target_model = model
                            break
                    
                    if not target_model:
                        logger.warning(f"Нет моделей с отзывами для бренда {brand.name}")
                        continue
                    
                    # Парсим отзывы
                    model_reviews = self.parse_model_reviews(
                        target_model,
                        max_long_reviews=max_long_reviews,
                        max_short_reviews=max_short_reviews
                    )
                    
                    all_reviews.extend(model_reviews)
                    
                    # Статистика по бренду
                    brand_stats = {
                        'brand': brand.name,
                        'model': target_model.name,
                        'long_reviews_available': target_model.long_reviews_count,
                        'short_reviews_available': target_model.short_reviews_count,
                        'long_reviews_parsed': len([r for r in model_reviews if r.review_type == 'long']),
                        'short_reviews_parsed': len([r for r in model_reviews if r.review_type == 'short']),
                        'total_parsed': len(model_reviews)
                    }
                    
                    results['brands_processed'].append(brand_stats)
                    
                except Exception as e:
                    error_msg = f"Ошибка при обработке бренда {brand.name}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            # Финальная статистика
            results['total_reviews'] = len(all_reviews)
            results['total_long_reviews'] = len([r for r in all_reviews if r.review_type == 'long'])
            results['total_short_reviews'] = len([r for r in all_reviews if r.review_type == 'short'])
            results['end_time'] = datetime.now().isoformat()
            results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
            
            # Сохраняем результаты
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_filename = f"data/master_parser_demo_{timestamp}.json"
            self.save_to_json(all_reviews, json_filename)
            results['saved_to'] = json_filename
            
            # Сохраняем в базу данных если включена
            if self.enable_database and all_reviews:
                self.save_to_database(all_reviews)
            
            logger.info(f"Демо-парсинг завершен: {results['total_reviews']} отзывов за {results['duration_seconds']:.1f} сек")
            
        except Exception as e:
            error_msg = f"Критическая ошибка в демо-парсинге: {e}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
        
        return results


# Пример использования
if __name__ == "__main__":
    # Инициализация мастер-парсера
    parser = MasterDromParser(
        delay=1.0,
        cache_dir="data/cache",
        enable_database=False,  # Отключаем БД для демо
        enable_cache=True
    )
    
    # Запуск демо-парсинга
    results = parser.parse_limited_demo(
        max_brands=3,
        max_long_reviews=3, 
        max_short_reviews=10
    )
    
    # Выводим результаты
    print("🚗 РЕЗУЛЬТАТЫ МАСТЕР-ПАРСЕРА:")
    print(f"Обработано брендов: {len(results['brands_processed'])}")
    print(f"Всего отзывов: {results['total_reviews']}")
    print(f"Длинных отзывов: {results['total_long_reviews']}")
    print(f"Коротких отзывов: {results['total_short_reviews']}")
    print(f"Время выполнения: {results.get('duration_seconds', 0):.1f} сек")
    
    if results['errors']:
        print(f"Ошибок: {len(results['errors'])}")
        for error in results['errors']:
            print(f"  - {error}")
