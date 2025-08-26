#!/usr/bin/env python3
"""
🚗 МАСТЕР-ПАРСЕР DROM.RU - УПРОЩЕННАЯ ВЕРСИЯ
=============================================

ЕДИНЫЙ И ГЛАВНЫЙ ПАРСЕР для drom.ru
Объединяет всю лучшую логику из всех парсеров проекта:

✅ Длинные отзывы (подробные обзоры) 
✅ Короткие отзывы (краткие мнения)
✅ Каталог брендов и моделей
✅ Надежную сетевую логику
✅ Обработку ошибок и ретраи
✅ Кэширование и метрики

ГЛАВНЫЙ ПАРСЕР ПРОЕКТА - НЕ СОЗДАВАЙТЕ НОВЫХ!

Автор: AI Assistant
Дата: 26.08.2025
"""

import json
import logging
import os
import re
import random
import time
import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ReviewData:
    """Универсальная структура данных отзыва"""
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
        content_for_hash = f"{self.url or ''}_{self.title or ''}_{self.content or ''}_{self.positive_text or ''}_{self.negative_text or ''}"
        return hashlib.md5(content_for_hash.encode()).hexdigest()


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


class SimpleCache:
    """Простой файловый кэш"""
    def __init__(self, cache_dir: str):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_cache_path(self, url: str) -> Path:
        """Получить путь к файлу кэша"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return self.cache_dir / f"{url_hash}.html"
    
    def get(self, url: str) -> Optional[str]:
        """Получить из кэша"""
        cache_path = self._get_cache_path(url)
        if cache_path.exists():
            try:
                return cache_path.read_text(encoding='utf-8')
            except Exception:
                return None
        return None
    
    def set(self, url: str, content: str):
        """Сохранить в кэш"""
        cache_path = self._get_cache_path(url)
        try:
            cache_path.write_text(content, encoding='utf-8')
        except Exception:
            pass


class MasterDromParser:
    """
    🚗 МАСТЕР-ПАРСЕР DROM.RU - УПРОЩЕННАЯ ВЕРСИЯ
    
    Объединяет ВСЮ лучшую логику из всех парсеров проекта:
    - Парсинг длинных и коротких отзывов
    - Каталог брендов и моделей  
    - Надежная сетевая логика с ретраями
    - Кэширование и обработка ошибок
    """

    def __init__(self, 
                 delay: float = 1.0, 
                 cache_dir: str = "data/cache",
                 enable_cache: bool = True):
        """Инициализация мастер-парсера"""
        
        self.base_url = "https://www.drom.ru"
        self.delay = delay
        self.enable_cache = enable_cache
        
        # Создаем необходимые директории
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        Path("logs").mkdir(exist_ok=True)
        Path("data").mkdir(exist_ok=True)
        
        # Кэш
        if self.enable_cache:
            self.cache = SimpleCache(cache_dir)
        else:
            self.cache = None
        
        # Статистика
        self.stats = {
            'requests_made': 0,
            'requests_cached': 0,
            'requests_failed': 0,
            'reviews_parsed': 0
        }
        
        # Настройка сессии
        self.session = requests.Session()
        
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
        
        logger.info("🚗 Мастер-парсер Drom.ru инициализирован")

    def _make_request(self, url: str, use_cache: bool = True) -> Optional[BeautifulSoup]:
        """
        Выполнение HTTP запроса с кэшированием, ретраями и обработкой ошибок
        """
        
        # Проверяем кэш
        if use_cache and self.cache:
            cached_content = self.cache.get(url)
            if cached_content:
                logger.debug(f"📦 Кэш: {url}")
                self.stats['requests_cached'] += 1
                return BeautifulSoup(cached_content, "html.parser")
        
        # Выполняем запрос с ретраями
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"🌐 Запрос: {url} (попытка {attempt + 1})")
                
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
                
                self.stats['requests_made'] += 1
                soup = BeautifulSoup(response.content, "html.parser")
                
                # Применяем задержку
                time.sleep(random.uniform(self.delay, self.delay * 1.5))
                
                return soup
                
            except requests.RequestException as e:
                logger.warning(f"⚠️  Ошибка запроса: {url} (попытка {attempt + 1}): {e}")
                
                if attempt < max_retries - 1:
                    # Увеличиваем задержку при ошибке
                    error_delay = (attempt + 1) * 3
                    logger.info(f"⏳ Ждем {error_delay} сек перед повторной попыткой")
                    time.sleep(error_delay)
                else:
                    logger.error(f"❌ Не удалось получить {url} после {max_retries} попыток")
                    self.stats['requests_failed'] += 1
                    
        return None

    def get_brands_catalog(self) -> List[BrandInfo]:
        """
        Получает каталог всех брендов из блока на главной странице отзывов
        (Используется проверенная логика из production_drom_parser.py)
        """
        try:
            logger.info("📋 Получение каталога брендов")
            
            # Парсим главную страницу отзывов
            url = f"{self.base_url}/reviews/"
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ищем блок с брендами
            brands_block = soup.find('div', {'data-ftid': 'component_cars-list'})
            if not brands_block or not hasattr(brands_block, 'find_all'):
                raise ValueError("Не найден блок с брендами")
            
            brands = []
            
            # Парсим каждый бренд
            brand_items = brands_block.find_all('div', class_='frg44i0')
            
            for item in brand_items:
                try:
                    # Получаем ссылку
                    link = item.find('a', {'data-ftid': 'component_cars-list-item_hidden-link'})
                    if not link:
                        continue
                    
                    brand_url = link.get('href')
                    if not brand_url:
                        continue
                    
                    # Извлекаем имя бренда из URL
                    url_name = brand_url.rstrip('/').split('/')[-1]
                    
                    # Получаем отображаемое имя
                    name_span = item.find('span', {'data-ftid': 'component_cars-list-item_name'})
                    if not name_span:
                        continue
                    
                    brand_name = name_span.get_text(strip=True)
                    
                    # Получаем количество отзывов
                    counter_span = item.find('span', {'data-ftid': 'component_cars-list-item_counter'})
                    reviews_count = 0
                    if counter_span:
                        counter_text = counter_span.get_text(strip=True)
                        # Извлекаем число из текста
                        count_match = re.search(r'\d+', counter_text.replace(' ', ''))
                        if count_match:
                            reviews_count = int(count_match.group())
                    
                    # Полный URL если нужно
                    if not brand_url.startswith('http'):
                        brand_url = f"{self.base_url}{brand_url}"
                    
                    brand_info = BrandInfo(
                        name=brand_name,
                        url=brand_url,
                        reviews_count=reviews_count,
                        url_name=url_name
                    )
                    brands.append(brand_info)
                    
                    logger.debug(f"Найден бренд: {brand_name} ({reviews_count} отзывов)")
                    
                except Exception as e:
                    logger.error(f"Ошибка при парсинге бренда: {e}")
                    continue
            
            time.sleep(self.delay)  # Задержка после запроса
            logger.info(f"✅ Найдено {len(brands)} брендов")
            return brands
            
        except Exception as e:
            logger.error(f"Ошибка получения каталога брендов: {e}")
            return []

    def get_models_for_brand(self, brand: BrandInfo) -> List[ModelInfo]:
        """
        Получает список моделей для бренда
        (Используется проверенная логика из production_drom_parser.py)
        """
        try:
            logger.info(f"🏭 Получение моделей для бренда {brand.name}")
            
            # Парсим страницу бренда
            time.sleep(self.delay)
            response = self.session.get(brand.url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            models = []
            
            # Ищем блок с моделями - ссылки на модели имеют конкретный паттерн
            model_items = soup.find_all('a', href=re.compile(rf'/reviews/{brand.url_name}/[^/]+/?$'))
            
            for item in model_items:
                try:
                    model_url = item.get('href')
                    if not model_url:
                        continue
                    
                    # Извлекаем имя модели из URL
                    url_parts = model_url.rstrip('/').split('/')
                    if len(url_parts) < 4:
                        continue
                    
                    model_url_name = url_parts[-1]
                    
                    # Получаем отображаемое имя модели
                    model_name = item.get_text(strip=True)
                    if not model_name:
                        continue
                    
                    # Получаем полный URL
                    if not model_url.startswith('http'):
                        full_model_url = f"{self.base_url}{model_url}"
                    else:
                        full_model_url = model_url
                    
                    # Проверяем что эта модель еще не добавлена
                    if any(m.url_name == model_url_name for m in models):
                        continue
                    
                    # Получаем количество отзывов для модели
                    long_count, short_count = self.get_review_counts_for_model_url(full_model_url)
                    
                    model_info = ModelInfo(
                        name=model_name,
                        brand=brand.name,
                        url=full_model_url,
                        long_reviews_count=long_count,
                        short_reviews_count=short_count,
                        url_name=model_url_name
                    )
                    models.append(model_info)
                    
                    logger.debug(f"🚗 Модель: {model_name} (длинных: {long_count}, коротких: {short_count})")
                    
                except Exception as e:
                    logger.error(f"Ошибка при парсинге модели: {e}")
                    continue
            
            logger.info(f"✅ Найдено {len(models)} моделей для {brand.name}")
            return models
            
        except Exception as e:
            logger.error(f"Ошибка получения моделей для {brand.name}: {e}")
            return []

    def get_review_counts_for_model_url(self, model_url: str) -> Tuple[int, int]:
        """Получение количества отзывов для модели по URL"""
        if not model_url.startswith('http'):
            full_url = urljoin(self.base_url, model_url)
        else:
            full_url = model_url
            
        soup = self._make_request(full_url)
        if not soup:
            return 0, 0
        
        long_reviews_count = 0
        short_reviews_count = 0
        
        try:
            # Используем проверенную логику из production_drom_parser.py
            # Ищем кнопки переключения между длинными и короткими отзывами
            tabs_block = soup.find('div', class_='_65ykvx0')
            if tabs_block and hasattr(tabs_block, 'find'):
                # Кнопка длинных отзывов
                long_reviews_tab = tabs_block.find('a', {'data-ftid': 'reviews_tab_button_long_reviews'})
                if long_reviews_tab and hasattr(long_reviews_tab, 'get_text'):
                    text = long_reviews_tab.get_text(strip=True)
                    # Удаляем пробелы из числа для корректного извлечения
                    match = re.search(r'(\d+)', text.replace(' ', ''))
                    if match:
                        long_reviews_count = int(match.group(1))
                
                # Кнопка коротких отзывов
                short_reviews_tab = tabs_block.find('a', {'data-ftid': 'reviews_tab_button_short_reviews'})
                if short_reviews_tab and hasattr(short_reviews_tab, 'get_text'):
                    text = short_reviews_tab.get_text(strip=True)
                    # Удаляем пробелы из числа для корректного извлечения
                    match = re.search(r'(\d+)', text.replace(' ', ''))
                    if match:
                        short_reviews_count = int(match.group(1))
            
            logger.debug(f"📊 {full_url}: {long_reviews_count} длинных, {short_reviews_count} коротких")
            
        except Exception as e:
            logger.warning(f"⚠️  Ошибка при подсчете отзывов для {full_url}: {e}")
        
        return long_reviews_count, short_reviews_count

    def parse_long_reviews(self, model: ModelInfo, limit: Optional[int] = None) -> List[ReviewData]:
        """Парсинг длинных отзывов"""
        logger.info(f"📝 Парсинг длинных отзывов для {model.brand} {model.name}")
        
        reviews = []
        page = 1
        max_pages = 10  # Ограничение для безопасности
        
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
                logger.info(f"📄 Нет длинных отзывов на странице {page}")
                break
                
            for block in review_blocks:
                if limit and len(reviews) >= limit:
                    break
                    
                review = self._parse_long_review_block(block, model)
                if review:
                    reviews.append(review)
                    self.stats['reviews_parsed'] += 1
                    
            page += 1
            
        logger.info(f"✅ Получено {len(reviews)} длинных отзывов")
        return reviews

    def parse_short_reviews(self, model: ModelInfo, limit: Optional[int] = None) -> List[ReviewData]:
        """Парсинг коротких отзывов"""
        logger.info(f"💭 Парсинг коротких отзывов для {model.brand} {model.name}")
        
        reviews = []
        page = 1
        max_pages = 10
        
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
                logger.info(f"📄 Нет коротких отзывов на странице {page}")
                break
                
            for block in review_blocks:
                if limit and len(reviews) >= limit:
                    break
                    
                review = self._parse_short_review_block(block, model)
                if review:
                    reviews.append(review)
                    self.stats['reviews_parsed'] += 1
                    
            page += 1
            
        logger.info(f"✅ Получено {len(reviews)} коротких отзывов")
        return reviews

    def _parse_long_review_block(self, block, model: ModelInfo) -> Optional[ReviewData]:
        """Парсинг блока длинного отзыва"""
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
            
            # Информация об авторе
            author_elem = block.find("a", class_="css-1u4ddp")
            if author_elem:
                review_data["author"] = author_elem.get_text(strip=True)
            
            # Дата
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
            return ReviewData(**review_data)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при парсинге длинного отзыва: {e}")
            return None

    def _parse_short_review_block(self, block, model: ModelInfo) -> Optional[ReviewData]:
        """Парсинг блока короткого отзыва"""
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
            return ReviewData(**review_data)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при парсинге короткого отзыва: {e}")
            return None

    def _extract_car_specs_from_title(self, title_div, review_data: dict):
        """Извлечение характеристик автомобиля из заголовка короткого отзыва"""
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
            logger.warning(f"⚠️  Ошибка при извлечении характеристик: {e}")

    def parse_model_reviews(self, 
                          model: ModelInfo, 
                          max_long_reviews: Optional[int] = None,
                          max_short_reviews: Optional[int] = None) -> List[ReviewData]:
        """Парсинг всех отзывов для модели (длинных и коротких)"""
        logger.info(f"🎯 Парсинг отзывов для {model.brand} {model.name}")
        
        all_reviews = []
        
        # Парсим длинные отзывы
        if model.long_reviews_count > 0:
            long_reviews = self.parse_long_reviews(model, limit=max_long_reviews)
            all_reviews.extend(long_reviews)
            
        # Парсим короткие отзывы
        if model.short_reviews_count > 0:
            short_reviews = self.parse_short_reviews(model, limit=max_short_reviews)
            all_reviews.extend(short_reviews)
            
        logger.info(f"✅ Всего получено {len(all_reviews)} отзывов для {model.brand} {model.name}")
        return all_reviews

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
                
            logger.info(f"💾 Сохранено {len(reviews)} отзывов в {filename}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении в JSON: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        """Получение статистики работы парсера"""
        return {
            'base_url': self.base_url,
            'cache_enabled': self.enable_cache,
            'delay': self.delay,
            **self.stats
        }

    def parse_limited_demo(self, 
                          max_brands: int = 3,
                          max_long_reviews: int = 3,
                          max_short_reviews: int = 10) -> Dict[str, Any]:
        """
        🎯 ДЕМО-ПАРСИНГ с ограничениями для тестирования
        """
        logger.info(f"🚀 Запуск демо-парсинга: {max_brands} брендов, {max_long_reviews} длинных, {max_short_reviews} коротких отзывов")
        
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
                    logger.info(f"🏭 Обрабатываем бренд: {brand.name}")
                    
                    # Получаем модели
                    models = self.get_models_for_brand(brand)
                    if not models:
                        logger.warning(f"⚠️  Нет моделей для бренда {brand.name}")
                        continue
                    
                    # Берем первую модель с отзывами - НЕ проверяем все подряд для демо
                    target_model = None
                    for model in models[:5]:  # Проверяем только первые 5 моделей
                        if model.long_reviews_count > 0 or model.short_reviews_count > 0:
                            target_model = model
                            break
                    
                    if not target_model:
                        logger.warning(f"⚠️  Нет моделей с отзывами для бренда {brand.name} (проверены первые 5)")
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
                    logger.error(f"❌ {error_msg}")
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
            
            logger.info(f"🎉 Демо-парсинг завершен: {results['total_reviews']} отзывов за {results['duration_seconds']:.1f} сек")
            
        except Exception as e:
            error_msg = f"Критическая ошибка в демо-парсинге: {e}"
            logger.error(f"❌ {error_msg}")
            results['errors'].append(error_msg)
        
        return results


# Пример использования
if __name__ == "__main__":
    # Инициализация мастер-парсера
    parser = MasterDromParser(
        delay=1.0,
        cache_dir="data/cache",
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
