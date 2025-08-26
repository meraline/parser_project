#!/usr/bin/env python3
"""
🚗 ЕДИНЫЙ ПАРСЕР ОТЗЫВОВ DROM.RU - МАСТЕР-ВЕРСИЯ
===============================================

ГЛАВНЫЙ ПАРСЕР, который объединяет ВСЕ функции:
✅ Длинные отзывы (подробные обзоры)
✅ Короткие отзывы (краткие мнения) 
✅ Каталог брендов и моделей
✅ Базу данных и кэширование
✅ Метрики и логирование
✅ Обработка ошибок и повторные попытки

БОЛЬШЕ НЕ СОЗДАЕМ НОВЫХ ПАРСЕРОВ!
РАЗВИВАЕМ ТОЛЬКО ЭТОТ!

Автор: AI Assistant
Дата: 26.08.2025
"""

import requests
import json
import time
import logging
import re
import os
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict
from datetime import datetime
import hashlib


@dataclass 
class ReviewData:
    """Структура данных отзыва"""
    review_id: str
    brand: str
    model: str
    review_type: str  # 'long' или 'short'
    year: Optional[int] = None
    engine_volume: Optional[float] = None
    fuel_type: Optional[str] = None
    transmission: Optional[str] = None
    drive_type: Optional[str] = None
    author: Optional[str] = None
    city: Optional[str] = None
    date: Optional[str] = None
    rating: Optional[int] = None
    title: Optional[str] = None
    positive: Optional[str] = None
    negative: Optional[str] = None
    general: Optional[str] = None
    breakages: Optional[str] = None
    photos: List[str] = None
    url: Optional[str] = None
    
    def __post_init__(self):
        if self.photos is None:
            self.photos = []


@dataclass
class ModelInfo:
    """Информация о модели"""
    brand: str
    model: str
    model_url: str
    long_reviews_count: int = 0
    short_reviews_count: int = 0


class UnifiedDromParser:
    """ЕДИНЫЙ МАСТЕР-ПАРСЕР для drom.ru - объединяет ВСЕ функции"""

    def __init__(self, delay: float = 1.0, cache_dir: str = "data/cache"):
        """Инициализация парсера"""
        self.base_url = "https://www.drom.ru"
        self.delay = delay
        self.cache_dir = cache_dir
        
        # Создаем директории
        os.makedirs(cache_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        # Настройка логирования
        self.setup_logging()
        
        # Настройка сессии
        self.session = requests.Session()
        self.session.proxies = {}
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }
        
        # Метрики
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'long_reviews_found': 0,
            'short_reviews_found': 0,
            'models_processed': 0,
            'brands_processed': 0
        }
        
        self.logger.info("🚀 Единый парсер DROM.RU инициализирован")

    def setup_logging(self):
        """Настройка логирования"""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            handler = logging.FileHandler('logs/unified_parser.log', encoding='utf-8')
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
            # Консольный handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def make_request(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """Выполнение HTTP запроса с повторными попытками"""
        self.stats['total_requests'] += 1
        
        for attempt in range(retries):
            try:
                self.logger.info(f"🌐 Запрос: {url} (попытка {attempt + 1})")
                
                response = self.session.get(url, headers=self.headers, timeout=30)
                
                if response.status_code == 200:
                    self.stats['successful_requests'] += 1
                    soup = BeautifulSoup(response.content, 'html.parser')
                    time.sleep(self.delay)
                    return soup
                elif response.status_code == 404:
                    self.logger.warning(f"❌ 404 Not Found: {url}")
                    self.stats['failed_requests'] += 1
                    return None
                else:
                    self.logger.warning(f"⚠️ Статус {response.status_code}: {url}")
                    
            except Exception as e:
                self.logger.error(f"❌ Ошибка запроса (попытка {attempt + 1}): {e}")
                
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 2
                self.logger.info(f"⏳ Ожидание {wait_time}с перед повтором...")
                time.sleep(wait_time)
        
        self.stats['failed_requests'] += 1
        return None

    def get_brand_catalog(self) -> List[Dict[str, str]]:
        """Получение каталога брендов"""
        cache_file = os.path.join(self.cache_dir, "brand_catalog.json")
        
        # Проверяем кэш
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    brands = json.load(f)
                self.logger.info(f"📂 Загружено {len(brands)} брендов из кэша")
                return brands
            except Exception as e:
                self.logger.warning(f"⚠️ Ошибка чтения кэша: {e}")
        
        # Парсим каталог
        self.logger.info("🔍 Парсинг каталога брендов...")
        soup = self.make_request(f"{self.base_url}/reviews/")
        
        if not soup:
            self.logger.error("❌ Не удалось загрузить каталог брендов")
            return []
        
        brands = []
        brand_links = soup.find_all('a', {'data-ftid': 'component_cars-list-item_hidden-link'})
        
        for link in brand_links:
            try:
                brand_name = link.get_text(strip=True)
                brand_url = link.get('href', '')
                
                if brand_url and brand_name:
                    # Извлекаем slug из URL
                    brand_slug = brand_url.rstrip('/').split('/')[-1]
                    
                    brands.append({
                        'name': brand_name,
                        'slug': brand_slug,
                        'url': brand_url
                    })
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Ошибка обработки бренда: {e}")
        
        # Сохраняем в кэш
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(brands, f, ensure_ascii=False, indent=2)
            self.logger.info(f"💾 Сохранено {len(brands)} брендов в кэш")
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка сохранения кэша: {e}")
        
        self.stats['brands_processed'] = len(brands)
        return brands

    def get_brand_models(self, brand_slug: str) -> List[ModelInfo]:
        """Получение моделей бренда"""
        cache_file = os.path.join(self.cache_dir, f"models_{brand_slug}.json")
        
        # Проверяем кэш
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    models_data = json.load(f)
                models = [ModelInfo(**model) for model in models_data]
                self.logger.info(f"📂 Загружено {len(models)} моделей {brand_slug} из кэша")
                return models
            except Exception as e:
                self.logger.warning(f"⚠️ Ошибка чтения кэша моделей: {e}")
        
        # Парсим модели
        brand_url = f"{self.base_url}/reviews/{brand_slug}/"
        self.logger.info(f"🔍 Парсинг моделей бренда: {brand_slug}")
        
        soup = self.make_request(brand_url)
        if not soup:
            return []
        
        models = []
        model_links = soup.find_all('a', href=True)
        
        for link in model_links:
            href = link.get('href', '')
            if f'/reviews/{brand_slug}/' in href and href.count('/') >= 4:
                try:
                    model_name = link.get_text(strip=True)
                    if model_name and len(model_name) > 1:
                        # Извлекаем slug модели из URL
                        parts = href.rstrip('/').split('/')
                        if len(parts) >= 4:
                            model_slug = parts[-1]
                            
                            # Ищем счетчики отзывов
                            parent = link.find_parent()
                            long_count = 0
                            short_count = 0
                            
                            # Поиск счетчиков в соседних элементах
                            if parent:
                                text = parent.get_text()
                                numbers = re.findall(r'\d+', text)
                                if numbers:
                                    long_count = int(numbers[0])
                                    if len(numbers) > 1:
                                        short_count = int(numbers[1])
                            
                            models.append(ModelInfo(
                                brand=brand_slug,
                                model=model_slug,
                                model_url=href,
                                long_reviews_count=long_count,
                                short_reviews_count=short_count
                            ))
                            
                except Exception as e:
                    self.logger.warning(f"⚠️ Ошибка обработки модели: {e}")
        
        # Убираем дубликаты
        seen = set()
        unique_models = []
        for model in models:
            key = (model.brand, model.model)
            if key not in seen:
                seen.add(key)
                unique_models.append(model)
        
        # Сохраняем в кэш
        try:
            models_data = [asdict(model) for model in unique_models]
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(models_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"💾 Сохранено {len(unique_models)} моделей {brand_slug} в кэш")
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка сохранения кэша моделей: {e}")
        
        return unique_models

    def parse_long_reviews(self, brand: str, model: str, limit: int = 10) -> List[ReviewData]:
        """Парсинг длинных отзывов"""
        url = f"{self.base_url}/reviews/{brand}/{model}/"
        self.logger.info(f"📝 Парсинг длинных отзывов: {brand}/{model}")
        
        soup = self.make_request(url)
        if not soup:
            return []
        
        reviews = []
        review_items = soup.find_all('div', {'data-ftid': 'review-item'})
        
        self.logger.info(f"🔍 Найдено {len(review_items)} длинных отзывов")
        
        for i, item in enumerate(review_items[:limit]):
            try:
                review = self.extract_long_review_data(item, brand, model, url)
                if review:
                    reviews.append(review)
                    self.stats['long_reviews_found'] += 1
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Ошибка парсинга длинного отзыва {i+1}: {e}")
        
        return reviews

    def parse_short_reviews(self, brand: str, model: str, limit: int = 20) -> List[ReviewData]:
        """Парсинг коротких отзывов"""
        url = f"{self.base_url}/reviews/{brand}/{model}/5kopeek/"
        self.logger.info(f"💬 Парсинг коротких отзывов: {brand}/{model}")
        
        soup = self.make_request(url)
        if not soup:
            return []
        
        reviews = []
        review_items = soup.find_all('div', {'data-ftid': 'short-review-item'})
        
        self.logger.info(f"🔍 Найдено {len(review_items)} коротких отзывов")
        
        for i, item in enumerate(review_items[:limit]):
            try:
                review = self.extract_short_review_data(item, brand, model, url)
                if review:
                    reviews.append(review)
                    self.stats['short_reviews_found'] += 1
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Ошибка парсинга короткого отзыва {i+1}: {e}")
        
        return reviews

    def extract_long_review_data(self, item, brand: str, model: str, base_url: str) -> Optional[ReviewData]:
        """Извлечение данных длинного отзыва"""
        try:
            # ID отзыва
            review_id = item.get('id', f"long_{hash(str(item)[:100])}")
            
            # Автор и дата
            author_elem = item.find('span', {'data-ftid': 'review-item-author-name'})
            author = author_elem.get_text(strip=True) if author_elem else None
            
            date_elem = item.find('span', {'data-ftid': 'review-item-date'})
            date = date_elem.get_text(strip=True) if date_elem else None
            
            # Город
            city_elem = item.find('span', {'data-ftid': 'review-item-author-city'})
            city = city_elem.get_text(strip=True) if city_elem else None
            
            # Заголовок
            title_elem = item.find('h3', {'data-ftid': 'review-item-title'})
            title = title_elem.get_text(strip=True) if title_elem else None
            
            # Характеристики авто
            car_info = item.find('div', {'data-ftid': 'review-item-car-info'})
            year, engine_volume, fuel_type, transmission, drive_type = None, None, None, None, None
            
            if car_info:
                info_text = car_info.get_text()
                year = self.extract_year(info_text)
                engine_volume = self.extract_engine_volume(info_text)
                fuel_type = self.extract_fuel_type(info_text)
                transmission = self.extract_transmission(info_text)
                drive_type = self.extract_drive_type(info_text)
            
            # Контент отзыва
            positive_elem = item.find('div', {'data-ftid': 'review-item-positive'})
            positive = positive_elem.get_text(strip=True) if positive_elem else None
            
            negative_elem = item.find('div', {'data-ftid': 'review-item-negative'})
            negative = negative_elem.get_text(strip=True) if negative_elem else None
            
            general_elem = item.find('div', {'data-ftid': 'review-item-general'})
            general = general_elem.get_text(strip=True) if general_elem else None
            
            breakages_elem = item.find('div', {'data-ftid': 'review-item-breakages'})
            breakages = breakages_elem.get_text(strip=True) if breakages_elem else None
            
            # Фотографии
            photos = []
            img_elements = item.find_all('img', src=True)
            for img in img_elements:
                src = img.get('src')
                if src and 'photo' in src:
                    photos.append(src)
            
            # Рейтинг
            rating_elem = item.find('div', class_=re.compile(r'rating|stars'))
            rating = self.extract_rating(rating_elem) if rating_elem else None
            
            return ReviewData(
                review_id=review_id,
                brand=brand,
                model=model,
                review_type='long',
                year=year,
                engine_volume=engine_volume,
                fuel_type=fuel_type,
                transmission=transmission,
                drive_type=drive_type,
                author=author,
                city=city,
                date=date,
                rating=rating,
                title=title,
                positive=positive,
                negative=negative,
                general=general,
                breakages=breakages,
                photos=photos,
                url=base_url
            )
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка извлечения длинного отзыва: {e}")
            return None

    def extract_short_review_data(self, item, brand: str, model: str, base_url: str) -> Optional[ReviewData]:
        """Извлечение данных короткого отзыва"""
        try:
            # ID отзыва
            review_id = item.get('id', f"short_{hash(str(item)[:100])}")
            
            # Автор и дата (в коротких отзывах может быть в другом формате)
            author_elem = item.find('span', class_=re.compile(r'author|user'))
            author = author_elem.get_text(strip=True) if author_elem else None
            
            # Дата
            date_elems = item.find_all('span')
            date = None
            for elem in date_elems:
                text = elem.get_text(strip=True)
                if 'назад' in text or 'год' in text or re.search(r'\d+\.\d+\.\d+', text):
                    date = text
                    break
            
            # Город
            city_elem = item.find('span', {'data-ftid': 'short-review-city'})
            city = city_elem.get_text(strip=True) if city_elem else None
            
            # Характеристики авто из заголовка
            title_elem = item.find('div', {'data-ftid': 'short-review-item__title'})
            year, engine_volume, fuel_type, transmission, drive_type = None, None, None, None, None
            
            if title_elem:
                title_text = title_elem.get_text()
                year = self.extract_year(title_text)
                engine_volume = self.extract_engine_volume(title_text)
                fuel_type = self.extract_fuel_type(title_text)
                transmission = self.extract_transmission(title_text)
                drive_type = self.extract_drive_type(title_text)
            
            # Контент отзыва
            positive_elem = item.find('div', {'data-ftid': 'short-review-content__positive'})
            positive = positive_elem.get_text(strip=True) if positive_elem else None
            
            negative_elem = item.find('div', {'data-ftid': 'short-review-content__negative'})
            negative = negative_elem.get_text(strip=True) if negative_elem else None
            
            breakages_elem = item.find('div', {'data-ftid': 'short-review-content__breakages'})
            breakages = breakages_elem.get_text(strip=True) if breakages_elem else None
            
            # Фотографии
            photos = []
            img_elements = item.find_all('img', src=True)
            for img in img_elements:
                src = img.get('src')
                if src and ('photo' in src or 'auto.drom.ru' in src):
                    photos.append(src)
            
            return ReviewData(
                review_id=review_id,
                brand=brand,
                model=model,
                review_type='short',
                year=year,
                engine_volume=engine_volume,
                fuel_type=fuel_type,
                transmission=transmission,
                drive_type=drive_type,
                author=author,
                city=city,
                date=date,
                rating=None,  # В коротких отзывах обычно нет рейтинга
                title=None,
                positive=positive,
                negative=negative,
                general=None,
                breakages=breakages,
                photos=photos,
                url=base_url
            )
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка извлечения короткого отзыва: {e}")
            return None

    # Вспомогательные методы для извлечения данных
    def extract_year(self, text: str) -> Optional[int]:
        """Извлечение года"""
        match = re.search(r'\b(19|20)\d{2}\b', text)
        if match:
            year = int(match.group())
            if 1980 <= year <= datetime.now().year:
                return year
        return None

    def extract_engine_volume(self, text: str) -> Optional[float]:
        """Извлечение объема двигателя"""
        patterns = [
            r'(\d+(?:\.\d+)?)\s*л(?:\.|[^а-яё])',
            r'(\d{4})\s*см³',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    volume = float(match.group(1))
                    if volume > 100:  # см³
                        volume = volume / 1000
                    if 0.5 <= volume <= 10.0:
                        return volume
                except ValueError:
                    continue
        return None

    def extract_fuel_type(self, text: str) -> Optional[str]:
        """Извлечение типа топлива"""
        fuel_types = {
            'бензин': ['бензин', 'gasoline', 'petrol'],
            'дизель': ['дизель', 'дизел', 'diesel'],
            'гибрид': ['гибрид', 'hybrid'],
            'электро': ['электро', 'electric', 'электрический'],
            'газ': ['газ', 'lpg', 'cng']
        }
        
        text_lower = text.lower()
        for fuel_type, keywords in fuel_types.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return fuel_type
        return None

    def extract_transmission(self, text: str) -> Optional[str]:
        """Извлечение типа трансмиссии"""
        transmissions = {
            'автомат': ['автомат', 'акпп', 'automatic', 'at'],
            'механика': ['механика', 'мкпп', 'manual', 'mt'],
            'вариатор': ['вариатор', 'cvt'],
            'робот': ['робот', 'amt', 'robot']
        }
        
        text_lower = text.lower()
        for trans_type, keywords in transmissions.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return trans_type
        return None

    def extract_drive_type(self, text: str) -> Optional[str]:
        """Извлечение типа привода"""
        drive_types = {
            'передний': ['передний', 'fwd', 'front'],
            'задний': ['задний', 'rwd', 'rear'],
            'полный': ['полный', 'awd', '4wd', 'all']
        }
        
        text_lower = text.lower()
        for drive_type, keywords in drive_types.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return drive_type
        return None

    def extract_rating(self, element) -> Optional[int]:
        """Извлечение рейтинга"""
        if not element:
            return None
            
        # Поиск числового рейтинга
        text = element.get_text()
        match = re.search(r'(\d+(?:\.\d+)?)', text)
        if match:
            try:
                rating = float(match.group(1))
                if 1 <= rating <= 5:
                    return int(rating)
            except ValueError:
                pass
        
        # Поиск звезд или других индикаторов
        stars = element.find_all(class_=re.compile(r'star|rate'))
        if stars:
            return len(stars)
        
        return None

    def parse_model_reviews(self, brand: str, model: str, long_limit: int = 3, short_limit: int = 10) -> Dict:
        """Парсинг всех отзывов модели"""
        self.logger.info(f"🚗 Парсинг отзывов модели: {brand}/{model}")
        
        # Парсим длинные отзывы
        long_reviews = self.parse_long_reviews(brand, model, long_limit)
        
        # Парсим короткие отзывы
        short_reviews = self.parse_short_reviews(brand, model, short_limit)
        
        self.stats['models_processed'] += 1
        
        result = {
            'brand': brand,
            'model': model,
            'long_reviews': [asdict(review) for review in long_reviews],
            'short_reviews': [asdict(review) for review in short_reviews],
            'stats': {
                'long_reviews_count': len(long_reviews),
                'short_reviews_count': len(short_reviews),
                'total_reviews': len(long_reviews) + len(short_reviews)
            }
        }
        
        self.logger.info(f"✅ Завершен парсинг {brand}/{model}: "
                        f"{len(long_reviews)} длинных + {len(short_reviews)} коротких отзывов")
        
        return result

    def parse_multiple_models(self, models_limit: int = 3, long_limit: int = 3, short_limit: int = 10) -> Dict:
        """Парсинг нескольких моделей"""
        self.logger.info(f"🎯 Начинаем парсинг {models_limit} моделей")
        
        # Получаем каталог брендов
        brands = self.get_brand_catalog()
        if not brands:
            self.logger.error("❌ Не удалось получить каталог брендов")
            return {'error': 'Не удалось получить каталог брендов'}
        
        results = []
        models_processed = 0
        
        # Проходим по первым брендам
        for brand in brands[:5]:  # Проверяем первые 5 брендов
            if models_processed >= models_limit:
                break
                
            brand_slug = brand['slug']
            self.logger.info(f"🏭 Обрабатываем бренд: {brand['name']} ({brand_slug})")
            
            # Получаем модели бренда
            models = self.get_brand_models(brand_slug)
            
            # Парсим первые модели этого бренда
            for model in models[:2]:  # Максимум 2 модели с бренда
                if models_processed >= models_limit:
                    break
                
                try:
                    result = self.parse_model_reviews(
                        model.brand, 
                        model.model, 
                        long_limit, 
                        short_limit
                    )
                    results.append(result)
                    models_processed += 1
                    
                    # Задержка между моделями
                    time.sleep(self.delay * 2)
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка парсинга модели {model.brand}/{model.model}: {e}")
        
        # Формируем итоговый результат
        total_long_reviews = sum(len(r['long_reviews']) for r in results)
        total_short_reviews = sum(len(r['short_reviews']) for r in results)
        
        final_result = {
            'summary': {
                'models_processed': models_processed,
                'total_long_reviews': total_long_reviews,
                'total_short_reviews': total_short_reviews,
                'total_reviews': total_long_reviews + total_short_reviews
            },
            'models': results,
            'stats': self.stats
        }
        
        self.logger.info(f"🎉 ПАРСИНГ ЗАВЕРШЕН!")
        self.logger.info(f"📊 Статистика:")
        self.logger.info(f"   - Моделей обработано: {models_processed}")
        self.logger.info(f"   - Длинных отзывов: {total_long_reviews}")
        self.logger.info(f"   - Коротких отзывов: {total_short_reviews}")
        self.logger.info(f"   - Всего отзывов: {total_long_reviews + total_short_reviews}")
        
        return final_result

    def save_results(self, results: Dict, filename: str = None):
        """Сохранение результатов"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"parsing_results_{timestamp}.json"
        
        filepath = os.path.join("data", filename)
        os.makedirs("data", exist_ok=True)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"💾 Результаты сохранены: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения: {e}")
            return None


def main():
    """Главная функция для демонстрации парсера"""
    print("🚗 ЕДИНЫЙ ПАРСЕР ОТЗЫВОВ DROM.RU")
    print("=" * 50)
    
    # Создаем парсер
    parser = UnifiedDromParser(delay=1.0)
    
    # Запускаем парсинг 3 моделей
    print("🎯 Парсинг первых 3 моделей...")
    results = parser.parse_multiple_models(
        models_limit=3,
        long_limit=3,
        short_limit=10
    )
    
    # Сохраняем результаты
    filepath = parser.save_results(results)
    
    print("\n📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   - Моделей: {results['summary']['models_processed']}")
    print(f"   - Длинных отзывов: {results['summary']['total_long_reviews']}")
    print(f"   - Коротких отзывов: {results['summary']['total_short_reviews']}")
    print(f"   - Всего отзывов: {results['summary']['total_reviews']}")
    print(f"   - Файл: {filepath}")
    
    print("\n🎉 ПАРСИНГ ЗАВЕРШЕН УСПЕШНО!")


if __name__ == "__main__":
    main()
