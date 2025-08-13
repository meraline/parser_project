#!/usr/bin/env python3
"""
Стабильный парсер отзывов и бортжурналов для автомобилей
Собирает данные с Drom.ru и Drive2.ru в базу данных
Работает в щадящем режиме для долгосрочного сбора данных
"""

import sqlite3
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re
import json
import logging
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
import hashlib
from pathlib import Path

from botasaurus.browser import browser, Driver
from botasaurus.request import request, Request
from botasaurus.soupify import soupify
from botasaurus import bt

# ==================== НАСТРОЙКИ ====================

class Config:
    """Конфигурация парсера"""
    
    # База данных
    DB_PATH = "auto_reviews.db"
    
    # Задержки (в секундах)
    MIN_DELAY = 5      # Минимальная задержка между запросами
    MAX_DELAY = 15     # Максимальная задержка между запросами
    ERROR_DELAY = 30   # Задержка при ошибке
    RATE_LIMIT_DELAY = 300  # Задержка при rate limit (5 минут)
    
    # Ограничения
    MAX_RETRIES = 3    # Максимальное количество повторов
    PAGES_PER_SESSION = 50  # Страниц за сессию
    MAX_REVIEWS_PER_MODEL = 1000  # Максимум отзывов на модель
    
    # User agents для ротации
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    
    # Список популярных брендов и моделей для парсинга
    TARGET_BRANDS = {
        'toyota': ['camry', 'corolla', 'rav4', 'highlander', 'prius', 'land-cruiser'],
        'volkswagen': ['polo', 'golf', 'passat', 'tiguan', 'touareg', 'jetta'],
        'nissan': ['qashqai', 'x-trail', 'almera', 'teana', 'murano', 'pathfinder'],
        'hyundai': ['solaris', 'elantra', 'tucson', 'santa-fe', 'creta', 'sonata'],
        'kia': ['rio', 'cerato', 'sportage', 'sorento', 'soul', 'optima'],
        'mazda': ['mazda3', 'mazda6', 'cx-5', 'cx-3', 'mx-5', 'cx-9'],
        'ford': ['focus', 'fiesta', 'mondeo', 'kuga', 'explorer', 'ecosport'],
        'chevrolet': ['cruze', 'aveo', 'captiva', 'lacetti', 'tahoe', 'suburban'],
        'skoda': ['octavia', 'rapid', 'fabia', 'superb', 'kodiaq', 'karoq'],
        'renault': ['logan', 'sandero', 'duster', 'kaptur', 'megane', 'fluence'],
        'mitsubishi': ['lancer', 'outlander', 'asx', 'pajero', 'eclipse-cross', 'l200'],
        'honda': ['civic', 'accord', 'cr-v', 'pilot', 'fit', 'hr-v'],
        'bmw': ['3-series', '5-series', 'x3', 'x5', 'x1', '1-series'],
        'mercedes-benz': ['c-class', 'e-class', 's-class', 'glc', 'gle', 'gla'],
        'audi': ['a3', 'a4', 'a6', 'q3', 'q5', 'q7'],
        'lada': ['granta', 'kalina', 'priora', 'vesta', 'xray', 'largus']
    }

# ==================== МОДЕЛИ ДАННЫХ ====================

@dataclass
class ReviewData:
    """Структура данных отзыва"""
    source: str          # drom.ru, drive2.ru
    type: str           # review, board_journal
    brand: str
    model: str
    generation: Optional[str] = None
    year: Optional[int] = None
    url: str = ""
    title: str = ""
    content: str = ""
    author: str = ""
    rating: Optional[float] = None
    pros: str = ""
    cons: str = ""
    mileage: Optional[int] = None
    engine_volume: Optional[float] = None
    fuel_type: str = ""
    transmission: str = ""
    body_type: str = ""
    drive_type: str = ""
    publish_date: Optional[datetime] = None
    views_count: Optional[int] = None
    likes_count: Optional[int] = None
    comments_count: Optional[int] = None
    parsed_at: datetime = None
    content_hash: str = ""
    
    def __post_init__(self):
        if self.parsed_at is None:
            self.parsed_at = datetime.now()
        
        # Создаем хеш контента для дедупликации
        content_for_hash = f"{self.url}_{self.title}_{self.content[:100]}"
        self.content_hash = hashlib.md5(content_for_hash.encode()).hexdigest()

# ==================== БАЗА ДАННЫХ ====================

class ReviewsDatabase:
    """Управление базой данных отзывов"""
    
    def __init__(self, db_path: str = Config.DB_PATH):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Таблица отзывов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                type TEXT NOT NULL,
                brand TEXT NOT NULL,
                model TEXT NOT NULL,
                generation TEXT,
                year INTEGER,
                url TEXT UNIQUE,
                title TEXT,
                content TEXT,
                author TEXT,
                rating REAL,
                pros TEXT,
                cons TEXT,
                mileage INTEGER,
                engine_volume REAL,
                fuel_type TEXT,
                transmission TEXT,
                body_type TEXT,
                drive_type TEXT,
                publish_date DATETIME,
                views_count INTEGER,
                likes_count INTEGER,
                comments_count INTEGER,
                parsed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                content_hash TEXT UNIQUE,
                UNIQUE(url, content_hash)
            )
        """)
        
        # Таблица статистики парсинга
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS parsing_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                brand TEXT,
                model TEXT,
                source TEXT,
                pages_parsed INTEGER DEFAULT 0,
                reviews_found INTEGER DEFAULT 0,
                errors_count INTEGER DEFAULT 0,
                started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                finished_at DATETIME,
                status TEXT DEFAULT 'running'
            )
        """)
        
        # Таблица источников (для отслеживания прогресса)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sources_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand TEXT,
                model TEXT,
                source TEXT,
                base_url TEXT,
                priority INTEGER DEFAULT 1,
                status TEXT DEFAULT 'pending',
                last_parsed DATETIME,
                total_pages INTEGER DEFAULT 0,
                parsed_pages INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Индексы для быстрого поиска
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_brand_model ON reviews(brand, model)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_type ON reviews(source, type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_parsed_at ON reviews(parsed_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_content_hash ON reviews(content_hash)")
        
        conn.commit()
        conn.close()
    
    def save_review(self, review: ReviewData) -> bool:
        """Сохранение отзыва в базу"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR IGNORE INTO reviews (
                    source, type, brand, model, generation, year, url, title, content,
                    author, rating, pros, cons, mileage, engine_volume, fuel_type,
                    transmission, body_type, drive_type, publish_date, views_count,
                    likes_count, comments_count, parsed_at, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                review.source, review.type, review.brand, review.model, review.generation,
                review.year, review.url, review.title, review.content, review.author,
                review.rating, review.pros, review.cons, review.mileage, review.engine_volume,
                review.fuel_type, review.transmission, review.body_type, review.drive_type,
                review.publish_date, review.views_count, review.likes_count,
                review.comments_count, review.parsed_at, review.content_hash
            ))
            
            success = cursor.rowcount > 0
            conn.commit()
            conn.close()
            return success
            
        except sqlite3.IntegrityError:
            # Дублирующая запись
            return False
        except Exception as e:
            logging.error(f"Ошибка сохранения отзыва: {e}")
            return False
    
    def get_reviews_count(self, brand: str = None, model: str = None) -> int:
        """Получение количества отзывов"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if brand and model:
            cursor.execute("SELECT COUNT(*) FROM reviews WHERE brand = ? AND model = ?", (brand, model))
        elif brand:
            cursor.execute("SELECT COUNT(*) FROM reviews WHERE brand = ?", (brand,))
        else:
            cursor.execute("SELECT COUNT(*) FROM reviews")
        
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def is_url_parsed(self, url: str) -> bool:
        """Проверка, был ли URL уже спарсен"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 FROM reviews WHERE url = ? LIMIT 1", (url,))
        exists = cursor.fetchone() is not None
        
        conn.close()
        return exists
    
    def get_parsing_stats(self) -> Dict:
        """Получение статистики парсинга"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Общая статистика
        cursor.execute("SELECT COUNT(*) FROM reviews")
        total_reviews = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT brand) FROM reviews")
        unique_brands = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT model) FROM reviews")
        unique_models = cursor.fetchone()[0]
        
        cursor.execute("SELECT source, COUNT(*) FROM reviews GROUP BY source")
        by_source = dict(cursor.fetchall())
        
        cursor.execute("SELECT type, COUNT(*) FROM reviews GROUP BY type")
        by_type = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            'total_reviews': total_reviews,
            'unique_brands': unique_brands,
            'unique_models': unique_models,
            'by_source': by_source,
            'by_type': by_type
        }

# ==================== ПАРСЕРЫ ====================

class BaseParser:
    """Базовый класс для парсеров"""
    
    def __init__(self, db: ReviewsDatabase):
        self.db = db
        self.session_stats = {
            'parsed': 0,
            'saved': 0,
            'errors': 0,
            'skipped': 0
        }
    
    def random_delay(self, min_delay: int = None, max_delay: int = None):
        """Случайная задержка между запросами"""
        min_delay = min_delay or Config.MIN_DELAY
        max_delay = max_delay or Config.MAX_DELAY
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def normalize_text(self, text: str) -> str:
        """Нормализация текста"""
        if not text:
            return ""
        
        # Убираем лишние пробелы и переносы
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Убираем HTML теги если остались
        text = re.sub(r'<[^>]+>', '', text)
        
        return text
    
    def extract_year(self, text: str) -> Optional[int]:
        """Извлечение года из текста"""
        year_match = re.search(r'\b(19|20)\d{2}\b', text)
        if year_match:
            year = int(year_match.group())
            if 1980 <= year <= datetime.now().year:
                return year
        return None
    
    def extract_mileage(self, text: str) -> Optional[int]:
        """Извлечение пробега из текста"""
        # Ищем числа с "км", "тыс", "k" и т.д.
        patterns = [
            r'(\d+(?:\s*\d{3})*)\s*(?:тыс\.?\s*)?км',
            r'(\d+)\s*(?:k|К)\s*км',
            r'пробег[:\s]*(\d+(?:\s*\d{3})*)',
            r'(\d+(?:\s*\d{3})*)\s*(?:тысяч|тыс)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                mileage_str = match.group(1).replace(' ', '')
                try:
                    mileage = int(mileage_str)
                    # Если число меньше 1000, возможно это тысячи километров
                    if mileage < 1000:
                        mileage *= 1000
                    return mileage
                except ValueError:
                    continue
        
        return None
    
    def extract_engine_volume(self, text: str) -> Optional[float]:
        """Извлечение объема двигателя из текста"""
        patterns = [
            r'(\d+(?:\.\d+)?)\s*л',
            r'(\d{4})\s*см³',  # 1600 см³
            r'(\d+\.\d+)',     # 1.6, 2.0 и т.д.
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                try:
                    volume = float(match)
                    # Преобразуем см³ в литры
                    if volume > 100:  # см³
                        volume = volume / 1000
                    
                    if 0.8 <= volume <= 8.0:  # Разумные пределы
                        return volume
                except ValueError:
                    continue
        
        return None

# @browser(
#     block_images=True,
#     cache=True,
#     reuse_driver=True,
#     max_retry=3,
#     user_agent=random.choice(Config.USER_AGENTS),
#     headless=True
# )
class DromParser(BaseParser):
    """Парсер отзывов с Drom.ru"""

    @staticmethod
    @browser(
        block_images=True,
        cache=False,
        reuse_driver=True,
        max_retry=3,
        user_agent=random.choice(Config.USER_AGENTS),
        headless=True,
    )
    def parse_brand_model_reviews(driver: Driver, data: Dict, parser) -> List[ReviewData]:
        """Парсинг отзывов для конкретной марки и модели"""
        brand = data['brand']
        model = data['model']
        max_pages = data.get('max_pages', 50)

        reviews = []
        base_url = f"https://www.drom.ru/reviews/{brand}/{model}/"

        try:
            print(f"  🔍 Drom.ru: Парсинг отзывов {brand} {model}")

            # Переходим на страницу отзывов
            driver.google_get(base_url, bypass_cloudflare=True)
            parser.random_delay(3, 7)

            # Проверяем, что страница загрузилась корректно
            if driver.select('.error-page') or "404" in driver.title:
                print(f"    ⚠️ Страница не найдена: {base_url}")
                return reviews

            # Парсим все страницы
            current_page = 1

            while current_page <= max_pages:
                print(f"    📄 Страница {current_page}")

                # Ищем карточки отзывов
                review_cards = driver.select_all('[data-ftid="component_reviews-item"]')
                if not review_cards:
                    review_cards = driver.select_all('.css-1ksh4lf')

                if not review_cards:
                    print(f"    ⚠️ Отзывы не найдены на странице {current_page}")
                    break

                page_reviews = 0

                for card in review_cards:
                    try:
                        review = parser._parse_review_card(card, brand, model, base_url)
                        if review and not parser.db.is_url_parsed(review.url):
                            reviews.append(review)
                            page_reviews += 1

                    except Exception as e:
                        parser.session_stats['errors'] += 1
                        logging.error(f"Ошибка парсинга карточки отзыва: {e}")

                print(f"    ✓ Найдено {page_reviews} новых отзывов")

                # Ищем ссылку на следующую страницу
                next_link = driver.select('a[rel="next"]')
                if not next_link:
                    print(f"    📋 Больше страниц нет")
                    break

                # Переходим на следующую страницу
                next_url = next_link.get_attribute('href')
                if next_url:
                    if not next_url.startswith('http'):
                        next_url = urljoin(base_url, next_url)

                    driver.get_via_this_page(next_url)
                    parser.random_delay()
                    current_page += 1
                else:
                    break

            print(f"  ✓ Drom.ru: Собрано {len(reviews)} отзывов для {brand} {model}")

        except Exception as e:
            logging.error(f"Ошибка парсинга Drom.ru {brand} {model}: {e}")
            parser.session_stats['errors'] += 1

        return reviews
    
    def _parse_drive2_card(self, card, brand: str, model: str, review_type: str, base_url: str) -> Optional[ReviewData]:
        """Парсинг одной карточки Drive2"""
        try:
            review = ReviewData(
                source="drive2.ru",
                type=review_type,
                brand=brand,
                model=model
            )
            
            # Заголовок и ссылка
            title_link = card.select('a.c-car-card__caption') or card.select('a.c-post-card__title') or card.select('h3 a')
            if title_link:
                review.title = self.normalize_text(title_link.get_text())
                href = title_link.get_attribute('href')
                if href:
                    review.url = urljoin(base_url, href)
            
            # Автор
            author_elem = card.select('.c-username__link') or card.select('.c-post-card__author')
            if author_elem:
                review.author = self.normalize_text(author_elem.get_text())
            
            # Информация об автомобиле
            info_elem = card.select('.c-car-card__info') or card.select('.c-post-card__car-info')
            if info_elem:
                info_text = info_elem.get_text()
                
                # Извлекаем характеристики
                review.year = self.extract_year(info_text)
                review.engine_volume = self.extract_engine_volume(info_text)
                review.mileage = self.extract_mileage(info_text)
                
                # Дополнительные характеристики
                if 'бензин' in info_text.lower():
                    review.fuel_type = 'бензин'
                elif 'дизель' in info_text.lower():
                    review.fuel_type = 'дизель'
                
                if 'автомат' in info_text.lower():
                    review.transmission = 'автомат'
                elif 'механик' in info_text.lower():
                    review.transmission = 'механика'
                
                # Привод
                if 'полный' in info_text.lower() or '4wd' in info_text.lower():
                    review.drive_type = 'полный'
                elif 'передний' in info_text.lower() or 'fwd' in info_text.lower():
                    review.drive_type = 'передний'
                elif 'задний' in info_text.lower() or 'rwd' in info_text.lower():
                    review.drive_type = 'задний'
            
            # Пробег отдельно
            mileage_elem = card.select('.c-car-card__param_mileage')
            if mileage_elem:
                mileage_text = mileage_elem.get_text()
                review.mileage = self.extract_mileage(mileage_text)
            
            # Краткое содержание
            preview_elem = card.select('.c-car-card__preview') or card.select('.c-post-card__preview')
            if preview_elem:
                review.content = self.normalize_text(preview_elem.get_text())
            
            # Статистика
            views_elem = card.select('.c-post-card__views')
            if views_elem:
                views_text = views_elem.get_text()
                views_match = re.search(r'(\d+)', views_text)
                if views_match:
                    review.views_count = int(views_match.group(1))
            
            likes_elem = card.select('.c-post-card__likes')
            if likes_elem:
                likes_text = likes_elem.get_text()
                likes_match = re.search(r'(\d+)', likes_text)
                if likes_match:
                    review.likes_count = int(likes_match.group(1))
            
            # Дата
            date_elem = card.select('.c-post-card__date') or card.select('.c-car-card__date')
            if date_elem:
                date_text = date_elem.get_text()
                review.publish_date = self._parse_date(date_text)
            
            return review if review.url else None
            
        except Exception as e:
            logging.error(f"Ошибка парсинга карточки Drive2: {e}")
            return None
    
    def _parse_date(self, date_text: str) -> Optional[datetime]:
        """Парсинг даты из текста Drive2"""
        try:
            # Убираем лишние символы
            date_text = re.sub(r'[^\d\.\s\w]', '', date_text).strip()
            
            # Обрабатываем относительные даты
            now = datetime.now()
            
            if 'сегодня' in date_text.lower():
                return now.replace(hour=12, minute=0, second=0, microsecond=0)
            elif 'вчера' in date_text.lower():
                return (now - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
            elif 'назад' in date_text.lower():
                if 'дн' in date_text:
                    days_match = re.search(r'(\d+)\s*дн', date_text)
                    if days_match:
                        days = int(days_match.group(1))
                        return now - timedelta(days=days)
                elif 'час' in date_text:
                    hours_match = re.search(r'(\d+)\s*час', date_text)
                    if hours_match:
                        hours = int(hours_match.group(1))
                        return now - timedelta(hours=hours)
            
            # Стандартные форматы
            patterns = [
                r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # 01.01.2023
                r'(\d{1,2})\s+(\w+)\s+(\d{4})',   # 1 января 2023
                r'(\d{4})-(\d{2})-(\d{2})',       # 2023-01-01
            ]
            
            months_map = {
                'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
            }
            
            for pattern in patterns:
                match = re.search(pattern, date_text)
                if match:
                    groups = match.groups()
                    
                    if len(groups) == 3:
                        if groups[1].isdigit():
                            day, month, year = map(int, groups)
                        else:
                            day = int(groups[0])
                            month = months_map.get(groups[1].lower(), 1)
                            year = int(groups[2])
                        
                        return datetime(year, month, day)
            
        except Exception:
            pass
        
        return None

# ==================== ГЛАВНЫЙ ПАРСЕР ====================

class AutoReviewsParser:
    """Главный класс парсера отзывов автомобилей"""
    
    def __init__(self, db_path: str = Config.DB_PATH):
        self.db = ReviewsDatabase(db_path)
        self.setup_logging()
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Инициализация парсеров
        self.drom_parser = DromParser(self.db)
        self.drive2_parser = Drive2Parser(self.db)
    
    def setup_logging(self):
        """Настройка логирования"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"parser_{datetime.now().strftime('%Y%m%d')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def initialize_sources_queue(self):
        """Инициализация очереди источников для парсинга"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        # Очищаем старую очередь
        cursor.execute("DELETE FROM sources_queue")
        
        # Добавляем все комбинации брендов и моделей
        for brand, models in Config.TARGET_BRANDS.items():
            for model in models:
                for source in ['drom.ru', 'drive2.ru']:
                    cursor.execute("""
                        INSERT INTO sources_queue (brand, model, source, priority)
                        VALUES (?, ?, ?, ?)
                    """, (brand, model, source, 1))
        
        conn.commit()
        conn.close()
        
        print(f"✅ Инициализирована очередь из {len(Config.TARGET_BRANDS) * sum(len(models) for models in Config.TARGET_BRANDS.values()) * 2} источников")
    
    def get_next_source(self) -> Optional[Tuple[str, str, str]]:
        """Получение следующего источника для парсинга"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        # Ищем неспарсенные источники, сортируем по приоритету
        cursor.execute("""
            SELECT id, brand, model, source FROM sources_queue 
            WHERE status = 'pending' 
            ORDER BY priority DESC, RANDOM()
            LIMIT 1
        """)
        
        result = cursor.fetchone()
        
        if result:
            source_id, brand, model, source = result
            
            # Отмечаем как обрабатываемый
            cursor.execute("""
                UPDATE sources_queue 
                SET status = 'processing', last_parsed = CURRENT_TIMESTAMP 
                WHERE id = ?
            """, (source_id,))
            
            conn.commit()
            conn.close()
            
            return brand, model, source
        
        conn.close()
        return None
    
    def mark_source_completed(self, brand: str, model: str, source: str, pages_parsed: int, reviews_found: int):
        """Отметка источника как завершенного"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE sources_queue 
            SET status = 'completed', parsed_pages = ?, total_pages = ?
            WHERE brand = ? AND model = ? AND source = ?
        """, (pages_parsed, pages_parsed, brand, model, source))
        
        conn.commit()
        conn.close()
    
    def parse_single_source(self, brand: str, model: str, source: str) -> int:
        """Парсинг одного источника"""
        print(f"\n🎯 Парсинг: {brand} {model} на {source}")
        
        reviews = []
        data = {
            'brand': brand,
            'model': model,
            'max_pages': Config.PAGES_PER_SESSION
        }
        
        try:
            if source == 'drom.ru':
                reviews = self.drom_parser.parse_brand_model_reviews(
                    data, metadata=self.drom_parser
                )
            elif source == 'drive2.ru':
                reviews = self.drive2_parser.parse_brand_model_reviews(
                    data, metadata=self.drive2_parser
                )
            
            # Сохраняем отзывы в базу
            saved_count = 0
            for review in reviews:
                if self.db.save_review(review):
                    saved_count += 1
            
            print(f"  💾 Сохранено {saved_count} из {len(reviews)} отзывов")
            
            # Отмечаем источник как завершенный
            self.mark_source_completed(brand, model, source, Config.PAGES_PER_SESSION, saved_count)
            
            return saved_count
            
        except Exception as e:
            logging.error(f"Критическая ошибка парсинга {brand} {model} {source}: {e}")
            return 0
    
    def run_parsing_session(self, max_sources: int = 10, session_duration_hours: int = 2):
        """Запуск сессии парсинга"""
        print(f"\n🚀 ЗАПУСК СЕССИИ ПАРСИНГА")
        print(f"{'='*60}")
        print(f"Максимум источников за сессию: {max_sources}")
        print(f"Максимальная длительность: {session_duration_hours} часов")
        print(f"{'='*60}")
        
        session_start = datetime.now()
        session_end = session_start + timedelta(hours=session_duration_hours)
        
        sources_processed = 0
        total_reviews_saved = 0
        
        while sources_processed < max_sources and datetime.now() < session_end:
            # Получаем следующий источник
            source_info = self.get_next_source()
            
            if not source_info:
                print("\n✅ Все источники обработаны!")
                break
            
            brand, model, source = source_info
            
            # Проверяем лимит отзывов для модели
            current_count = self.db.get_reviews_count(brand, model)
            if current_count >= Config.MAX_REVIEWS_PER_MODEL:
                print(f"  ⚠️ Лимит отзывов для {brand} {model} достигнут ({current_count})")
                self.mark_source_completed(brand, model, source, 0, 0)
                continue
            
            # Парсим источник
            try:
                reviews_saved = self.parse_single_source(brand, model, source)
                total_reviews_saved += reviews_saved
                sources_processed += 1
                
                # Пауза между источниками
                if sources_processed < max_sources:
                    delay = random.uniform(30, 60)  # 30-60 секунд между источниками
                    print(f"  ⏳ Пауза {delay:.1f} сек...")
                    time.sleep(delay)
                    
            except Exception as e:
                logging.error(f"Ошибка обработки источника {brand} {model} {source}: {e}")
                sources_processed += 1
                
                # Увеличенная пауза при ошибке
                time.sleep(Config.ERROR_DELAY)
        
        # Статистика сессии
        session_duration = datetime.now() - session_start
        
        print(f"\n📊 СТАТИСТИКА СЕССИИ")
        print(f"{'='*60}")
        print(f"Длительность: {session_duration}")
        print(f"Источников обработано: {sources_processed}")
        print(f"Отзывов сохранено: {total_reviews_saved}")
        print(f"{'='*60}")
        
        # Общая статистика базы
        stats = self.db.get_parsing_stats()
        print(f"\n📈 ОБЩАЯ СТАТИСТИКА БАЗЫ ДАННЫХ")
        print(f"{'='*60}")
        print(f"Всего отзывов: {stats['total_reviews']}")
        print(f"Уникальных брендов: {stats['unique_brands']}")
        print(f"Уникальных моделей: {stats['unique_models']}")
        print(f"По источникам: {stats['by_source']}")
        print(f"По типам: {stats['by_type']}")
        print(f"{'='*60}")
    
    def run_continuous_parsing(self, daily_sessions: int = 4, session_sources: int = 10):
        """Непрерывный парсинг с интервалами"""
        print(f"\n🔄 РЕЖИМ НЕПРЕРЫВНОГО ПАРСИНГА")
        print(f"Сессий в день: {daily_sessions}")
        print(f"Источников за сессию: {session_sources}")
        print(f"Интервал между сессиями: {24 // daily_sessions} часов")
        
        session_interval = timedelta(hours=24 // daily_sessions)
        
        while True:
            try:
                # Запускаем сессию парсинга
                self.run_parsing_session(max_sources=session_sources, session_duration_hours=2)
                
                # Ждем до следующей сессии
                next_session = datetime.now() + session_interval
                print(f"\n⏰ Следующая сессия: {next_session.strftime('%Y-%m-%d %H:%M:%S')}")
                
                while datetime.now() < next_session:
                    remaining = next_session - datetime.now()
                    print(f"⏳ До следующей сессии: {remaining}", end='\r')
                    time.sleep(60)  # Проверяем каждую минуту
                
            except KeyboardInterrupt:
                print("\n👋 Парсинг остановлен пользователем")
                break
            except Exception as e:
                logging.error(f"Критическая ошибка в непрерывном парсинге: {e}")
                print(f"❌ Критическая ошибка: {e}")
                print("⏳ Пауза 30 минут перед повтором...")
                time.sleep(1800)  # 30 минут пауза при критической ошибке

# ==================== УТИЛИТЫ УПРАВЛЕНИЯ ====================

class ParserManager:
    """Менеджер для управления парсером"""
    
    def __init__(self, db_path: str = Config.DB_PATH):
        self.parser = AutoReviewsParser(db_path)
    
    def show_status(self):
        """Показать статус базы данных и очереди"""
        stats = self.parser.db.get_parsing_stats()
        
        print(f"\n📊 СТАТУС БАЗЫ ДАННЫХ")
        print(f"{'='*50}")
        print(f"Всего отзывов: {stats['total_reviews']:,}")
        print(f"Уникальных брендов: {stats['unique_brands']}")
        print(f"Уникальных моделей: {stats['unique_models']}")
        
        if stats['by_source']:
            print(f"\nПо источникам:")
            for source, count in stats['by_source'].items():
                print(f"  {source}: {count:,}")
        
        if stats['by_type']:
            print(f"\nПо типам:")
            for type_name, count in stats['by_type'].items():
                print(f"  {type_name}: {count:,}")
        
        # Статистика очереди
        conn = sqlite3.connect(self.parser.db.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT status, COUNT(*) FROM sources_queue GROUP BY status")
        queue_stats = dict(cursor.fetchall())
        
        conn.close()
        
        print(f"\n📋 СТАТУС ОЧЕРЕДИ")
        print(f"{'='*50}")
        total_sources = sum(queue_stats.values())
        
        for status, count in queue_stats.items():
            percentage = (count / total_sources * 100) if total_sources > 0 else 0
            print(f"{status}: {count} ({percentage:.1f}%)")
        
        print(f"Всего источников: {total_sources}")
    
    def reset_queue(self):
        """Сброс очереди парсинга"""
        print("🔄 Сброс очереди парсинга...")
        self.parser.initialize_sources_queue()
    
    def export_data(self, output_format: str = 'xlsx'):
        """Экспорт данных из базы"""
        print(f"📤 Экспорт данных в формате {output_format}...")
        
        conn = sqlite3.connect(self.parser.db.db_path)
        
        # Получаем все отзывы
        query = """
            SELECT 
                source, type, brand, model, year, title, author, rating,
                content, pros, cons, mileage, engine_volume, fuel_type,
                transmission, body_type, drive_type, publish_date, 
                views_count, likes_count, comments_count, url, parsed_at
            FROM reviews
            ORDER BY brand, model, parsed_at DESC
        """
        
        df_data = []
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [description[0] for description in cursor.description]
        
        for row in cursor.fetchall():
            df_data.append(dict(zip(columns, row)))
        
        conn.close()
        
        if not df_data:
            print("❌ Нет данных для экспорта")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format.lower() == 'xlsx':
            filename = f"auto_reviews_export_{timestamp}.xlsx"
            bt.write_excel(df_data, filename.replace('.xlsx', ''))
            print(f"✅ Данные экспортированы в {filename}")
        
        elif output_format.lower() == 'json':
            filename = f"auto_reviews_export_{timestamp}.json"
            bt.write_json(df_data, filename.replace('.json', ''))
            print(f"✅ Данные экспортированы в {filename}")
        
        else:
            print(f"❌ Неподдерживаемый формат: {output_format}")

# ==================== ГЛАВНАЯ ФУНКЦИЯ ====================

def main():
    """Главная функция для запуска парсера"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Парсер отзывов автомобилей")
    parser.add_argument('command', choices=['init', 'parse', 'continuous', 'status', 'export'], 
                       help='Команда для выполнения')
    parser.add_argument('--sources', type=int, default=10, 
                       help='Количество источников за сессию (по умолчанию: 10)')
    parser.add_argument('--sessions', type=int, default=4, 
                       help='Количество сессий в день для непрерывного режима (по умолчанию: 4)')
    parser.add_argument('--format', default='xlsx', choices=['xlsx', 'json'],
                       help='Формат экспорта данных (по умолчанию: xlsx)')
    
    args = parser.parse_args()
    
    manager = ParserManager()
    
    if args.command == 'init':
        print("🚀 Инициализация парсера...")
        manager.reset_queue()
        print("✅ Парсер готов к работе!")
    
    elif args.command == 'parse':
        print("🎯 Запуск разовой сессии парсинга...")
        manager.parser.run_parsing_session(max_sources=args.sources)
    
    elif args.command == 'continuous':
        print("🔄 Запуск непрерывного парсинга...")
        manager.parser.run_continuous_parsing(
            daily_sessions=args.sessions, 
            session_sources=args.sources
        )
    
    elif args.command == 'status':
        manager.show_status()
    
    elif args.command == 'export':
        manager.export_data(output_format=args.format)

if __name__ == "__main__":
    main()swith('http'):
                        next_url = urljoin(base_url, next_url)
                    
                    driver.get_via_this_page(next_url)
                    self.random_delay()
                    current_page += 1
                else:
                    break
            
            print(f"  ✓ Drom.ru: Собрано {len(reviews)} отзывов для {brand} {model}")
            
        except Exception as e:
            logging.error(f"Ошибка парсинга Drom.ru {brand} {model}: {e}")
            self.session_stats['errors'] += 1
        
        return reviews
    
    def _parse_review_card(self, card, brand: str, model: str, base_url: str) -> Optional[ReviewData]:
        """Парсинг одной карточки отзыва"""
        try:
            review = ReviewData(
                source="drom.ru",
                type="review",
                brand=brand,
                model=model
            )
            
            # Заголовок и ссылка
            title_link = card.select('h3 a') or card.select('a[data-ftid="component_reviews-item-title"]')
            if title_link:
                review.title = self.normalize_text(title_link.get_text())
                href = title_link.get_attribute('href')
                if href:
                    review.url = urljoin(base_url, href)
            
            # Рейтинг
            rating_elem = card.select('.css-kxziuu') or card.select('[data-ftid="component_rating"]')
            if rating_elem:
                rating_text = rating_elem.get_text()
                rating_match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
                if rating_match:
                    review.rating = float(rating_match.group(1))
            
            # Автор
            author_elem = card.select('.css-username') or card.select('[data-ftid="component_username"]')
            if author_elem:
                review.author = self.normalize_text(author_elem.get_text())
            
            # Информация об автомобиле
            specs_elem = card.select('.css-1x4jntm') or card.select('.css-car-info')
            if specs_elem:
                specs_text = specs_elem.get_text()
                
                # Извлекаем характеристики
                review.year = self.extract_year(specs_text)
                review.engine_volume = self.extract_engine_volume(specs_text)
                review.mileage = self.extract_mileage(specs_text)
                
                # Тип топлива
                if 'бензин' in specs_text.lower():
                    review.fuel_type = 'бензин'
                elif 'дизель' in specs_text.lower():
                    review.fuel_type = 'дизель'
                elif 'гибрид' in specs_text.lower():
                    review.fuel_type = 'гибрид'
                
                # Коробка передач
                if 'автомат' in specs_text.lower() or 'акпп' in specs_text.lower():
                    review.transmission = 'автомат'
                elif 'механик' in specs_text.lower() or 'мкпп' in specs_text.lower():
                    review.transmission = 'механика'
                elif 'вариатор' in specs_text.lower():
                    review.transmission = 'вариатор'
            
            # Краткое содержание
            content_elem = card.select('.css-1wdvlz0') or card.select('.review-preview')
            if content_elem:
                review.content = self.normalize_text(content_elem.get_text())
            
            # Дата публикации
            date_elem = card.select('.css-date') or card.select('[data-ftid="component_date"]')
            if date_elem:
                date_text = date_elem.get_text()
                review.publish_date = self._parse_date(date_text)
            
            return review if review.url else None
            
        except Exception as e:
            logging.error(f"Ошибка парсинга карточки отзыва Drom: {e}")
            return None
    
    def _parse_date(self, date_text: str) -> Optional[datetime]:
        """Парсинг даты из текста"""
        try:
            # Убираем лишние символы
            date_text = re.sub(r'[^\d\.\s\w]', '', date_text).strip()
            
            # Различные форматы дат
            patterns = [
                r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # 01.01.2023
                r'(\d{1,2})\s+(\w+)\s+(\d{4})',   # 1 января 2023
                r'(\d{4})-(\d{2})-(\d{2})',       # 2023-01-01
            ]
            
            months_map = {
                'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
            }
            
            for pattern in patterns:
                match = re.search(pattern, date_text)
                if match:
                    groups = match.groups()
                    
                    if len(groups) == 3:
                        if groups[1].isdigit():  # Формат dd.mm.yyyy
                            day, month, year = map(int, groups)
                        else:  # Формат с названием месяца
                            day = int(groups[0])
                            month = months_map.get(groups[1].lower(), 1)
                            year = int(groups[2])
                        
                        return datetime(year, month, day)
            
        except Exception:
            pass
        
        return None

class Drive2Parser(BaseParser):
    """Парсер отзывов и бортжурналов с Drive2.ru"""

    @staticmethod
    @browser(
        block_images=True,
        cache=True,
        reuse_driver=True,
        max_retry=3,
        user_agent=random.choice(Config.USER_AGENTS),
        headless=True,
    )
    def parse_brand_model_reviews(driver: Driver, data: Dict, parser) -> List[ReviewData]:
        """Парсинг отзывов для конкретной марки и модели"""
        brand = data['brand']
        model = data['model']
        max_pages = data.get('max_pages', 50)

        reviews = []

        # Парсим и отзывы, и бортжурналы
        for content_type in ['experience', 'logbook']:
            try:
                type_reviews = parser._parse_content_type(
                    driver, brand, model, content_type, max_pages // 2
                )
                reviews.extend(type_reviews)
                parser.random_delay(5, 10)  # Пауза между типами контента

            except Exception as e:
                logging.error(
                    f"Ошибка парсинга {content_type} Drive2.ru {brand} {model}: {e}"
                )
                parser.session_stats['errors'] += 1

        return reviews
    
    def _parse_content_type(self, driver: Driver, brand: str, model: str, content_type: str, max_pages: int) -> List[ReviewData]:
        """Парсинг конкретного типа контента"""
        reviews = []
        
        # URL в зависимости от типа контента
        if content_type == 'experience':
            base_url = f"https://www.drive2.ru/experience/{brand}/{model}/"
            review_type = "review"
        else:  # logbook
            base_url = f"https://www.drive2.ru/cars/{brand}/{model}/logbook/"
            review_type = "board_journal"
        
        print(f"  🔍 Drive2.ru: Парсинг {review_type} {brand} {model}")
        
        try:
            driver.google_get(base_url, bypass_cloudflare=True)
            self.random_delay(3, 7)
            
            # Проверяем наличие ошибки
            if driver.select('.c-error') or "404" in driver.title:
                print(f"    ⚠️ Страница не найдена: {base_url}")
                return reviews
            
            current_page = 1
            
            while current_page <= max_pages:
                print(f"    📄 Страница {current_page} ({review_type})")
                
                # Ищем карточки
                if content_type == 'experience':
                    cards = driver.select_all('.c-car-card')
                else:
                    cards = driver.select_all('.c-post-card') or driver.select_all('.c-logbook-card')
                
                if not cards:
                    print(f"    ⚠️ Контент не найден на странице {current_page}")
                    break
                
                page_reviews = 0
                
                for card in cards:
                    try:
                        review = self._parse_drive2_card(card, brand, model, review_type, base_url)
                        if review and not self.db.is_url_parsed(review.url):
                            reviews.append(review)
                            page_reviews += 1
                        
                    except Exception as e:
                        self.session_stats['errors'] += 1
                        logging.error(f"Ошибка парсинга карточки Drive2: {e}")
                
                print(f"    ✓ Найдено {page_reviews} новых записей")
                
                # Поиск следующей страницы
                next_link = driver.select('.c-pagination__next') or driver.select('a[rel="next"]')
                if not next_link or 'disabled' in next_link.get_attribute('class', ''):
                    print(f"    📋 Больше страниц нет")
                    break
                
                # Переходим на следующую страницу
                next_url = next_link.get_attribute('href')
                if next_url:
                    if not next_url.startswith('http'):
                        next_url = base_url + next_url
                    driver.google_get(next_url, bypass_cloudflare=True)
                    self.random_delay(3, 7)
                    current_page += 1
                
                else:
                    print(f"    📋 Больше страниц нет")
                    break
            
        except Exception as e:
            logging.error(f"Ошибка парсинга Drive2.ru {brand} {model}: {e}")
            self.session_stats['errors'] += 1
        
        return reviews