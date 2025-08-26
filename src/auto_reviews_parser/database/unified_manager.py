#!/usr/bin/env python3
"""
🗄️ УНИВЕРСАЛЬНЫЙ МЕНЕДЖЕР БАЗ ДАННЫХ ДЛЯ ПАРСЕРА ОТЗЫВОВ

Поддерживает как SQLite (для разработки), так и PostgreSQL (для продуктива).
Автоматически определяет тип базы по строке подключения.
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, Union
import sqlite3

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import RealDictCursor  # type: ignore
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


class DatabaseManager:
    """Универсальный менеджер базы данных"""

    def __init__(self, connection_string: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        
        # Если строка подключения не указана, используем SQLite
        if connection_string is None:
            connection_string = "auto_reviews.db"
        
        self.connection_string = connection_string
        self.db_type = self._detect_db_type()
        self.logger.info(f"🗄️ Используется база данных: {self.db_type}")

    def _detect_db_type(self) -> str:
        """Определение типа базы данных по строке подключения"""
        if self.connection_string.startswith('postgresql://') or \
           self.connection_string.startswith('postgres://') or \
           ('host=' in self.connection_string and 'dbname=' in self.connection_string):
            return 'postgresql'
        return 'sqlite'

    def _get_postgres_connection(self):
        """Создание подключения к PostgreSQL"""
        if not POSTGRES_AVAILABLE:
            raise ImportError("psycopg2 не установлен. Установите: pip install psycopg2-binary")
        
        # Если это строка подключения PostgreSQL
        if self.connection_string.startswith(('postgresql://', 'postgres://')):
            return psycopg2.connect(self.connection_string, cursor_factory=RealDictCursor)  # type: ignore
        
        # Иначе используем параметры из окружения
        params = {
            'host': os.getenv('DATABASE_HOST', 'localhost'),
            'port': os.getenv('DATABASE_PORT', '5432'),
            'database': os.getenv('DATABASE_NAME', 'auto_reviews'),
            'user': os.getenv('DATABASE_USER', 'parser'),
            'password': os.getenv('DATABASE_PASSWORD', 'parser')
        }
        
        return psycopg2.connect(**params, cursor_factory=RealDictCursor)  # type: ignore

    def _get_sqlite_connection(self):
        """Создание подключения к SQLite"""
        conn = sqlite3.connect(self.connection_string)
        conn.row_factory = sqlite3.Row  # Для работы с результатами как со словарем
        return conn

    def get_connection(self):
        """Получение подключения к базе данных"""
        if self.db_type == 'postgresql':
            return self._get_postgres_connection()
        else:
            return self._get_sqlite_connection()

    def create_database(self) -> bool:
        """Создание базы данных с нормализованной структурой"""
        try:
            if self.db_type == 'postgresql':
                return self._create_postgres_database()
            else:
                return self._create_sqlite_database()
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания базы данных: {e}")
            return False

    def _create_postgres_database(self) -> bool:
        """Создание схемы PostgreSQL"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # Устанавливаем схему
            cursor.execute("SET search_path TO auto_reviews, public;")
            
            # Создаем таблицы с русскими названиями
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS бренды (
                    ид SERIAL PRIMARY KEY,
                    название TEXT NOT NULL UNIQUE,
                    название_в_url TEXT NOT NULL UNIQUE,
                    полная_ссылка TEXT,
                    количество_отзывов INTEGER DEFAULT 0,
                    дата_создания TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS модели (
                    ид SERIAL PRIMARY KEY,
                    ид_бренда INTEGER NOT NULL,
                    название TEXT NOT NULL,
                    название_в_url TEXT NOT NULL,
                    полная_ссылка TEXT,
                    количество_отзывов INTEGER DEFAULT 0,
                    дата_создания TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ид_бренда) REFERENCES бренды(ид) ON DELETE CASCADE,
                    UNIQUE(ид_бренда, название_в_url)
                );
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS отзывы (
                    ид SERIAL PRIMARY KEY,
                    ид_модели INTEGER NOT NULL,
                    тип_отзыва TEXT NOT NULL DEFAULT 'длинный',
                    заголовок TEXT,
                    содержание TEXT,
                    плюсы TEXT,
                    минусы TEXT,
                    поломки TEXT,
                    имя_автора TEXT,
                    город_автора TEXT,
                    дата_отзыва DATE,
                    год_автомобиля INTEGER,
                    объем_двигателя DECIMAL(3,1),
                    тип_топлива TEXT,
                    коробка_передач TEXT,
                    тип_привода TEXT,
                    тип_кузова TEXT,
                    цвет TEXT,
                    пробег INTEGER,
                    общая_оценка DECIMAL(2,1),
                    оценка_комфорта DECIMAL(2,1),
                    оценка_надежности DECIMAL(2,1),
                    оценка_расхода_топлива DECIMAL(2,1),
                    оценка_управления DECIMAL(2,1),
                    оценка_внешнего_вида DECIMAL(2,1),
                    стоимость_покупки DECIMAL(12,2),
                    стоимость_обслуживания DECIMAL(12,2),
                    исходный_url TEXT,
                    ид_отзыва_на_сайте TEXT,
                    количество_фото INTEGER DEFAULT 0,
                    полезность INTEGER DEFAULT 0,
                    дата_создания TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ид_модели) REFERENCES модели(ид) ON DELETE CASCADE
                );
            """)
            
            # Создаем индексы
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_бренды_название_в_url ON бренды(название_в_url);",
                "CREATE INDEX IF NOT EXISTS idx_модели_ид_бренда ON модели(ид_бренда);",
                "CREATE INDEX IF NOT EXISTS idx_модели_название_в_url ON модели(название_в_url);",
                "CREATE INDEX IF NOT EXISTS idx_отзывы_ид_модели ON отзывы(ид_модели);",
                "CREATE INDEX IF NOT EXISTS idx_отзывы_тип ON отзывы(тип_отзыва);",
                "CREATE INDEX IF NOT EXISTS idx_отзывы_дата ON отзывы(дата_отзыва);"
            ]
            
            for index_sql in indexes:
                cursor.execute(index_sql)
            
            conn.commit()
            cursor.close()
            self.logger.info("✅ PostgreSQL схема создана успешно")
            return True

    def _create_sqlite_database(self) -> bool:
        """Создание схемы SQLite (совместимость со старой схемой)"""
        with self.get_connection() as conn:
            # Для SQLite создаем таблицы поэтапно
            cursor = conn.cursor()
            
            cursor.execute("PRAGMA foreign_keys = OFF;")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS brands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    url_name TEXT NOT NULL UNIQUE,
                    full_url TEXT,
                    reviews_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS models (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    brand_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    url_name TEXT NOT NULL,
                    full_url TEXT,
                    reviews_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
                    UNIQUE(brand_id, url_name)
                );
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_id INTEGER NOT NULL,
                    review_type TEXT NOT NULL DEFAULT 'long',
                    title TEXT,
                    content TEXT,
                    positive_text TEXT,
                    negative_text TEXT,
                    breakages_text TEXT,
                    author_name TEXT,
                    author_city TEXT,
                    review_date TEXT,
                    car_year INTEGER,
                    car_engine_volume REAL,
                    car_fuel_type TEXT,
                    car_transmission TEXT,
                    car_drive_type TEXT,
                    car_body_type TEXT,
                    car_color TEXT,
                    car_mileage INTEGER,
                    overall_rating REAL,
                    comfort_rating REAL,
                    reliability_rating REAL,
                    fuel_consumption_rating REAL,
                    driving_rating REAL,
                    appearance_rating REAL,
                    source_url TEXT,
                    review_id TEXT,
                    photos_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (model_id) REFERENCES models(id) ON DELETE CASCADE
                );
            """)
            
            # Создаем индексы
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_brands_url_name ON brands(url_name);",
                "CREATE INDEX IF NOT EXISTS idx_models_brand_id ON models(brand_id);",
                "CREATE INDEX IF NOT EXISTS idx_models_url_name ON models(url_name);",
                "CREATE INDEX IF NOT EXISTS idx_reviews_model_id ON reviews(model_id);",
                "CREATE INDEX IF NOT EXISTS idx_reviews_type ON reviews(review_type);",
                "CREATE INDEX IF NOT EXISTS idx_reviews_date ON reviews(review_date);"
            ]
            
            for index_sql in indexes:
                cursor.execute(index_sql)
            
            cursor.execute("PRAGMA foreign_keys = ON;")
            conn.commit()
            cursor.close()
            
            self.logger.info("✅ SQLite схема создана успешно")
            return True

    def add_brand(self, name: str, url_name: str, full_url: Optional[str] = None, reviews_count: int = 0) -> Optional[int]:
        """Добавление бренда в базу данных"""
        try:
            with self.get_connection() as conn:
                if self.db_type == 'postgresql':
                    cursor = conn.cursor()
                    cursor.execute("SET search_path TO auto_reviews, public;")
                    cursor.execute("""
                        INSERT INTO бренды (название, название_в_url, полная_ссылка, количество_отзывов)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (название) DO NOTHING
                        RETURNING ид;
                    """, (name, url_name, full_url, reviews_count))
                    
                    result = cursor.fetchone()
                    if result:
                        brand_id = result['ид']  # type: ignore
                        conn.commit()
                        cursor.close()
                        self.logger.info(f"✅ Бренд добавлен: {name} (ID: {brand_id})")
                        return brand_id
                    else:
                        # Бренд уже существует, получаем его ID
                        cursor.execute("SELECT ид FROM бренды WHERE название = %s", (name,))
                        result = cursor.fetchone()
                        cursor.close()
                        return result['ид'] if result else None  # type: ignore
                else:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT OR IGNORE INTO brands (name, url_name, full_url, reviews_count)
                        VALUES (?, ?, ?, ?)
                    """, (name, url_name, full_url, reviews_count))
                    
                    if cursor.rowcount > 0:
                        brand_id = cursor.lastrowid
                        self.logger.info(f"✅ Бренд добавлен: {name} (ID: {brand_id})")
                        return brand_id
                    else:
                        cursor.execute("SELECT id FROM brands WHERE url_name = ?", (url_name,))
                        result = cursor.fetchone()
                        return result[0] if result else None

        except Exception as e:
            self.logger.error(f"❌ Ошибка добавления бренда {name}: {e}")
            return None

    def get_statistics(self) -> Dict[str, Any]:
        """Получение статистики базы данных"""
        try:
            with self.get_connection() as conn:
                if self.db_type == 'postgresql':
                    cursor = conn.cursor()
                    cursor.execute("SET search_path TO auto_reviews, public;")
                    
                    # Количество брендов
                    cursor.execute("SELECT COUNT(*) as count FROM бренды")
                    result = cursor.fetchone()
                    brands_count = result['count'] if result else 0  # type: ignore
                    
                    # Количество моделей  
                    cursor.execute("SELECT COUNT(*) as count FROM модели")
                    result = cursor.fetchone()
                    models_count = result['count'] if result else 0  # type: ignore
                    
                    # Количество отзывов
                    cursor.execute("SELECT COUNT(*) as count FROM отзывы")
                    result = cursor.fetchone()
                    total_reviews = result['count'] if result else 0  # type: ignore
                    
                    # Длинные отзывы
                    cursor.execute("SELECT COUNT(*) as count FROM отзывы WHERE тип_отзыва = 'длинный'")
                    result = cursor.fetchone()
                    long_reviews = result['count'] if result else 0  # type: ignore
                    
                    # Короткие отзывы
                    cursor.execute("SELECT COUNT(*) as count FROM отзывы WHERE тип_отзыва = 'короткий'")
                    result = cursor.fetchone()
                    short_reviews = result['count'] if result else 0  # type: ignore
                    
                    cursor.close()
                        
                else:
                    cursor = conn.cursor()
                    
                    cursor.execute("SELECT COUNT(*) FROM brands")
                    result = cursor.fetchone()
                    brands_count = result[0] if result else 0
                    
                    cursor.execute("SELECT COUNT(*) FROM models")
                    result = cursor.fetchone()
                    models_count = result[0] if result else 0
                    
                    cursor.execute("SELECT COUNT(*) FROM reviews")
                    result = cursor.fetchone()
                    total_reviews = result[0] if result else 0
                    
                    cursor.execute("SELECT COUNT(*) FROM reviews WHERE review_type = 'long'")
                    result = cursor.fetchone()
                    long_reviews = result[0] if result else 0
                    
                    cursor.execute("SELECT COUNT(*) FROM reviews WHERE review_type = 'short'")
                    result = cursor.fetchone()
                    short_reviews = result[0] if result else 0

                return {
                    'brands_count': brands_count,
                    'models_count': models_count,
                    'total_reviews': total_reviews,
                    'long_reviews': long_reviews,
                    'short_reviews': short_reviews,
                    'db_type': self.db_type
                }

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}

    def add_model(self, brand_id: int, name: str, url_name: str, 
                  full_url: Optional[str] = None, reviews_count: int = 0) -> Optional[int]:
        """Добавление модели в базу данных"""
        try:
            with self.get_connection() as conn:
                if self.db_type == 'postgresql':
                    cursor = conn.cursor()
                    cursor.execute("SET search_path TO auto_reviews, public;")
                    cursor.execute("""
                        INSERT INTO модели (ид_бренда, название, название_в_url, полная_ссылка, количество_отзывов)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (ид_бренда, название) DO NOTHING
                        RETURNING ид;
                    """, (brand_id, name, url_name, full_url, reviews_count))
                    
                    result = cursor.fetchone()
                    if result:
                        model_id = result['ид']  # type: ignore
                        conn.commit()
                        cursor.close()
                        self.logger.info(f"✅ Модель добавлена: {name} (ID: {model_id})")
                        return model_id
                    else:
                        # Модель уже существует, получаем её ID
                        cursor.execute("""
                            SELECT ид FROM модели 
                            WHERE ид_бренда = %s AND название = %s
                        """, (brand_id, name))
                        result = cursor.fetchone()
                        cursor.close()
                        return result['ид'] if result else None  # type: ignore
                else:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT OR IGNORE INTO models (brand_id, name, url_name, full_url, reviews_count)
                        VALUES (?, ?, ?, ?, ?)
                    """, (brand_id, name, url_name, full_url, reviews_count))
                    
                    if cursor.rowcount > 0:
                        model_id = cursor.lastrowid
                        self.logger.info(f"✅ Модель добавлена: {name} (ID: {model_id})")
                        return model_id
                    else:
                        cursor.execute("""
                            SELECT id FROM models 
                            WHERE brand_id = ? AND url_name = ?
                        """, (brand_id, url_name))
                        result = cursor.fetchone()
                        return result[0] if result else None

        except Exception as e:
            self.logger.error(f"❌ Ошибка добавления модели {name}: {e}")
            return None
