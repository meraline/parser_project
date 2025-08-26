#!/usr/bin/env python3
"""
🔄 МИГРАЦИЯ ДАННЫХ ИЗ SQLITE В POSTGRESQL

Переносит все данные из SQLite базы в PostgreSQL с сохранением
структуры и отношений между таблицами.
"""

import os
import sys
import sqlite3
import logging
from typing import Dict, Any, List
from pathlib import Path

# Добавляем путь к модулям проекта
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False


def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('migration.log')
        ]
    )
    return logging.getLogger(__name__)


class MigrationManager:
    """Менеджер миграции данных"""
    
    def __init__(self, sqlite_path: str):
        self.logger = logging.getLogger(__name__)
        self.sqlite_path = sqlite_path
        self.brand_mapping = {}
        self.model_mapping = {}
        
        # Подключение к SQLite
        self.sqlite_conn = sqlite3.connect(sqlite_path)
        self.sqlite_conn.row_factory = sqlite3.Row
        
        # Подключение к PostgreSQL
        if not HAS_POSTGRES:
            raise ImportError("psycopg2 не установлен. Выполните: pip install psycopg2-binary")
        
        self.postgres_conn = psycopg2.connect(
            host=os.environ.get('DATABASE_HOST', 'localhost'),
            port=os.environ.get('DATABASE_PORT', '5432'),
            database=os.environ.get('DATABASE_NAME', 'auto_reviews'),
            user=os.environ.get('DATABASE_USER', 'parser'),
            password=os.environ.get('DATABASE_PASSWORD', 'parser')
        )
        self.postgres_conn.set_session(autocommit=False)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sqlite_conn.close()
        self.postgres_conn.close()
    
    def create_postgres_schema(self):
        """Создание схемы PostgreSQL"""
        self.logger.info("🗄️ Создаем схему PostgreSQL...")
        
        with self.postgres_conn.cursor() as cursor:
            # Создаем схему auto_reviews
            cursor.execute("CREATE SCHEMA IF NOT EXISTS auto_reviews;")
            cursor.execute("SET search_path TO auto_reviews, public;")
            
            # Создаем таблицы
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS бренды (
                    ид SERIAL PRIMARY KEY,
                    название TEXT NOT NULL UNIQUE,
                    название_в_url TEXT NOT NULL,
                    полная_ссылка TEXT NOT NULL,
                    количество_отзывов INTEGER DEFAULT 0,
                    дата_создания TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS модели (
                    ид SERIAL PRIMARY KEY,
                    ид_бренда INTEGER NOT NULL REFERENCES бренды(ид),
                    название TEXT NOT NULL,
                    название_в_url TEXT NOT NULL,
                    полная_ссылка TEXT NOT NULL,
                    количество_отзывов INTEGER DEFAULT 0,
                    дата_создания TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ид_бренда, название)
                );
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS отзывы (
                    ид SERIAL PRIMARY KEY,
                    ид_модели INTEGER NOT NULL REFERENCES модели(ид),
                    тип_отзыва TEXT NOT NULL CHECK (тип_отзыва IN ('длинный', 'короткий')),
                    заголовок TEXT,
                    содержание TEXT,
                    плюсы TEXT,
                    минусы TEXT,
                    поломки TEXT,
                    имя_автора TEXT,
                    город_автора TEXT,
                    дата_отзыва DATE,
                    год_автомобиля INTEGER,
                    объем_двигателя REAL,
                    тип_топлива TEXT,
                    коробка_передач TEXT,
                    тип_привода TEXT,
                    тип_кузова TEXT,
                    цвет TEXT,
                    пробег INTEGER,
                    общая_оценка INTEGER CHECK (общая_оценка BETWEEN 1 AND 5),
                    оценка_комфорта INTEGER CHECK (оценка_комфорта BETWEEN 1 AND 5),
                    оценка_надежности INTEGER CHECK (оценка_надежности BETWEEN 1 AND 5),
                    оценка_расхода_топлива INTEGER CHECK (оценка_расхода_топлива BETWEEN 1 AND 5),
                    оценка_управления INTEGER CHECK (оценка_управления BETWEEN 1 AND 5),
                    оценка_внешнего_вида INTEGER CHECK (оценка_внешнего_вида BETWEEN 1 AND 5),
                    исходный_url TEXT,
                    ид_отзыва_на_сайте TEXT,
                    количество_фото INTEGER DEFAULT 0,
                    дата_создания TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Создаем индексы
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_модели_ид_бренда ON модели(ид_бренда);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_отзывы_ид_модели ON отзывы(ид_модели);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_отзывы_тип ON отзывы(тип_отзыва);")
            
            self.postgres_conn.commit()
            self.logger.info("✅ Схема PostgreSQL создана")
    
    def migrate_brands(self):
        """Миграция брендов"""
        self.logger.info("🚀 Начинаем миграцию брендов...")
        
        sqlite_cursor = self.sqlite_conn.cursor()
        sqlite_cursor.execute("SELECT * FROM brands ORDER BY id")
        brands = sqlite_cursor.fetchall()
        
        self.logger.info(f"📊 Найдено {len(brands)} брендов для миграции")
        
        with self.postgres_conn.cursor() as postgres_cursor:
            postgres_cursor.execute("SET search_path TO auto_reviews, public;")
            
            for brand in brands:
                old_id = brand['id']
                
                try:
                    postgres_cursor.execute("""
                        INSERT INTO бренды (название, название_в_url, полная_ссылка, количество_отзывов)
                        VALUES (%s, %s, %s, %s) RETURNING ид
                    """, (
                        brand['name'],
                        brand['url_name'], 
                        brand['full_url'],
                        brand['reviews_count']
                    ))
                    
                    new_id = postgres_cursor.fetchone()[0]
                    self.brand_mapping[old_id] = new_id
                    self.logger.info(f"✅ Бренд '{brand['name']}': {old_id} → {new_id}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка миграции бренда '{brand['name']}': {e}")
            
            self.postgres_conn.commit()
        
        self.logger.info(f"✅ Миграция брендов завершена: {len(self.brand_mapping)} записей")
    
    def migrate_models(self):
        """Миграция моделей"""
        self.logger.info("🚀 Начинаем миграцию моделей...")
        
        sqlite_cursor = self.sqlite_conn.cursor()
        sqlite_cursor.execute("SELECT * FROM models ORDER BY id")
        models = sqlite_cursor.fetchall()
        
        self.logger.info(f"📊 Найдено {len(models)} моделей для миграции")
        
        with self.postgres_conn.cursor() as postgres_cursor:
            postgres_cursor.execute("SET search_path TO auto_reviews, public;")
            
            for model in models:
                old_id = model['id']
                old_brand_id = model['brand_id']
                
                if old_brand_id not in self.brand_mapping:
                    self.logger.error(f"❌ Бренд ID {old_brand_id} не найден в маппинге для модели '{model['name']}'")
                    continue
                
                new_brand_id = self.brand_mapping[old_brand_id]
                
                try:
                    postgres_cursor.execute("""
                        INSERT INTO модели (ид_бренда, название, название_в_url, полная_ссылка, количество_отзывов)
                        VALUES (%s, %s, %s, %s, %s) RETURNING ид
                    """, (
                        new_brand_id,
                        model['name'],
                        model['url_name'],
                        model['full_url'],
                        model['reviews_count']
                    ))
                    
                    new_id = postgres_cursor.fetchone()[0]
                    self.model_mapping[old_id] = new_id
                    self.logger.info(f"✅ Модель '{model['name']}': {old_id} → {new_id}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка миграции модели '{model['name']}': {e}")
            
            self.postgres_conn.commit()
        
        self.logger.info(f"✅ Миграция моделей завершена: {len(self.model_mapping)} записей")
    
    def migrate_reviews(self):
        """Миграция отзывов"""
        self.logger.info("🚀 Начинаем миграцию отзывов...")
        
        sqlite_cursor = self.sqlite_conn.cursor()
        sqlite_cursor.execute("SELECT COUNT(*) as count FROM reviews")
        total_reviews = sqlite_cursor.fetchone()['count']
        
        self.logger.info(f"📊 Найдено {total_reviews} отзывов для миграции")
        
        # Мигрируем порциями
        batch_size = 1000
        migrated_count = 0
        
        for offset in range(0, total_reviews, batch_size):
            sqlite_cursor.execute(f"SELECT * FROM reviews ORDER BY id LIMIT {batch_size} OFFSET {offset}")
            reviews_batch = sqlite_cursor.fetchall()
            
            with self.postgres_conn.cursor() as postgres_cursor:
                postgres_cursor.execute("SET search_path TO auto_reviews, public;")
                
                for review in reviews_batch:
                    old_model_id = review['model_id']
                    
                    if old_model_id not in self.model_mapping:
                        self.logger.warning(f"⚠️ Модель ID {old_model_id} не найдена в маппинге для отзыва ID {review['id']}")
                        continue
                    
                    new_model_id = self.model_mapping[old_model_id]
                    review_type = 'длинный' if review['review_type'] == 'long' else 'короткий'
                    
                    try:
                        postgres_cursor.execute("""
                            INSERT INTO отзывы (
                                ид_модели, тип_отзыва, заголовок, содержание,
                                плюсы, минусы, поломки, имя_автора, город_автора,
                                дата_отзыва, год_автомобиля, объем_двигателя,
                                тип_топлива, коробка_передач, тип_привода,
                                тип_кузова, цвет, пробег, общая_оценка,
                                оценка_комфорта, оценка_надежности,
                                оценка_расхода_топлива, оценка_управления,
                                оценка_внешнего_вида, исходный_url,
                                ид_отзыва_на_сайте, количество_фото
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s
                            )
                        """, (
                            new_model_id, review_type, review['title'], review['content'],
                            review['positive_text'], review['negative_text'], review['breakages_text'],
                            review['author_name'], review['author_city'], review['review_date'],
                            review['car_year'], review['car_engine_volume'], review['car_fuel_type'],
                            review['car_transmission'], review['car_drive_type'], review['car_body_type'],
                            review['car_color'], review['car_mileage'], review['overall_rating'],
                            review['comfort_rating'], review['reliability_rating'],
                            review['fuel_consumption_rating'], review['driving_rating'],
                            review['appearance_rating'], review['source_url'],
                            review['review_id'], review['photos_count']
                        ))
                        migrated_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"❌ Ошибка миграции отзыва ID {review['id']}: {e}")
                
                self.postgres_conn.commit()
                
            self.logger.info(f"📈 Обработано {min(offset + batch_size, total_reviews)} из {total_reviews} отзывов")
        
        self.logger.info(f"✅ Миграция отзывов завершена: {migrated_count} записей")
    
    def get_statistics(self):
        """Получение статистики миграции"""
        sqlite_stats = {}
        postgres_stats = {}
        
        # SQLite статистика
        sqlite_cursor = self.sqlite_conn.cursor()
        sqlite_cursor.execute("SELECT COUNT(*) FROM brands")
        sqlite_stats['brands'] = sqlite_cursor.fetchone()[0]
        
        sqlite_cursor.execute("SELECT COUNT(*) FROM models")
        sqlite_stats['models'] = sqlite_cursor.fetchone()[0]
        
        sqlite_cursor.execute("SELECT COUNT(*) FROM reviews")
        sqlite_stats['reviews'] = sqlite_cursor.fetchone()[0]
        
        # PostgreSQL статистика
        with self.postgres_conn.cursor() as postgres_cursor:
            postgres_cursor.execute("SET search_path TO auto_reviews, public;")
            
            postgres_cursor.execute("SELECT COUNT(*) FROM бренды")
            postgres_stats['brands'] = postgres_cursor.fetchone()[0]
            
            postgres_cursor.execute("SELECT COUNT(*) FROM модели")
            postgres_stats['models'] = postgres_cursor.fetchone()[0]
            
            postgres_cursor.execute("SELECT COUNT(*) FROM отзывы")
            postgres_stats['reviews'] = postgres_cursor.fetchone()[0]
        
        return sqlite_stats, postgres_stats


def main():
    """Основная функция миграции"""
    logger = setup_logging()
    
    logger.info("🚀 Начинаем миграцию данных из SQLite в PostgreSQL")
    
    # Проверяем наличие SQLite базы
    sqlite_path = "нормализованная_бд_v3.db"
    if not os.path.exists(sqlite_path):
        logger.error(f"❌ SQLite база данных не найдена: {sqlite_path}")
        return False
    
    try:
        with MigrationManager(sqlite_path) as migration:
            # Создаем схему PostgreSQL
            migration.create_postgres_schema()
            
            # Получаем исходную статистику
            sqlite_stats, _ = migration.get_statistics()
            logger.info(f"📊 Исходная статистика SQLite: {sqlite_stats}")
            
            # Выполняем миграцию
            migration.migrate_brands()
            migration.migrate_models()
            migration.migrate_reviews()
            
            # Получаем финальную статистику
            sqlite_stats, postgres_stats = migration.get_statistics()
            
            logger.info(f"📊 Финальная статистика SQLite: {sqlite_stats}")
            logger.info(f"📊 Финальная статистика PostgreSQL: {postgres_stats}")
            
            # Проверяем результат
            success = (
                sqlite_stats['brands'] == postgres_stats['brands'] and
                sqlite_stats['models'] == postgres_stats['models']
            )
            
            if success:
                logger.info("🎉 Миграция завершена успешно!")
                logger.info(f"✅ Перенесено брендов: {postgres_stats['brands']}")
                logger.info(f"✅ Перенесено моделей: {postgres_stats['models']}")
                logger.info(f"✅ Перенесено отзывов: {postgres_stats['reviews']}")
            else:
                logger.warning("⚠️ Миграция завершена с расхождениями в данных")
            
            return success
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка миграции: {e}")
        return False


if __name__ == "__main__":
    # Устанавливаем переменные окружения для PostgreSQL
    os.environ.setdefault('DATABASE_HOST', 'localhost')
    os.environ.setdefault('DATABASE_PORT', '5432')
    os.environ.setdefault('DATABASE_NAME', 'auto_reviews')
    os.environ.setdefault('DATABASE_USER', 'parser')
    os.environ.setdefault('DATABASE_PASSWORD', 'parser')
    
    success = main()
    sys.exit(0 if success else 1)
