#!/usr/bin/env python3
"""
Миграция данных из нормализованной SQLite базы в PostgreSQL
"""

import os
import sys
import sqlite3
import psycopg2
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Any

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.auto_reviews_parser.database.unified_manager import DatabaseManager

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_normalized.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class NormalizedDataMigrator:
    """Класс для миграции нормализованных данных из SQLite в PostgreSQL"""
    
    def __init__(self, sqlite_path: str, pg_config: Dict[str, str]):
        self.sqlite_path = sqlite_path
        self.pg_config = pg_config
        self.sqlite_conn = None
        self.pg_conn = None
        self.unified_manager = None
        
        # Маппинг таблиц SQLite -> PostgreSQL
        self.table_mapping = {
            'автомобили': 'auto_reviews.модели',
            'авторы': 'auto_reviews.авторы',
            'города': 'auto_reviews.города',
            'отзывы_нормализованные': 'auto_reviews.отзывы',
            'комментарии_норм': 'auto_reviews.комментарии',
            'характеристики_норм': 'auto_reviews.характеристики',
            'рейтинги_деталей': 'auto_reviews.рейтинги_деталей',
            'расход_топлива': 'auto_reviews.расход_топлива'
        }
    
    def connect(self):
        """Подключение к базам данных"""
        try:
            # SQLite
            self.sqlite_conn = sqlite3.connect(self.sqlite_path)
            self.sqlite_conn.row_factory = sqlite3.Row
            logger.info(f"Подключен к SQLite: {self.sqlite_path}")
            
            # PostgreSQL через DatabaseManager
            self.unified_manager = DatabaseManager(
                db_type='postgresql',
                host=self.pg_config['host'],
                port=self.pg_config['port'],
                database=self.pg_config['database'],
                username=self.pg_config['username'],
                password=self.pg_config['password']
            )
            logger.info("Подключен к PostgreSQL")
            
            # Прямое подключение для некоторых операций
            self.pg_conn = psycopg2.connect(
                host=self.pg_config['host'],
                port=self.pg_config['port'],
                database=self.pg_config['database'],
                user=self.pg_config['username'],
                password=self.pg_config['password']
            )
            self.pg_conn.autocommit = False
            
        except Exception as e:
            logger.error(f"Ошибка подключения к базам данных: {e}")
            raise
    
    def disconnect(self):
        """Закрытие соединений"""
        if self.sqlite_conn:
            self.sqlite_conn.close()
        if self.pg_conn:
            self.pg_conn.close()
        logger.info("Соединения закрыты")
    
    def get_table_counts(self) -> Dict[str, int]:
        """Получение количества записей в каждой таблице SQLite"""
        counts = {}
        cursor = self.sqlite_conn.cursor()
        
        for sqlite_table in self.table_mapping.keys():
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {sqlite_table}")
                count = cursor.fetchone()[0]
                counts[sqlite_table] = count
                logger.info(f"Таблица {sqlite_table}: {count} записей")
            except Exception as e:
                logger.warning(f"Не удалось получить количество записей для {sqlite_table}: {e}")
                counts[sqlite_table] = 0
        
        return counts
    
    def clear_postgresql_data(self):
        """Очистка данных в PostgreSQL перед миграцией"""
        logger.info("Очистка данных в PostgreSQL...")
        
        cursor = self.pg_conn.cursor()
        try:
            # Очищаем таблицы в правильном порядке (с учетом внешних ключей)
            tables_to_clear = [
                'auto_reviews.характеристики',
                'auto_reviews.рейтинги_деталей',
                'auto_reviews.расход_топлива',
                'auto_reviews.комментарии',
                'auto_reviews.отзывы',
                'auto_reviews.модели',
                'auto_reviews.бренды',
                'auto_reviews.авторы',
                'auto_reviews.города'
            ]
            
            for table in tables_to_clear:
                cursor.execute(f"DELETE FROM {table}")
                logger.info(f"Очищена таблица {table}")
            
            # Сброс последовательностей
            sequences = [
                'auto_reviews.города_id_seq',
                'auto_reviews.авторы_id_seq',
                'auto_reviews.бренды_id_seq',
                'auto_reviews.модели_id_seq',
                'auto_reviews.отзывы_id_seq',
                'auto_reviews.комментарии_id_seq',
                'auto_reviews.характеристики_id_seq',
                'auto_reviews.рейтинги_деталей_id_seq',
                'auto_reviews.расход_топлива_id_seq'
            ]
            
            for seq in sequences:
                try:
                    cursor.execute(f"ALTER SEQUENCE {seq} RESTART WITH 1")
                except Exception as e:
                    logger.warning(f"Не удалось сбросить последовательность {seq}: {e}")
            
            self.pg_conn.commit()
            logger.info("Данные в PostgreSQL очищены")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Ошибка при очистке данных PostgreSQL: {e}")
            raise
    
    def migrate_cities(self):
        """Миграция городов"""
        logger.info("Миграция городов...")
        
        cursor = self.sqlite_conn.cursor()
        cursor.execute("SELECT * FROM города")
        cities = cursor.fetchall()
        
        if not cities:
            logger.info("Нет городов для миграции")
            return {}
        
        city_id_mapping = {}
        pg_cursor = self.pg_conn.cursor()
        
        for city in cities:
            try:
                pg_cursor.execute(
                    "INSERT INTO auto_reviews.города (название) VALUES (%s) RETURNING id",
                    (city['название'],)
                )
                new_id = pg_cursor.fetchone()[0]
                city_id_mapping[city['id']] = new_id
                logger.debug(f"Город '{city['название']}': {city['id']} -> {new_id}")
                
            except Exception as e:
                logger.error(f"Ошибка при вставке города {city['название']}: {e}")
                raise
        
        self.pg_conn.commit()
        logger.info(f"Мигрировано {len(cities)} городов")
        return city_id_mapping
    
    def migrate_authors(self, city_id_mapping: Dict[int, int]):
        """Миграция авторов"""
        logger.info("Миграция авторов...")
        
        cursor = self.sqlite_conn.cursor()
        cursor.execute("SELECT * FROM авторы")
        authors = cursor.fetchall()
        
        if not authors:
            logger.info("Нет авторов для миграции")
            return {}
        
        author_id_mapping = {}
        pg_cursor = self.pg_conn.cursor()
        
        for author in authors:
            try:
                city_id = city_id_mapping.get(author['город_id']) if author['город_id'] else None
                
                pg_cursor.execute("""
                    INSERT INTO auto_reviews.авторы 
                    (псевдоним, настоящее_имя, город_id, дата_регистрации) 
                    VALUES (%s, %s, %s, %s) RETURNING id
                """, (
                    author['псевдоним'],
                    author['настоящее_имя'],
                    city_id,
                    author['дата_регистрации']
                ))
                
                new_id = pg_cursor.fetchone()[0]
                author_id_mapping[author['id']] = new_id
                logger.debug(f"Автор '{author['псевдоним']}': {author['id']} -> {new_id}")
                
            except Exception as e:
                logger.error(f"Ошибка при вставке автора {author['псевдоним']}: {e}")
                raise
        
        self.pg_conn.commit()
        logger.info(f"Мигрировано {len(authors)} авторов")
        return author_id_mapping
    
    def migrate_cars_and_brands(self):
        """Миграция автомобилей с созданием брендов"""
        logger.info("Миграция автомобилей и брендов...")
        
        cursor = self.sqlite_conn.cursor()
        cursor.execute("SELECT * FROM автомобили")
        cars = cursor.fetchall()
        
        if not cars:
            logger.info("Нет автомобилей для миграции")
            return {}, {}
        
        brand_id_mapping = {}
        car_id_mapping = {}
        pg_cursor = self.pg_conn.cursor()
        
        # Сначала создаем уникальные бренды
        brands = set()
        for car in cars:
            brands.add(car['марка'])
        
        for brand_name in brands:
            try:
                pg_cursor.execute(
                    "INSERT INTO auto_reviews.бренды (название, название_в_url) VALUES (%s, %s) RETURNING id",
                    (brand_name, brand_name.lower())
                )
                brand_id = pg_cursor.fetchone()[0]
                brand_id_mapping[brand_name] = brand_id
                logger.debug(f"Бренд '{brand_name}': -> {brand_id}")
                
            except Exception as e:
                logger.error(f"Ошибка при создании бренда {brand_name}: {e}")
                raise
        
        # Теперь создаем модели
        for car in cars:
            try:
                brand_id = brand_id_mapping[car['марка']]
                
                pg_cursor.execute("""
                    INSERT INTO auto_reviews.модели 
                    (название, название_в_url, бренд_id, поколение, тип_кузова, трансмиссия, 
                     тип_привода, руль, объем_двигателя_куб_см, мощность_двигателя_лс, тип_топлива) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (
                    car['модель'],
                    car['модель'].lower().replace(' ', '_'),
                    brand_id,
                    car['поколение'],
                    car['тип_кузова'],
                    car['трансмиссия'],
                    car['тип_привода'],
                    car['руль'],
                    car['объем_двигателя_куб_см'],
                    car['мощность_двигателя_лс'],
                    car['тип_топлива']
                ))
                
                new_id = pg_cursor.fetchone()[0]
                car_id_mapping[car['id']] = new_id
                logger.debug(f"Модель '{car['марка']} {car['модель']}': {car['id']} -> {new_id}")
                
            except Exception as e:
                logger.error(f"Ошибка при создании модели {car['марка']} {car['модель']}: {e}")
                raise
        
        self.pg_conn.commit()
        logger.info(f"Мигрировано {len(brands)} брендов и {len(cars)} моделей")
        return brand_id_mapping, car_id_mapping
    
    def migrate_reviews(self, car_id_mapping: Dict[int, int], author_id_mapping: Dict[int, int]):
        """Миграция отзывов"""
        logger.info("Миграция отзывов...")
        
        cursor = self.sqlite_conn.cursor()
        cursor.execute("SELECT * FROM отзывы_нормализованные")
        reviews = cursor.fetchall()
        
        if not reviews:
            logger.info("Нет отзывов для миграции")
            return {}
        
        review_id_mapping = {}
        pg_cursor = self.pg_conn.cursor()
        
        for review in reviews:
            try:
                car_id = car_id_mapping.get(review['автомобиль_id'])
                author_id = author_id_mapping.get(review['автор_id']) if review['автор_id'] else None
                
                if not car_id:
                    logger.warning(f"Не найден автомобиль с ID {review['автомобиль_id']} для отзыва {review['id']}")
                    continue
                
                pg_cursor.execute("""
                    INSERT INTO auto_reviews.отзывы 
                    (модель_id, автор_id, ссылка, заголовок, содержание, плюсы, минусы,
                     общий_рейтинг, рейтинг_владельца, год_приобретения, пробег_км,
                     цвет_кузова, цвет_салона, количество_просмотров, количество_лайков,
                     количество_дизлайков, количество_голосов, дата_публикации, дата_парсинга,
                     длина_контента, хеш_содержания, статус_парсинга, детали_ошибки)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    car_id,
                    author_id,
                    review['ссылка'],
                    review['заголовок'],
                    review['содержание'],
                    review['плюсы'],
                    review['минусы'],
                    review['общий_рейтинг'],
                    review['рейтинг_владельца'],
                    review['год_приобретения'],
                    review['пробег_км'],
                    review['цвет_кузова'],
                    review['цвет_салона'],
                    review['количество_просмотров'],
                    review['количество_лайков'],
                    review['количество_дизлайков'],
                    review['количество_голосов'],
                    review['дата_публикации'],
                    review['дата_парсинга'],
                    review['длина_контента'],
                    review['хеш_содержания'],
                    review['статус_парсинга'],
                    review['детали_ошибки']
                ))
                
                new_id = pg_cursor.fetchone()[0]
                review_id_mapping[review['id']] = new_id
                logger.debug(f"Отзыв {review['id']} -> {new_id}")
                
            except Exception as e:
                logger.error(f"Ошибка при вставке отзыва {review['id']}: {e}")
                raise
        
        self.pg_conn.commit()
        logger.info(f"Мигрировано {len(review_id_mapping)} отзывов")
        return review_id_mapping
    
    def migrate_characteristics(self, review_id_mapping: Dict[int, int]):
        """Миграция характеристик"""
        logger.info("Миграция характеристик...")
        
        cursor = self.sqlite_conn.cursor()
        cursor.execute("SELECT * FROM характеристики_норм")
        characteristics = cursor.fetchall()
        
        if not characteristics:
            logger.info("Нет характеристик для миграции")
            return
        
        pg_cursor = self.pg_conn.cursor()
        migrated_count = 0
        
        for char in characteristics:
            try:
                review_id = review_id_mapping.get(char['отзыв_id'])
                if not review_id:
                    logger.warning(f"Не найден отзыв с ID {char['отзыв_id']} для характеристики {char['id']}")
                    continue
                
                pg_cursor.execute("""
                    INSERT INTO auto_reviews.характеристики 
                    (отзыв_id, название, значение)
                    VALUES (%s, %s, %s)
                """, (
                    review_id,
                    char['название'],
                    char['значение']
                ))
                
                migrated_count += 1
                
            except Exception as e:
                logger.error(f"Ошибка при вставке характеристики {char['id']}: {e}")
                raise
        
        self.pg_conn.commit()
        logger.info(f"Мигрировано {migrated_count} характеристик")
    
    def migrate_fuel_consumption(self, review_id_mapping: Dict[int, int]):
        """Миграция данных о расходе топлива"""
        logger.info("Миграция данных о расходе топлива...")
        
        cursor = self.sqlite_conn.cursor()
        cursor.execute("SELECT * FROM расход_топлива")
        fuel_data = cursor.fetchall()
        
        if not fuel_data:
            logger.info("Нет данных о расходе топлива для миграции")
            return
        
        pg_cursor = self.pg_conn.cursor()
        migrated_count = 0
        
        for fuel in fuel_data:
            try:
                review_id = review_id_mapping.get(fuel['отзыв_id'])
                if not review_id:
                    logger.warning(f"Не найден отзыв с ID {fuel['отзыв_id']} для расхода топлива {fuel['id']}")
                    continue
                
                pg_cursor.execute("""
                    INSERT INTO auto_reviews.расход_топлива 
                    (отзыв_id, расход_город_л_100км, расход_трасса_л_100км, расход_смешанный_л_100км)
                    VALUES (%s, %s, %s, %s)
                """, (
                    review_id,
                    fuel['расход_город_л_100км'],
                    fuel['расход_трасса_л_100км'],
                    fuel['расход_смешанный_л_100км']
                ))
                
                migrated_count += 1
                
            except Exception as e:
                logger.error(f"Ошибка при вставке данных о расходе топлива {fuel['id']}: {e}")
                raise
        
        self.pg_conn.commit()
        logger.info(f"Мигрировано {migrated_count} записей о расходе топлива")
    
    def verify_migration(self):
        """Проверка результатов миграции"""
        logger.info("Проверка результатов миграции...")
        
        pg_cursor = self.pg_conn.cursor()
        
        tables_to_check = [
            'auto_reviews.города',
            'auto_reviews.авторы',
            'auto_reviews.бренды',
            'auto_reviews.модели',
            'auto_reviews.отзывы',
            'auto_reviews.характеристики',
            'auto_reviews.расход_топлива'
        ]
        
        for table in tables_to_check:
            try:
                pg_cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = pg_cursor.fetchone()[0]
                logger.info(f"PostgreSQL {table}: {count} записей")
                
            except Exception as e:
                logger.error(f"Ошибка при проверке таблицы {table}: {e}")
    
    def run_migration(self):
        """Запуск полной миграции"""
        logger.info("Запуск миграции нормализованных данных...")
        
        try:
            self.connect()
            
            # Получаем статистику исходных данных
            source_counts = self.get_table_counts()
            
            # Очищаем PostgreSQL
            self.clear_postgresql_data()
            
            # Выполняем миграцию в правильном порядке
            city_id_mapping = self.migrate_cities()
            author_id_mapping = self.migrate_authors(city_id_mapping)
            brand_id_mapping, car_id_mapping = self.migrate_cars_and_brands()
            review_id_mapping = self.migrate_reviews(car_id_mapping, author_id_mapping)
            
            # Мигрируем связанные данные
            self.migrate_characteristics(review_id_mapping)
            self.migrate_fuel_consumption(review_id_mapping)
            
            # Проверяем результаты
            self.verify_migration()
            
            logger.info("Миграция завершена успешно!")
            
        except Exception as e:
            logger.error(f"Ошибка при миграции: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
            raise
        finally:
            self.disconnect()


def main():
    """Главная функция"""
    
    # Конфигурация PostgreSQL
    pg_config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'auto_reviews',
        'username': 'parser',
        'password': 'parser'
    }
    
    # Путь к SQLite базе
    sqlite_path = os.path.join(
        os.path.dirname(__file__), 
        '..', '..', 
        'data', 'databases', 
        'нормализованная_бд_v3.db'
    )
    
    if not os.path.exists(sqlite_path):
        logger.error(f"SQLite база не найдена: {sqlite_path}")
        return 1
    
    # Запуск миграции
    migrator = NormalizedDataMigrator(sqlite_path, pg_config)
    
    try:
        migrator.run_migration()
        logger.info("Миграция завершена успешно!")
        return 0
        
    except Exception as e:
        logger.error(f"Ошибка миграции: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
