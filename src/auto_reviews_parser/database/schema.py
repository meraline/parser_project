#!/usr/bin/env python3
"""
🗄️ СХЕМА БАЗЫ ДАННЫХ ДЛЯ ПАРСЕРА ОТЗЫВОВ DROM.RU

Создает нормализованную структуру базы данных с отдельными таблицами:
- brands: каталог всех брендов
- models: модели для каждого бренда
- reviews: отзывы по моделям

Преимущества нормализованной структуры:
- Избежание дублирования данных
- Простота обновления каталогов
- Эффективные запросы и связи
- Контроль целостности данных
"""

import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional


class DatabaseManager:
    """Менеджер базы данных для парсера отзывов"""

    def __init__(self, db_path: str = "auto_reviews.db"):
        self.db_path = Path(db_path)
        self.logger = logging.getLogger(__name__)

    def create_database(self):
        """Создание базы данных с нормализованной структурой"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.executescript(
                    """
                    -- Отключение внешних ключей для создания таблиц
                    PRAGMA foreign_keys = OFF;

                    -- Таблица брендов
                    CREATE TABLE IF NOT EXISTS brands (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL UNIQUE,
                        url_name TEXT NOT NULL UNIQUE,
                        full_url TEXT,
                        reviews_count INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    -- Таблица моделей
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

                    -- Таблица отзывов с поддержкой длинных и коротких отзывов
                    CREATE TABLE IF NOT EXISTS reviews (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        model_id INTEGER NOT NULL,
                        review_type TEXT NOT NULL DEFAULT 'long', -- 'long' или 'short'
                        
                        -- Основная информация
                        title TEXT,
                        content TEXT,
                        positive_text TEXT,
                        negative_text TEXT,
                        breakages_text TEXT,
                        
                        -- Информация об авторе
                        author_name TEXT,
                        author_city TEXT,
                        review_date TEXT,
                        
                        -- Характеристики автомобиля
                        car_year INTEGER,
                        car_engine_volume REAL,
                        car_fuel_type TEXT,
                        car_transmission TEXT,
                        car_drive_type TEXT,
                        car_body_type TEXT,
                        car_color TEXT,
                        car_mileage INTEGER,
                        
                        -- Оценки
                        overall_rating REAL,
                        comfort_rating REAL,
                        reliability_rating REAL,
                        fuel_consumption_rating REAL,
                        driving_rating REAL,
                        appearance_rating REAL,
                        
                        -- Метаданные
                        source_url TEXT,
                        review_id TEXT,
                        photos_count INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        
                        FOREIGN KEY (model_id) REFERENCES models(id) ON DELETE CASCADE
                    );

                    -- Включение внешних ключей
                    PRAGMA foreign_keys = ON;

                    -- Индексы для оптимизации запросов
                    CREATE INDEX IF NOT EXISTS idx_brands_url_name ON brands(url_name);
                    CREATE INDEX IF NOT EXISTS idx_models_brand_id ON models(brand_id);
                    CREATE INDEX IF NOT EXISTS idx_models_url_name ON models(url_name);
                    CREATE INDEX IF NOT EXISTS idx_reviews_model_id ON reviews(model_id);
                    CREATE INDEX IF NOT EXISTS idx_reviews_type ON reviews(review_type);
                    CREATE INDEX IF NOT EXISTS idx_reviews_date ON reviews(review_date);
                """
                )
                conn.commit()
                self.logger.info(
                    "✅ База данных успешно создана с нормализованной структурой"
                )
                return True

        except sqlite3.Error as e:
            self.logger.error(f"❌ Ошибка создания базы данных: {e}")
            return False

    def add_brand(
        self, name: str, url_name: str, full_url: str = None, reviews_count: int = 0
    ) -> Optional[int]:
        """Добавление бренда в базу данных"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO brands (name, url_name, full_url, reviews_count)
                    VALUES (?, ?, ?, ?)
                """,
                    (name, url_name, full_url, reviews_count),
                )

                if cursor.rowcount > 0:
                    brand_id = cursor.lastrowid
                    self.logger.info(f"✅ Бренд добавлен: {name} (ID: {brand_id})")
                    return brand_id
                else:
                    # Бренд уже существует, получаем его ID
                    cursor.execute(
                        "SELECT id FROM brands WHERE url_name = ?", (url_name,)
                    )
                    result = cursor.fetchone()
                    if result:
                        return result[0]

        except sqlite3.Error as e:
            self.logger.error(f"❌ Ошибка добавления бренда {name}: {e}")
            return None

    def add_model(
        self,
        brand_id: int,
        name: str,
        url_name: str,
        full_url: str = None,
        reviews_count: int = 0,
    ) -> Optional[int]:
        """Добавление модели в базу данных"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO models (brand_id, name, url_name, full_url, reviews_count)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (brand_id, name, url_name, full_url, reviews_count),
                )

                if cursor.rowcount > 0:
                    model_id = cursor.lastrowid
                    self.logger.info(
                        f"✅ Модель добавлена: {name} для бренда ID {brand_id} (ID: {model_id})"
                    )
                    return model_id
                else:
                    # Модель уже существует, получаем её ID
                    cursor.execute(
                        "SELECT id FROM models WHERE brand_id = ? AND url_name = ?",
                        (brand_id, url_name),
                    )
                    result = cursor.fetchone()
                    if result:
                        return result[0]

        except sqlite3.Error as e:
            self.logger.error(f"❌ Ошибка добавления модели {name}: {e}")
            return None

    def add_review(
        self, model_id: int, review_type: str = "long", **review_data
    ) -> Optional[int]:
        """
        Добавление отзыва в базу данных

        Args:
            model_id: ID модели
            review_type: тип отзыва ('long' или 'short')
            **review_data: данные отзыва
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Подготовка данных для вставки
                fields = ["model_id", "review_type"]
                values = [model_id, review_type]
                placeholders = ["?", "?"]

                # Добавляем поля из review_data
                for key, value in review_data.items():
                    fields.append(key)
                    values.append(value)
                    placeholders.append("?")

                query = f"""
                    INSERT INTO reviews ({', '.join(fields)})
                    VALUES ({', '.join(placeholders)})
                """

                cursor.execute(query, values)
                review_id = cursor.lastrowid

                self.logger.info(
                    f"✅ Отзыв добавлен: {review_type} для модели ID {model_id} (ID: {review_id})"
                )
                return review_id

        except sqlite3.Error as e:
            self.logger.error(f"❌ Ошибка добавления отзыва: {e}")
            return None

    def get_brand_by_url_name(self, url_name: str) -> Optional[dict]:
        """Получение бренда по URL имени"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM brands WHERE url_name = ?", (url_name,))
                result = cursor.fetchone()
                return dict(result) if result else None
        except sqlite3.Error as e:
            self.logger.error(f"❌ Ошибка получения бренда {url_name}: {e}")
            return None

    def get_model_by_url_name(self, brand_id: int, url_name: str) -> Optional[dict]:
        """Получение модели по URL имени и ID бренда"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM models WHERE brand_id = ? AND url_name = ?",
                    (brand_id, url_name),
                )
                result = cursor.fetchone()
                return dict(result) if result else None
        except sqlite3.Error as e:
            self.logger.error(f"❌ Ошибка получения модели {url_name}: {e}")
            return None

    def get_models_by_brand(self, brand_id: int) -> list:
        """Получение всех моделей бренда"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM models WHERE brand_id = ? ORDER BY name", (brand_id,)
                )
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            self.logger.error(f"❌ Ошибка получения моделей для бренда {brand_id}: {e}")
            return []

    def get_statistics(self) -> dict:
        """Получение статистики базы данных"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Количество брендов
                cursor.execute("SELECT COUNT(*) FROM brands")
                brands_count = cursor.fetchone()[0]

                # Количество моделей
                cursor.execute("SELECT COUNT(*) FROM models")
                models_count = cursor.fetchone()[0]

                # Количество отзывов по типам
                cursor.execute(
                    "SELECT review_type, COUNT(*) FROM reviews GROUP BY review_type"
                )
                reviews_by_type = dict(cursor.fetchall())

                # Общее количество отзывов
                cursor.execute("SELECT COUNT(*) FROM reviews")
                total_reviews = cursor.fetchone()[0]

                return {
                    "brands_count": brands_count,
                    "models_count": models_count,
                    "total_reviews": total_reviews,
                    "long_reviews": reviews_by_type.get("long", 0),
                    "short_reviews": reviews_by_type.get("short", 0),
                }

        except sqlite3.Error as e:
            self.logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}

    def update_reviews_count(self):
        """Обновление счетчиков отзывов для брендов и моделей"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Обновляем счетчики для моделей
                cursor.execute(
                    """
                    UPDATE models SET reviews_count = (
                        SELECT COUNT(*) FROM reviews WHERE reviews.model_id = models.id
                    )
                """
                )

                # Обновляем счетчики для брендов
                cursor.execute(
                    """
                    UPDATE brands SET reviews_count = (
                        SELECT SUM(models.reviews_count) FROM models WHERE models.brand_id = brands.id
                    )
                """
                )

                conn.commit()
                self.logger.info("✅ Счетчики отзывов обновлены")

        except sqlite3.Error as e:
            self.logger.error(f"❌ Ошибка обновления счетчиков: {e}")


if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Создание базы данных
    db_manager = DatabaseManager("data/auto_reviews_normalized.db")

    if db_manager.create_database():
        print("\n🎉 База данных создана успешно!")

        # Показываем статистику
        stats = db_manager.get_statistics()
        print(f"\n📊 Статистика базы данных:")
        print(f"   🏢 Брендов: {stats.get('brands_count', 0)}")
        print(f"   🚗 Моделей: {stats.get('models_count', 0)}")
        print(f"   📝 Отзывов всего: {stats.get('total_reviews', 0)}")
        print(f"   📄 Длинных отзывов: {stats.get('long_reviews', 0)}")
        print(f"   📋 Коротких отзывов: {stats.get('short_reviews', 0)}")
    else:
        print("\n❌ Ошибка создания базы данных!")
