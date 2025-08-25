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

    def create_database(self) -> bool:
        """Создание базы данных с нормализованной структурой"""
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()

            # 🏢 ТАБЛИЦА БРЕНДОВ
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS brands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    review_count INTEGER DEFAULT 0,
                    url TEXT,
                    logo_url TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # 🚗 ТАБЛИЦА МОДЕЛЕЙ
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS models (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    brand_id INTEGER NOT NULL,
                    slug TEXT NOT NULL,
                    name TEXT NOT NULL,
                    review_count INTEGER DEFAULT 0,
                    url TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (brand_id) REFERENCES brands (id),
                    UNIQUE(brand_id, slug)
                )
            """
            )

            # 📝 ТАБЛИЦА ОТЗЫВОВ (ДЛИННЫЕ И КОРОТКИЕ)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    brand_id INTEGER NOT NULL,
                    model_id INTEGER NOT NULL,
                    review_id TEXT NOT NULL,
                    review_type TEXT NOT NULL DEFAULT 'long',  -- 'long' или 'short'
                    url TEXT UNIQUE NOT NULL,
                    title TEXT,
                    content TEXT,
                    author TEXT,
                    city TEXT,
                    publish_date DATE,
                    rating REAL,
                    pros TEXT,
                    cons TEXT,
                    general_impression TEXT,
                    breakages TEXT,  -- для коротких отзывов: поломки
                    experience_period TEXT,
                    car_year INTEGER,
                    car_volume REAL,  -- объем двигателя
                    car_fuel_type TEXT,  -- тип топлива
                    car_transmission TEXT,  -- КПП
                    car_drive TEXT,  -- привод
                    car_modification TEXT,
                    photos TEXT,  -- JSON массив ссылок на фото
                    is_complete BOOLEAN DEFAULT 0,
                    parse_attempts INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (brand_id) REFERENCES brands (id),
                    FOREIGN KEY (model_id) REFERENCES models (id),
                    UNIQUE(review_id, review_type)
                )
            """
            )

            # 📊 ИНДЕКСЫ ДЛЯ ОПТИМИЗАЦИИ
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_brands_slug ON brands(slug)")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_models_brand_slug "
                "ON models(brand_id, slug)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_reviews_brand_model "
                "ON reviews(brand_id, model_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_reviews_type " "ON reviews(review_type)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_reviews_complete "
                "ON reviews(is_complete)"
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_url ON reviews(url)")

            # 🔧 TRIGGERS ДЛЯ АВТОМАТИЧЕСКОГО ОБНОВЛЕНИЯ ВРЕМЕНИ
            cursor.execute(
                """
                CREATE TRIGGER IF NOT EXISTS update_brands_timestamp 
                AFTER UPDATE ON brands
                BEGIN
                    UPDATE brands SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                END
            """
            )

            cursor.execute(
                """
                CREATE TRIGGER IF NOT EXISTS update_models_timestamp 
                AFTER UPDATE ON models
                BEGIN
                    UPDATE models SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                END
            """
            )

            cursor.execute(
                """
                CREATE TRIGGER IF NOT EXISTS update_reviews_timestamp 
                AFTER UPDATE ON reviews
                BEGIN
                    UPDATE reviews SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                END
            """
            )

            conn.commit()
            conn.close()

            self.logger.info(f"✅ База данных создана: {self.db_path}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка создания базы данных: {e}")
            return False

    def get_connection(self) -> sqlite3.Connection:
        """Получение подключения к базе данных"""
        return sqlite3.connect(str(self.db_path))

    def add_brand(
        self,
        slug: str,
        name: str,
        review_count: int = 0,
        url: str = None,
        logo_url: str = None,
    ) -> Optional[int]:
        """Добавление бренда в каталог"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT OR REPLACE INTO brands (slug, name, review_count, url, logo_url)
                VALUES (?, ?, ?, ?, ?)
            """,
                (slug, name, review_count, url, logo_url),
            )

            brand_id = cursor.lastrowid
            conn.commit()
            conn.close()

            return brand_id

        except Exception as e:
            self.logger.error(f"❌ Ошибка добавления бренда {slug}: {e}")
            return None

    def add_model(
        self,
        brand_slug: str,
        model_slug: str,
        model_name: str,
        review_count: int = 0,
        url: str = None,
    ) -> Optional[int]:
        """Добавление модели в каталог"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Получаем ID бренда
            cursor.execute("SELECT id FROM brands WHERE slug = ?", (brand_slug,))
            brand_result = cursor.fetchone()

            if not brand_result:
                self.logger.error(f"❌ Бренд не найден: {brand_slug}")
                conn.close()
                return None

            brand_id = brand_result[0]

            cursor.execute(
                """
                INSERT OR REPLACE INTO models (brand_id, slug, name, review_count, url)
                VALUES (?, ?, ?, ?, ?)
            """,
                (brand_id, model_slug, model_name, review_count, url),
            )

            model_id = cursor.lastrowid
            conn.commit()
            conn.close()

            return model_id

        except Exception as e:
            self.logger.error(f"❌ Ошибка добавления модели {model_slug}: {e}")
            return None

    def get_brand_models(self, brand_slug: str) -> list:
        """Получение всех моделей бренда"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT m.slug, m.name, m.review_count 
                FROM models m
                JOIN brands b ON m.brand_id = b.id
                WHERE b.slug = ? AND m.is_active = 1
                ORDER BY m.name
            """,
                (brand_slug,),
            )

            models = cursor.fetchall()
            conn.close()

            return [{"slug": m[0], "name": m[1], "review_count": m[2]} for m in models]

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения моделей для {brand_slug}: {e}")
            return []

    def get_all_brands(self) -> list:
        """Получение всех активных брендов"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT slug, name, review_count 
                FROM brands 
                WHERE is_active = 1 
                ORDER BY name
            """
            )

            brands = cursor.fetchall()
            conn.close()

            return [{"slug": b[0], "name": b[1], "review_count": b[2]} for b in brands]

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения брендов: {e}")
            return []

    def add_review(
        self,
        brand_slug: str,
        model_slug: str,
        review_id: str,
        review_type: str = "long",  # 'long' или 'short'
        url: str = None,
        title: str = None,
        content: str = None,
        author: str = None,
        city: str = None,
        publish_date: str = None,
        rating: float = None,
        pros: str = None,
        cons: str = None,
        general_impression: str = None,
        breakages: str = None,
        experience_period: str = None,
        car_year: int = None,
        car_volume: float = None,
        car_fuel_type: str = None,
        car_transmission: str = None,
        car_drive: str = None,
        car_modification: str = None,
        photos: str = None,  # JSON строка с массивом ссылок
    ) -> Optional[int]:
        """Добавление отзыва в базу данных"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Получаем ID бренда и модели
            cursor.execute(
                """
                SELECT b.id, m.id 
                FROM brands b
                JOIN models m ON m.brand_id = b.id
                WHERE b.slug = ? AND m.slug = ?
                """,
                (brand_slug, model_slug),
            )

            result = cursor.fetchone()
            if not result:
                self.logger.error(
                    f"❌ Бренд/модель не найдены: {brand_slug}/{model_slug}"
                )
                conn.close()
                return None

            brand_id, model_id = result

            cursor.execute(
                """
                INSERT OR REPLACE INTO reviews (
                    brand_id, model_id, review_id, review_type, url, title, 
                    content, author, city, publish_date, rating, pros, cons,
                    general_impression, breakages, experience_period, car_year,
                    car_volume, car_fuel_type, car_transmission, car_drive,
                    car_modification, photos
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?
                )
                """,
                (
                    brand_id,
                    model_id,
                    review_id,
                    review_type,
                    url,
                    title,
                    content,
                    author,
                    city,
                    publish_date,
                    rating,
                    pros,
                    cons,
                    general_impression,
                    breakages,
                    experience_period,
                    car_year,
                    car_volume,
                    car_fuel_type,
                    car_transmission,
                    car_drive,
                    car_modification,
                    photos,
                ),
            )

            review_row_id = cursor.lastrowid
            conn.commit()
            conn.close()

            return review_row_id

        except Exception as e:
            self.logger.error(f"❌ Ошибка добавления отзыва {review_id}: {e}")
            return None

    def get_model_reviews_count(
        self, brand_slug: str, model_slug: str, review_type: str = None
    ) -> dict:
        """Получение количества отзывов по модели"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            if review_type:
                cursor.execute(
                    """
                    SELECT COUNT(*) 
                    FROM reviews r
                    JOIN brands b ON r.brand_id = b.id
                    JOIN models m ON r.model_id = m.id
                    WHERE b.slug = ? AND m.slug = ? AND r.review_type = ?
                    """,
                    (brand_slug, model_slug, review_type),
                )
                count = cursor.fetchone()[0]
                conn.close()
                return {review_type: count}
            else:
                # Получаем оба типа
                cursor.execute(
                    """
                    SELECT r.review_type, COUNT(*) 
                    FROM reviews r
                    JOIN brands b ON r.brand_id = b.id
                    JOIN models m ON r.model_id = m.id
                    WHERE b.slug = ? AND m.slug = ?
                    GROUP BY r.review_type
                    """,
                    (brand_slug, model_slug),
                )

                results = cursor.fetchall()
                conn.close()

                counts = {"long": 0, "short": 0}
                for review_type_db, count in results:
                    counts[review_type_db] = count

                return counts

        except Exception as e:
            self.logger.error(
                f"❌ Ошибка получения отзывов {brand_slug}/{model_slug}: {e}"
            )
            return {"long": 0, "short": 0}

    def get_database_stats(self) -> dict:
        """Получение статистики базы данных"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Подсчет записей
            cursor.execute("SELECT COUNT(*) FROM brands WHERE is_active = 1")
            brands_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM models WHERE is_active = 1")
            models_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM reviews")
            reviews_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM reviews WHERE review_type = 'long'")
            long_reviews = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM reviews WHERE review_type = 'short'")
            short_reviews = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM reviews WHERE is_complete = 1")
            complete_reviews = cursor.fetchone()[0]

            conn.close()

            return {
                "brands": brands_count,
                "models": models_count,
                "reviews": reviews_count,
                "long_reviews": long_reviews,
                "short_reviews": short_reviews,
                "complete_reviews": complete_reviews,
                "completion_rate": (
                    round(complete_reviews / reviews_count * 100, 2)
                    if reviews_count > 0
                    else 0
                ),
            }

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}


def main():
    """Инициализация базы данных"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    print("🗄️ СОЗДАНИЕ НОРМАЛИЗОВАННОЙ БАЗЫ ДАННЫХ")
    print("=" * 50)

    # Создание базы данных
    db_manager = DatabaseManager()

    if db_manager.create_database():
        print("✅ База данных успешно создана!")

        # Показать структуру
        print("\n📋 СТРУКТУРА БАЗЫ ДАННЫХ:")
        print("- brands: каталог брендов автомобилей")
        print("- models: модели для каждого бренда")
        print("- reviews: отзывы по моделям")
        print("- Индексы для оптимизации запросов")
        print("- Triggers для автоматического обновления времени")

        # Статистика
        stats = db_manager.get_database_stats()
        print("\n📊 ТЕКУЩАЯ СТАТИСТИКА:")
        print(f"Брендов: {stats.get('brands', 0)}")
        print(f"Моделей: {stats.get('models', 0)}")
        print(f"Отзывов всего: {stats.get('reviews', 0)}")
        print(f"- Длинных отзывов: {stats.get('long_reviews', 0)}")
        print(f"- Коротких отзывов: {stats.get('short_reviews', 0)}")
        print(f"Завершенных: {stats.get('complete_reviews', 0)}")
        print(f"Прогресс: {stats.get('completion_rate', 0)}%")

    else:
        print("❌ Ошибка создания базы данных!")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
