import sqlite3
import logging
from typing import Dict

from src.config.settings import settings
from src.models import Review


class ReviewsDatabase:
    """Управление базой данных отзывов"""

    def __init__(self, db_path: str = settings.db_path):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Инициализация базы данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Таблица отзывов
        cursor.execute(
            """
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
        """
        )

        # Таблица статистики парсинга
        cursor.execute(
            """
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
        """
        )

        # Таблица источников (для отслеживания прогресса)
        cursor.execute(
            """
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
        """
        )

        # Индексы для быстрого поиска
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_brand_model ON reviews(brand, model)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_source_type ON reviews(source, type)"
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_parsed_at ON reviews(parsed_at)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_content_hash ON reviews(content_hash)"
        )

        conn.commit()
        conn.close()

    def save_review(self, review: Review) -> bool:
        """Сохранение отзыва в базу"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT OR IGNORE INTO reviews (
                    source, type, brand, model, generation, year, url, title, content,
                    author, rating, pros, cons, mileage, engine_volume, fuel_type,
                    transmission, body_type, drive_type, publish_date, views_count,
                    likes_count, comments_count, parsed_at, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    review.source,
                    review.type,
                    review.brand,
                    review.model,
                    review.generation,
                    review.year,
                    review.url,
                    review.title,
                    review.content,
                    review.author,
                    review.rating,
                    review.pros,
                    review.cons,
                    review.mileage,
                    review.engine_volume,
                    review.fuel_type,
                    review.transmission,
                    review.body_type,
                    review.drive_type,
                    review.publish_date,
                    review.views_count,
                    review.likes_count,
                    review.comments_count,
                    review.parsed_at,
                    review.content_hash,
                ),
            )

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
            cursor.execute(
                "SELECT COUNT(*) FROM reviews WHERE brand = ? AND model = ?",
                (brand, model),
            )
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
            "total_reviews": total_reviews,
            "unique_brands": unique_brands,
            "unique_models": unique_models,
            "by_source": by_source,
            "by_type": by_type,
        }
