import logging
import sqlite3
from typing import Dict, Optional

from ..base import Database
from parsers.models import Review


class ReviewRepository:
    """CRUD-операции для таблицы отзывов."""

    def __init__(self, db: Database):
        self.db = db
        self._init_table()

    def _init_table(self) -> None:
        with self.db.connection() as conn:
            cursor = conn.cursor()
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
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_brand_model ON reviews(brand, model)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_source_type ON reviews(source, type)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_parsed_at ON reviews(parsed_at)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_content_hash ON reviews(content_hash)"
            )
            conn.commit()

    def save(self, review: Review) -> bool:
        """Сохранение отзыва."""
        try:
            with self.db.connection() as conn:
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
                return success
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            logging.error(f"Ошибка сохранения отзыва: {e}")
            return False

    def get_reviews_count(self, brand: Optional[str] = None, model: Optional[str] = None) -> int:
        with self.db.connection() as conn:
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
            return cursor.fetchone()[0]

    def is_url_parsed(self, url: str) -> bool:
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM reviews WHERE url = ? LIMIT 1", (url,))
            return cursor.fetchone() is not None

    def get_parsing_stats(self) -> Dict:
        with self.db.connection() as conn:
            cursor = conn.cursor()
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
            return {
                "total_reviews": total_reviews,
                "unique_brands": unique_brands,
                "unique_models": unique_models,
                "by_source": by_source,
                "by_type": by_type,
            }
