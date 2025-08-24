import sqlite3
from typing import Dict, List, Optional, Tuple

from ..base import Database
from ...models import Review
from ...utils.logger import get_logger

logger = get_logger(__name__)


class ReviewRepository:
    """CRUD-операции для работы с отзывами в БД."""

    def __init__(self, db: Database):
        self.db = db
        self._init_table()

    def _init_table(self) -> None:
        """Инициализация таблицы и индексов."""
        with self.db.transaction() as conn:
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
                """
                CREATE INDEX IF NOT EXISTS 
                idx_source_type ON reviews(source, type)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS 
                idx_parsed_at ON reviews(parsed_at)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS 
                idx_content_hash ON reviews(content_hash)
                """
            )

    def _review_to_tuple(self, review: Review) -> Tuple:
        """Преобразует объект Review в кортеж для вставки в БД."""
        return (
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
        )

    def add(self, review: Review) -> bool:
        """Добавляет один отзыв в базу данных.

        Args:
            review: Отзыв для сохранения

        Returns:
            bool: True если отзыв успешно добавлен
        """
        try:
            with self.db.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO reviews (
                        source, type, brand, model, generation, year,
                        url, title, content, author, rating, pros,
                        cons, mileage, engine_volume, fuel_type,
                        transmission, body_type, drive_type,
                        publish_date, views_count, likes_count,
                        comments_count, parsed_at, content_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    self._review_to_tuple(review),
                )
                return cursor.rowcount > 0
        except sqlite3.Error as e:
            logger.error(f"Ошибка добавления отзыва: {e}")
            return False

    def save_batch(self, reviews: List[Review]) -> int:
        """Пакетное сохранение отзывов.

        Args:
            reviews: Список отзывов для сохранения

        Returns:
            int: Количество успешно сохраненных отзывов
        """
        if not reviews:
            return 0

        params = [self._review_to_tuple(review) for review in reviews]

        try:
            with self.db.transaction() as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    """
                    INSERT OR IGNORE INTO reviews (
                        source, type, brand, model, generation, year,
                        url, title, content, author, rating, pros,
                        cons, mileage, engine_volume, fuel_type,
                        transmission, body_type, drive_type,
                        publish_date, views_count, likes_count,
                        comments_count, parsed_at, content_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    params,
                )
                return cursor.rowcount
        except sqlite3.Error as e:
            logger.error(f"Ошибка пакетного сохранения отзывов: {e}")
            return 0

    def stats(self) -> Dict:
        """Получает статистику из базы данных.

        Returns:
            Dict: Словарь со статистикой:
                - total_reviews: общее количество отзывов
                - unique_brands: количество уникальных марок
                - unique_models: количество уникальных моделей
                - by_source: распределение по источникам
        """
        try:
            with self.db.transaction() as conn:
                cursor = conn.cursor()

                # Общее количество отзывов
                cursor.execute("SELECT COUNT(*) FROM reviews")
                total = cursor.fetchone()[0]

                # Уникальные марки
                cursor.execute("SELECT COUNT(DISTINCT brand) FROM reviews")
                brands = cursor.fetchone()[0]

                # Уникальные модели
                cursor.execute("SELECT COUNT(DISTINCT model) FROM reviews")
                models = cursor.fetchone()[0]

                # Распределение по источникам
                cursor.execute(
                    """
                    SELECT source, COUNT(*) as count 
                    FROM reviews 
                    GROUP BY source
                    """
                )
                by_source = dict(cursor.fetchall())

                return {
                    "total_reviews": total,
                    "unique_brands": brands,
                    "unique_models": models,
                    "by_source": by_source,
                }

        except sqlite3.Error as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {
                "total_reviews": 0,
                "unique_brands": 0,
                "unique_models": 0,
                "by_source": {},
            }

    def save(self, review: Review) -> bool:
        """Сохранение одиночного отзыва.

        Args:
            review: Отзыв для сохранения

        Returns:
            bool: True если отзыв успешно сохранен
        """
        return self.save_batch([review]) == 1

    def get_reviews_count(
        self, brand: Optional[str] = None, model: Optional[str] = None
    ) -> int:
        """Получение количества отзывов с фильтрацией.

        Args:
            brand: Фильтр по бренду
            model: Фильтр по модели

        Returns:
            int: Количество отзывов
        """
        with self.db.connection() as conn:
            cursor = conn.cursor()
            if brand and model:
                query = """
                    SELECT COUNT(*)
                    FROM reviews
                    WHERE brand = ?
                    AND model = ?
                """
                cursor.execute(query, (brand, model))
            elif brand:
                query = """
                    SELECT COUNT(*)
                    FROM reviews
                    WHERE brand = ?
                """
                cursor.execute(query, (brand,))
            else:
                cursor.execute("SELECT COUNT(*) FROM reviews")
            return cursor.fetchone()[0]

    def is_url_parsed(self, url: str) -> bool:
        """Проверяет был ли URL уже обработан.

        Args:
            url: URL для проверки

        Returns:
            bool: True если URL уже обработан
        """
        with self.db.connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT 1
                FROM reviews
                WHERE url = ?
                LIMIT 1
            """
            cursor.execute(query, (url,))
            return cursor.fetchone() is not None

    def get_parsing_stats(self) -> Dict:
        """Получает статистику по парсингу.

        Returns:
            Dict: Словарь со статистикой
        """
        with self.db.connection() as conn:
            cursor = conn.cursor()

            # Общее количество отзывов
            cursor.execute("SELECT COUNT(*) FROM reviews")
            total_reviews = cursor.fetchone()[0]

            # Уникальные бренды
            cursor.execute("SELECT COUNT(DISTINCT brand) FROM reviews")
            unique_brands = cursor.fetchone()[0]

            # Уникальные модели
            cursor.execute("SELECT COUNT(DISTINCT model) FROM reviews")
            unique_models = cursor.fetchone()[0]

            # Группировка по источникам
            cursor.execute(
                """
                SELECT source, COUNT(*)
                FROM reviews
                GROUP BY source
                """
            )
            by_source = dict(cursor.fetchall())

            # Группировка по типам
            cursor.execute(
                """
                SELECT type, COUNT(*)
                FROM reviews
                GROUP BY type
                """
            )
            by_type = dict(cursor.fetchall())

            return {
                "total_reviews": total_reviews,
                "unique_brands": unique_brands,
                "unique_models": unique_models,
                "by_source": by_source,
                "by_type": by_type,
            }
