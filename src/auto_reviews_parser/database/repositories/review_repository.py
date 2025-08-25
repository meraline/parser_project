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
                    источник TEXT NOT NULL,
                    тип TEXT NOT NULL,
                    марка TEXT NOT NULL,
                    модель TEXT NOT NULL,
                    поколение TEXT,
                    год_выпуска INTEGER,
                    ссылка TEXT UNIQUE,
                    заголовок TEXT,
                    содержание TEXT,
                    автор TEXT,
                    рейтинг REAL,
                    общая_оценка REAL,
                    оценка_внешнего_вида INTEGER,
                    оценка_салона INTEGER,
                    оценка_двигателя INTEGER,
                    оценка_ходовых_качеств INTEGER,
                    плюсы TEXT,
                    минусы TEXT,
                    пробег INTEGER,
                    объем_двигателя REAL,
                    мощность_двигателя INTEGER,
                    тип_топлива TEXT,
                    расход_в_городе REAL,
                    расход_на_трассе REAL,
                    трансмиссия TEXT,
                    тип_кузова TEXT,
                    тип_привода TEXT,
                    руль TEXT,
                    год_приобретения INTEGER,
                    дата_публикации DATETIME,
                    количество_просмотров INTEGER,
                    количество_лайков INTEGER,
                    количество_комментариев INTEGER,
                    дата_парсинга DATETIME DEFAULT CURRENT_TIMESTAMP,
                    хеш_содержания TEXT UNIQUE,
                    UNIQUE(ссылка, хеш_содержания)
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_brand_model ON reviews(марка, модель)"
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS 
                idx_source_type ON reviews(источник, тип)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS 
                idx_parsed_at ON reviews(дата_парсинга)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS 
                idx_content_hash ON reviews(хеш_содержания)
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
            review.overall_rating,
            review.exterior_rating,
            review.interior_rating,
            review.engine_rating,
            review.driving_rating,
            review.pros,
            review.cons,
            review.mileage,
            review.engine_volume,
            review.engine_power,
            review.fuel_type,
            review.fuel_consumption_city,
            review.fuel_consumption_highway,
            review.transmission,
            review.body_type,
            review.drive_type,
            review.steering_wheel,
            review.year_purchased,
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
                        источник, тип, марка, модель, поколение, год_выпуска,
                        ссылка, заголовок, содержание, автор, рейтинг,
                        общая_оценка, оценка_внешнего_вида, оценка_салона,
                        оценка_двигателя, оценка_ходовых_качеств, плюсы,
                        минусы, пробег, объем_двигателя, мощность_двигателя,
                        тип_топлива, расход_в_городе,
                        расход_на_трассе, трансмиссия,
                        тип_кузова, тип_привода, руль,
                        год_приобретения, дата_публикации, количество_просмотров,
                        количество_лайков, количество_комментариев, дата_парсинга,
                        хеш_содержания
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        источник, тип, марка, модель, поколение, год_выпуска,
                        ссылка, заголовок, содержание, автор, рейтинг,
                        общая_оценка, оценка_внешнего_вида, оценка_салона,
                        оценка_двигателя, оценка_ходовых_качеств, плюсы,
                        минусы, пробег, объем_двигателя, мощность_двигателя,
                        тип_топлива, расход_в_городе,
                        расход_на_трассе, трансмиссия,
                        тип_кузова, тип_привода, руль,
                        год_приобретения, дата_публикации, количество_просмотров,
                        количество_лайков, количество_комментариев, дата_парсинга,
                        хеш_содержания
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                cursor.execute("SELECT COUNT(DISTINCT марка) FROM reviews")
                brands = cursor.fetchone()[0]

                # Уникальные модели
                cursor.execute("SELECT COUNT(DISTINCT модель) FROM reviews")
                models = cursor.fetchone()[0]

                # Распределение по источникам
                cursor.execute(
                    """
                    SELECT источник, COUNT(*) as count 
                    FROM reviews 
                    GROUP BY источник
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
                    WHERE марка = ?
                    AND модель = ?
                """
                cursor.execute(query, (brand, model))
            elif brand:
                query = """
                    SELECT COUNT(*)
                    FROM reviews
                    WHERE марка = ?
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
                WHERE ссылка = ?
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
            cursor.execute("SELECT COUNT(DISTINCT марка) FROM reviews")
            unique_brands = cursor.fetchone()[0]

            # Уникальные модели
            cursor.execute("SELECT COUNT(DISTINCT модель) FROM reviews")
            unique_models = cursor.fetchone()[0]

            # Группировка по источникам
            cursor.execute(
                """
                SELECT источник, COUNT(*)
                FROM reviews
                GROUP BY источник
                """
            )
            by_source = dict(cursor.fetchall())

            # Группировка по типам
            cursor.execute(
                """
                SELECT тип, COUNT(*)
                FROM reviews
                GROUP BY тип
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
