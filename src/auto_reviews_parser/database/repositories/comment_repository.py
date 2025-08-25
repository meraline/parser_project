"""Репозиторий для работы с комментариями в базе данных."""

import sqlite3
from typing import Dict, List, Optional, Tuple

from ..base import Database
from ...models.comment import Comment
from ...utils.logger import get_logger

logger = get_logger(__name__)


class CommentRepository:
    """CRUD-операции для работы с комментариями в БД."""

    def __init__(self, db: Database):
        self.db = db
        self._init_table()

    def _init_table(self) -> None:
        """Инициализация таблицы комментариев и индексов."""
        with self.db.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    отзыв_id INTEGER NOT NULL,
                    автор TEXT NOT NULL,
                    содержание TEXT NOT NULL,
                    рейтинг REAL,
                    дата_публикации DATETIME,
                    количество_лайков INTEGER DEFAULT 0,
                    количество_дизлайков INTEGER DEFAULT 0,
                    источник_url TEXT,
                    дата_парсинга DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (отзыв_id) REFERENCES reviews (id) ON DELETE CASCADE
                )
                """
            )
            # Создаем индексы для быстрого поиска
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_comments_review_id ON comments(отзыв_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_comments_author ON comments(автор)"
            )

    def _comment_to_tuple(self, comment: Comment) -> Tuple:
        """Преобразует объект Comment в кортеж для вставки в БД."""
        return (
            comment.review_id,
            comment.author,
            comment.content,
            comment.rating,
            comment.publish_date,
            comment.likes_count,
            comment.dislikes_count,
            comment.source_url,
            comment.parsed_at,
        )

    def save(self, comment: Comment) -> bool:
        """Сохраняет комментарий в базу данных.

        Args:
            comment: Комментарий для сохранения

        Returns:
            bool: True если комментарий успешно сохранен
        """
        try:
            with self.db.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO comments (
                        отзыв_id, автор, содержание, рейтинг,
                        дата_публикации, количество_лайков,
                        количество_дизлайков, источник_url, дата_парсинга
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    self._comment_to_tuple(comment),
                )
                return cursor.rowcount > 0
        except sqlite3.Error as e:
            logger.error(f"Ошибка сохранения комментария: {e}")
            return False

    def save_batch(self, comments: List[Comment]) -> int:
        """Пакетное сохранение комментариев.

        Args:
            comments: Список комментариев для сохранения

        Returns:
            int: Количество успешно сохраненных комментариев
        """
        if not comments:
            return 0

        params = [self._comment_to_tuple(comment) for comment in comments]

        try:
            with self.db.transaction() as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    """
                    INSERT INTO comments (
                        отзыв_id, автор, содержание, рейтинг,
                        дата_публикации, количество_лайков,
                        количество_дизлайков, источник_url, дата_парсинга
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    params,
                )
                return cursor.rowcount
        except sqlite3.Error as e:
            logger.error(f"Ошибка пакетного сохранения комментариев: {e}")
            return 0

    def get_comments_by_review_id(self, review_id: int) -> List[Comment]:
        """Получает все комментарии для конкретного отзыва.

        Args:
            review_id: ID отзыва

        Returns:
            Список комментариев
        """
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, отзыв_id, автор, содержание, рейтинг,
                       дата_публикации, количество_лайков,
                       количество_дизлайков, источник_url, дата_парсинга
                FROM comments
                WHERE отзыв_id = ?
                ORDER BY дата_публикации ASC
                """,
                (review_id,),
            )

            comments = []
            for row in cursor.fetchall():
                comment = Comment(
                    id=row[0],
                    review_id=row[1],
                    author=row[2],
                    content=row[3],
                    rating=row[4],
                    publish_date=row[5],
                    likes_count=row[6] or 0,
                    dislikes_count=row[7] or 0,
                    source_url=row[8] or "",
                    parsed_at=row[9],
                )
                comments.append(comment)

            return comments

    def get_comments_count(self, review_id: Optional[int] = None) -> int:
        """Получает количество комментариев.

        Args:
            review_id: ID отзыва для фильтрации (опционально)

        Returns:
            Количество комментариев
        """
        with self.db.connection() as conn:
            cursor = conn.cursor()
            if review_id:
                cursor.execute(
                    "SELECT COUNT(*) FROM comments WHERE отзыв_id = ?", (review_id,)
                )
            else:
                cursor.execute("SELECT COUNT(*) FROM comments")
            return cursor.fetchone()[0]

    def delete_comments_by_review_id(self, review_id: int) -> int:
        """Удаляет все комментарии для конкретного отзыва.

        Args:
            review_id: ID отзыва

        Returns:
            Количество удаленных комментариев
        """
        try:
            with self.db.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM comments WHERE отзыв_id = ?", (review_id,))
                return cursor.rowcount
        except sqlite3.Error as e:
            logger.error(f"Ошибка удаления комментариев: {e}")
            return 0

    def stats(self) -> Dict:
        """Получает статистику по комментариям.

        Returns:
            Dict: Словарь со статистикой
        """
        try:
            with self.db.connection() as conn:
                cursor = conn.cursor()

                # Общее количество комментариев
                cursor.execute("SELECT COUNT(*) FROM comments")
                total_comments = cursor.fetchone()[0]

                # Количество уникальных авторов
                cursor.execute("SELECT COUNT(DISTINCT автор) FROM comments")
                unique_authors = cursor.fetchone()[0]

                # Средняя длина комментария
                cursor.execute("SELECT AVG(LENGTH(содержание)) FROM comments")
                avg_length = cursor.fetchone()[0] or 0

                # Топ авторов по количеству комментариев
                cursor.execute(
                    """
                    SELECT автор, COUNT(*) as count
                    FROM comments
                    GROUP BY автор
                    ORDER BY count DESC
                    LIMIT 10
                    """
                )
                top_authors = dict(cursor.fetchall())

                return {
                    "total_comments": total_comments,
                    "unique_authors": unique_authors,
                    "avg_comment_length": round(avg_length, 1),
                    "top_authors": top_authors,
                }

        except sqlite3.Error as e:
            logger.error(f"Ошибка получения статистики комментариев: {e}")
            return {
                "total_comments": 0,
                "unique_authors": 0,
                "avg_comment_length": 0,
                "top_authors": {},
            }
