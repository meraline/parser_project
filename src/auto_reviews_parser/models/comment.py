"""Модель комментария к отзыву."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Comment:
    """Модель комментария к отзыву."""

    # Основные поля
    review_id: int  # ID отзыва, к которому относится комментарий
    author: str  # Автор комментария
    content: str  # Текст комментария

    # Опциональные поля
    rating: Optional[float] = None  # Рейтинг комментария (лайки/дизлайки)
    publish_date: Optional[datetime] = None  # Дата публикации
    likes_count: int = 0  # Количество лайков
    dislikes_count: int = 0  # Количество дизлайков

    # Метаданные
    source_url: str = ""  # URL отзыва, откуда взят комментарий
    parsed_at: Optional[datetime] = None  # Дата парсинга

    # Системные поля (заполняются автоматически)
    id: Optional[int] = None  # ID комментария в БД
