"""Расширенная модель отзыва с дополнительными данными для ML."""

from dataclasses import dataclass, field
from typing import Dict, Optional
from .review import Review


@dataclass
class ExtendedReview:
    """Расширенная модель отзыва с дополнительными данными."""

    # Основные данные отзыва
    review: Review

    # Рейтинги и оценки
    user_rating: Optional[float] = None  # Рейтинг от пользователей сайта
    owner_rating: Optional[float] = None  # Оценка владельца автомобиля

    # Детальные оценки владельца
    exterior_rating: Optional[int] = None  # Внешний вид
    interior_rating: Optional[int] = None  # Салон
    engine_rating: Optional[int] = None  # Двигатель
    driving_rating: Optional[int] = None  # Ходовые качества

    # Технические характеристики автомобиля
    car_specs: Dict[str, str] = field(default_factory=dict)

    # Статистика отзыва
    user_votes_count: Optional[int] = None  # Количество голосов пользователей
    five_star_votes: Optional[int] = None  # Количество пятёрок

    def to_dict(self) -> Dict:
        """Преобразует в словарь для анализа данных."""
        return {
            # Основные данные
            "url": self.review.url,
            "brand": self.review.brand,
            "model": self.review.model,
            "year": self.review.year,
            "author": self.review.author,
            "content": self.review.content,
            "publish_date": self.review.publish_date,
            # Рейтинги
            "user_rating": self.user_rating,
            "owner_rating": self.owner_rating,
            "exterior_rating": self.exterior_rating,
            "interior_rating": self.interior_rating,
            "engine_rating": self.engine_rating,
            "driving_rating": self.driving_rating,
            # Статистика
            "views_count": self.review.views_count,
            "comments_count": self.review.comments_count,
            "likes_count": self.review.likes_count,
            "user_votes_count": self.user_votes_count,
            "five_star_votes": self.five_star_votes,
            # Технические характеристики
            **self.car_specs,
        }

    def get_ml_features(self) -> Dict:
        """Возвращает признаки для машинного обучения."""
        features = {
            "content_length": len(self.review.content) if self.review.content else 0,
            "views_count": self.review.views_count or 0,
            "comments_count": self.review.comments_count or 0,
            "likes_count": self.review.likes_count or 0,
            "user_rating": self.user_rating or 0,
            "owner_rating": self.owner_rating or 0,
            "exterior_rating": self.exterior_rating or 0,
            "interior_rating": self.interior_rating or 0,
            "engine_rating": self.engine_rating or 0,
            "driving_rating": self.driving_rating or 0,
            "has_year": 1 if self.review.year else 0,
            "has_mileage": 1 if self.review.mileage else 0,
        }

        # Добавляем средний рейтинг по категориям
        ratings = [
            self.exterior_rating,
            self.interior_rating,
            self.engine_rating,
            self.driving_rating,
        ]
        valid_ratings = [r for r in ratings if r is not None and r > 0]
        features["avg_category_rating"] = (
            sum(valid_ratings) / len(valid_ratings) if valid_ratings else 0
        )

        return features
