#!/usr/bin/env python3
"""
Демонстрация интеграции парсинга комментариев в основной парсер.
"""

import sys
import os
from datetime import datetime

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.models.comment import Comment
from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.comment_repository import (
    CommentRepository,
)


def parse_review_with_comments():
    """Парсит отзыв и его комментарии, сохраняет в БД."""

    # URL отзыва для тестирования
    review_url = "https://www.drom.ru/reviews/moskvitch/3/1456282/"

    print("ДЕМОНСТРАЦИЯ ПАРСИНГА ОТЗЫВА С КОММЕНТАРИЯМИ")
    print("=" * 60)
    print(f"URL: {review_url}")
    print()

    # Инициализируем парсер и БД
    parser = DromParser()
    db = Database("demo_reviews_with_comments.db")
    comment_repo = CommentRepository(db)

    # 1. Парсим основной отзыв
    print("1. Парсинг основного отзыва...")
    reviews = parser.parse_single_review(review_url)

    if not reviews:
        print("   ❌ Отзыв не найден")
        return

    review = reviews[0]
    print(f"   ✓ Найден отзыв от {review.author}")
    print(f"   ✓ Марка: {review.brand}, Модель: {review.model}")
    print(f"   ✓ Общая оценка: {review.overall_rating}")
    print()

    # 2. Парсим комментарии к отзыву
    print("2. Парсинг комментариев к отзыву...")
    comments_data = parser.parse_comments(review_url)

    if not comments_data:
        print("   ❌ Комментарии не найдены")
        return

    print(f"   ✓ Найдено {len(comments_data)} комментариев")

    # 3. Преобразуем в объекты Comment
    print("3. Создание объектов комментариев...")
    comments = []
    review_id = 1  # В реальном приложении это будет ID из БД

    for comment_data in comments_data:
        comment = Comment(
            review_id=review_id,
            author=comment_data.get("author", "Неизвестен"),
            content=comment_data.get("content", ""),
            likes_count=comment_data.get("likes_count", 0),
            dislikes_count=comment_data.get("dislikes_count", 0),
            publish_date=comment_data.get("publish_date"),
            source_url=review_url,
            parsed_at=datetime.now(),
        )
        comments.append(comment)

    print(f"   ✓ Создано {len(comments)} объектов Comment")
    print()

    # 4. Сохраняем комментарии в БД
    print("4. Сохранение комментариев в БД...")
    saved_count = comment_repo.save_batch(comments)
    print(f"   ✓ Сохранено {saved_count} комментариев")
    print()

    # 5. Статистика и примеры
    print("5. Статистика и примеры:")
    stats = comment_repo.stats()
    print(f"   • Всего комментариев в БД: {stats.get('total_comments', 0)}")

    # Получаем комментарии для нашего отзыва
    saved_comments = comment_repo.get_comments_by_review_id(review_id)
    print(f"   • Комментариев к отзыву #{review_id}: {len(saved_comments)}")

    # Показываем топ-3 комментария по лайкам
    top_comments = sorted(saved_comments, key=lambda x: x.likes_count, reverse=True)[:3]

    print("\n   Топ-3 комментария по лайкам:")
    for i, comment in enumerate(top_comments, 1):
        print(
            f"   {i}. {comment.author} ({comment.likes_count} 👍 | "
            f"{comment.dislikes_count} 👎)"
        )
        content = (
            comment.content[:60] + "..."
            if len(comment.content) > 60
            else comment.content
        )
        print(f'      "{content}"')

    print()
    print("=" * 60)
    print("ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА")
    print("=" * 60)


if __name__ == "__main__":
    try:
        parse_review_with_comments()
    except KeyboardInterrupt:
        print("\nПрервано пользователем")
    except Exception as e:
        print(f"\nОшибка: {e}")
        import traceback

        traceback.print_exc()
