#!/usr/bin/env python3
"""
Тестируем парсинг комментариев к отзывам
"""

import os
import sys
from datetime import datetime

# Добавляем корневую папку проекта в путь
project_root = "/home/analityk/Документы/projects/parser_project"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

from src.auto_reviews_parser.parsers.drom import DromParser
from src.auto_reviews_parser.database.repositories.review_repository import (
    ReviewRepository,
)
from src.auto_reviews_parser.database.repositories.comment_repository import (
    CommentRepository,
)
from src.auto_reviews_parser.database.base import Database
from src.auto_reviews_parser.models.comment import Comment


def test_comments_parsing():
    """Тестируем парсинг комментариев"""

    print("🔍 Тестируем парсинг комментариев к отзывам...")
    print("=" * 60)

    # Тестовые URL отзывов с комментариями
    test_urls = [
        "https://www.drom.ru/reviews/toyota/camry/1440434/",  # Михаил
        "https://www.drom.ru/reviews/toyota/camry/1344577/",  # Другой отзыв
    ]

    parser = DromParser(gentle_mode=True)

    # Инициализируем базу данных
    db = Database("auto_reviews.db")
    review_repo = ReviewRepository(db)
    comment_repo = CommentRepository(db)

    for url in test_urls:
        print(f"\n🎯 Тестируем отзыв: {url}")
        print("-" * 50)

        try:
            # Сначала проверим, есть ли отзыв в базе
            reviews = parser.parse_single_review(url)
            if not reviews:
                print("❌ Не удалось спарсить отзыв")
                continue

            review = reviews[0]
            print(f"✅ Отзыв спарсен: {review.author}")

            # Сохраняем отзыв в базу (если еще нет)
            try:
                review_repo.save(review)
                print("💾 Отзыв сохранен в базу")
            except Exception as e:
                if "UNIQUE constraint failed" in str(e):
                    print("⚠️  Отзыв уже существует в базе")
                else:
                    print(f"❌ Ошибка сохранения отзыва: {e}")
                    continue

            # Парсим комментарии
            print("📝 Парсим комментарии...")
            comments_data = parser.parse_comments(url)

            if not comments_data:
                print("❌ Комментарии не найдены")
                continue

            print(f"✅ Найдено {len(comments_data)} комментариев")

            # Получаем ID отзыва из базы
            # Пока для теста используем заглушку
            review_id = 1  # Это нужно получать из базы по URL

            # Создаем объекты Comment и сохраняем
            saved_count = 0
            for comment_data in comments_data:
                if not comment_data.get("content") or not comment_data.get("author"):
                    continue

                comment = Comment(
                    review_id=review_id,
                    author=comment_data["author"],
                    content=comment_data["content"],
                    likes_count=comment_data.get("likes_count", 0),
                    source_url=url,
                    parsed_at=datetime.now(),
                )

                try:
                    comment_repo.save(comment)
                    saved_count += 1
                    print(f"  💬 Комментарий сохранен: {comment.author[:20]}...")
                except Exception as e:
                    print(f"  ❌ Ошибка сохранения комментария: {e}")

            print(f"💾 Сохранено комментариев: {saved_count}/{len(comments_data)}")

            # Показываем примеры комментариев
            print(f"\n📄 Примеры комментариев:")
            for i, comment_data in enumerate(comments_data[:3], 1):
                if comment_data.get("content"):
                    print(
                        f"   {i}. {comment_data.get('author', 'Аноним')}: {comment_data['content'][:100]}..."
                    )

        except Exception as e:
            print(f"❌ Ошибка обработки отзыва: {e}")
            import traceback

            traceback.print_exc()

    # Показываем итоговую статистику
    print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
    print("=" * 60)

    try:
        comment_stats = comment_repo.stats()
        print(f"💬 Всего комментариев в базе: {comment_stats['total_comments']}")
        print(f"👥 Уникальных авторов: {comment_stats['unique_authors']}")
        print(
            f"📏 Средняя длина комментария: {comment_stats['avg_comment_length']} символов"
        )

        if comment_stats["top_authors"]:
            print(f"🏆 Топ авторов комментариев:")
            for author, count in list(comment_stats["top_authors"].items())[:5]:
                print(f"   {author}: {count} комментариев")

    except Exception as e:
        print(f"❌ Ошибка получения статистики: {e}")


if __name__ == "__main__":
    test_comments_parsing()
