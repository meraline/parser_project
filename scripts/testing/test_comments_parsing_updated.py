#!/usr/bin/env python3
"""
Тестовый скрипт для проверки обновленного парсинга комментариев.
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


def test_comment_parsing():
    """Тестирует парсинг комментариев с реальной страницы."""

    # URL отзыва с комментариями (тот, который вы показали)
    test_url = "https://www.drom.ru/reviews/moskvitch/3/1456282/"

    print(f"Тестирование парсинга комментариев...")
    print(f"URL: {test_url}")
    print("-" * 80)

    # Инициализируем парсер
    parser = DromParser()

    # Парсим комментарии
    print("Парсинг комментариев...")
    comments_data = parser.parse_comments(test_url)

    print(f"\nПолучено комментариев: {len(comments_data)}")

    if comments_data:
        print("\nПримеры комментариев:")
        print("=" * 80)

        # Показываем первые 3 комментария
        for i, comment_data in enumerate(comments_data[:3], 1):
            print(f"\nКомментарий {i}:")
            print(f"  Автор: {comment_data.get('author', 'Не указан')}")
            print(f"  Дата: {comment_data.get('publish_date', 'Не указана')}")
            print(f"  Лайки: {comment_data.get('likes_count', 0)}")
            print(f"  Дизлайки: {comment_data.get('dislikes_count', 0)}")

            content = comment_data.get("content", "")
            if len(content) > 100:
                content = content[:100] + "..."
            print(f"  Содержание: {content}")
            print("-" * 40)

    return comments_data


def test_database_integration():
    """Тестирует интеграцию с базой данных."""

    print("\n" + "=" * 80)
    print("ТЕСТИРОВАНИЕ ИНТЕГРАЦИИ С БАЗОЙ ДАННЫХ")
    print("=" * 80)

    # Инициализируем БД и репозиторий
    db = Database("test_comments.db")
    comment_repo = CommentRepository(db)

    # Создаем тестовый комментарий
    test_comment = Comment(
        review_id=1,
        author="Тестовый автор",
        content="Это тестовый комментарий для проверки работы с БД",
        likes_count=5,
        dislikes_count=2,
        source_url="https://test.url",
        parsed_at=datetime.now(),
    )

    print("Сохранение тестового комментария...")
    success = comment_repo.save(test_comment)
    print(f"Результат сохранения: {'✓ Успешно' if success else '✗ Ошибка'}")

    # Получаем комментарии по review_id
    print("\nПолучение комментариев из БД...")
    saved_comments = comment_repo.get_comments_by_review_id(1)
    print(f"Найдено комментариев: {len(saved_comments)}")

    if saved_comments:
        comment = saved_comments[0]
        print(f"\nТестовый комментарий:")
        print(f"  ID: {comment.id}")
        print(f"  Автор: {comment.author}")
        print(f"  Лайки: {comment.likes_count}")
        print(f"  Дизлайки: {comment.dislikes_count}")
        print(f"  Содержание: {comment.content}")

    # Получаем статистику
    stats = comment_repo.stats()
    print(f"\nСтатистика БД:")
    print(f"  Всего комментариев: {stats.get('total_comments', 0)}")

    return True


def test_real_comments_with_db():
    """Тестирует полный цикл: парсинг + сохранение в БД."""

    print("\n" + "=" * 80)
    print("ТЕСТИРОВАНИЕ ПОЛНОГО ЦИКЛА")
    print("=" * 80)

    # Парсим реальные комментарии
    test_url = "https://www.drom.ru/reviews/moskvitch/3/1456282/"
    parser = DromParser()

    print("Парсинг реальных комментариев...")
    comments_data = parser.parse_comments(test_url)

    if not comments_data:
        print("Комментарии не найдены. Завершение теста.")
        return False

    print(f"Получено {len(comments_data)} комментариев")

    # Преобразуем в объекты Comment
    db = Database("test_real_comments.db")
    comment_repo = CommentRepository(db)

    comments = []
    for comment_data in comments_data:
        comment = Comment(
            review_id=999,  # Тестовый ID отзыва
            author=comment_data.get("author", "Неизвестен"),
            content=comment_data.get("content", ""),
            likes_count=comment_data.get("likes_count", 0),
            dislikes_count=comment_data.get("dislikes_count", 0),
            source_url=comment_data.get("source_url", ""),
            parsed_at=datetime.now(),
        )
        comments.append(comment)

    # Сохраняем пакетом
    print("Сохранение комментариев в БД...")
    saved_count = comment_repo.save_batch(comments)
    print(f"Сохранено комментариев: {saved_count}")

    # Проверяем сохранение
    saved_comments = comment_repo.get_comments_by_review_id(999)
    print(f"Проверка: найдено в БД {len(saved_comments)} комментариев")

    return True


if __name__ == "__main__":
    try:
        print("ЗАПУСК ТЕСТОВ ПАРСИНГА КОММЕНТАРИЕВ")
        print("=" * 80)

        # Тест 1: Парсинг комментариев
        comments_data = test_comment_parsing()

        # Тест 2: Работа с БД
        test_database_integration()

        # Тест 3: Полный цикл
        test_real_comments_with_db()

        print("\n" + "=" * 80)
        print("ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ")
        print("=" * 80)

    except KeyboardInterrupt:
        print("\n\nТестирование прервано пользователем")
    except Exception as e:
        print(f"\n\nОшибка во время тестирования: {e}")
        import traceback

        traceback.print_exc()
