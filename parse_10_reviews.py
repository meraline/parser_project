#!/usr/bin/env python3
"""
Запуск парсера на первые 10 отзывов Toyota Camry с сохранением в базу
"""

import os
import sys
from typing import List

# Добавляем корневую папку проекта в путь
project_root = "/home/analityk/Документы/projects/parser_project"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

from src.auto_reviews_parser.parsers.drom import DromParser
from src.auto_reviews_parser.database.repositories.review_repository import (
    ReviewRepository,
)
from src.auto_reviews_parser.database.base import Database


def parse_first_10_reviews():
    """Парсим первые 10 отзывов Toyota Camry и сохраняем в базу"""

    print("🚀 Запускаем парсинг первых 10 отзывов Toyota Camry...")
    print("=" * 60)

    # Инициализируем парсер в щадящем режиме
    parser = DromParser(gentle_mode=True)

    # Инициализируем базу данных и репозиторий
    db = Database("auto_reviews.db")
    repository = ReviewRepository(db)

    try:
        # Парсим отзывы (max_reviews=10 для ограничения)
        print("📡 Начинаем парсинг отзывов...")
        reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=10)

        if not reviews:
            print("❌ Отзывы не найдены")
            return

        print(f"✅ Найдено {len(reviews)} отзывов")

        # Сохраняем все найденные отзывы
        reviews_to_save = reviews
        print(f"💾 Сохраняем {len(reviews_to_save)} отзывов в базу...")

        saved_count = 0
        skipped_count = 0
        error_count = 0

        for i, review in enumerate(reviews_to_save, 1):
            print(f"\n📝 Обрабатываем отзыв {i}/{len(reviews_to_save)}")
            print(f"   URL: {review.url}")
            print(f"   Автор: {review.author}")
            print(f"   Общий рейтинг: {review.overall_rating}")
            print(f"   Рейтинги по категориям:")
            print(f"     - Внешний вид: {review.exterior_rating}")
            print(f"     - Салон: {review.interior_rating}")
            print(f"     - Двигатель: {review.engine_rating}")
            print(f"     - Ходовые качества: {review.driving_rating}")

            try:
                # Пытаемся сохранить отзыв
                repository.save(review)
                saved_count += 1
                print(f"   ✅ Сохранен в базу")

            except Exception as e:
                if "UNIQUE constraint failed" in str(e):
                    skipped_count += 1
                    print(f"   ⚠️  Пропущен (уже существует)")
                else:
                    error_count += 1
                    print(f"   ❌ Ошибка сохранения: {e}")

        print("\n" + "=" * 60)
        print("📊 ИТОГИ ПАРСИНГА:")
        print(f"   💾 Сохранено новых отзывов: {saved_count}")
        print(f"   ⚠️  Пропущено дублей: {skipped_count}")
        print(f"   ❌ Ошибок: {error_count}")
        print(f"   📋 Всего обработано: {len(reviews_to_save)}")

        # Показываем статистику из базы
        print(f"\n📈 СТАТИСТИКА ИЗ БАЗЫ:")
        try:
            stats = repository.stats()
            total_reviews = stats.get("total_reviews", 0)
            print(f"   📋 Всего отзывов в базе: {total_reviews}")

            # Получаем количество отзывов Toyota Camry
            camry_count = repository.get_reviews_count("toyota", "camry")
            print(f"   🚗 Отзывов Toyota Camry: {camry_count}")
        except Exception as e:
            print(f"   ❌ Ошибка получения статистики: {e}")

    except Exception as e:
        print(f"❌ Критическая ошибка парсинга: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    parse_first_10_reviews()
