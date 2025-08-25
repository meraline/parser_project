#!/usr/bin/env python3
"""
Тестирование парсера с полным сохранением характеристик
"""

import os
import sys

# Добавляем корневую папку проекта в путь
project_root = "/home/analityk/Документы/projects/parser_project"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

from src.auto_reviews_parser.parsers.drom import DromParser
from src.auto_reviews_parser.database.repositories.review_repository import (
    ReviewRepository,
)
from src.auto_reviews_parser.database.base import Database


def test_full_parsing():
    """Полный тест парсера с сохранением 10 отзывов"""

    print("🚀 Тестируем парсер на 10 отзывах с полным сохранением характеристик...")

    # Удаляем старую базу
    if os.path.exists("auto_reviews.db"):
        os.remove("auto_reviews.db")
        print("🗑️  Очистили старую базу данных")

    # Создаем парсер в щадящем режиме
    parser = DromParser(gentle_mode=True)
    db = Database("auto_reviews.db")
    repository = ReviewRepository(db)

    try:
        # Парсим 10 отзывов Toyota Camry
        reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=10)

        print(f"\n📄 Получено отзывов: {len(reviews)}")

        # Сохраняем в базу
        saved_count = 0
        for review in reviews:
            try:
                repository.save(review)
                saved_count += 1
            except Exception as e:
                if "UNIQUE constraint failed" in str(e):
                    print(f"⚠️  Отзыв {review.url} уже существует")
                else:
                    print(f"❌ Ошибка сохранения {review.url}: {e}")

        print(f"💾 Сохранено в базу: {saved_count} отзывов")

        # Проверяем статистику характеристик
        import sqlite3

        conn = sqlite3.connect("auto_reviews.db")
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM reviews")
        total = cursor.fetchone()[0]

        checks = [
            ("поколение", "Поколение"),
            ("год_выпуска", "Год выпуска"),
            ("тип_кузова", "Тип кузова"),
            ("трансмиссия", "Трансмиссия"),
            ("тип_привода", "Привод"),
            ("руль", "Руль"),
            ("пробег", "Пробег"),
            ("объем_двигателя", "Объем двигателя"),
            ("мощность_двигателя", "Мощность двигателя"),
            ("тип_топлива", "Тип топлива"),
            ("расход_в_городе", "Расход в городе"),
            ("расход_на_трассе", "Расход на трассе"),
            ("год_приобретения", "Год приобретения"),
            ("общая_оценка", "Общая оценка"),
            ("оценка_внешнего_вида", "Внешний вид"),
            ("оценка_салона", "Салон"),
            ("оценка_двигателя", "Двигатель (оценка)"),
            ("оценка_ходовых_качеств", "Ходовые качества"),
        ]

        print(f"\n📊 Статистика заполненности (из {total} отзывов):")
        for field, description in checks:
            cursor.execute(
                f"SELECT COUNT(*) FROM reviews WHERE {field} IS NOT NULL AND {field} != ''"
            )
            count = cursor.fetchone()[0]
            percentage = count / total * 100 if total > 0 else 0
            print(f"   {description:25s}: {count:2d}/{total} ({percentage:.0f}%)")

        # Показываем примеры с полными характеристиками
        cursor.execute(
            """
            SELECT автор, год_выпуска, тип_кузова, объем_двигателя, мощность_двигателя, тип_топлива, 
                   общая_оценка, оценка_внешнего_вида, пробег, поколение
            FROM reviews 
            WHERE объем_двигателя IS NOT NULL 
            ORDER BY дата_парсинга DESC 
            LIMIT 3
        """
        )

        examples = cursor.fetchall()
        if examples:
            print(f"\n🔍 Примеры отзывов с полными характеристиками:")
            for ex in examples:
                (
                    author,
                    year,
                    body,
                    vol,
                    power,
                    fuel,
                    rating,
                    ext_rating,
                    mileage,
                    gen,
                ) = ex
                print(
                    f"   {author}: {year} {body}, {vol}л {power}л.с. {fuel}, оценка {rating}, пробег {mileage}"
                )

        conn.close()

        print(f"\n✅ Тест завершен успешно!")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_full_parsing()
