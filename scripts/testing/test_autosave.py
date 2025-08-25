#!/usr/bin/env python3
"""
ТЕСТОВЫЙ ПАРСЕР ДЛЯ ПРОВЕРКИ АВТОСОХРАНЕНИЯ
=============================================

Простая версия для проверки сохранения каждого отзыва.
"""

import time
import sqlite3
import traceback
import sys
from pathlib import Path
from datetime import datetime

# Добавляем системный путь
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.auto_reviews_parser.parsers.drom import DromParser


class AutoSaveParser:
    """Парсер с автосохранением каждого отзыва."""

    def __init__(self):
        self.parser = DromParser(gentle_mode=True)
        self.db_path = "data/databases/test_autosave.db"
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._create_simple_table()

    def _create_simple_table(self):
        """Создание простой таблицы для тестирования."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS simple_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    brand TEXT,
                    model TEXT, 
                    author TEXT,
                    rating REAL,
                    content TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
            conn.commit()

    def save_review(self, result):
        """Сохранение одного отзыва."""
        if result["status"] != "success":
            return False

        review = result["review"]

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO simple_reviews 
                    (url, brand, model, author, rating, content)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        result["url"],
                        review.brand,
                        review.model,
                        review.author,
                        review.rating,
                        review.content[:500],  # Ограничиваем длину
                    ),
                )
                conn.commit()
                print(
                    f"       💾 СОХРАНЕНО: {review.brand} {review.model} - ⭐{review.rating}"
                )
                return True
        except Exception as e:
            print(f"       ❌ Ошибка сохранения: {e}")
            return False

    def test_autosave(self, limit=5):
        """Тестирование автосохранения."""
        print("🧪 ТЕСТИРОВАНИЕ АВТОСОХРАНЕНИЯ КАЖДОГО ОТЗЫВА")
        print("=" * 50)

        # Тестовые URL для AC Cobra
        test_urls = [
            "https://www.drom.ru/reviews/ac/cobra/1442150/",
        ]

        # Получаем больше URL для AITO
        print("🔍 Получение URL для AITO M5...")
        try:
            models = self.parser.get_all_models_for_brand("aito")
            print(f"✅ Найдено моделей: {models}")

            if "m5" in models:
                print("📋 Парсим каталог AITO M5...")
                m5_reviews = self.parser.parse_catalog_model(
                    "aito", "m5", max_reviews=limit - 1
                )
                print(f"✅ Найдено {len(m5_reviews)} отзывов для AITO M5")

                # Преобразуем в список URL
                for review in m5_reviews[: limit - 1]:
                    if hasattr(review, "url") and review.url:
                        test_urls.append(review.url)

        except Exception as e:
            print(f"❌ Ошибка получения URL: {e}")

        print(f"📋 Будем тестировать {len(test_urls)} URL:")
        for i, url in enumerate(test_urls, 1):
            print(f"  {i}. {url}")

        print("\n" + "=" * 50)
        saved_count = 0

        for i, url in enumerate(test_urls, 1):
            print(f"\n📄 Парсинг {i}/{len(test_urls)}: {url}")

            try:
                reviews = self.parser.parse_single_review(url)

                if reviews and len(reviews) > 0:
                    review = reviews[0]  # Берем первый отзыв
                    print(
                        f"  ✅ Спарсено: {review.brand} {review.model} - ⭐{review.rating}"
                    )

                    # Создаем результат в нужном формате
                    result = {"status": "success", "url": url, "review": review}

                    # Автосохранение каждого успешного отзыва
                    if self.save_review(result):
                        saved_count += 1

                else:
                    print(f"  ❌ Отзыв не найден или пустой результат")

            except Exception as e:
                print(f"  ❌ Исключение: {str(e)[:100]}")

            # Небольшая пауза
            time.sleep(1)

        print("\n" + "=" * 50)
        print("📊 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ")
        print("=" * 50)
        print(f"✅ Успешно сохранено: {saved_count}")
        print(f"📋 Всего обработано: {len(test_urls)}")

        # Проверяем БД
        self._check_database()

    def _check_database(self):
        """Проверка содержимого БД."""
        print("\n💾 СОДЕРЖИМОЕ БАЗЫ ДАННЫХ:")
        print("-" * 30)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM simple_reviews")
                count = cursor.fetchone()[0]
                print(f"📊 Всего записей в БД: {count}")

                if count > 0:
                    cursor = conn.execute(
                        """
                        SELECT brand, model, author, rating, created_at 
                        FROM simple_reviews 
                        ORDER BY created_at DESC 
                        LIMIT 5
                    """
                    )

                    print("\n🔍 Последние записи:")
                    for row in cursor.fetchall():
                        brand, model, author, rating, created_at = row
                        print(f"  • {brand} {model} - ⭐{rating} ({created_at})")

        except Exception as e:
            print(f"❌ Ошибка проверки БД: {e}")


def main():
    """Главная функция."""
    try:
        parser = AutoSaveParser()
        parser.test_autosave(limit=5)

    except KeyboardInterrupt:
        print("\n⏹️ Тестирование прервано пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
