#!/usr/bin/env python3
"""
Полный парсинг отзывов по брендам с использованием реальных методов DromParser
"""

import time
import sqlite3
import traceback
from typing import Set, List
from pathlib import Path

# Добавляем путь к проекту
import sys

project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.auto_reviews_parser.parsers.drom import DromParser


class RealBrandParser:
    """Реальный парсер для полного извлечения отзывов по брендам."""

    def __init__(self):
        self.brands = ["toyota", "honda", "bmw", "audi", "mercedes-benz", "volkswagen"]
        self.parser = DromParser(gentle_mode=True)
        self.db_path = "data/databases/real_brand_test.db"

        # Создаем директорию если нужно
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        self.stats = {
            "start_time": time.time(),
            "total_parsed": 0,
            "successful_parsed": 0,
            "current_brand": "",
            "current_model": "",
        }

        self._init_database()

    def _init_database(self):
        """Создание таблицы БД."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    source TEXT NOT NULL,
                    brand TEXT NOT NULL,
                    model TEXT,
                    year INTEGER,
                    rating REAL,
                    content TEXT,
                    author TEXT,
                    date_parsed TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
            conn.commit()

    def get_existing_urls(self) -> Set[str]:
        """Получение уже спарсенных URL."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT url FROM reviews")
            return {row[0] for row in cursor.fetchall()}

    def get_brand_models(self, brand: str) -> List[str]:
        """Получение популярных моделей бренда."""
        models_map = {
            "toyota": ["camry", "corolla", "rav4"],
            "honda": ["civic", "accord", "cr-v"],
            "bmw": ["3-series", "5-series", "x5"],
            "audi": ["a4", "a6", "q5"],
            "mercedes-benz": ["c-class", "e-class", "glc"],
            "volkswagen": ["golf", "passat", "tiguan"],
        }
        return models_map.get(brand, [brand])

    def parse_brand_reviews(self, brand: str, limit: int = 10) -> List[dict]:
        """Парсинг отзывов для всех моделей бренда."""
        print(f"   🔍 Парсинг всех моделей бренда: {brand}")

        all_reviews = []
        models = self.get_brand_models(brand)

        for model in models:
            if len(all_reviews) >= limit:
                break

            self.stats["current_model"] = model
            print(f"     📄 Модель: {model}")

            try:
                # Используем реальный метод DromParser
                model_reviews = self.parser.parse_reviews(brand, model)

                # Конвертируем Review объекты в словари
                for review in model_reviews:
                    if len(all_reviews) >= limit:
                        break

                    review_dict = {
                        "url": review.url,
                        "source": review.source,
                        "brand": review.brand,
                        "model": review.model,
                        "year": review.year,
                        "rating": review.rating,
                        "content": review.content,
                        "author": review.author,
                        "success": True,
                    }
                    all_reviews.append(review_dict)

                print(f"       ✅ Получено: {len(model_reviews)} отзывов")

            except Exception as e:
                print(f"       ❌ Ошибка для {model}: {e}")

            time.sleep(2)  # Пауза между моделями

        print(f"   📊 Всего для {brand}: {len(all_reviews)} отзывов")
        return all_reviews

    def save_reviews(self, reviews: List[dict]):
        """Сохранение отзывов в БД."""
        if not reviews:
            return

        print(f"   💾 Сохраняем {len(reviews)} отзывов...")

        saved_count = 0
        with sqlite3.connect(self.db_path) as conn:
            for review_data in reviews:
                if review_data.get("success"):
                    try:
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO reviews 
                            (url, source, brand, model, year, rating, 
                             content, author)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                            (
                                review_data["url"],
                                review_data["source"],
                                review_data["brand"],
                                review_data["model"],
                                review_data["year"],
                                review_data["rating"],
                                review_data["content"],
                                review_data["author"],
                            ),
                        )
                        saved_count += 1
                    except Exception as e:
                        print(f"     ❌ Ошибка сохранения: {e}")
            conn.commit()

        print(f"   ✅ Сохранено в БД: {saved_count}")

    def start_parsing(self, target_reviews: int = 30):
        """Запуск полного парсинга по брендам."""
        print("\n🎯 ПОЛНЫЙ ПАРСИНГ ПО БРЕНДАМ (РЕАЛЬНЫЙ)")
        print("=" * 50)
        print(f"📊 Цель: {target_reviews} отзывов")
        print("🏷️ Логика: ВСЕ отзывы бренда → следующий")
        print("💾 Сохранение каждые 10 отзывов")
        print("🤖 Используем реальный DromParser")
        print("=" * 50)

        existing_urls = self.get_existing_urls()
        print(f"📋 В БД уже есть: {len(existing_urls)} URL")

        batch = []

        try:
            for brand in self.brands:
                if self.stats["total_parsed"] >= target_reviews:
                    break

                self.stats["current_brand"] = brand
                print(f"\n🏷️ ПАРСИНГ БРЕНДА: {brand.upper()}")

                # Получаем отзывы для всех моделей бренда
                remaining = target_reviews - self.stats["total_parsed"]
                brand_reviews = self.parse_brand_reviews(brand, min(remaining, 15))

                # Фильтруем дубликаты
                new_reviews = [
                    r for r in brand_reviews if r["url"] not in existing_urls
                ]

                print(f"   📋 Новых отзывов: {len(new_reviews)}")
                print(f"   ⚠️ Дубликатов: {len(brand_reviews) - len(new_reviews)}")

                brand_count = 0

                for review in new_reviews:
                    if self.stats["total_parsed"] >= target_reviews:
                        print("   🎯 Достигнута цель!")
                        break

                    batch.append(review)
                    existing_urls.add(review["url"])
                    brand_count += 1
                    self.stats["total_parsed"] += 1
                    self.stats["successful_parsed"] += 1

                    print(
                        f"   ✅ {review['brand']} {review['model']} "
                        f"({review['year']}) - {review['rating']}★"
                    )

                    # Сохраняем каждые 10
                    if len(batch) >= 10:
                        self.save_reviews(batch)
                        batch = []

                print(f"   📊 Бренд {brand} завершен: {brand_count} отзывов")
                time.sleep(3)  # Пауза между брендами

            # Сохраняем остатки
            if batch:
                self.save_reviews(batch)

            print("\n✅ ПОЛНЫЙ ПАРСИНГ ЗАВЕРШЕН!")

        except KeyboardInterrupt:
            print("\n⏹️ Прервано пользователем")
            if batch:
                self.save_reviews(batch)

        except Exception as e:
            print(f"\n💥 Ошибка: {e}")
            traceback.print_exc()
            if batch:
                self.save_reviews(batch)

        finally:
            self.print_stats()

    def print_stats(self):
        """Финальная статистика."""
        elapsed = time.time() - self.stats["start_time"]

        print("\n" + "=" * 50)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print("=" * 50)
        print(f"⏱️ Время: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")
        print(f"✅ Успешно: {self.stats['successful_parsed']}")
        print(f"📝 Всего: {self.stats['total_parsed']}")
        print(f"🏷️ Бренд: {self.stats['current_brand']}")
        print(f"🚗 Модель: {self.stats['current_model']}")

        if elapsed > 0:
            speed = self.stats["total_parsed"] / elapsed * 3600
            print(f"📈 Скорость: {speed:.0f} отзывов/час")

        print(f"💾 БД: {self.db_path}")
        print("=" * 50)


def main():
    """Точка входа."""
    print("🚀 РЕАЛЬНЫЙ ПОЛНЫЙ ПАРСИНГ ПО БРЕНДАМ")
    print("Используем настоящий DromParser.parse_reviews()")

    parser = RealBrandParser()
    parser.start_parsing(target_reviews=30)


if __name__ == "__main__":
    main()
