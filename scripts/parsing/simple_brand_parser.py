#!/usr/bin/env python3
"""
Полный парсинг всех отзывов по брендам (упрощенная версия)
- Все отзывы бренда → следующий бренд
- 30 отзывов для теста
- Сохранение каждые 10 отзывов
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


class SimpleBrandParser:
    """Упрощенный парсер для полного извлечения отзывов по брендам."""

    def __init__(self):
        self.brands = ["toyota", "honda", "bmw", "audi", "mercedes-benz", "volkswagen"]
        self.parser = DromParser()
        self.db_path = "data/databases/brand_test.db"

        # Создаем директорию если нужно
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        self.stats = {
            "start_time": time.time(),
            "total_parsed": 0,
            "successful_parsed": 0,
            "current_brand": "",
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
                    brand TEXT NOT NULL,
                    model TEXT,
                    year INTEGER,
                    rating REAL,
                    title TEXT,
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

    def get_brand_review_urls(self, brand: str, limit: int = 20) -> List[str]:
        """Получение URL отзывов для бренда (упрощенно)."""
        print(f"   🔍 Поиск отзывов для: {brand}")

        # Упрощенная логика - берем популярные модели
        popular_models = {
            "toyota": ["camry", "corolla", "rav4"],
            "honda": ["civic", "accord", "cr-v"],
            "bmw": ["3-series", "5-series", "x3"],
            "audi": ["a4", "a6", "q5"],
            "mercedes-benz": ["c-class", "e-class", "glc"],
            "volkswagen": ["golf", "passat", "tiguan"],
        }

        models = popular_models.get(brand, [brand])
        all_urls = []

        for model in models:
            try:
                # Получаем отзывы для модели
                urls = self.parser.get_model_review_urls(brand, model)
                all_urls.extend(urls[: limit // len(models)])
                print(f"     📄 {model}: {len(urls)} найдено")
                time.sleep(1)
            except Exception as e:
                print(f"     ❌ Ошибка {model}: {e}")

        print(f"   📊 Всего URL для {brand}: {len(all_urls)}")
        return all_urls

    def parse_review(self, url: str) -> dict:
        """Парсинг одного отзыва."""
        try:
            result = self.parser.parse_review_page(url)
            if result and result.get("review"):
                return {"success": True, "review": result["review"], "url": url}
            else:
                return {"success": False, "error": "Нет данных", "url": url}
        except Exception as e:
            return {"success": False, "error": str(e), "url": url}

    def save_reviews(self, reviews: List[dict]):
        """Сохранение отзывов в БД."""
        print(f"   💾 Сохраняем {len(reviews)} отзывов...")

        with sqlite3.connect(self.db_path) as conn:
            for review_data in reviews:
                if review_data["success"]:
                    review = review_data["review"]
                    try:
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO reviews 
                            (url, brand, model, year, rating, title, 
                             content, author)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                            (
                                review_data["url"],
                                review.brand,
                                review.model,
                                review.year,
                                review.rating,
                                review.title,
                                review.content,
                                review.author,
                            ),
                        )
                    except Exception as e:
                        print(f"     ❌ Ошибка сохранения: {e}")
            conn.commit()

        print("   ✅ Сохранено в БД")

    def start_parsing(self, target_reviews: int = 30):
        """Запуск полного парсинга по брендам."""
        print("\n🎯 ПОЛНЫЙ ПАРСИНГ ПО БРЕНДАМ")
        print("=" * 50)
        print(f"📊 Цель: {target_reviews} отзывов")
        print("🏷️ Логика: ВСЕ отзывы бренда → следующий")
        print("💾 Сохранение каждые 10 отзывов")
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

                # Получаем URL отзывов бренда
                brand_urls = self.get_brand_review_urls(brand)

                # Фильтруем дубликаты
                new_urls = [url for url in brand_urls if url not in existing_urls]

                print(f"   📋 Новых URL: {len(new_urls)}")
                print(f"   ⚠️ Дубликатов: {len(brand_urls) - len(new_urls)}")

                brand_count = 0

                for url in new_urls:
                    if self.stats["total_parsed"] >= target_reviews:
                        print("   🎯 Достигнута цель!")
                        break

                    print(
                        f"   📄 Парсинг {self.stats['total_parsed'] + 1}/"
                        f"{target_reviews}: {url}"
                    )

                    result = self.parse_review(url)
                    batch.append(result)

                    if result["success"]:
                        existing_urls.add(url)
                        brand_count += 1
                        self.stats["total_parsed"] += 1
                        self.stats["successful_parsed"] += 1

                        review = result["review"]
                        print(
                            f"   ✅ {review.brand} {review.model} " f"({review.year})"
                        )
                    else:
                        print(f"   ❌ Ошибка: {result['error'][:30]}...")

                    # Сохраняем каждые 10
                    if len(batch) >= 10:
                        self.save_reviews(batch)
                        batch = []

                    time.sleep(2)  # Пауза между запросами

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
        print(f"⏱️ Время: {elapsed:.1f} сек")
        print(f"✅ Успешно: {self.stats['successful_parsed']}")
        print(f"📝 Всего: {self.stats['total_parsed']}")
        print(f"🏷️ Текущий бренд: {self.stats['current_brand']}")

        if elapsed > 0:
            speed = self.stats["total_parsed"] / elapsed * 3600
            print(f"📈 Скорость: {speed:.0f} отзывов/час")

        print(f"💾 БД: {self.db_path}")
        print("=" * 50)


def main():
    """Точка входа."""
    print("🚀 УПРОЩЕННЫЙ ПОЛНЫЙ ПАРСИНГ ПО БРЕНДАМ")
    print("Логика: ВСЕ отзывы бренда → следующий бренд")

    parser = SimpleBrandParser()
    parser.start_parsing(target_reviews=30)


if __name__ == "__main__":
    main()
