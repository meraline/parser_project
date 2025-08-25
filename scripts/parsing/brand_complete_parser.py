#!/usr/bin/env python3
"""
Полный парсинг всех отзывов по брендам (алфавитно)
- Получает ВСЕ отзывы бренда, затем следующий бренд
- Проверяет дубликаты через базу данных
- Сохраняет каждые 10 отзывов
- 30 отзывов для теста
"""

import time
import sqlite3
import traceback
from typing import Set, List
from pathlib import Path

# Добавляем системный путь
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.auto_reviews_parser.parsers.drom import DromParser
from src.auto_reviews_parser.database.base import DatabaseManager
from src.auto_reviews_parser.utils.delay_manager import DelayManager
from src.auto_reviews_parser.models.review import Review


class BrandCompleteParser:
    """Парсер для полного извлечения всех отзывов по брендам."""

    def __init__(self):
        self.brands = self._get_brands_alphabetical()
        self.parser = DromParser()
        self.db_manager = DatabaseManager("data/databases/brand_complete_test.db")
        self.delay_manager = DelayManager()

        self.stats = {
            "start_time": time.time(),
            "total_parsed": 0,
            "successful_parsed": 0,
            "comments_parsed": 0,
            "current_brand": "",
            "last_save_time": time.time(),
        }

        # Инициализация БД
        self._init_database()

    def _get_brands_alphabetical(self) -> List[str]:
        """Список брендов в алфавитном порядке."""
        brands = [
            "audi",
            "bmw",
            "chevrolet",
            "daewoo",
            "ford",
            "honda",
            "hyundai",
            "kia",
            "mazda",
            "mercedes-benz",
            "mitsubishi",
            "nissan",
            "opel",
            "peugeot",
            "renault",
            "skoda",
            "subaru",
            "suzuki",
            "toyota",
            "volkswagen",
            "volvo",
        ]
        return sorted(brands)

    def _init_database(self):
        """Инициализация таблиц БД."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS parsed_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    brand TEXT NOT NULL,
                    model TEXT NOT NULL,
                    year INTEGER,
                    rating REAL,
                    title TEXT,
                    content TEXT,
                    author TEXT,
                    date_published TEXT,
                    date_parsed TEXT DEFAULT CURRENT_TIMESTAMP,
                    comments_count INTEGER DEFAULT 0
                )
            """
            )
            conn.commit()

    def get_existing_urls(self) -> Set[str]:
        """Получение уже спарсенных URL из БД."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.execute("SELECT url FROM parsed_reviews")
            return {row[0] for row in cursor.fetchall()}

    def get_all_brand_review_urls(self, brand: str) -> List[str]:
        """Получение ВСЕХ URL отзывов для бренда."""
        print(f"   🔍 Поиск всех отзывов для бренда: {brand}")

        models = self._get_brand_models(brand)
        all_urls = []

        for model in models[:5]:  # Ограничиваем для теста
            try:
                model_urls = self.parser.get_model_review_urls(brand, model)
                all_urls.extend(model_urls)
                print(f"     📄 {model}: {len(model_urls)} отзывов")
                time.sleep(1)  # Пауза между моделями
            except Exception as e:
                print(f"     ❌ Ошибка для {model}: {e}")

        print(f"   📊 Всего URL для {brand}: {len(all_urls)}")
        return all_urls

    def _get_brand_models(self, brand: str) -> List[str]:
        """Получение списка моделей бренда."""
        models_map = {
            "toyota": ["camry", "corolla", "rav4", "land-cruiser", "prius"],
            "honda": ["civic", "accord", "cr-v", "pilot", "fit"],
            "bmw": ["3-series", "5-series", "x3", "x5", "x1"],
            "audi": ["a4", "a6", "q5", "q7", "a3"],
            "mercedes-benz": ["c-class", "e-class", "glc", "gle", "a-class"],
            "volkswagen": ["golf", "passat", "tiguan", "polo", "jetta"],
            "hyundai": ["elantra", "tucson", "santa-fe", "accent", "sonata"],
            "kia": ["rio", "cerato", "sportage", "sorento", "optima"],
            "mazda": ["3", "6", "cx-5", "cx-9", "2"],
            "nissan": ["qashqai", "x-trail", "juke", "almera", "teana"],
            "ford": ["focus", "fiesta", "kuga", "mondeo", "ecosport"],
            "chevrolet": ["cruze", "captiva", "aveo", "malibu", "tahoe"],
            "skoda": ["octavia", "rapid", "superb", "kodiaq", "fabia"],
            "renault": ["logan", "duster", "megane", "kaptur", "sandero"],
            "opel": ["astra", "corsa", "insignia", "mokka", "zafira"],
            "peugeot": ["308", "3008", "2008", "408", "5008"],
            "mitsubishi": ["outlander", "asx", "lancer", "pajero", "l200"],
            "subaru": ["forester", "outback", "impreza", "xv", "legacy"],
            "suzuki": ["vitara", "swift", "sx4", "jimny", "baleno"],
            "volvo": ["xc60", "xc90", "v60", "s60", "v40"],
            "daewoo": ["nexia", "matiz", "gentra", "lacetti", "espero"],
        }
        return models_map.get(brand, [brand])

    def parse_single_review(self, url: str) -> dict:
        """Парсинг одного отзыва."""
        try:
            review_data = self.parser.parse_review_page(url)
            if review_data and review_data.get("review"):
                return {
                    "status": "success",
                    "review": review_data["review"],
                    "comments": review_data.get("comments", []),
                    "url": url,
                }
            else:
                return {"status": "error", "error": "Нет данных отзыва", "url": url}
        except Exception as e:
            return {"status": "error", "error": str(e), "url": url}

    def save_batch(self, results_batch: List[dict]):
        """Сохранение пакета результатов в БД."""
        print(f"   💾 Сохранение {len(results_batch)} отзывов в БД...")

        with sqlite3.connect(self.db_manager.db_path) as conn:
            for result in results_batch:
                if result["status"] == "success":
                    review = result["review"]
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO parsed_reviews 
                        (url, brand, model, year, rating, title, content, 
                         author, date_published, comments_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            result["url"],
                            review.brand,
                            review.model,
                            review.year,
                            review.rating,
                            review.title,
                            review.content,
                            review.author,
                            review.date_published,
                            len(result.get("comments", [])),
                        ),
                    )
            conn.commit()

        self.stats["last_save_time"] = time.time()
        print(f"   ✅ Сохранено в БД")

    def start_parsing(self, target_reviews: int = 30):
        """Запуск полного парсинга по брендам."""
        self.stats["start_time"] = time.time()

        print("\n🎯 СТАРТ ПОЛНОГО ПАРСИНГА ПО БРЕНДАМ")
        print("=" * 50)
        print(f"📊 Тестовая цель: {target_reviews} отзывов")
        print("🏷️ Логика: ВСЕ отзывы бренда → следующий бренд")
        print("💾 Сохранение каждые 10 отзывов")
        print("🔍 Проверка дубликатов включена")
        print("=" * 50)

        # Получаем уже существующие URL
        existing_urls = self.get_existing_urls()
        print(f"📋 Уже в БД: {len(existing_urls)} URL")

        results_batch = []

        try:
            for brand in self.brands:
                if self.stats["total_parsed"] >= target_reviews:
                    break

                self.stats["current_brand"] = brand
                print(f"\n🏷️ ПОЛНЫЙ ПАРСИНГ БРЕНДА: {brand.upper()}")

                # Получаем ВСЕ URL отзывов для бренда
                brand_urls = self.get_all_brand_review_urls(brand)

                # Фильтруем дубликаты
                new_urls = [url for url in brand_urls if url not in existing_urls]

                print(f"   📋 Новых URL: {len(new_urls)}")
                print(f"   ⚠️ Дубликатов: {len(brand_urls) - len(new_urls)}")

                brand_parsed = 0

                for url in new_urls:
                    if self.stats["total_parsed"] >= target_reviews:
                        print(f"   🎯 Достигнута цель: {target_reviews}")
                        break

                    if (time.time() - self.stats["last_save_time"]) > 30:
                        print(
                            f"   📄 Прогресс: {self.stats['total_parsed'] + 1}"
                            f"/{target_reviews}"
                        )

                    # Парсим отзыв
                    result = self.parse_single_review(url)

                    if result["status"] == "success":
                        results_batch.append(result)
                        existing_urls.add(url)
                        brand_parsed += 1
                        self.stats["total_parsed"] += 1
                        self.stats["successful_parsed"] += 1
                        self.stats["comments_parsed"] += len(result.get("comments", []))

                        review = result["review"]
                        print(
                            f"   ✅ {review.brand} {review.model} " f"({review.year})"
                        )
                    else:
                        print(f"   ❌ Ошибка: {result.get('error', '?')[:50]}")

                    # Сохраняем каждые 10 отзывов
                    if len(results_batch) >= 10:
                        self.save_batch(results_batch)
                        results_batch = []

                    self.delay_manager.wait()

                print(f"   📊 Бренд {brand} завершен: {brand_parsed} отзывов")

            # Сохраняем оставшиеся
            if results_batch:
                self.save_batch(results_batch)

            print("\n✅ ПОЛНЫЙ ПАРСИНГ ЗАВЕРШЕН!")

        except KeyboardInterrupt:
            print("\n⏹️ Парсинг прерван пользователем")
            if results_batch:
                self.save_batch(results_batch)

        except Exception as e:
            print(f"\n💥 Критическая ошибка: {e}")
            traceback.print_exc()
            if results_batch:
                self.save_batch(results_batch)

        finally:
            self.print_final_stats()

    def print_final_stats(self):
        """Печать финальной статистики."""
        elapsed = time.time() - self.stats["start_time"]

        print("\n" + "=" * 50)
        print("📊 ФИНАЛЬНАЯ СТАТИСТИКА")
        print("=" * 50)
        print(f"⏱️ Время: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")
        print(f"✅ Успешно: {self.stats['successful_parsed']}")
        print(f"📝 Всего: {self.stats['total_parsed']}")
        print(f"💬 Комментариев: {self.stats['comments_parsed']}")
        print(f"🏷️ Текущий бренд: {self.stats['current_brand']}")

        if elapsed > 0:
            speed = self.stats["total_parsed"] / elapsed * 3600
            print(f"📈 Скорость: {speed:.0f} отзывов/час")

        print("💾 БД: data/databases/brand_complete_test.db")
        print("=" * 50)


def main():
    """Точка входа."""
    print("🚀 ПОЛНЫЙ ПАРСИНГ ОТЗЫВОВ ПО БРЕНДАМ")
    print("Логика: ВСЕ отзывы бренда → следующий бренд")

    parser = BrandCompleteParser()
    parser.start_parsing(target_reviews=30)


if __name__ == "__main__":
    main()
