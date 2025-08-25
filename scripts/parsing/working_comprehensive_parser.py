#!/usr/bin/env python3
"""
ПОЛНЫЙ ПАРСЕР АВТО-ОТЗЫВОВ С АВТОСОХРАНЕНИЕМ
============================================

Парсинг отзывов по брендам в алфавитном порядке с автосохранением каждого отзыва.
"""

import time
import sqlite3
import traceback
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Добавляем системный путь
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.auto_reviews_parser.parsers.drom import DromParser


class ComprehensiveBrandParser:
    """Полнофункциональный парсер с автосохранением каждого отзыва."""

    def __init__(self, mode: str = "test"):
        """Инициализация парсера."""
        self.parser = DromParser(gentle_mode=True)

        # Пути к БД
        if mode == "production":
            self.db_path = "auto_reviews.db"
        else:
            self.db_path = "data/databases/test_reviews.db"

        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._create_comprehensive_table()

        # Статистика
        self.stats = {
            "start_time": time.time(),
            "total_parsed": 0,
            "successful_parsed": 0,
            "failed_parsed": 0,
            "saved_count": 0,
            "save_errors": 0,
        }

        # Конфигурация
        self.config = {
            "delay_between_requests": 2.0,
            "delay_between_brands": 10.0,
            "enable_comments": True,
        }

    def _create_comprehensive_table(self):
        """Создание таблицы для полных отзывов."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS comprehensive_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    source TEXT DEFAULT 'drom.ru',
                    brand TEXT,
                    model TEXT,
                    title TEXT,
                    content TEXT,
                    author TEXT,
                    author_city TEXT,
                    date_published TEXT,
                    overall_rating REAL,
                    owner_rating REAL,
                    views_count INTEGER,
                    exterior_rating INTEGER,
                    interior_rating INTEGER,
                    engine_rating INTEGER,
                    handling_rating INTEGER,
                    year INTEGER,
                    generation TEXT,
                    body_type TEXT,
                    transmission TEXT,
                    drive_type TEXT,
                    steering_wheel TEXT,
                    mileage TEXT,
                    engine_volume TEXT,
                    engine_power TEXT,
                    fuel_type TEXT,
                    fuel_consumption_city TEXT,
                    fuel_consumption_highway TEXT,
                    fuel_consumption_mixed TEXT,
                    purchase_year INTEGER,
                    color_exterior TEXT,
                    color_interior TEXT,
                    comments_count INTEGER DEFAULT 0,
                    review_type TEXT DEFAULT 'review',
                    parsing_status TEXT DEFAULT 'success',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Таблица для комментариев
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS review_comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    review_url TEXT,
                    author TEXT,
                    content TEXT,
                    date_published TEXT,
                    likes_count INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (review_url) REFERENCES comprehensive_reviews(url)
                )
            """
            )
            conn.commit()

    def save_review(self, result: Dict[str, Any]) -> bool:
        """Сохранение одного отзыва с полными данными."""
        if result["status"] != "success":
            self.stats["save_errors"] += 1
            return False

        review = result["review"]
        comments = result.get("comments", [])

        try:
            with sqlite3.connect(self.db_path) as conn:
                # Сохраняем основной отзыв
                conn.execute(
                    """
                    INSERT OR IGNORE INTO comprehensive_reviews 
                    (url, brand, model, title, content, author, author_city, 
                     date_published, overall_rating, owner_rating, views_count,
                     exterior_rating, interior_rating, engine_rating, handling_rating,
                     year, generation, body_type, transmission, drive_type, 
                     steering_wheel, mileage, engine_volume, engine_power, fuel_type,
                     fuel_consumption_city, fuel_consumption_highway, purchase_year,
                     comments_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        result["url"],
                        review.brand,
                        review.model,
                        getattr(review, "title", ""),
                        review.content,
                        review.author,
                        getattr(review, "author_city", ""),
                        getattr(review, "date_published", ""),
                        review.rating,
                        getattr(review, "owner_rating", None),
                        getattr(review, "views_count", 0),
                        getattr(review, "exterior_rating", None),
                        getattr(review, "interior_rating", None),
                        getattr(review, "engine_rating", None),
                        getattr(review, "handling_rating", None),
                        getattr(review, "year", None),
                        getattr(review, "generation", ""),
                        getattr(review, "body_type", ""),
                        getattr(review, "transmission", ""),
                        getattr(review, "drive_type", ""),
                        getattr(review, "steering_wheel", ""),
                        getattr(review, "mileage", ""),
                        getattr(review, "engine_volume", ""),
                        getattr(review, "engine_power", ""),
                        getattr(review, "fuel_type", ""),
                        getattr(review, "fuel_consumption_city", ""),
                        getattr(review, "fuel_consumption_highway", ""),
                        getattr(review, "purchase_year", None),
                        len(comments),
                    ),
                )

                # Сохраняем комментарии
                for comment in comments:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO review_comments 
                        (review_url, author, content, date_published, likes_count)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            result["url"],
                            comment.get("author", ""),
                            comment.get("content", ""),
                            comment.get("date_published", ""),
                            comment.get("likes_count", 0),
                        ),
                    )

                conn.commit()
                self.stats["saved_count"] += 1

                year_text = (
                    f"({getattr(review, 'year', 'N/A')})"
                    if getattr(review, "year", None)
                    else ""
                )
                comment_text = f"+{len(comments)}💬" if comments else ""
                print(
                    f"       💾 СОХРАНЕНО: {review.brand} {review.model} {year_text} - ⭐{review.rating} {comment_text}"
                )
                return True

        except Exception as e:
            print(f"       ❌ Ошибка сохранения: {e}")
            self.stats["save_errors"] += 1
            return False

    def parse_single_review(self, url: str) -> Dict[str, Any]:
        """Парсинг одного отзыва со всеми данными."""
        try:
            # Используем метод парсера
            reviews = self.parser.parse_single_review(url)

            if not reviews:
                return {"status": "error", "error": "Пустой результат", "url": url}

            review = reviews[0]

            # Парсим комментарии если включено
            comments = []
            if self.config["enable_comments"]:
                try:
                    comments = self.parser.parse_comments(url)
                except Exception as e:
                    print(f"       ⚠️ Ошибка комментариев: {str(e)[:50]}")

            return {
                "status": "success",
                "url": url,
                "review": review,
                "comments": comments,
                "parsed_at": datetime.now().isoformat(),
            }

        except Exception as e:
            return {"status": "error", "error": str(e), "url": url}

    def get_existing_urls(self) -> set:
        """Получение уже имеющихся URL из БД."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT url FROM comprehensive_reviews")
                return {row[0] for row in cursor.fetchall()}
        except Exception:
            return set()

    def get_brands(self) -> List[str]:
        """Получение списка брендов в алфавитном порядке."""
        brands = [
            "ac",
            "aito",
            "audi",
            "bmw",
            "chery",
            "ford",
            "honda",
            "hyundai",
            "kia",
            "lexus",
            "mazda",
            "mercedes-benz",
            "mitsubishi",
            "nissan",
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

    def parse_brand_comprehensive(self, brand: str, max_reviews: int) -> int:
        """Комплексный парсинг бренда с автосохранением."""
        print(f"\n🏷️ БРЕНД: {brand.upper()}")
        print("-" * 50)

        existing_urls = self.get_existing_urls()
        brand_saved = 0

        try:
            # Получаем все модели для бренда
            print(f"   🔍 Получение всех моделей для {brand}...")
            models = self.parser.get_all_models_for_brand(brand)
            print(
                f"   ✅ Найдено {len(models)} моделей: {models[:5]}{'...' if len(models) > 5 else ''}"
            )

            # Собираем URL из всех моделей
            all_urls = []
            for model in models:
                print(f"   📄 Получение отзывов для {brand} {model}...")
                try:
                    model_reviews = self.parser.parse_catalog_model(
                        brand, model, max_reviews=50
                    )
                    model_urls = [
                        r.url for r in model_reviews if hasattr(r, "url") and r.url
                    ]
                    all_urls.extend(model_urls)
                    print(f"      ✅ Найдено {len(model_urls)} URL для {model}")
                except Exception as e:
                    print(f"      ❌ Ошибка для {model}: {str(e)[:50]}")

                time.sleep(1)  # Пауза между моделями

            # Убираем дубликаты и уже существующие
            unique_urls = list(set(all_urls))
            new_urls = [url for url in unique_urls if url not in existing_urls]

            print(f"   📊 Найдено {len(unique_urls)} уникальных URL")
            print(f"   📋 Новых для парсинга: {len(new_urls)}")
            print(f"   ⚠️ Дубликатов пропущено: {len(unique_urls) - len(new_urls)}")

            # Парсим новые URL с автосохранением
            for i, url in enumerate(new_urls[:max_reviews], 1):
                if self.stats["total_parsed"] >= max_reviews:
                    break

                print(f"   📄 Парсинг {i}/{min(len(new_urls), max_reviews)}: {url}")

                result = self.parse_single_review(url)
                self.stats["total_parsed"] += 1

                if result["status"] == "success":
                    self.stats["successful_parsed"] += 1
                    review = result["review"]

                    # Автосохранение каждого успешного отзыва
                    if self.save_review(result):
                        brand_saved += 1

                else:
                    self.stats["failed_parsed"] += 1
                    error = result.get("error", "Неизвестно")[:50]
                    print(f"       ❌ Ошибка: {error}")

                # Пауза между запросами
                time.sleep(self.config["delay_between_requests"])

        except Exception as e:
            print(f"   ❌ Критическая ошибка для {brand}: {e}")

        print(f"   📊 Бренд {brand} завершен: сохранено {brand_saved} отзывов")
        return brand_saved

    def start_comprehensive_parsing(self, target_reviews: int = 30):
        """Запуск полнофункционального парсинга."""
        print("🔥 ПОЛНЫЙ ПАРСЕР АВТО-ОТЗЫВОВ С АВТОСОХРАНЕНИЕМ")
        print("=" * 60)
        print(f"📊 Целевое количество отзывов: {target_reviews}")
        print(f"💾 Автосохранение: каждый успешный отзыв")
        print(f"🌐 База данных: {self.db_path}")
        print("=" * 60)

        existing_count = len(self.get_existing_urls())
        print(f"📋 Уже в базе данных: {existing_count} URL")

        brands = self.get_brands()

        for brand_num, brand in enumerate(brands, 1):
            print(f"\n🏷️ БРЕНД {brand_num}/{len(brands)}: {brand.upper()}")

            saved_this_brand = self.parse_brand_comprehensive(brand, target_reviews)

            # Проверяем лимит
            if self.stats["total_parsed"] >= target_reviews:
                print(f"\n✅ Достигнут лимит {target_reviews} отзывов!")
                break

            # Пауза между брендами
            if brand_num < len(brands):
                print(
                    f"   ⏸️ Пауза между брендами: {self.config['delay_between_brands']}с"
                )
                time.sleep(self.config["delay_between_brands"])

        self.print_final_stats()

    def print_final_stats(self):
        """Печать финальной статистики."""
        elapsed = time.time() - self.stats["start_time"]

        print("\n" + "=" * 60)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print("=" * 60)
        print(f"⏱️ Время работы: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")
        print(f"✅ Всего обработано: {self.stats['total_parsed']}")
        print(f"🎯 Успешно спарсено: {self.stats['successful_parsed']}")
        print(f"❌ Ошибок парсинга: {self.stats['failed_parsed']}")
        print(f"💾 Успешно сохранено: {self.stats['saved_count']}")
        print(f"⚠️ Ошибок сохранения: {self.stats['save_errors']}")

        if elapsed > 0:
            speed = self.stats["total_parsed"] / elapsed * 3600
            print(f"📈 Средняя скорость: {speed:.1f} отзывов/час")

        # Проверяем БД
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM comprehensive_reviews")
                total_in_db = cursor.fetchone()[0]
                print(f"🗄️ Всего в БД: {total_in_db} отзывов")
        except Exception as e:
            print(f"❌ Ошибка проверки БД: {e}")


def main():
    """Главная функция."""
    print("🔥 ИСПРАВЛЕННЫЙ ПОЛНЫЙ ПАРСЕР АВТО-ОТЗЫВОВ")
    print("Парсинг в алфавитном порядке со ВСЕМИ моделями и данными\n")

    print("📋 РЕЖИМЫ РАБОТЫ:")
    print("1. Тестовый режим (30 отзывов)")
    print("2. Боевой режим (без ограничений)")

    try:
        choice = input("\nВыберите режим (1/2): ").strip()

        if choice == "1":
            print("\n🧪 ТЕСТОВЫЙ РЕЖИМ\n")
            parser = ComprehensiveBrandParser(mode="test")
            parser.start_comprehensive_parsing(target_reviews=30)
        elif choice == "2":
            print("\n💪 БОЕВОЙ РЕЖИМ\n")
            parser = ComprehensiveBrandParser(mode="production")
            parser.start_comprehensive_parsing(target_reviews=10000)
        else:
            print("❌ Неверный выбор!")
            return

    except KeyboardInterrupt:
        print("\n⏹️ Парсинг прерван пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
