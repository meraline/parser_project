#!/usr/bin/env python3
"""
🚀 ЕДИНЫЙ ПАРСЕР С РУССКИМИ КОЛОНКАМИ И АВТОСОХРАНЕНИЕМ
======================================================

Объединяет лучшие функции всех парсеров:
- ✅ Русские колонки в базе данных (марка, модель, автор)
- ✅ Автосохранение каждого отзыва
- ✅ Полный функционал DromParser
- ✅ Защита от дубликатов
- ✅ Подробная статистика
"""

import time
import sqlite3
import sys
import os
from pathlib import Path

# Добавляем корневую папку в путь для импорта
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.auto_reviews_parser.parsers.drom import DromParser


class UnifiedRussianParser:
    """Единый парсер с русскими колонками и автосохранением."""

    def __init__(self, db_path="data/databases/russian_reviews.db"):
        """Инициализация парсера."""
        self.db_path = Path(project_root) / db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Инициализируем DromParser
        self.drom_parser = DromParser(gentle_mode=True)

        # Создаем базу данных с русскими колонками
        self.init_database()

        # Статистика
        self.stats = {
            "всего_спарсено": 0,
            "сохранено": 0,
            "пропущено_дубликатов": 0,
            "ошибок": 0,
        }

    def init_database(self):
        """Создает базу данных с русскими колонками."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Создаем таблицу отзывов с русскими колонками
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS отзывы (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                источник TEXT NOT NULL DEFAULT 'drom.ru',
                тип TEXT NOT NULL DEFAULT 'отзыв',
                марка TEXT NOT NULL,
                модель TEXT NOT NULL,
                поколение TEXT,
                год_выпуска INTEGER,
                ссылка TEXT UNIQUE,
                заголовок TEXT,
                содержание TEXT,
                автор TEXT,
                город_автора TEXT,
                дата_публикации TEXT,
                рейтинг REAL,
                общая_оценка REAL,
                рейтинг_владельца REAL,
                просмотры INTEGER,
                оценка_внешности INTEGER,
                оценка_салона INTEGER,
                оценка_двигателя INTEGER,
                оценка_управления INTEGER,
                год_покупки INTEGER,
                поколение_авто TEXT,
                тип_кузова TEXT,
                трансмиссия TEXT,
                тип_привода TEXT,
                руль TEXT,
                пробег TEXT,
                объем_двигателя TEXT,
                мощность_двигателя TEXT,
                тип_топлива TEXT,
                расход_в_городе TEXT,
                расход_на_трассе TEXT,
                расход_смешанный TEXT,
                год_приобретения INTEGER,
                цвет_кузова TEXT,
                цвет_салона TEXT,
                количество_комментариев INTEGER DEFAULT 0,
                тип_отзыва TEXT DEFAULT 'отзыв',
                статус_парсинга TEXT DEFAULT 'успех',
                дата_создания TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ссылка)
            )
        """
        )

        # Создаем индексы для ускорения поиска
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_марка ON отзывы(марка)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_модель ON отзывы(модель)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_марка_модель ON отзывы(марка, модель)"
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ссылка ON отзывы(ссылка)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_дата ON отзывы(дата_создания)")

        conn.commit()
        conn.close()

        print(f"📊 База данных создана: {self.db_path}")

    def save_review(self, review_data):
        """Сохраняет отзыв в базу данных с русскими колонками."""
        if not review_data:
            return False

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Проверяем на дубликаты
            cursor.execute(
                "SELECT id FROM отзывы WHERE ссылка = ?", (review_data.get("url", ""),)
            )
            if cursor.fetchone():
                self.stats["пропущено_дубликатов"] += 1
                return False

            # Подготавливаем данные для вставки
            insert_data = (
                review_data.get("source", "drom.ru"),  # источник
                review_data.get("type", "отзыв"),  # тип
                review_data.get("brand", ""),  # марка
                review_data.get("model", ""),  # модель
                review_data.get("generation", ""),  # поколение
                review_data.get("year"),  # год_выпуска
                review_data.get("url", ""),  # ссылка
                review_data.get("title", ""),  # заголовок
                review_data.get("content", ""),  # содержание
                review_data.get("author", ""),  # автор
                review_data.get("author_city", ""),  # город_автора
                review_data.get("date_published", ""),  # дата_публикации
                review_data.get("rating"),  # рейтинг
                review_data.get("overall_rating"),  # общая_оценка
                review_data.get("owner_rating"),  # рейтинг_владельца
                review_data.get("views_count"),  # просмотры
                review_data.get("exterior_rating"),  # оценка_внешности
                review_data.get("interior_rating"),  # оценка_салона
                review_data.get("engine_rating"),  # оценка_двигателя
                review_data.get("handling_rating"),  # оценка_управления
                review_data.get("year_purchased"),  # год_покупки
                review_data.get("generation"),  # поколение_авто
                review_data.get("body_type", ""),  # тип_кузова
                review_data.get("transmission", ""),  # трансмиссия
                review_data.get("drive_type", ""),  # тип_привода
                review_data.get("steering_wheel", ""),  # руль
                review_data.get("mileage", ""),  # пробег
                review_data.get("engine_volume", ""),  # объем_двигателя
                review_data.get("engine_power", ""),  # мощность_двигателя
                review_data.get("fuel_type", ""),  # тип_топлива
                review_data.get("fuel_consumption_city", ""),  # расход_в_городе
                review_data.get("fuel_consumption_highway", ""),  # расход_на_трассе
                review_data.get("fuel_consumption_mixed", ""),  # расход_смешанный
                review_data.get("purchase_year"),  # год_приобретения
                review_data.get("color_exterior", ""),  # цвет_кузова
                review_data.get("color_interior", ""),  # цвет_салона
                review_data.get("comments_count", 0),  # количество_комментариев
                review_data.get("review_type", "отзыв"),  # тип_отзыва
                "успех",  # статус_парсинга
            )

            # Вставляем отзыв
            cursor.execute(
                """
                INSERT INTO отзывы (
                    источник, тип, марка, модель, поколение, год_выпуска, ссылка, заголовок, 
                    содержание, автор, город_автора, дата_публикации, рейтинг, общая_оценка, 
                    рейтинг_владельца, просмотры, оценка_внешности, оценка_салона, 
                    оценка_двигателя, оценка_управления, год_покупки, поколение_авто, 
                    тип_кузова, трансмиссия, тип_привода, руль, пробег, объем_двигателя, 
                    мощность_двигателя, тип_топлива, расход_в_городе, расход_на_трассе, 
                    расход_смешанный, год_приобретения, цвет_кузова, цвет_салона, 
                    количество_комментариев, тип_отзыва, статус_парсинга
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                insert_data,
            )

            conn.commit()
            self.stats["сохранено"] += 1
            return True

        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed" in str(e):
                self.stats["пропущено_дубликатов"] += 1
                return False
            else:
                print(f"❌ Ошибка целостности БД: {e}")
                self.stats["ошибок"] += 1
                return False
        except Exception as e:
            print(f"❌ Ошибка сохранения: {e}")
            self.stats["ошибок"] += 1
            return False
        finally:
            conn.close()

    def parse_single_review_with_autosave(self, url):
        """Парсит один отзыв и сразу сохраняет его."""
        try:
            # Используем метод DromParser для парсинга
            result = self.drom_parser.parse_single_review(url)

            if result and result.get("status") == "success":
                review = result["review"]
                self.stats["всего_спарсено"] += 1

                # Преобразуем Review в словарь
                review_data = {
                    "source": review.source,
                    "type": review.type,
                    "brand": review.brand,
                    "model": review.model,
                    "generation": review.generation,
                    "year": review.year,
                    "url": review.url,
                    "title": review.title,
                    "content": review.content,
                    "author": review.author,
                    "rating": review.rating,
                    "overall_rating": review.overall_rating,
                    "exterior_rating": review.exterior_rating,
                    "interior_rating": review.interior_rating,
                    "engine_rating": review.engine_rating,
                    "driving_rating": review.driving_rating,
                    "views_count": review.views_count,
                    "comments_count": review.comments_count,
                    "fuel_type": review.fuel_type,
                    "transmission": review.transmission,
                    "body_type": review.body_type,
                    "drive_type": review.drive_type,
                    "steering_wheel": review.steering_wheel,
                    "year_purchased": review.year_purchased,
                    "engine_volume": review.engine_volume,
                    "engine_power": review.engine_power,
                    "fuel_consumption_city": review.fuel_consumption_city,
                    "fuel_consumption_highway": review.fuel_consumption_highway,
                    "mileage": review.mileage,
                }

                # Сохраняем с автосохранением
                if self.save_review(review_data):
                    print(
                        f"💾 СОХРАНЕНО: {review.brand} {review.model} - ⭐{review.rating or 0}"
                    )
                    return True
                else:
                    print(f"⚠️  ПРОПУЩЕНО: {review.brand} {review.model} (дубликат)")
                    return False
            else:
                print(f"❌ Ошибка парсинга: {url}")
                self.stats["ошибок"] += 1
                return False

        except Exception as e:
            print(f"❌ Критическая ошибка: {e}")
            self.stats["ошибок"] += 1
            return False

    def parse_brand_with_autosave(self, brand, max_reviews=50):
        """Парсит отзывы бренда с автосохранением каждого отзыва."""
        print(f"\n🚀 НАЧАЛО ПАРСИНГА БРЕНДА: {brand.upper()}")
        print("=" * 60)

        try:
            # Получаем все модели бренда
            models = self.drom_parser.get_all_models_for_brand(brand)
            print(f"📋 Найдено моделей: {len(models)}")

            parsed_count = 0

            for i, model in enumerate(models, 1):
                if parsed_count >= max_reviews:
                    print(f"🎯 Достигнут лимит отзывов: {max_reviews}")
                    break

                print(f"\n📄 [{i}/{len(models)}] Модель: {model}")

                try:
                    # Парсим отзывы модели
                    reviews = self.drom_parser.parse_catalog_model(
                        brand, model, max_reviews=10
                    )

                    for review in reviews:
                        if parsed_count >= max_reviews:
                            break

                        # Преобразуем и сохраняем
                        review_data = {
                            "source": review.source,
                            "brand": review.brand,
                            "model": review.model,
                            "url": review.url,
                            "title": review.title,
                            "content": review.content,
                            "author": review.author,
                            "rating": review.rating,
                            "overall_rating": review.overall_rating,
                            "exterior_rating": review.exterior_rating,
                            "interior_rating": review.interior_rating,
                            "engine_rating": review.engine_rating,
                            "driving_rating": review.driving_rating,
                            "views_count": review.views_count,
                            "comments_count": review.comments_count,
                        }

                        if self.save_review(review_data):
                            parsed_count += 1
                            print(
                                f"  💾 [{parsed_count}] СОХРАНЕНО: {review.brand} {review.model} - ⭐{review.rating or 0}"
                            )

                        time.sleep(0.5)  # Небольшая задержка

                except Exception as e:
                    print(f"  ❌ Ошибка парсинга модели {model}: {e}")
                    continue

                time.sleep(1.0)  # Задержка между моделями

            print(f"\n✅ ЗАВЕРШЕН ПАРСИНГ БРЕНДА: {brand}")
            print(f"📊 Спарсено отзывов: {parsed_count}")

        except Exception as e:
            print(f"❌ Критическая ошибка парсинга бренда {brand}: {e}")

    def get_database_stats(self):
        """Возвращает статистику базы данных."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Общее количество отзывов
            cursor.execute("SELECT COUNT(*) FROM отзывы")
            total = cursor.fetchone()[0]

            # По брендам
            cursor.execute(
                "SELECT марка, COUNT(*) FROM отзывы GROUP BY марка ORDER BY COUNT(*) DESC LIMIT 10"
            )
            brands = cursor.fetchall()

            # По моделям
            cursor.execute(
                "SELECT марка || ' ' || модель, COUNT(*) FROM отзывы GROUP BY марка, модель ORDER BY COUNT(*) DESC LIMIT 10"
            )
            models = cursor.fetchall()

            return {"всего_отзывов": total, "топ_бренды": brands, "топ_модели": models}
        finally:
            conn.close()

    def print_stats(self):
        """Выводит подробную статистику."""
        print("\n" + "=" * 60)
        print("📊 СТАТИСТИКА ПАРСИНГА")
        print("=" * 60)

        # Статистика парсинга
        print(f"🔍 Всего спарсено: {self.stats['всего_спарсено']}")
        print(f"💾 Сохранено: {self.stats['сохранено']}")
        print(f"⚠️  Пропущено дубликатов: {self.stats['пропущено_дубликатов']}")
        print(f"❌ Ошибок: {self.stats['ошибок']}")

        # Статистика БД
        db_stats = self.get_database_stats()
        print(f"\n📊 ВСЕГО ОТЗЫВОВ В БД: {db_stats['всего_отзывов']}")

        if db_stats["топ_бренды"]:
            print("\n🏆 ТОП БРЕНДЫ:")
            for brand, count in db_stats["топ_бренды"]:
                print(f"  {brand}: {count} отзывов")

        if db_stats["топ_модели"]:
            print("\n🚗 ТОП МОДЕЛИ:")
            for model, count in db_stats["топ_модели"]:
                print(f"  {model}: {count} отзывов")


def main():
    """Главная функция для тестирования единого парсера."""
    print("🚀 ЗАПУСК ЕДИНОГО ПАРСЕРА С РУССКИМИ КОЛОНКАМИ")
    print("=" * 60)

    # Создаем парсер
    parser = UnifiedRussianParser()

    print("\nВыберите режим:")
    print("1. Тестовый парсинг (5 отзывов AITO)")
    print("2. Парсинг бренда (до 20 отзывов)")
    print("3. Показать статистику БД")

    try:
        choice = input("\nВведите номер (1-3): ").strip()

        if choice == "1":
            print("\n🧪 ТЕСТОВЫЙ РЕЖИМ: Парсинг 5 отзывов AITO")
            parser.parse_brand_with_autosave("aito", max_reviews=5)

        elif choice == "2":
            brand = (
                input("Введите название бренда (например, toyota): ").strip().lower()
            )
            if brand:
                print(f"\n🚀 ПАРСИНГ БРЕНДА: {brand.upper()}")
                parser.parse_brand_with_autosave(brand, max_reviews=20)
            else:
                print("❌ Не указан бренд")

        elif choice == "3":
            print("\n📊 СТАТИСТИКА БАЗЫ ДАННЫХ:")
            stats = parser.get_database_stats()
            print(f"Всего отзывов: {stats['всего_отзывов']}")

        else:
            print("❌ Неверный выбор")

    except KeyboardInterrupt:
        print("\n\n⚠️  ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
    finally:
        parser.print_stats()


if __name__ == "__main__":
    main()
