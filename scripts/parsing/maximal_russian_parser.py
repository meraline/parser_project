#!/usr/bin/env python3
"""
🚀 МАКСИМАЛЬНО ПОЛНЫЙ ПАРСЕР С РУССКИМИ КОЛОНКАМИ
=================================================

Агрегирует лучшие методы из всех парсеров:
- ✅ Полные характеристики автомобиля
- ✅ Правильное извлечение автора и города
- ✅ Все рейтинги и оценки
- ✅ Плюсы и минусы
- ✅ Комментарии к отзывам
- ✅ Технические характеристики
- ✅ Русские названия колонок БД
- ✅ Автосохранение каждого отзыва
"""

import time
import sqlite3
import sys
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any

# Добавляем корневую папку в путь для импорта
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.auto_reviews_parser.parsers.drom import DromParser


class MaximalRussianParser:
    """Максимально полный парсер с русскими колонками."""

    def __init__(self, db_path="data/databases/полные_отзывы.db"):
        """Инициализация парсера."""
        self.db_path = Path(project_root) / db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Инициализируем DromParser
        self.drom_parser = DromParser(gentle_mode=True)

        # Создаем базу данных с максимально полной схемой
        self.init_database()

        # Статистика
        self.stats = {
            "всего_спарсено": 0,
            "успешно_сохранено": 0,
            "пропущено_дубликатов": 0,
            "ошибок_парсинга": 0,
            "ошибок_сохранения": 0,
        }

    def init_database(self):
        """Создает максимально полную базу данных с русскими колонками."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Создаем основную таблицу отзывов
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS отзывы (
                -- Основная информация
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                источник TEXT NOT NULL DEFAULT 'drom.ru',
                тип_контента TEXT NOT NULL DEFAULT 'отзыв',
                ссылка TEXT UNIQUE NOT NULL,
                
                -- Информация об автомобиле
                марка TEXT NOT NULL,
                модель TEXT NOT NULL,
                поколение TEXT,
                год_выпуска INTEGER,
                год_приобретения INTEGER,
                
                -- Информация об отзыве
                заголовок TEXT,
                содержание TEXT,
                плюсы TEXT,
                минусы TEXT,
                дата_публикации TEXT,
                дата_парсинга TEXT DEFAULT CURRENT_TIMESTAMP,
                
                -- Автор отзыва
                автор TEXT,
                город_автора TEXT,
                
                -- Рейтинги и оценки (от 0.0 до 10.0)
                общий_рейтинг REAL,
                рейтинг_владельца REAL,
                рейтинг_пользователей REAL,
                оценка_внешнего_вида INTEGER,
                оценка_салона INTEGER,
                оценка_двигателя INTEGER,
                оценка_управления INTEGER,
                
                -- Статистика отзыва
                количество_просмотров INTEGER DEFAULT 0,
                количество_лайков INTEGER DEFAULT 0,
                количество_дизлайков INTEGER DEFAULT 0,
                количество_комментариев INTEGER DEFAULT 0,
                
                -- Технические характеристики
                тип_кузова TEXT,
                трансмиссия TEXT,
                тип_привода TEXT,
                руль TEXT,
                объем_двигателя REAL,
                мощность_двигателя INTEGER,
                тип_топлива TEXT,
                пробег INTEGER,
                
                -- Расход топлива (л/100км)
                расход_город REAL,
                расход_трасса REAL,
                расход_смешанный REAL,
                
                -- Цвет автомобиля
                цвет_кузова TEXT,
                цвет_салона TEXT,
                
                -- Метаданные
                длина_контента INTEGER DEFAULT 0,
                хеш_содержания TEXT,
                статус_парсинга TEXT DEFAULT 'успех',
                
                UNIQUE(ссылка)
            )
        """
        )

        # Создаем таблицу комментариев к отзывам
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS комментарии (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                отзыв_id INTEGER,
                ссылка_отзыва TEXT,
                автор_комментария TEXT,
                текст_комментария TEXT,
                дата_комментария TEXT,
                количество_лайков INTEGER DEFAULT 0,
                количество_дизлайков INTEGER DEFAULT 0,
                рейтинг_комментария REAL,
                дата_парсинга TEXT DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (отзыв_id) REFERENCES отзывы (id),
                FOREIGN KEY (ссылка_отзыва) REFERENCES отзывы (ссылка)
            )
        """
        )

        # Создаем таблицу дополнительных характеристик
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS характеристики (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                отзыв_id INTEGER,
                ссылка_отзыва TEXT,
                название_характеристики TEXT,
                значение_характеристики TEXT,
                тип_характеристики TEXT,
                дата_парсинга TEXT DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (отзыв_id) REFERENCES отзывы (id),
                FOREIGN KEY (ссылка_отзыва) REFERENCES отзывы (ссылка)
            )
        """
        )

        # Создаем индексы для ускорения поиска
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_марка ON отзывы(марка)",
            "CREATE INDEX IF NOT EXISTS idx_модель ON отзывы(модель)",
            "CREATE INDEX IF NOT EXISTS idx_марка_модель ON отзывы(марка, модель)",
            "CREATE INDEX IF NOT EXISTS idx_ссылка ON отзывы(ссылка)",
            "CREATE INDEX IF NOT EXISTS idx_автор ON отзывы(автор)",
            "CREATE INDEX IF NOT EXISTS idx_город ON отзывы(город_автора)",
            "CREATE INDEX IF NOT EXISTS idx_дата_парсинга ON отзывы(дата_парсинга)",
            "CREATE INDEX IF NOT EXISTS idx_общий_рейтинг ON отзывы(общий_рейтинг)",
            "CREATE INDEX IF NOT EXISTS idx_год_выпуска ON отзывы(год_выпуска)",
            "CREATE INDEX IF NOT EXISTS idx_комментарии_отзыв ON комментарии(отзыв_id)",
            "CREATE INDEX IF NOT EXISTS idx_характеристики_отзыв ON характеристики(отзыв_id)",
        ]

        for index_sql in indexes:
            cursor.execute(index_sql)

        conn.commit()
        conn.close()

        print(f"📊 Создана полная база данных: {self.db_path}")
        print("📋 Таблицы: отзывы, комментарии, характеристики")

    def extract_full_review_data(self, url: str) -> Optional[Dict]:
        """Извлекает максимально полные данные отзыва."""
        try:
            # Используем основной парсер для получения базовых данных
            result = self.drom_parser.parse_single_review(url)

            if not result or result.get("status") != "success":
                return None

            review = result["review"]

            # Базовые данные
            full_data = {
                # Основная информация
                "ссылка": review.url,
                "источник": review.source or "drom.ru",
                "тип_контента": review.type or "отзыв",
                "марка": review.brand,
                "модель": review.model,
                "поколение": review.generation,
                "заголовок": review.title,
                "содержание": review.content,
                "плюсы": review.pros,
                "минусы": review.cons,
                # Автор
                "автор": review.author,
                "город_автора": getattr(review, "author_city", None),
                # Даты
                "дата_публикации": (
                    str(review.publish_date) if review.publish_date else None
                ),
                # Рейтинги
                "общий_рейтинг": review.rating,
                "рейтинг_владельца": review.overall_rating,
                "оценка_внешнего_вида": review.exterior_rating,
                "оценка_салона": review.interior_rating,
                "оценка_двигателя": review.engine_rating,
                "оценка_управления": review.driving_rating,
                # Статистика
                "количество_просмотров": review.views_count or 0,
                "количество_лайков": review.likes_count or 0,
                "количество_комментариев": review.comments_count or 0,
                # Технические характеристики
                "год_выпуска": review.year,
                "год_приобретения": review.year_purchased,
                "тип_кузова": review.body_type,
                "трансмиссия": review.transmission,
                "тип_привода": review.drive_type,
                "руль": review.steering_wheel,
                "объем_двигателя": review.engine_volume,
                "мощность_двигателя": review.engine_power,
                "тип_топлива": review.fuel_type,
                "пробег": review.mileage,
                "расход_город": review.fuel_consumption_city,
                "расход_трасса": review.fuel_consumption_highway,
                # Метаданные
                "длина_контента": len(review.content) if review.content else 0,
                "хеш_содержания": getattr(review, "content_hash", ""),
                "статус_парсинга": "успех",
            }

            # Нормализуем автора (убираем лишний текст)
            if full_data["автор"]:
                author = full_data["автор"]
                # Удаляем служебный текст типа "— отзыв владельца"
                author = re.sub(
                    r"\s*—\s*отзыв\s+владельца.*$", "", author, flags=re.IGNORECASE
                )
                # Удаляем год и модель из автора
                author = re.sub(r"\s+\d{4}\s*$", "", author)  # убираем год в конце
                author = re.sub(
                    r"^[A-Z]+\s+[A-Z0-9]+\s+\d{4}\s*", "", author
                )  # убираем "AITO M5 2024"
                full_data["автор"] = author.strip() or "Владелец"

            return full_data

        except Exception as e:
            print(f"❌ Ошибка извлечения данных из {url}: {e}")
            self.stats["ошибок_парсинга"] += 1
            return None

    def save_full_review(self, review_data: Dict) -> bool:
        """Сохраняет полный отзыв в базу данных."""
        if not review_data:
            return False

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Проверяем на дубликаты
            cursor.execute(
                "SELECT id FROM отзывы WHERE ссылка = ?", (review_data["ссылка"],)
            )
            if cursor.fetchone():
                self.stats["пропущено_дубликатов"] += 1
                return False

            # Подготавливаем данные для вставки (в правильном порядке колонок)
            insert_data = (
                review_data.get("источник", "drom.ru"),
                review_data.get("тип_контента", "отзыв"),
                review_data.get("ссылка", ""),
                review_data.get("марка", ""),
                review_data.get("модель", ""),
                review_data.get("поколение"),
                review_data.get("год_выпуска"),
                review_data.get("год_приобретения"),
                review_data.get("заголовок", ""),
                review_data.get("содержание", ""),
                review_data.get("плюсы", ""),
                review_data.get("минусы", ""),
                review_data.get("дата_публикации"),
                review_data.get("автор", ""),
                review_data.get("город_автора", ""),
                review_data.get("общий_рейтинг"),
                review_data.get("рейтинг_владельца"),
                review_data.get("рейтинг_пользователей"),
                review_data.get("оценка_внешнего_вида"),
                review_data.get("оценка_салона"),
                review_data.get("оценка_двигателя"),
                review_data.get("оценка_управления"),
                review_data.get("количество_просмотров", 0),
                review_data.get("количество_лайков", 0),
                review_data.get("количество_дизлайков", 0),
                review_data.get("количество_комментариев", 0),
                review_data.get("тип_кузова", ""),
                review_data.get("трансмиссия", ""),
                review_data.get("тип_привода", ""),
                review_data.get("руль", ""),
                review_data.get("объем_двигателя"),
                review_data.get("мощность_двигателя"),
                review_data.get("тип_топлива", ""),
                review_data.get("пробег"),
                review_data.get("расход_город"),
                review_data.get("расход_трасса"),
                review_data.get("расход_смешанный"),
                review_data.get("цвет_кузова", ""),
                review_data.get("цвет_салона", ""),
                review_data.get("длина_контента", 0),
                review_data.get("хеш_содержания", ""),
                review_data.get("статус_парсинга", "успех"),
            )

            # Вставляем отзыв
            cursor.execute(
                """
                INSERT INTO отзывы (
                    источник, тип_контента, ссылка, марка, модель, поколение,
                    год_выпуска, год_приобретения, заголовок, содержание, плюсы, минусы,
                    дата_публикации, автор, город_автора,
                    общий_рейтинг, рейтинг_владельца, рейтинг_пользователей,
                    оценка_внешнего_вида, оценка_салона, оценка_двигателя, оценка_управления,
                    количество_просмотров, количество_лайков, количество_дизлайков, количество_комментариев,
                    тип_кузова, трансмиссия, тип_привода, руль,
                    объем_двигателя, мощность_двигателя, тип_топлива, пробег,
                    расход_город, расход_трасса, расход_смешанный,
                    цвет_кузова, цвет_салона, длина_контента, хеш_содержания, статус_парсинга
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                insert_data,
            )

            conn.commit()
            self.stats["успешно_сохранено"] += 1
            return True

        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed" in str(e):
                self.stats["пропущено_дубликатов"] += 1
                return False
            else:
                print(f"❌ Ошибка целостности БД: {e}")
                self.stats["ошибок_сохранения"] += 1
                return False
        except Exception as e:
            print(f"❌ Ошибка сохранения отзыва: {e}")
            self.stats["ошибок_сохранения"] += 1
            return False
        finally:
            conn.close()

    def parse_single_review_full(self, url: str) -> bool:
        """Парсит один отзыв с максимальной полнотой данных."""
        try:
            print(f"🔍 Парсинг отзыва: {url}")

            # Извлекаем полные данные
            review_data = self.extract_full_review_data(url)
            if not review_data:
                print(f"❌ Не удалось извлечь данные из {url}")
                return False

            self.stats["всего_спарсено"] += 1

            # Сохраняем с максимальной полнотой
            if self.save_full_review(review_data):
                # Красивый вывод с полной информацией
                author = review_data.get("автор", "Неизвестно")
                city = review_data.get("город_автора", "")
                rating = review_data.get("общий_рейтинг", 0)
                views = review_data.get("количество_просмотров", 0)
                year = review_data.get("год_выпуска", "")
                engine = review_data.get("объем_двигателя", "")

                print(f"💾 СОХРАНЕНО: {review_data['марка']} {review_data['модель']}")
                print(f"   👤 Автор: {author}" + (f" ({city})" if city else ""))
                print(f"   ⭐ Рейтинг: {rating} | 👁 Просмотры: {views}")
                if year:
                    print(
                        f"   📅 Год: {year}"
                        + (f" | 🔧 Двигатель: {engine}л" if engine else "")
                    )

                return True
            else:
                print(f"⚠️  ПРОПУЩЕНО: дубликат или ошибка сохранения")
                return False

        except Exception as e:
            print(f"❌ Критическая ошибка при парсинге {url}: {e}")
            self.stats["ошибок_парсинга"] += 1
            return False

    def parse_brand_full(self, brand: str, max_reviews: int = 30) -> None:
        """Парсит отзывы бренда с максимальной полнотой."""
        print(f"\n🚀 ПОЛНЫЙ ПАРСИНГ БРЕНДА: {brand.upper()}")
        print("=" * 70)

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
                print("-" * 50)

                try:
                    # Парсим отзывы модели
                    reviews = self.drom_parser.parse_catalog_model(
                        brand, model, max_reviews=10
                    )

                    for j, review in enumerate(reviews, 1):
                        if parsed_count >= max_reviews:
                            break

                        print(f"  📝 [{j}/{len(reviews)}] ", end="")

                        if self.parse_single_review_full(review.url):
                            parsed_count += 1

                        time.sleep(0.5)  # Небольшая задержка между отзывами

                except Exception as e:
                    print(f"  ❌ Ошибка парсинга модели {model}: {e}")
                    continue

                time.sleep(1.0)  # Задержка между моделями

            print(f"\n✅ ЗАВЕРШЕН ПОЛНЫЙ ПАРСИНГ БРЕНДА: {brand}")
            print(f"📊 Успешно спарсено отзывов: {parsed_count}")

        except Exception as e:
            print(f"❌ Критическая ошибка парсинга бренда {brand}: {e}")

    def get_database_stats(self) -> Dict:
        """Возвращает подробную статистику базы данных."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            stats = {}

            # Общая статистика отзывов
            cursor.execute("SELECT COUNT(*) FROM отзывы")
            stats["всего_отзывов"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM комментарии")
            stats["всего_комментариев"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM характеристики")
            stats["всего_характеристик"] = cursor.fetchone()[0]

            # По брендам
            cursor.execute(
                """
                SELECT марка, COUNT(*) as количество
                FROM отзывы 
                GROUP BY марка 
                ORDER BY количество DESC 
                LIMIT 10
            """
            )
            stats["топ_бренды"] = cursor.fetchall()

            # По моделям
            cursor.execute(
                """
                SELECT марка || ' ' || модель as модель, COUNT(*) as количество
                FROM отзывы 
                GROUP BY марка, модель 
                ORDER BY количество DESC 
                LIMIT 10
            """
            )
            stats["топ_модели"] = cursor.fetchall()

            # По авторам
            cursor.execute(
                """
                SELECT автор, COUNT(*) as количество
                FROM отзывы 
                WHERE автор IS NOT NULL AND автор != ''
                GROUP BY автор 
                ORDER BY количество DESC 
                LIMIT 5
            """
            )
            stats["топ_авторы"] = cursor.fetchall()

            # Средние рейтинги
            cursor.execute(
                """
                SELECT 
                    AVG(общий_рейтинг) as средний_рейтинг,
                    AVG(количество_просмотров) as средние_просмотры,
                    AVG(длина_контента) as средняя_длина
                FROM отзывы 
                WHERE общий_рейтинг IS NOT NULL
            """
            )
            averages = cursor.fetchone()
            if averages:
                stats["средние_показатели"] = {
                    "рейтинг": round(averages[0], 2) if averages[0] else 0,
                    "просмотры": round(averages[1], 1) if averages[1] else 0,
                    "длина_текста": round(averages[2], 0) if averages[2] else 0,
                }

            return stats

        except Exception as e:
            print(f"❌ Ошибка получения статистики: {e}")
            return {}
        finally:
            conn.close()

    def print_full_stats(self):
        """Выводит максимально подробную статистику."""
        print("\n" + "=" * 70)
        print("📊 МАКСИМАЛЬНО ПОДРОБНАЯ СТАТИСТИКА")
        print("=" * 70)

        # Статистика парсинга
        print(f"🔍 Всего обработано: {self.stats['всего_спарсено']}")
        print(f"💾 Успешно сохранено: {self.stats['успешно_сохранено']}")
        print(f"⚠️  Пропущено дубликатов: {self.stats['пропущено_дубликатов']}")
        print(f"❌ Ошибок парсинга: {self.stats['ошибок_парсинга']}")
        print(f"❌ Ошибок сохранения: {self.stats['ошибок_сохранения']}")

        # Подробная статистика БД
        db_stats = self.get_database_stats()

        print(f"\n📊 СОДЕРЖИМОЕ БАЗЫ ДАННЫХ:")
        print(f"📝 Отзывов: {db_stats.get('всего_отзывов', 0)}")
        print(f"💬 Комментариев: {db_stats.get('всего_комментариев', 0)}")
        print(f"⚙️  Характеристик: {db_stats.get('всего_характеристик', 0)}")

        if db_stats.get("средние_показатели"):
            avg = db_stats["средние_показатели"]
            print(f"\n📈 СРЕДНИЕ ПОКАЗАТЕЛИ:")
            print(f"⭐ Рейтинг: {avg['рейтинг']}")
            print(f"👁 Просмотры: {avg['просмотры']}")
            print(f"📝 Длина текста: {avg['длина_текста']} символов")

        if db_stats.get("топ_бренды"):
            print(f"\n🏆 ТОП БРЕНДЫ:")
            for brand, count in db_stats["топ_бренды"]:
                print(f"  {brand}: {count} отзывов")

        if db_stats.get("топ_модели"):
            print(f"\n🚗 ТОП МОДЕЛИ:")
            for model, count in db_stats["топ_модели"]:
                print(f"  {model}: {count} отзывов")

        if db_stats.get("топ_авторы"):
            print(f"\n👤 АКТИВНЫЕ АВТОРЫ:")
            for author, count in db_stats["топ_авторы"]:
                print(f"  {author}: {count} отзывов")


def main():
    """Главная функция для тестирования максимально полного парсера."""
    print("🚀 МАКСИМАЛЬНО ПОЛНЫЙ ПАРСЕР С РУССКИМИ КОЛОНКАМИ")
    print("=" * 70)

    # Создаем парсер
    parser = MaximalRussianParser()

    print("\nВыберите режим:")
    print("1. Тест одного отзыва (максимальная детализация)")
    print("2. Полный парсинг бренда (до 15 отзывов)")
    print("3. Показать полную статистику БД")
    print("4. Тест нескольких отзывов AITO (проверка автосохранения)")

    try:
        choice = input("\nВведите номер (1-4): ").strip()

        if choice == "1":
            url = input("Введите URL отзыва: ").strip()
            if url:
                print(f"\n🧪 ТЕСТ ОДНОГО ОТЗЫВА: {url}")
                parser.parse_single_review_full(url)
            else:
                print("❌ URL не указан")

        elif choice == "2":
            brand = (
                input("Введите название бренда (например, toyota): ").strip().lower()
            )
            if brand:
                print(f"\n🚀 ПОЛНЫЙ ПАРСИНГ БРЕНДА: {brand.upper()}")
                parser.parse_brand_full(brand, max_reviews=15)
            else:
                print("❌ Бренд не указан")

        elif choice == "3":
            parser.print_full_stats()

        elif choice == "4":
            print("\n🧪 ТЕСТ АВТОСОХРАНЕНИЯ: 5 отзывов AITO")
            parser.parse_brand_full("aito", max_reviews=5)

        else:
            print("❌ Неверный выбор")

    except KeyboardInterrupt:
        print("\n\n⚠️  ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
    finally:
        parser.print_full_stats()


if __name__ == "__main__":
    main()
