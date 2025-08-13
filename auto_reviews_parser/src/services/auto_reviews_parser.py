"""Стабильный парсер отзывов и бортжурналов для автомобилей
Собирает данные с Drom.ru и Drive2.ru в базу данных
Работает в щадящем режиме для долгосрочного сбора данных
"""

import sqlite3
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re
import json
import logging
from urllib.parse import urljoin, urlparse
import hashlib
from pathlib import Path

from botasaurus.browser import browser, Driver
from botasaurus.request import request, Request
from botasaurus.soupify import soupify
from botasaurus import bt

from src.config.settings import Config
from src.database.reviews_database import ReviewsDatabase
from src.parsers import DromParser, Drive2Parser
from src.models import ReviewData

# ==================== ГЛАВНЫЙ ПАРСЕР ====================


class AutoReviewsParser:
    """Главный класс парсера отзывов автомобилей"""

    def __init__(self, db_path: str = Config.DB_PATH):
        self.db = ReviewsDatabase(db_path)
        self.setup_logging()
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Инициализация парсеров
        self.drom_parser = DromParser(self.db)
        self.drive2_parser = Drive2Parser(self.db)

    def setup_logging(self):
        """Настройка логирования"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        log_file = log_dir / f"parser_{datetime.now().strftime('%Y%m%d')}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file, encoding="utf-8"),
                logging.StreamHandler(),
            ],
        )

    def initialize_sources_queue(self):
        """Инициализация очереди источников для парсинга"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()

        # Очищаем старую очередь
        cursor.execute("DELETE FROM sources_queue")

        # Добавляем все комбинации брендов и моделей
        for brand, models in Config.TARGET_BRANDS.items():
            for model in models:
                for source in ["drom.ru", "drive2.ru"]:
                    cursor.execute(
                        """
                        INSERT INTO sources_queue (brand, model, source, priority)
                        VALUES (?, ?, ?, ?)
                    """,
                        (brand, model, source, 1),
                    )

        conn.commit()
        conn.close()

        total_sources = (
            len(Config.TARGET_BRANDS)
            * sum(len(models) for models in Config.TARGET_BRANDS.values())
            * 2
        )
        print(f"✅ Инициализирована очередь из {total_sources} источников")

    def get_next_source(self) -> Optional[Tuple[str, str, str]]:
        """Получение следующего источника для парсинга"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()

        # Ищем неспарсенные источники, сортируем по приоритету
        cursor.execute(
            """
            SELECT id, brand, model, source FROM sources_queue 
            WHERE status = 'pending' 
            ORDER BY priority DESC, RANDOM()
            LIMIT 1
        """
        )

        result = cursor.fetchone()

        if result:
            source_id, brand, model, source = result

            # Отмечаем как обрабатываемый
            cursor.execute(
                """
                UPDATE sources_queue 
                SET status = 'processing', last_parsed = CURRENT_TIMESTAMP 
                WHERE id = ?
            """,
                (source_id,),
            )

            conn.commit()
            conn.close()

            return brand, model, source

        conn.close()
        return None

    def mark_source_completed(
        self, brand: str, model: str, source: str, pages_parsed: int, reviews_found: int
    ):
        """Отметка источника как завершенного"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE sources_queue 
            SET status = 'completed', parsed_pages = ?, total_pages = ?
            WHERE brand = ? AND model = ? AND source = ?
        """,
            (pages_parsed, pages_parsed, brand, model, source),
        )

        conn.commit()
        conn.close()

    def parse_single_source(self, brand: str, model: str, source: str) -> int:
        """Парсинг одного источника"""
        print(f"\n🎯 Парсинг: {brand} {model} на {source}")

        reviews = []
        data = {"brand": brand, "model": model, "max_pages": Config.PAGES_PER_SESSION}

        try:
            if source == "drom.ru":
                # Вызываем метод с передачей экземпляра парсера через metadata
                reviews = self.drom_parser.parse_brand_model_reviews(
                    data, metadata=self.drom_parser
                )
            elif source == "drive2.ru":
                # Вызываем метод с правильной сигнатурой
                reviews = self.drive2_parser.parse_brand_model_reviews(data)
            if reviews is None:
                logging.warning(
                    f"Parser returned no reviews for {brand} {model} on {source}"
                )
                reviews = []

            # Сохраняем отзывы в базу
            saved_count = 0
            for review in reviews:
                if self.db.save_review(review):
                    saved_count += 1

            print(f"  💾 Сохранено {saved_count} из {len(reviews)} отзывов")

            # Отмечаем источник как завершенный
            self.mark_source_completed(
                brand, model, source, Config.PAGES_PER_SESSION, saved_count
            )

            return saved_count

        except Exception as e:
            logging.error(f"Критическая ошибка парсинга {brand} {model} {source}: {e}")
            return 0

    def run_parsing_session(
        self, max_sources: int = 10, session_duration_hours: int = 2
    ):
        """Запуск сессии парсинга"""
        print(f"\n🚀 ЗАПУСК СЕССИИ ПАРСИНГА")
        print(f"{'='*60}")
        print(f"Максимум источников за сессию: {max_sources}")
        print(f"Максимальная длительность: {session_duration_hours} часов")
        print(f"{'='*60}")

        session_start = datetime.now()
        session_end = session_start + timedelta(hours=session_duration_hours)

        sources_processed = 0
        total_reviews_saved = 0

        while sources_processed < max_sources and datetime.now() < session_end:
            # Получаем следующий источник
            source_info = self.get_next_source()

            if not source_info:
                print("\n✅ Все источники обработаны!")
                break

            brand, model, source = source_info

            # Проверяем лимит отзывов для модели
            current_count = self.db.get_reviews_count(brand, model)
            if current_count >= Config.MAX_REVIEWS_PER_MODEL:
                print(
                    f"  ⚠️ Лимит отзывов для {brand} {model} достигнут ({current_count})"
                )
                self.mark_source_completed(brand, model, source, 0, 0)
                continue

            # Парсим источник
            try:
                reviews_saved = self.parse_single_source(brand, model, source)
                total_reviews_saved += reviews_saved
                sources_processed += 1

                # Пауза между источниками
                if sources_processed < max_sources:
                    delay = random.uniform(30, 60)  # 30-60 секунд между источниками
                    print(f"  ⏳ Пауза {delay:.1f} сек...")
                    time.sleep(delay)

            except Exception as e:
                logging.error(
                    f"Ошибка обработки источника {brand} {model} {source}: {e}"
                )
                sources_processed += 1

                # Увеличенная пауза при ошибке
                time.sleep(Config.ERROR_DELAY)

        # Статистика сессии
        session_duration = datetime.now() - session_start

        print(f"\n📊 СТАТИСТИКА СЕССИИ")
        print(f"{'='*60}")
        print(f"Длительность: {session_duration}")
        print(f"Источников обработано: {sources_processed}")
        print(f"Отзывов сохранено: {total_reviews_saved}")
        print(f"{'='*60}")

        # Общая статистика базы
        stats = self.db.get_parsing_stats()
        print(f"\n📈 ОБЩАЯ СТАТИСТИКА БАЗЫ ДАННЫХ")
        print(f"{'='*60}")
        print(f"Всего отзывов: {stats['total_reviews']}")
        print(f"Уникальных брендов: {stats['unique_brands']}")
        print(f"Уникальных моделей: {stats['unique_models']}")
        print(f"По источникам: {stats['by_source']}")
        print(f"По типам: {stats['by_type']}")
        print(f"{'='*60}")

    def run_continuous_parsing(
        self, daily_sessions: int = 4, session_sources: int = 10
    ):
        """Непрерывный парсинг с интервалами"""
        print(f"\n🔄 РЕЖИМ НЕПРЕРЫВНОГО ПАРСИНГА")
        print(f"Сессий в день: {daily_sessions}")
        print(f"Источников за сессию: {session_sources}")
        print(f"Интервал между сессиями: {24 // daily_sessions} часов")

        session_interval = timedelta(hours=24 // daily_sessions)

        while True:
            try:
                # Запускаем сессию парсинга
                self.run_parsing_session(
                    max_sources=session_sources, session_duration_hours=2
                )

                # Ждем до следующей сессии
                next_session = datetime.now() + session_interval
                print(
                    f"\n⏰ Следующая сессия: {next_session.strftime('%Y-%m-%d %H:%M:%S')}"
                )

                while datetime.now() < next_session:
                    remaining = next_session - datetime.now()
                    print(f"⏳ До следующей сессии: {remaining}", end="\r")
                    time.sleep(60)  # Проверяем каждую минуту

            except KeyboardInterrupt:
                print("\n👋 Парсинг остановлен пользователем")
                break
            except Exception as e:
                logging.error(f"Критическая ошибка в непрерывном парсинге: {e}")
                print(f"❌ Критическая ошибка: {e}")
                print("⏳ Пауза 30 минут перед повтором...")
                time.sleep(1800)  # 30 минут пауза при критической ошибке


# ==================== УТИЛИТЫ УПРАВЛЕНИЯ ====================


class ParserManager:
    """Менеджер для управления парсером"""

    def __init__(self, db_path: str = Config.DB_PATH):
        self.parser = AutoReviewsParser(db_path)

    def show_status(self):
        """Показать статус базы данных и очереди"""
        stats = self.parser.db.get_parsing_stats()

        print(f"\n📊 СТАТУС БАЗЫ ДАННЫХ")
        print(f"{'='*50}")
        print(f"Всего отзывов: {stats['total_reviews']:,}")
        print(f"Уникальных брендов: {stats['unique_brands']}")
        print(f"Уникальных моделей: {stats['unique_models']}")

        if stats["by_source"]:
            print(f"\nПо источникам:")
            for source, count in stats["by_source"].items():
                print(f"  {source}: {count:,}")

        if stats["by_type"]:
            print(f"\nПо типам:")
            for type_name, count in stats["by_type"].items():
                print(f"  {type_name}: {count:,}")

        # Статистика очереди
        conn = sqlite3.connect(self.parser.db.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT status, COUNT(*) FROM sources_queue GROUP BY status")
        queue_stats = dict(cursor.fetchall())

        conn.close()

        print(f"\n📋 СТАТУС ОЧЕРЕДИ")
        print(f"{'='*50}")
        total_sources = sum(queue_stats.values())

        for status, count in queue_stats.items():
            percentage = (count / total_sources * 100) if total_sources > 0 else 0
            print(f"{status}: {count} ({percentage:.1f}%)")

        print(f"Всего источников: {total_sources}")

    def reset_queue(self):
        """Сброс очереди парсинга"""
        print("🔄 Сброс очереди парсинга...")
        self.parser.initialize_sources_queue()

    def export_data(self, output_format: str = "xlsx"):
        """Экспорт данных из базы"""
        print(f"📤 Экспорт данных в формате {output_format}...")

        conn = sqlite3.connect(self.parser.db.db_path)

        # Получаем все отзывы
        query = """
            SELECT 
                source, type, brand, model, year, title, author, rating,
                content, pros, cons, mileage, engine_volume, fuel_type,
                transmission, body_type, drive_type, publish_date, 
                views_count, likes_count, comments_count, url, parsed_at
            FROM reviews
            ORDER BY brand, model, parsed_at DESC
        """

        df_data = []
        cursor = conn.cursor()
        cursor.execute(query)

        columns = [description[0] for description in cursor.description]

        for row in cursor.fetchall():
            df_data.append(dict(zip(columns, row)))

        conn.close()

        if not df_data:
            print("❌ Нет данных для экспорта")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if output_format.lower() == "xlsx":
            filename = f"auto_reviews_export_{timestamp}.xlsx"
            bt.write_excel(df_data, filename.replace(".xlsx", ""))
            print(f"✅ Данные экспортированы в {filename}")

        elif output_format.lower() == "json":
            filename = f"auto_reviews_export_{timestamp}.json"
            bt.write_json(df_data, filename.replace(".json", ""))
            print(f"✅ Данные экспортированы в {filename}")

        else:
            print(f"❌ Неподдерживаемый формат: {output_format}")


# ==================== ГЛАВНАЯ ФУНКЦИЯ ====================


def main():
    """Главная функция для запуска парсера"""
    import argparse

    parser = argparse.ArgumentParser(description="Парсер отзывов автомобилей")
    parser.add_argument(
        "command",
        choices=["init", "parse", "continuous", "status", "export"],
        help="Команда для выполнения",
    )
    parser.add_argument(
        "--sources",
        type=int,
        default=10,
        help="Количество источников за сессию (по умолчанию: 10)",
    )
    parser.add_argument(
        "--sessions",
        type=int,
        default=4,
        help="Количество сессий в день для непрерывного режима (по умолчанию: 4)",
    )
    parser.add_argument(
        "--format",
        default="xlsx",
        choices=["xlsx", "json"],
        help="Формат экспорта данных (по умолчанию: xlsx)",
    )

    args = parser.parse_args()

    manager = ParserManager()

    if args.command == "init":
        print("🚀 Инициализация парсера...")
        manager.reset_queue()
        print("✅ Парсер готов к работе!")

    elif args.command == "parse":
        print("🎯 Запуск разовой сессии парсинга...")
        manager.parser.run_parsing_session(max_sources=args.sources)

    elif args.command == "continuous":
        print("🔄 Запуск непрерывного парсинга...")
        manager.parser.run_continuous_parsing(
            daily_sessions=args.sessions, session_sources=args.sources
        )

    elif args.command == "status":
        manager.show_status()

    elif args.command == "export":
        manager.export_data(output_format=args.format)


if __name__ == "__main__":
    main()

    # parser = AutoReviewsParser()
    # parser.run_parsing_session(max_sources=10)
    # parser.run_continuous_parsing(daily_sessions=4, session_sources=10)
    # parser.run_parsing_session(max_sources=10)
