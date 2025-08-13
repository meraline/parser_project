#!/usr/bin/env python3
"""
Стабильный парсер отзывов и бортжурналов для автомобилей
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

# ==================== НАСТРОЙКИ ====================


class Config:
    """Конфигурация парсера"""

    # База данных
    DB_PATH = "auto_reviews.db"

    # Задержки (в секундах)
    MIN_DELAY = 5  # Минимальная задержка между запросами
    MAX_DELAY = 15  # Максимальная задержка между запросами
    ERROR_DELAY = 30  # Задержка при ошибке
    RATE_LIMIT_DELAY = 300  # Задержка при rate limit (5 минут)

    # Ограничения
    MAX_RETRIES = 3  # Максимальное количество повторов
    PAGES_PER_SESSION = 50  # Страниц за сессию
    MAX_REVIEWS_PER_MODEL = 1000  # Максимум отзывов на модель

    # User agents для ротации
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    # Список популярных брендов и моделей для парсинга
    TARGET_BRANDS = {
        "toyota": ["camry", "corolla", "rav4", "highlander", "prius", "land-cruiser"],
        "volkswagen": ["polo", "golf", "passat", "tiguan", "touareg", "jetta"],
        "nissan": ["qashqai", "x-trail", "almera", "teana", "murano", "pathfinder"],
        "hyundai": ["solaris", "elantra", "tucson", "santa-fe", "creta", "sonata"],
        "kia": ["rio", "cerato", "sportage", "sorento", "soul", "optima"],
        "mazda": ["mazda3", "mazda6", "cx-5", "cx-3", "mx-5", "cx-9"],
        "ford": ["focus", "fiesta", "mondeo", "kuga", "explorer", "ecosport"],
        "chevrolet": ["cruze", "aveo", "captiva", "lacetti", "tahoe", "suburban"],
        "skoda": ["octavia", "rapid", "fabia", "superb", "kodiaq", "karoq"],
        "renault": ["logan", "sandero", "duster", "kaptur", "megane", "fluence"],
        "mitsubishi": ["lancer", "outlander", "asx", "pajero", "eclipse-cross", "l200"],
        "honda": ["civic", "accord", "cr-v", "pilot", "fit", "hr-v"],
        "bmw": ["3-series", "5-series", "x3", "x5", "x1", "1-series"],
        "mercedes-benz": ["c-class", "e-class", "s-class", "glc", "gle", "gla"],
        "audi": ["a3", "a4", "a6", "q3", "q5", "q7"],
        "lada": ["granta", "kalina", "priora", "vesta", "xray", "largus"],
    }


# ==================== МОДЕЛИ ДАННЫХ ====================

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import hashlib


@dataclass
class ReviewData:
    """Структура данных отзыва"""

    source: str  # drom.ru, drive2.ru
    type: str  # review, board_journal
    brand: str
    model: str
    generation: Optional[str] = None
    year: Optional[int] = None
    url: str = ""
    title: str = ""
    content: str = ""
    author: str = ""
    rating: Optional[float] = None
    pros: str = ""
    cons: str = ""
    mileage: Optional[int] = None
    engine_volume: Optional[float] = None
    fuel_type: str = ""
    transmission: str = ""
    body_type: str = ""
    drive_type: str = ""
    publish_date: Optional[datetime] = None
    views_count: Optional[int] = None
    likes_count: Optional[int] = None
    comments_count: Optional[int] = None
    parsed_at: datetime = None
    content_hash: str = ""

    def __post_init__(self):
        if self.parsed_at is None:
            self.parsed_at = datetime.now()
        content_for_hash = (
            f"{self.url}_{self.title}_{self.content[:100] if self.content else ''}"
        )
        self.content_hash = hashlib.md5(content_for_hash.encode()).hexdigest()

from src.services.queue_service import QueueService

# ==================== БАЗА ДАННЫХ ====================


class ReviewsDatabase:
    """Управление базой данных отзывов"""

    def __init__(self, db_path: str = Config.DB_PATH):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Инициализация базы данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Таблица отзывов
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                type TEXT NOT NULL,
                brand TEXT NOT NULL,
                model TEXT NOT NULL,
                generation TEXT,
                year INTEGER,
                url TEXT UNIQUE,
                title TEXT,
                content TEXT,
                author TEXT,
                rating REAL,
                pros TEXT,
                cons TEXT,
                mileage INTEGER,
                engine_volume REAL,
                fuel_type TEXT,
                transmission TEXT,
                body_type TEXT,
                drive_type TEXT,
                publish_date DATETIME,
                views_count INTEGER,
                likes_count INTEGER,
                comments_count INTEGER,
                parsed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                content_hash TEXT UNIQUE,
                UNIQUE(url, content_hash)
            )
        """
        )

        # Таблица статистики парсинга
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS parsing_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                brand TEXT,
                model TEXT,
                source TEXT,
                pages_parsed INTEGER DEFAULT 0,
                reviews_found INTEGER DEFAULT 0,
                errors_count INTEGER DEFAULT 0,
                started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                finished_at DATETIME,
                status TEXT DEFAULT 'running'
            )
        """
        )

        # Таблица источников (для отслеживания прогресса)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sources_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand TEXT,
                model TEXT,
                source TEXT,
                base_url TEXT,
                priority INTEGER DEFAULT 1,
                status TEXT DEFAULT 'pending',
                last_parsed DATETIME,
                total_pages INTEGER DEFAULT 0,
                parsed_pages INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Индексы для быстрого поиска
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_brand_model ON reviews(brand, model)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_source_type ON reviews(source, type)"
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_parsed_at ON reviews(parsed_at)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_content_hash ON reviews(content_hash)"
        )

        conn.commit()
        conn.close()

    def save_review(self, review: ReviewData) -> bool:
        """Сохранение отзыва в базу"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT OR IGNORE INTO reviews (
                    source, type, brand, model, generation, year, url, title, content,
                    author, rating, pros, cons, mileage, engine_volume, fuel_type,
                    transmission, body_type, drive_type, publish_date, views_count,
                    likes_count, comments_count, parsed_at, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    review.source,
                    review.type,
                    review.brand,
                    review.model,
                    review.generation,
                    review.year,
                    review.url,
                    review.title,
                    review.content,
                    review.author,
                    review.rating,
                    review.pros,
                    review.cons,
                    review.mileage,
                    review.engine_volume,
                    review.fuel_type,
                    review.transmission,
                    review.body_type,
                    review.drive_type,
                    review.publish_date,
                    review.views_count,
                    review.likes_count,
                    review.comments_count,
                    review.parsed_at,
                    review.content_hash,
                ),
            )

            success = cursor.rowcount > 0
            conn.commit()
            conn.close()
            return success

        except sqlite3.IntegrityError:
            # Дублирующая запись
            return False
        except Exception as e:
            logging.error(f"Ошибка сохранения отзыва: {e}")
            return False

    def get_reviews_count(self, brand: str = None, model: str = None) -> int:
        """Получение количества отзывов"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if brand and model:
            cursor.execute(
                "SELECT COUNT(*) FROM reviews WHERE brand = ? AND model = ?",
                (brand, model),
            )
        elif brand:
            cursor.execute("SELECT COUNT(*) FROM reviews WHERE brand = ?", (brand,))
        else:
            cursor.execute("SELECT COUNT(*) FROM reviews")

        count = cursor.fetchone()[0]
        conn.close()
        return count

    def is_url_parsed(self, url: str) -> bool:
        """Проверка, был ли URL уже спарсен"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM reviews WHERE url = ? LIMIT 1", (url,))
        exists = cursor.fetchone() is not None

        conn.close()
        return exists

    def get_parsing_stats(self) -> Dict:
        """Получение статистики парсинга"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Общая статистика
        cursor.execute("SELECT COUNT(*) FROM reviews")
        total_reviews = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT brand) FROM reviews")
        unique_brands = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT model) FROM reviews")
        unique_models = cursor.fetchone()[0]

        cursor.execute("SELECT source, COUNT(*) FROM reviews GROUP BY source")
        by_source = dict(cursor.fetchall())

        cursor.execute("SELECT type, COUNT(*) FROM reviews GROUP BY type")
        by_type = dict(cursor.fetchall())

        conn.close()

        return {
            "total_reviews": total_reviews,
            "unique_brands": unique_brands,
            "unique_models": unique_models,
            "by_source": by_source,
            "by_type": by_type,
        }


# ==================== ПАРСЕРЫ ====================


from parsers import DromParser, Drive2Parser


# ==================== ГЛАВНЫЙ ПАРСЕР ====================


class AutoReviewsParser:
    """Главный класс парсера отзывов автомобилей"""

    def __init__(self, db_path: str = Config.DB_PATH, queue_service: Optional[QueueService] = None):
        self.db = ReviewsDatabase(db_path)
        self.queue_service = queue_service or QueueService(self.db.db_path, Config.TARGET_BRANDS)
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


    def parse_single_source(self, brand: str, model: str, source: str) -> int:
        """Парсинг одного источника"""
        print(f"\n🎯 Парсинг: {brand} {model} на {source}")

        reviews = []
        data = {"brand": brand, "model": model, "max_pages": Config.PAGES_PER_SESSION}

        try:
            if source == "drom.ru":
                try:
                    reviews = self.drom_parser.parse_brand_model_reviews(
                        data, metadata=self.drom_parser
                    )
                except TypeError:
                    reviews = self.drom_parser.parse_brand_model_reviews(data)
            elif source == "drive2.ru":
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

            # Отмечаем источник как завершенный только если есть сохраненные отзывы
            if saved_count:
                if hasattr(self, "queue_service") and self.queue_service:
                    self.queue_service.mark_source_completed(
                        brand, model, source, Config.PAGES_PER_SESSION, saved_count
                    )
                elif hasattr(self, "mark_source_completed"):
                    self.mark_source_completed(
                        brand, model, source, Config.PAGES_PER_SESSION, saved_count
                    )

            return saved_count or False

        except Exception as e:
            logging.error(f"Критическая ошибка парсинга {brand} {model} {source}: {e}")
            return False

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
            source_info = self.queue_service.get_next_source()

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
                self.queue_service.mark_source_completed(brand, model, source, 0, 0)
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

    from src.services.parser_service import ParserService

    service = ParserService()

    if args.command == "init":
        print("🚀 Инициализация парсера...")
        service.reset_queue()
        print("✅ Парсер готов к работе!")

    elif args.command == "parse":
        print("🎯 Запуск разовой сессии парсинга...")
        service.parser.run_parsing_session(max_sources=args.sources)

    elif args.command == "continuous":
        print("🔄 Запуск непрерывного парсинга...")
        service.parser.run_continuous_parsing(
            daily_sessions=args.sessions, session_sources=args.sources
        )

    elif args.command == "status":
        service.show_status()

    elif args.command == "export":
        service.export_data(output_format=args.format)


if __name__ == "__main__":
    main()

    # parser = AutoReviewsParser()
    # parser.run_parsing_session(max_sources=10)
    # parser.run_continuous_parsing(daily_sessions=4, session_sources=10)
    # parser.run_parsing_session(max_sources=10)
