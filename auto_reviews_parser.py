#!/usr/bin/env python3
"""
Стабильный парсер отзывов и бортжурналов для автомобилей
Собирает данные с Drom.ru и Drive2.ru в базу данных
Работает в щадящем режиме для долгосрочного сбора данных
"""

import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re
import json
from urllib.parse import urljoin, urlparse
import hashlib
from pathlib import Path
import os

from dotenv import load_dotenv
from prometheus_client import Counter, start_http_server, REGISTRY

try:
    import redis
except ImportError:  # pragma: no cover - redis is optional
    redis = None

from src.utils.logger import get_logger
from src.utils.validators import validate_non_empty_string

from botasaurus.browser import browser, Driver
from botasaurus.request import request, Request
from botasaurus.soupify import soupify
from botasaurus import bt

<<<<<<< HEAD
<<<<<<< HEAD
logger = get_logger(__name__)
=======
from src.database.base import Database
from src.database.repositories.review_repository import ReviewRepository
from src.database.repositories.queue_repository import QueueRepository
>>>>>>> origin/codex/create-database-abstraction-and-repositories
=======
# Load environment variables
load_dotenv()
>>>>>>> origin/codex/create-docker-folder-with-configuration-files

# ==================== НАСТРОЙКИ ====================


class Config:
    """Конфигурация парсера"""

    # База данных
    DB_PATH = os.getenv("DB_PATH", "auto_reviews.db")

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


# ==================== METRICS ====================

PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 8000))


def _get_counter(name: str, description: str) -> Counter:
    try:
        return Counter(name, description)
    except ValueError:  # counter already exists (e.g. in tests)
        return REGISTRY._names_to_collectors[name]


SOURCE_COUNTER = _get_counter("sources_processed_total", "Sources processed")
REVIEW_COUNTER = _get_counter("reviews_saved_total", "Reviews saved")
ERROR_COUNTER = _get_counter("parser_errors_total", "Errors during parsing")


# ==================== МОДЕЛИ ДАННЫХ ====================

from dataclasses import dataclass
from typing import Optional


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

<<<<<<< HEAD
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
            logger.error(f"Ошибка сохранения отзыва: {e}")
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

=======
>>>>>>> origin/codex/create-database-abstraction-and-repositories

# ==================== ПАРСЕРЫ ====================


from parsers import DromParser, Drive2Parser


# ==================== ГЛАВНЫЙ ПАРСЕР ====================


class AutoReviewsParser:
    """Главный класс парсера отзывов автомобилей"""

    def __init__(self, db_path: str = Config.DB_PATH):
<<<<<<< HEAD
        validate_non_empty_string(db_path, "db_path")
        self.db = ReviewsDatabase(db_path)
=======
        self.db = Database(db_path)
        self.review_repo = ReviewRepository(self.db)
        self.queue_repo = QueueRepository(self.db)
        self.setup_logging()
>>>>>>> origin/codex/create-database-abstraction-and-repositories
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Optional Redis cache
        self.redis = None
        if redis and os.getenv("REDIS_URL"):
            try:
                self.redis = redis.from_url(os.getenv("REDIS_URL"))
            except Exception:
                self.redis = None

        # Инициализация парсеров
        self.drom_parser = DromParser(self.review_repo)
        self.drive2_parser = Drive2Parser(self.review_repo)

    def initialize_sources_queue(self):
        """Инициализация очереди источников для парсинга"""
        self.queue_repo.initialize(Config.TARGET_BRANDS)
        total_sources = (
            len(Config.TARGET_BRANDS)
            * sum(len(models) for models in Config.TARGET_BRANDS.values())
            * 2
        )
        print(f"✅ Инициализирована очередь из {total_sources} источников")

    def get_next_source(self) -> Optional[Tuple[str, str, str]]:
        """Получение следующего источника для парсинга"""
        return self.queue_repo.get_next()

    def mark_source_completed(
        self, brand: str, model: str, source: str, pages_parsed: int, reviews_found: int
    ):
        """Отметка источника как завершенного"""
        self.queue_repo.mark_completed(
            brand, model, source, pages_parsed, reviews_found
        )

    def parse_single_source(self, brand: str, model: str, source: str) -> int:
        """Парсинг одного источника"""
        brand = validate_non_empty_string(brand, "brand")
        model = validate_non_empty_string(model, "model")
        print(f"\n🎯 Парсинг: {brand} {model} на {source}")

        reviews = []
        data = {"brand": brand, "model": model, "max_pages": Config.PAGES_PER_SESSION}

        try:
            if source == "drom.ru":
                reviews = self.drom_parser.parse_brand_model_reviews(data)
            elif source == "drive2.ru":
                reviews = self.drive2_parser.parse_brand_model_reviews(data)
            if reviews is None:
                logger.warning(
                    f"Parser returned no reviews for {brand} {model} on {source}"
                )
                return False

            # Сохраняем отзывы в базу
            saved_count = 0
            redis_client = getattr(self, "redis", None)
            for review in reviews:
<<<<<<< HEAD
                if self.review_repo.save(review):
=======
                if redis_client and redis_client.sismember("processed_reviews", review.url):
                    continue
                if self.db.save_review(review):
>>>>>>> origin/codex/create-docker-folder-with-configuration-files
                    saved_count += 1
                    REVIEW_COUNTER.inc()
                    if redis_client:
                        redis_client.sadd("processed_reviews", review.url)

            SOURCE_COUNTER.inc()
            print(f"  💾 Сохранено {saved_count} из {len(reviews)} отзывов")

            # Отмечаем источник как завершенный
            self.mark_source_completed(
                brand, model, source, Config.PAGES_PER_SESSION, saved_count
            )

            return saved_count

        except Exception as e:
<<<<<<< HEAD
            logger.error(f"Критическая ошибка парсинга {brand} {model} {source}: {e}")
=======
            ERROR_COUNTER.inc()
            logging.error(f"Критическая ошибка парсинга {brand} {model} {source}: {e}")
>>>>>>> origin/codex/create-docker-folder-with-configuration-files
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
            current_count = self.review_repo.get_reviews_count(brand, model)
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
                logger.error(
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
        stats = self.review_repo.get_parsing_stats()
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
                logger.error(f"Критическая ошибка в непрерывном парсинге: {e}")
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
        stats = self.parser.review_repo.get_parsing_stats()

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
        queue_stats = self.parser.queue_repo.get_stats()

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
        with self.parser.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [description[0] for description in cursor.description]
            for row in cursor.fetchall():
                df_data.append(dict(zip(columns, row)))

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

    # Запускаем HTTP-сервер метрик
    start_http_server(PROMETHEUS_PORT)

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
