#!/usr/bin/env python3
"""
Система полного парсинга всех отзывов по алфавиту брендов.
Многопоточный парсер с защитой от банов и умной балансировкой нагрузки.
"""

import sys
import os
import time
import threading
import queue
import random
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import requests
from urllib.parse import urljoin, urlparse
import logging

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.models.review import Review
from auto_reviews_parser.models.comment import Comment
from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository
from auto_reviews_parser.database.repositories.comment_repository import (
    CommentRepository,
)


@dataclass
class ParsingConfig:
    """Конфигурация парсинга."""

    # Производительность
    num_threads: int = 8  # Количество потоков
    delay_min: float = 2.0  # Минимальная задержка между запросами (сек)
    delay_max: float = 5.0  # Максимальная задержка между запросами (сек)
    timeout: int = 30  # Тайм-аут страницы (сек)

    # Защита от банов
    requests_per_hour: int = 500  # Максимум запросов в час
    ban_check_interval: int = 10  # Проверка бана каждые N запросов
    retry_attempts: int = 3  # Попытки повтора при ошибке

    # Управление процессом
    save_interval: int = 100  # Сохранение в БД каждые N отзывов
    checkpoint_interval: int = 1000  # Создание checkpoint'а каждые N отзывов

    # Опции парсинга
    parse_comments: bool = True  # Парсить комментарии
    gentle_mode: bool = True  # Щадящий режим

    # Ночной режим (экономия ресурсов)
    night_mode_start: int = 23  # Начало ночного режима (час)
    night_mode_end: int = 7  # Конец ночного режима (час)
    night_delay_multiplier: float = 1.5  # Множитель задержки ночью


@dataclass
class BrandInfo:
    """Информация о бренде."""

    name: str
    models_count: int = 0
    reviews_count: int = 0
    parsed_count: int = 0
    is_completed: bool = False


class RateLimiter:
    """Ограничитель скорости запросов."""

    def __init__(self, max_requests_per_hour: int):
        self.max_requests = max_requests_per_hour
        self.requests = []
        self.lock = threading.Lock()

    def wait_if_needed(self):
        """Ожидает, если превышен лимит запросов."""
        with self.lock:
            now = datetime.now()
            # Удаляем запросы старше часа
            self.requests = [
                req_time
                for req_time in self.requests
                if now - req_time < timedelta(hours=1)
            ]

            if len(self.requests) >= self.max_requests:
                # Ждем до освобождения слота
                oldest_request = min(self.requests)
                wait_time = (oldest_request + timedelta(hours=1) - now).total_seconds()
                if wait_time > 0:
                    print(f"⏳ Лимит запросов достигнут. Ожидание {wait_time:.1f}с...")
                    time.sleep(wait_time)

            self.requests.append(now)


class BanDetector:
    """Детектор блокировок."""

    def __init__(self):
        self.consecutive_errors = 0
        self.last_error_time = None
        self.ban_indicators = [
            "captcha",
            "robot",
            "blocked",
            "access denied",
            "rate limit",
            "too many requests",
            "temporarily unavailable",
        ]

    def check_response(self, content: str, status_code: int) -> bool:
        """Проверяет ответ на признаки бана."""
        if status_code in [429, 403, 503]:
            self.consecutive_errors += 1
            self.last_error_time = datetime.now()
            return True

        content_lower = content.lower()
        for indicator in self.ban_indicators:
            if indicator in content_lower:
                self.consecutive_errors += 1
                self.last_error_time = datetime.now()
                return True

        # Сброс счетчика при успешном ответе
        self.consecutive_errors = 0
        return False

    def is_likely_banned(self) -> bool:
        """Определяет, вероятно ли мы заблокированы."""
        return self.consecutive_errors >= 3

    def get_recovery_time(self) -> int:
        """Возвращает время ожидания для восстановления (сек)."""
        if self.consecutive_errors <= 3:
            return 30
        elif self.consecutive_errors <= 6:
            return 300  # 5 минут
        else:
            return 1800  # 30 минут


class FullParsingSystem:
    """Основная система полного парсинга."""

    def __init__(self, config: ParsingConfig):
        self.config = config
        self.db = Database("full_parsing.db")
        self.review_repo = ReviewRepository(self.db)
        self.comment_repo = CommentRepository(self.db)

        # Управление потоками
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.stop_event = threading.Event()

        # Статистика
        self.stats = {
            "total_brands": 0,
            "completed_brands": 0,
            "total_reviews": 0,
            "parsed_reviews": 0,
            "failed_reviews": 0,
            "total_comments": 0,
            "start_time": None,
            "last_checkpoint": None,
            "requests_made": 0,
        }

        # Защита от банов
        self.rate_limiter = RateLimiter(config.requests_per_hour)
        self.ban_detector = BanDetector()

        # Настройка логирования
        self.setup_logging()

        # Создание таблиц для отслеживания прогресса
        self.init_progress_tables()

    def setup_logging(self):
        """Настройка логирования."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler("full_parsing.log"), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

    def init_progress_tables(self):
        """Создание таблиц для отслеживания прогресса."""
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()

            # Таблица брендов
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS brands_progress (
                    name TEXT PRIMARY KEY,
                    models_count INTEGER DEFAULT 0,
                    reviews_count INTEGER DEFAULT 0,
                    parsed_count INTEGER DEFAULT 0,
                    is_completed BOOLEAN DEFAULT FALSE,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP
                )
            """
            )

            # Таблица checkpoint'ов
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS parsing_checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_reviews INTEGER,
                    parsed_reviews INTEGER,
                    current_brand TEXT,
                    config_json TEXT
                )
            """
            )

            conn.commit()

    def get_all_brands(self) -> List[str]:
        """Получает список всех брендов в алфавитном порядке."""
        try:
            from catalog_extractor import DromCatalogExtractor

            extractor = DromCatalogExtractor(delay=1.0)
            brands = extractor.get_all_brands()
            self.logger.info(f"Получено {len(brands)} брендов из каталога")
            return brands
        except Exception as e:
            self.logger.warning(
                f"Ошибка получения брендов: {e}. Используем базовый список."
            )
            # Базовый список известных брендов
            return [
                "acura",
                "alfa-romeo",
                "audi",
                "bmw",
                "cadillac",
                "chevrolet",
                "chery",
                "citroen",
                "daewoo",
                "datsun",
                "fiat",
                "ford",
                "genesis",
                "geely",
                "honda",
                "hyundai",
                "infiniti",
                "jaguar",
                "jeep",
                "kia",
                "lada",
                "land-rover",
                "lexus",
                "mazda",
                "mercedes",
                "mini",
                "mitsubishi",
                "nissan",
                "opel",
                "peugeot",
                "porsche",
                "renault",
                "skoda",
                "subaru",
                "suzuki",
                "toyota",
                "volkswagen",
                "volvo",
            ]

    def get_brand_models(self, brand: str) -> List[str]:
        """Получает список моделей бренда."""
        try:
            from catalog_extractor import DromCatalogExtractor

            extractor = DromCatalogExtractor(delay=1.0)
            models = extractor.get_brand_models(brand)
            self.logger.info(f"Получено {len(models)} моделей для бренда {brand}")
            return models
        except Exception as e:
            self.logger.error(f"Ошибка получения моделей для {brand}: {e}")
            return []

    def get_model_review_urls(self, brand: str, model: str) -> List[str]:
        """Получает все URL отзывов для модели."""
        try:
            from catalog_extractor import DromCatalogExtractor

            extractor = DromCatalogExtractor(delay=1.0)
            urls = extractor.get_model_review_urls(brand, model, max_pages=50)
            self.logger.info(f"Получено {len(urls)} отзывов для {brand} {model}")
            return urls
        except Exception as e:
            self.logger.error(f"Ошибка получения отзывов для {brand} {model}: {e}")
            return []

    def is_night_mode(self) -> bool:
        """Проверяет, активен ли ночной режим."""
        current_hour = datetime.now().hour
        return (
            current_hour >= self.config.night_mode_start
            or current_hour < self.config.night_mode_end
        )

    def get_delay(self) -> float:
        """Возвращает задержку с учетом ночного режима."""
        base_delay = random.uniform(self.config.delay_min, self.config.delay_max)
        if self.is_night_mode():
            base_delay *= self.config.night_delay_multiplier
        return base_delay

    def worker_thread(self, thread_id: int):
        """Рабочий поток для парсинга."""
        parser = DromParser(gentle_mode=self.config.gentle_mode)

        while not self.stop_event.is_set():
            try:
                # Получаем задачу
                try:
                    task = self.task_queue.get(timeout=1)
                except queue.Empty:
                    continue

                if task is None:  # Сигнал завершения
                    break

                # Проверяем на бан
                if self.ban_detector.is_likely_banned():
                    recovery_time = self.ban_detector.get_recovery_time()
                    self.logger.warning(
                        f"Поток {thread_id}: возможна блокировка, ожидание {recovery_time}с"
                    )
                    time.sleep(recovery_time)

                # Ограничение скорости
                self.rate_limiter.wait_if_needed()

                # Парсим отзыв
                result = self.parse_review_safe(parser, task, thread_id)
                self.result_queue.put(result)

                # Задержка
                delay = self.get_delay()
                time.sleep(delay)

                self.task_queue.task_done()

            except Exception as e:
                self.logger.error(f"Поток {thread_id}: ошибка {e}")
                if "task" in locals():
                    self.task_queue.task_done()

    def parse_review_safe(self, parser: DromParser, url: str, thread_id: int) -> Dict:
        """Безопасный парсинг отзыва с повторными попытками."""
        result = {
            "url": url,
            "success": False,
            "review": None,
            "comments": [],
            "error": None,
            "thread_id": thread_id,
        }

        for attempt in range(self.config.retry_attempts):
            try:
                # Парсинг отзыва
                reviews = parser.parse_single_review(url)
                if not reviews:
                    result["error"] = "Отзыв не найден"
                    continue

                result["review"] = reviews[0]

                # Парсинг комментариев (если включено)
                if self.config.parse_comments:
                    comments_data = parser.parse_comments(url)
                    if comments_data:
                        comments = []
                        for comment_data in comments_data:
                            comment = Comment(
                                review_id=1,  # Будет обновлен при сохранении
                                author=comment_data.get("author", "Неизвестен"),
                                content=comment_data.get("content", ""),
                                likes_count=comment_data.get("likes_count", 0),
                                dislikes_count=comment_data.get("dislikes_count", 0),
                                publish_date=comment_data.get("publish_date"),
                                source_url=url,
                                parsed_at=datetime.now(),
                            )
                            comments.append(comment)
                        result["comments"] = comments

                result["success"] = True
                self.stats["requests_made"] += 1
                break

            except Exception as e:
                error_msg = str(e)
                self.logger.warning(
                    f"Поток {thread_id}, попытка {attempt + 1}: {error_msg}"
                )

                # Проверка на бан
                if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                    self.ban_detector.consecutive_errors += 1

                if attempt == self.config.retry_attempts - 1:
                    result["error"] = error_msg
                else:
                    time.sleep(2**attempt)  # Экспоненциальная задержка

        return result

    def save_results_batch(self, results: List[Dict]):
        """Сохранение пакета результатов в БД."""
        reviews_to_save = []
        comments_to_save = []

        for result in results:
            if result["success"] and result["review"]:
                reviews_to_save.append(result["review"])

                if result["comments"]:
                    comments_to_save.extend(result["comments"])

        if reviews_to_save:
            try:
                # Сохраняем отзывы
                saved_ids = self.review_repo.save_batch(reviews_to_save)

                # Обновляем review_id в комментариях и сохраняем
                if comments_to_save and saved_ids:
                    review_id_map = {}
                    for i, review in enumerate(reviews_to_save):
                        if i < len(saved_ids):
                            review_id_map[review.source_url] = saved_ids[i]

                    for comment in comments_to_save:
                        if comment.source_url in review_id_map:
                            comment.review_id = review_id_map[comment.source_url]

                    self.comment_repo.save_batch(comments_to_save)

                self.logger.info(
                    f"Сохранено: {len(reviews_to_save)} отзывов, {len(comments_to_save)} комментариев"
                )

            except Exception as e:
                self.logger.error(f"Ошибка сохранения в БД: {e}")

    def create_checkpoint(self):
        """Создание checkpoint'а прогресса."""
        checkpoint_data = {
            "total_reviews": self.stats["total_reviews"],
            "parsed_reviews": self.stats["parsed_reviews"],
            "current_brand": getattr(self, "current_brand", "unknown"),
            "config": str(self.config.__dict__),
        }

        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO parsing_checkpoints 
                (total_reviews, parsed_reviews, current_brand, config_json)
                VALUES (?, ?, ?, ?)
            """,
                (
                    checkpoint_data["total_reviews"],
                    checkpoint_data["parsed_reviews"],
                    checkpoint_data["current_brand"],
                    checkpoint_data["config"],
                ),
            )
            conn.commit()

        self.stats["last_checkpoint"] = datetime.now()
        self.logger.info(f"Checkpoint создан: {checkpoint_data}")

    def start_full_parsing(self):
        """Запуск полного парсинга."""
        print("🚀 ЗАПУСК ПОЛНОГО ПАРСИНГА БАЗЫ ОТЗЫВОВ")
        print("=" * 70)
        print(f"Потоков: {self.config.num_threads}")
        print(f"Задержка: {self.config.delay_min}-{self.config.delay_max}с")
        print(f"Лимит запросов: {self.config.requests_per_hour}/час")
        print(f"Парсинг комментариев: {'Да' if self.config.parse_comments else 'Нет'}")
        print("=" * 70)

        self.stats["start_time"] = datetime.now()
        brands = self.get_all_brands()
        self.stats["total_brands"] = len(brands)

        # Запуск рабочих потоков
        with ThreadPoolExecutor(max_workers=self.config.num_threads) as executor:
            # Запускаем рабочие потоки
            futures = [
                executor.submit(self.worker_thread, i)
                for i in range(self.config.num_threads)
            ]

            try:
                results_batch = []

                # Обрабатываем бренды по алфавиту
                for brand in brands:
                    self.current_brand = brand
                    print(f"\n🏷️ Обработка бренда: {brand.upper()}")

                    # Получаем модели бренда
                    models = self.get_brand_models(brand)

                    # Обрабатываем каждую модель
                    for model in models:
                        print(f"   📱 Модель: {model}")

                        # Получаем URL отзывов модели
                        review_urls = self.get_model_review_urls(brand, model)

                        # Добавляем задачи в очередь
                        for url in review_urls:
                            self.task_queue.put(url)

                        self.stats["total_reviews"] += len(review_urls)

                    # Обрабатываем результаты
                    while not self.task_queue.empty() or not self.result_queue.empty():
                        try:
                            result = self.result_queue.get(timeout=1)
                            results_batch.append(result)

                            if result["success"]:
                                self.stats["parsed_reviews"] += 1
                            else:
                                self.stats["failed_reviews"] += 1

                            # Сохранение пакетами
                            if len(results_batch) >= self.config.save_interval:
                                self.save_results_batch(results_batch)
                                results_batch = []

                            # Создание checkpoint'ов
                            if (
                                self.stats["parsed_reviews"]
                                % self.config.checkpoint_interval
                                == 0
                            ):
                                self.create_checkpoint()

                            # Вывод прогресса
                            if self.stats["parsed_reviews"] % 100 == 0:
                                self.print_progress()

                        except queue.Empty:
                            time.sleep(0.1)

                    print(f"   ✅ Бренд {brand} завершен")
                    self.stats["completed_brands"] += 1

                # Сохраняем оставшиеся результаты
                if results_batch:
                    self.save_results_batch(results_batch)

                # Завершение потоков
                for _ in range(self.config.num_threads):
                    self.task_queue.put(None)

                # Ожидание завершения
                for future in futures:
                    future.result()

            except KeyboardInterrupt:
                print("\n⏹️ Прерывание пользователем...")
                self.stop_event.set()

            finally:
                self.print_final_stats()

    def print_progress(self):
        """Вывод текущего прогресса."""
        elapsed = datetime.now() - self.stats["start_time"]
        parsed = self.stats["parsed_reviews"]
        total = self.stats["total_reviews"]

        if parsed > 0:
            avg_time = elapsed.total_seconds() / parsed
            remaining = (total - parsed) * avg_time
            eta = datetime.now() + timedelta(seconds=remaining)

            print(f"📊 Прогресс: {parsed}/{total} ({parsed/total*100:.1f}%)")
            print(f"⏱️ Время: {elapsed}, ETA: {eta.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"🚀 Скорость: {parsed/elapsed.total_seconds()*3600:.0f} отзывов/час")

    def print_final_stats(self):
        """Вывод финальной статистики."""
        elapsed = datetime.now() - self.stats["start_time"]

        print("\n" + "=" * 70)
        print("📈 ФИНАЛЬНАЯ СТАТИСТИКА")
        print("=" * 70)
        print(f"⏱️ Общее время: {elapsed}")
        print(
            f"🏷️ Брендов обработано: {self.stats['completed_brands']}/{self.stats['total_brands']}"
        )
        print(f"✅ Отзывов обработано: {self.stats['parsed_reviews']}")
        print(f"❌ Неудачных попыток: {self.stats['failed_reviews']}")
        print(
            f"📈 Средняя скорость: {self.stats['parsed_reviews']/elapsed.total_seconds()*3600:.0f} отзывов/час"
        )
        print(f"🌐 Всего запросов: {self.stats['requests_made']}")
        print("=" * 70)


def main():
    """Главная функция."""
    # Конфигурация для быстрого и надежного парсинга
    config = ParsingConfig(
        num_threads=8,  # 8 потоков для баланса скорости/стабильности
        delay_min=2.0,  # 2-5 секунд между запросами
        delay_max=5.0,
        requests_per_hour=400,  # 400 запросов/час (консервативно)
        parse_comments=True,  # Парсим комментарии
        gentle_mode=True,  # Щадящий режим браузера
        save_interval=50,  # Сохраняем каждые 50 отзывов
        checkpoint_interval=500,  # Checkpoint каждые 500 отзывов
        night_delay_multiplier=1.3,  # Немного больше задержки ночью
    )

    print("🎯 ОПТИМАЛЬНАЯ КОНФИГУРАЦИЯ ДЛЯ НАДЕЖНОГО ПАРСИНГА:")
    print(f"   • {config.num_threads} потоков")
    print(f"   • {config.delay_min}-{config.delay_max}с задержки")
    print(f"   • {config.requests_per_hour} запросов/час")
    print(f"   • Ночной режим с коэффициентом {config.night_delay_multiplier}")
    print(f"   • Сохранение каждые {config.save_interval} отзывов")
    print()

    system = FullParsingSystem(config)

    # Оценка времени
    total_reviews = 1_141_479
    avg_time_per_review = 39.2  # Из бенчмарка
    estimated_hours = (total_reviews * avg_time_per_review) / (
        config.num_threads * 3600
    )
    estimated_days = estimated_hours / 24

    print(f"📊 ПРОГНОЗ ВРЕМЕНИ ПАРСИНГА:")
    print(f"   • Общий объем: {total_reviews:,} отзывов")
    print(f"   • Ожидаемое время: {estimated_days:.1f} дней")
    print(
        f"   • Скорость: ~{config.num_threads * 3600 / avg_time_per_review:.0f} отзывов/час"
    )
    print()

    answer = input("🚀 Начать полный парсинг? (y/N): ")
    if answer.lower() in ["y", "yes", "да"]:
        system.start_full_parsing()
    else:
        print("Отменено пользователем.")


if __name__ == "__main__":
    main()
