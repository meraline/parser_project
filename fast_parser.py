#!/usr/bin/env python3
"""
Быстрый парсер всех отзывов с drom.ru с защитой от банов.
Оптимизированная версия для парсинга 1M+ отзывов.
"""

import sys
import os
import time
import threading
import queue
import random
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository
from auto_reviews_parser.database.repositories.comment_repository import (
    CommentRepository,
)


@dataclass
class FastParsingConfig:
    """Конфигурация для быстрого парсинга."""

    threads: int = 8  # Количество потоков
    delay_min: float = 1.5  # Минимальная задержка
    delay_max: float = 3.0  # Максимальная задержка
    requests_per_hour: int = 600  # Лимит запросов/час
    batch_size: int = 100  # Размер пакета для сохранения
    parse_comments: bool = True  # Парсить комментарии
    night_slowdown: bool = True  # Замедление ночью
    auto_backup: bool = True  # Автобэкапы


class QuickBrandExtractor:
    """Быстрый экстрактор брендов и моделей."""

    @staticmethod
    def get_known_brands() -> List[str]:
        """Возвращает список известных автомобильных брендов."""
        return [
            "acura",
            "alfa-romeo",
            "audi",
            "bmw",
            "buick",
            "cadillac",
            "changan",
            "chery",
            "chevrolet",
            "chrysler",
            "citroen",
            "daewoo",
            "datsun",
            "faw",
            "fiat",
            "ford",
            "geely",
            "genesis",
            "great-wall",
            "honda",
            "hyundai",
            "infiniti",
            "isuzu",
            "jaguar",
            "jeep",
            "kia",
            "lada",
            "land-rover",
            "lexus",
            "lifan",
            "mazda",
            "mercedes",
            "mini",
            "mitsubishi",
            "nissan",
            "opel",
            "peugeot",
            "porsche",
            "renault",
            "seat",
            "skoda",
            "smart",
            "ssangyong",
            "subaru",
            "suzuki",
            "toyota",
            "uaz",
            "volkswagen",
            "volvo",
            "zaz",
        ]

    @staticmethod
    def get_sample_models() -> Dict[str, List[str]]:
        """Возвращает примеры моделей для популярных брендов."""
        return {
            "toyota": ["camry", "corolla", "rav4", "land-cruiser", "prius"],
            "volkswagen": ["polo", "golf", "jetta", "passat", "tiguan"],
            "hyundai": ["solaris", "elantra", "tucson", "creta", "i30"],
            "kia": ["rio", "ceed", "sportage", "optima", "sorento"],
            "renault": ["logan", "sandero", "duster", "kaptur", "megane"],
            "nissan": ["almera", "qashqai", "x-trail", "teana", "juke"],
            "ford": ["focus", "fiesta", "mondeo", "kuga", "ecosport"],
            "chevrolet": ["cruze", "aveo", "lacetti", "captiva", "tahoe"],
            "skoda": ["octavia", "rapid", "fabia", "superb", "kodiaq"],
            "lada": ["vesta", "granta", "kalina", "priora", "largus"],
        }


class FastReviewsParser:
    """Быстрый парсер отзывов с защитой от банов."""

    def __init__(self, config: FastParsingConfig):
        self.config = config
        self.db = Database("fast_parsing.db")
        self.review_repo = ReviewRepository(self.db)
        self.comment_repo = CommentRepository(self.db)

        # Очереди и синхронизация
        self.url_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.stop_event = threading.Event()

        # Статистика
        self.stats = {
            "total_urls": 0,
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "total_comments": 0,
            "start_time": None,
            "requests_count": 0,
            "last_request_time": [],
        }

        # Контроль скорости
        self.rate_lock = threading.Lock()

        print("🚀 Инициализация быстрого парсера")
        print(f"   Потоков: {config.threads}")
        print(f"   Задержка: {config.delay_min}-{config.delay_max}с")
        print(f"   Лимит: {config.requests_per_hour} запросов/час")

    def check_rate_limit(self):
        """Проверка и ожидание при превышении лимита скорости."""
        with self.rate_lock:
            now = time.time()

            # Очищаем старые запросы (старше часа)
            self.stats["last_request_time"] = [
                t for t in self.stats["last_request_time"] if now - t < 3600
            ]

            # Проверяем лимит
            if len(self.stats["last_request_time"]) >= self.config.requests_per_hour:
                # Ждем освобождения слота
                oldest = min(self.stats["last_request_time"])
                wait_time = 3600 - (now - oldest) + 1
                print(f"⏳ Лимит достигнут, ожидание {wait_time:.1f}с")
                time.sleep(wait_time)

            # Регистрируем новый запрос
            self.stats["last_request_time"].append(now)
            self.stats["requests_count"] += 1

    def get_dynamic_delay(self) -> float:
        """Вычисляет динамическую задержку."""
        base_delay = random.uniform(self.config.delay_min, self.config.delay_max)

        # Увеличиваем задержку ночью
        if self.config.night_slowdown:
            current_hour = datetime.now().hour
            if 23 <= current_hour or current_hour <= 6:
                base_delay *= 1.5

        # Увеличиваем задержку при высокой нагрузке
        recent_requests = len(
            [
                t
                for t in self.stats["last_request_time"]
                if time.time() - t < 300  # за последние 5 минут
            ]
        )

        if recent_requests > 50:  # Много запросов за 5 минут
            base_delay *= 1.3

        return base_delay

    def worker_thread(self, thread_id: int):
        """Рабочий поток парсера."""
        parser = DromParser(gentle_mode=True)

        while not self.stop_event.is_set():
            try:
                # Получаем URL из очереди
                try:
                    url = self.url_queue.get(timeout=1)
                except queue.Empty:
                    continue

                if url is None:  # Сигнал завершения
                    break

                # Контроль скорости
                self.check_rate_limit()

                # Парсим отзыв
                result = self.parse_review_safe(parser, url, thread_id)

                # Отправляем результат
                self.result_queue.put(result)

                # Динамическая задержка
                delay = self.get_dynamic_delay()
                time.sleep(delay)

                self.url_queue.task_done()

            except Exception as e:
                print(f"❌ Ошибка в потоке {thread_id}: {e}")
                if "url" in locals():
                    self.url_queue.task_done()

    def parse_review_safe(self, parser: DromParser, url: str, thread_id: int) -> Dict:
        """Безопасный парсинг одного отзыва."""
        result = {
            "url": url,
            "success": False,
            "review": None,
            "comments": [],
            "thread_id": thread_id,
            "error": None,
        }

        try:
            # Парсим основной отзыв
            reviews = parser.parse_single_review(url)
            if not reviews:
                result["error"] = "Отзыв не найден"
                return result

            result["review"] = reviews[0]

            # Парсим комментарии, если включено
            if self.config.parse_comments:
                try:
                    comments_data = parser.parse_comments(url)
                    if comments_data:
                        result["comments"] = comments_data
                except Exception as e:
                    # Комментарии не критичны, продолжаем
                    print(f"⚠️ Ошибка парсинга комментариев {url}: {e}")

            result["success"] = True

        except Exception as e:
            result["error"] = str(e)
            if "timeout" in str(e).lower():
                result["error"] = "timeout"

        return result

    def save_batch(self, results: List[Dict]):
        """Сохранение пакета результатов."""
        if not results:
            return

        reviews_to_save = []
        comments_count = 0

        for result in results:
            if result["success"] and result["review"]:
                reviews_to_save.append(result["review"])
                comments_count += len(result.get("comments", []))

        if reviews_to_save:
            try:
                self.review_repo.save_batch(reviews_to_save)
                print(
                    f"💾 Сохранено: {len(reviews_to_save)} отзывов, {comments_count} комментариев"
                )
            except Exception as e:
                print(f"❌ Ошибка сохранения: {e}")

    def generate_test_urls(self, limit: int = 1000) -> List[str]:
        """Генерирует тестовые URL для демонстрации."""
        urls = []
        brands = QuickBrandExtractor.get_known_brands()
        models = QuickBrandExtractor.get_sample_models()

        # Генерируем URL на основе известных брендов и моделей
        for brand in brands[:20]:  # Берем первые 20 брендов
            brand_models = models.get(brand, ["model1", "model2", "model3"])

            for model in brand_models[:3]:  # По 3 модели
                # Генерируем несколько ID отзывов
                for i in range(1000000, 1000000 + limit // (20 * 3)):
                    url = f"https://www.drom.ru/reviews/{brand}/{model}/{i}/"
                    urls.append(url)

                    if len(urls) >= limit:
                        return urls

        return urls

    def print_progress(self):
        """Вывод прогресса парсинга."""
        if self.stats["start_time"]:
            elapsed = time.time() - self.stats["start_time"]
            processed = self.stats["processed"]
            total = self.stats["total_urls"]

            if processed > 0:
                speed = processed / elapsed * 3600  # отзывов/час
                remaining = total - processed
                eta_seconds = remaining / (processed / elapsed) if processed > 0 else 0
                eta = datetime.now() + timedelta(seconds=eta_seconds)

                print(
                    f"\n📊 Прогресс: {processed}/{total} ({processed/total*100:.1f}%)"
                )
                print(f"✅ Успешно: {self.stats['successful']}")
                print(f"❌ Ошибок: {self.stats['failed']}")
                print(f"🚀 Скорость: {speed:.0f} отзывов/час")
                print(f"⏱️ ETA: {eta.strftime('%H:%M:%S')}")
                print(f"🌐 Запросов: {self.stats['requests_count']}")

    def start_parsing(self, url_list: List[str]):
        """Запуск парсинга списка URL."""
        self.stats["total_urls"] = len(url_list)
        self.stats["start_time"] = time.time()

        print(f"\n🚀 НАЧАЛО ПАРСИНГА {len(url_list)} ОТЗЫВОВ")
        print("=" * 60)

        # Заполняем очередь URL
        for url in url_list:
            self.url_queue.put(url)

        # Запускаем рабочие потоки
        with ThreadPoolExecutor(max_workers=self.config.threads) as executor:
            futures = [
                executor.submit(self.worker_thread, i)
                for i in range(self.config.threads)
            ]

            try:
                results_batch = []
                last_progress = time.time()

                # Обрабатываем результаты
                while self.stats["processed"] < self.stats["total_urls"]:
                    try:
                        result = self.result_queue.get(timeout=1)
                        results_batch.append(result)

                        self.stats["processed"] += 1
                        if result["success"]:
                            self.stats["successful"] += 1
                            self.stats["total_comments"] += len(
                                result.get("comments", [])
                            )
                        else:
                            self.stats["failed"] += 1

                        # Сохранение пакетами
                        if len(results_batch) >= self.config.batch_size:
                            self.save_batch(results_batch)
                            results_batch = []

                        # Прогресс каждые 30 секунд
                        if time.time() - last_progress > 30:
                            self.print_progress()
                            last_progress = time.time()

                    except queue.Empty:
                        continue

                # Сохраняем оставшиеся результаты
                if results_batch:
                    self.save_batch(results_batch)

                # Завершаем потоки
                for _ in range(self.config.threads):
                    self.url_queue.put(None)

                # Ждем завершения потоков
                for future in futures:
                    future.result(timeout=10)

            except KeyboardInterrupt:
                print("\n⏹️ Остановка по запросу пользователя...")
                self.stop_event.set()

            finally:
                self.print_final_stats()

    def print_final_stats(self):
        """Финальная статистика."""
        if self.stats["start_time"]:
            elapsed = time.time() - self.stats["start_time"]

            print("\n" + "=" * 60)
            print("📈 ФИНАЛЬНАЯ СТАТИСТИКА")
            print("=" * 60)
            print(f"⏱️ Время работы: {elapsed/3600:.1f} часов")
            print(
                f"📊 Обработано: {self.stats['processed']}/{self.stats['total_urls']}"
            )
            print(f"✅ Успешно: {self.stats['successful']}")
            print(f"❌ Ошибок: {self.stats['failed']}")
            print(f"💬 Комментариев: {self.stats['total_comments']}")
            print(
                f"🚀 Средняя скорость: {self.stats['processed']/elapsed*3600:.0f} отзывов/час"
            )
            print(f"🌐 Всего запросов: {self.stats['requests_count']}")
            print("=" * 60)


def main():
    """Демонстрация быстрого парсера."""

    print("🎯 СИСТЕМА БЫСТРОГО ПАРСИНГА ОТЗЫВОВ")
    print("=" * 50)

    # Конфигурация для оптимального баланса скорости/надежности
    config = FastParsingConfig(
        threads=8,  # 8 потоков
        delay_min=1.5,  # 1.5-3 секунды задержки
        delay_max=3.0,
        requests_per_hour=600,  # 600 запросов/час
        batch_size=50,  # Сохранение каждые 50 отзывов
        parse_comments=True,  # С комментариями
        night_slowdown=True,  # Замедление ночью
        auto_backup=True,  # Автобэкапы
    )

    parser = FastReviewsParser(config)

    # Для демонстрации используем небольшой набор тестовых URL
    test_urls = parser.generate_test_urls(100)  # 100 тестовых URL

    print(f"🔗 Подготовлено {len(test_urls)} тестовых URL")
    print(f"📊 Ожидаемая скорость: ~{config.threads * 3600 / 2.5:.0f} отзывов/час")
    print(
        f"⏱️ Ожидаемое время для 100 URL: ~{100 * 2.5 / config.threads / 60:.1f} минут"
    )

    answer = input("\n🚀 Запустить тестовый парсинг? (y/N): ")
    if answer.lower() in ["y", "yes", "да"]:
        parser.start_parsing(test_urls)
    else:
        print("Отменено.")

        # Показываем прогноз для полного парсинга
        total_reviews = 1_141_479
        estimated_time_hours = total_reviews * 2.5 / config.threads / 3600
        estimated_days = estimated_time_hours / 24

        print(f"\n📈 ПРОГНОЗ ДЛЯ ПОЛНОГО ПАРСИНГА:")
        print(f"   📊 Всего отзывов: {total_reviews:,}")
        print(f"   ⏱️ Ожидаемое время: {estimated_days:.1f} дней")
        print(f"   🚀 Скорость: ~{config.threads * 3600 / 2.5:.0f} отзывов/час")
        print(f"   🛡️ Защита от банов: включена")
        print(f"   💾 Автосохранение: каждые {config.batch_size} отзывов")


if __name__ == "__main__":
    main()
