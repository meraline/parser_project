#!/usr/bin/env python3
"""
Тестовый парсинг 20 отзывов по алфавиту для оценки времени и планирования полного парсинга.
"""

import sys
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict

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


class BenchmarkParser:
    """Класс для тестового парсинга и оценки производительности."""

    def __init__(self):
        self.parser = DromParser(gentle_mode=True)  # Щадящий режим
        self.db = Database("benchmark_test.db")
        self.review_repo = ReviewRepository(self.db)
        self.comment_repo = CommentRepository(self.db)

        # Статистика
        self.stats = {
            "reviews_parsed": 0,
            "reviews_failed": 0,
            "comments_parsed": 0,
            "total_time": 0,
            "avg_time_per_review": 0,
            "avg_time_per_comment": 0,
            "start_time": None,
            "end_time": None,
        }

    def get_test_urls(self) -> List[str]:
        """Получает 20 тестовых URL отзывов по алфавиту (разные марки)."""

        # Тестовые URL отзывов разных марок для разнообразия
        test_urls = [
            # Acura
            "https://www.drom.ru/reviews/acura/mdx/1445981/",
            # Audi
            "https://www.drom.ru/reviews/audi/a3/1445976/",
            # BMW
            "https://www.drom.ru/reviews/bmw/x3/1445962/",
            # Chery
            "https://www.drom.ru/reviews/chery/tiggo7/1445953/",
            # Daewoo
            "https://www.drom.ru/reviews/daewoo/nexia/1445945/",
            # Ford
            "https://www.drom.ru/reviews/ford/focus/1445937/",
            # Honda
            "https://www.drom.ru/reviews/honda/civic/1445929/",
            # Hyundai
            "https://www.drom.ru/reviews/hyundai/solaris/1445921/",
            # Infiniti
            "https://www.drom.ru/reviews/infiniti/qx50/1445913/",
            # Kia
            "https://www.drom.ru/reviews/kia/rio/1445905/",
            # Lada
            "https://www.drom.ru/reviews/lada/vesta/1445897/",
            # Mazda
            "https://www.drom.ru/reviews/mazda/cx5/1445889/",
            # Mercedes
            "https://www.drom.ru/reviews/mercedes/c_class/1445881/",
            # Nissan
            "https://www.drom.ru/reviews/nissan/qashqai/1445873/",
            # Opel
            "https://www.drom.ru/reviews/opel/astra/1445865/",
            # Peugeot
            "https://www.drom.ru/reviews/peugeot/308/1445857/",
            # Renault
            "https://www.drom.ru/reviews/renault/duster/1445849/",
            # Skoda
            "https://www.drom.ru/reviews/skoda/octavia/1445841/",
            # Toyota
            "https://www.drom.ru/reviews/toyota/camry/1445833/",
            # Volkswagen
            "https://www.drom.ru/reviews/volkswagen/polo/1445825/",
        ]

        return test_urls[:20]  # Берем первые 20

    def parse_single_review_with_timing(self, url: str) -> Dict:
        """Парсит один отзыв с замером времени."""

        result = {
            "url": url,
            "success": False,
            "review": None,
            "comments": [],
            "parse_time": 0,
            "comments_time": 0,
            "error": None,
        }

        print(f"\n🔍 Парсинг: {url}")
        start_time = time.time()

        try:
            # Парсим основной отзыв
            review_start = time.time()
            reviews = self.parser.parse_single_review(url)
            review_end = time.time()

            if not reviews:
                result["error"] = "Отзыв не найден"
                return result

            review = reviews[0]
            result["review"] = review
            result["parse_time"] = review_end - review_start
            result["success"] = True

            print(f"   ✓ Отзыв: {review.brand} {review.model} от {review.author}")
            print(f"   ⏱️ Время парсинга отзыва: {result['parse_time']:.1f}с")

            # Парсим комментарии
            comments_start = time.time()
            comments_data = self.parser.parse_comments(url)
            comments_end = time.time()

            result["comments_time"] = comments_end - comments_start

            if comments_data:
                # Создаем объекты Comment
                comments = []
                for comment_data in comments_data:
                    comment = Comment(
                        review_id=1,  # Временный ID
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
                print(f"   ✓ Комментарии: {len(comments)}")
            else:
                print("   ⚠️ Комментарии не найдены")

            print(f"   ⏱️ Время парсинга комментариев: {result['comments_time']:.1f}с")

        except Exception as e:
            result["error"] = str(e)
            print(f"   ❌ Ошибка: {e}")

        end_time = time.time()
        total_time = end_time - start_time
        print(f"   ⏱️ Общее время: {total_time:.1f}с")

        return result

    def run_benchmark(self):
        """Запускает тестовый парсинг 20 отзывов."""

        print("🚀 ЗАПУСК ТЕСТОВОГО ПАРСИНГА 20 ОТЗЫВОВ")
        print("=" * 70)
        print(f"Режим: щадящий (задержки между запросами)")
        print(f"Время начала: {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 70)

        test_urls = self.get_test_urls()
        self.stats["start_time"] = time.time()

        results = []

        for i, url in enumerate(test_urls, 1):
            print(f"\n📊 Прогресс: {i}/20")

            result = self.parse_single_review_with_timing(url)
            results.append(result)

            if result["success"]:
                self.stats["reviews_parsed"] += 1
                if result["comments"]:
                    self.stats["comments_parsed"] += len(result["comments"])
            else:
                self.stats["reviews_failed"] += 1

            # Задержка между запросами (щадящий режим)
            if i < len(test_urls):
                print("   💤 Пауза 3 секунды...")
                time.sleep(3)

        self.stats["end_time"] = time.time()
        self.stats["total_time"] = self.stats["end_time"] - self.stats["start_time"]

        return results

    def analyze_results(self, results: List[Dict]):
        """Анализирует результаты и делает прогнозы."""

        print("\n" + "=" * 70)
        print("📈 АНАЛИЗ РЕЗУЛЬТАТОВ")
        print("=" * 70)

        successful_results = [r for r in results if r["success"]]

        if successful_results:
            # Среднее время на отзыв
            avg_review_time = sum(r["parse_time"] for r in successful_results) / len(
                successful_results
            )
            avg_comments_time = sum(
                r["comments_time"] for r in successful_results
            ) / len(successful_results)
            avg_total_time = avg_review_time + avg_comments_time

            self.stats["avg_time_per_review"] = avg_total_time

            print(f"✅ Успешно обработано: {self.stats['reviews_parsed']}/20 отзывов")
            print(f"❌ Неудачных попыток: {self.stats['reviews_failed']}")
            print(f"💬 Всего комментариев: {self.stats['comments_parsed']}")
            print(f"⏱️ Общее время тестирования: {self.stats['total_time']:.1f} секунд")
            print(f"⏱️ Среднее время на отзыв: {avg_review_time:.1f}с")
            print(f"⏱️ Среднее время на комментарии: {avg_comments_time:.1f}с")
            print(f"⏱️ Среднее общее время: {avg_total_time:.1f}с")

            # Прогноз для полного парсинга
            self.make_forecast()
        else:
            print("❌ Нет успешных результатов для анализа")

    def make_forecast(self):
        """Делает прогноз времени полного парсинга."""

        print("\n" + "=" * 70)
        print("🔮 ПРОГНОЗ ПОЛНОГО ПАРСИНГА")
        print("=" * 70)

        total_reviews = 1_141_479
        avg_time_per_review = self.stats["avg_time_per_review"]

        # Прогноз времени
        total_seconds = total_reviews * avg_time_per_review
        total_hours = total_seconds / 3600
        total_days = total_hours / 24

        print(f"📊 Общее количество отзывов: {total_reviews:,}")
        print(f"⏱️ Среднее время на отзыв: {avg_time_per_review:.1f} секунд")
        print(f"⏱️ Прогнозируемое время:")
        print(f"   • Секунд: {total_seconds:,.0f}")
        print(f"   • Часов: {total_hours:,.1f}")
        print(f"   • Дней: {total_days:.1f}")

        # Рекомендации по параллелизации
        print(f"\n🚀 РЕКОМЕНДАЦИИ:")

        if total_days > 30:
            print("⚠️ Слишком долго для одного потока!")

            # Рассчитываем необходимое количество потоков для разумного времени
            target_days = 7  # Цель: 7 дней
            needed_threads = int(total_days / target_days) + 1

            print(
                f"💡 Для парсинга за {target_days} дней нужно ~{needed_threads} потоков"
            )
            print(f"💡 Рекомендуется использовать 5-10 потоков с ротацией IP")

        print(f"\n🛡️ ЗАЩИТА ОТ БАНОВ:")
        print(f"   • Используйте задержки 2-5 секунд между запросами")
        print(f"   • Ротация User-Agent'ов")
        print(f"   • Ротация IP-адресов/прокси")
        print(f"   • Парсинг в ночное время")
        print(f"   • Мониторинг ответов сервера")

        # Экономия времени при парсинге только отзывов (без комментариев)
        if self.stats["comments_parsed"] > 0:
            review_only_time = total_reviews * (
                self.stats["avg_time_per_review"]
                - (
                    self.stats["total_time"] / self.stats["reviews_parsed"]
                    - self.stats["avg_time_per_review"]
                )
            )
            review_only_days = review_only_time / 3600 / 24

            print(f"\n💡 БЕЗ КОММЕНТАРИЕВ:")
            print(f"   • Время парсинга: {review_only_days:.1f} дней")
            print(f"   • Экономия времени: {total_days - review_only_days:.1f} дней")


def main():
    """Главная функция."""

    try:
        benchmark = BenchmarkParser()
        results = benchmark.run_benchmark()
        benchmark.analyze_results(results)

        print("\n" + "=" * 70)
        print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
        print("=" * 70)

    except KeyboardInterrupt:
        print("\n\n❌ Тестирование прервано пользователем")
    except Exception as e:
        print(f"\n\n❌ Ошибка: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
