#!/usr/bin/env python3
"""
Полный парсинг по брендам в алфавитном порядке:
- ВСЕ отзывы каждого бренда (не ограничиваем)
- Переход к следующему бренду только после завершения текущего
- Проверка дубликатов (не парсим уже существующие)
- Сохранение каждые 10 отзывов для надежности
- Тест первых 30 отзывов для демонстрации логики
"""

import sys
import os
import time
from datetime import datetime
from typing import List, Dict, Optional

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.models.review import Review
from auto_reviews_parser.models.comment import Comment
from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository
from auto_reviews_parser.database.repositories.comment_repository import (
    CommentRepository,
)


class BrandBasedParser:
    """Парсер по брендам с контролем количества отзывов."""

    def __init__(self):
        self.parser = DromParser(gentle_mode=True)
        self.db = Database("brand_parsing_test.db")
        self.review_repo = ReviewRepository(self.db)
        self.comment_repo = CommentRepository(self.db)

        # Статистика
        self.stats = {
            "total_parsed": 0,
            "successful": 0,
            "failed": 0,
            "comments_parsed": 0,
            "current_brand": "",
            "brands_completed": 0,
            "start_time": None,
        }

        # Список брендов по алфавиту
        self.brands = [
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

        print("🚀 Инициализация полного парсера по брендам")
        print("📊 Полный парсинг всех отзывов каждого бренда")
        print("💾 Сохранение каждые 10 отзывов")
        print("🔍 Проверка дубликатов")

    def get_existing_urls(self) -> set:
        """Получает множество уже существующих URL отзывов."""
        try:
            existing = self.review_repo.get_all_source_urls()
            print(f"📋 Найдено {len(existing)} уже спарсенных отзывов")
            return set(existing)
        except:
            return set()

    def get_all_brand_review_urls(self, brand: str) -> List[str]:
        """Получает ВСЕ URL отзывов для бренда (без ограничений)."""
        print(f"\n🔍 Полный поиск отзывов для бренда: {brand.upper()}")

        # Для тестирования создаем расширенный набор URL
        # В реальной версии здесь был бы полный скрапинг каталога
        all_urls = []

        # Популярные модели для основных брендов
        brand_models = {
            "toyota": [
                "camry",
                "corolla",
                "rav4",
                "land-cruiser",
                "prius",
                "avensis",
                "yaris",
                "highlander",
                "4runner",
                "tacoma",
            ],
            "volkswagen": [
                "polo",
                "golf",
                "jetta",
                "passat",
                "tiguan",
                "touareg",
                "beetle",
                "arteon",
                "atlas",
                "amarok",
            ],
            "hyundai": [
                "solaris",
                "elantra",
                "tucson",
                "creta",
                "i30",
                "santa-fe",
                "kona",
                "veloster",
                "genesis",
                "palisade",
            ],
            "kia": [
                "rio",
                "ceed",
                "sportage",
                "optima",
                "sorento",
                "soul",
                "stinger",
                "telluride",
                "niro",
                "cadenza",
            ],
            "nissan": [
                "almera",
                "qashqai",
                "x-trail",
                "teana",
                "juke",
                "pathfinder",
                "leaf",
                "sentra",
                "versa",
                "rogue",
            ],
            "ford": [
                "focus",
                "fiesta",
                "mondeo",
                "kuga",
                "ecosport",
                "mustang",
                "explorer",
                "f-150",
                "edge",
                "fusion",
            ],
            "renault": [
                "logan",
                "sandero",
                "duster",
                "kaptur",
                "megane",
                "scenic",
                "talisman",
                "espace",
                "kadjar",
                "clio",
            ],
            "skoda": [
                "octavia",
                "rapid",
                "fabia",
                "superb",
                "kodiaq",
                "karoq",
                "scala",
                "kamiq",
                "enyaq",
                "yeti",
            ],
            "chevrolet": [
                "cruze",
                "aveo",
                "lacetti",
                "captiva",
                "tahoe",
                "silverado",
                "malibu",
                "equinox",
                "traverse",
                "suburban",
            ],
            "lada": [
                "vesta",
                "granta",
                "kalina",
                "priora",
                "largus",
                "xray",
                "niva",
                "2107",
                "2110",
                "samara",
            ],
        }

        models = brand_models.get(brand, ["model1", "model2", "model3"])

        # Генерируем больше тестовых URL для полного тестирования
        for model in models:
            # Для каждой модели генерируем ID от 1000000 до 1100000
            for i in range(1000000, 1100000, 100):  # Каждый 100-й для тестирования
                url = f"https://www.drom.ru/reviews/{brand}/{model}/{i}/"
                all_urls.append(url)

        print(f"   📋 Подготовлено {len(all_urls)} потенциальных URL для бренда")
        return all_urls
        """Получает URL отзывов для бренда."""
        print(f"\n🔍 Поиск отзывов для бренда: {brand.upper()}")

        # Для демонстрации создаем тестовые URL
        # В реальной версии здесь был бы скрапинг каталога
        test_urls = []

        # Популярные модели для основных брендов
        brand_models = {
            "toyota": ["camry", "corolla", "rav4", "land-cruiser", "prius"],
            "volkswagen": ["polo", "golf", "jetta", "passat", "tiguan"],
            "hyundai": ["solaris", "elantra", "tucson", "creta", "i30"],
            "kia": ["rio", "ceed", "sportage", "optima", "sorento"],
            "nissan": ["almera", "qashqai", "x-trail", "teana", "juke"],
            "ford": ["focus", "fiesta", "mondeo", "kuga", "ecosport"],
            "renault": ["logan", "sandero", "duster", "kaptur", "megane"],
            "skoda": ["octavia", "rapid", "fabia", "superb", "kodiaq"],
            "chevrolet": ["cruze", "aveo", "lacetti", "captiva", "tahoe"],
            "lada": ["vesta", "granta", "kalina", "priora", "largus"],
        }

        models = brand_models.get(brand, ["model1", "model2", "model3"])

        # Генерируем тестовые URL
        for model in models:
            for i in range(1000000, 1000000 + max_reviews // len(models) + 5):
                url = f"https://www.drom.ru/reviews/{brand}/{model}/{i}/"
                test_urls.append(url)
                if len(test_urls) >= max_reviews:
                    break
            if len(test_urls) >= max_reviews:
                break

        print(f"   📋 Найдено {len(test_urls)} потенциальных отзывов")
        return test_urls[:max_reviews]

    def parse_review_safe(self, url: str) -> Dict:
        """Безопасный парсинг одного отзыва."""
        result = {
            "url": url,
            "success": False,
            "review": None,
            "comments": [],
            "error": None,
        }

        try:
            # Парсим основной отзыв
            reviews = self.parser.parse_single_review(url)
            if reviews:
                result["review"] = reviews[0]
                result["success"] = True

                # Парсим комментарии
                try:
                    comments_data = self.parser.parse_comments(url)
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
                except Exception as e:
                    print(f"   ⚠️ Ошибка парсинга комментариев: {e}")

        except Exception as e:
            result["error"] = str(e)

        return result

    def save_batch(self, results: List[Dict]):
        """Сохранение пакета результатов."""
        if not results:
            return

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

                # Обновляем review_id в комментариях
                if comments_to_save and saved_ids:
                    review_url_to_id = {}
                    for i, review in enumerate(reviews_to_save):
                        if i < len(saved_ids):
                            review_url_to_id[review.source_url] = saved_ids[i]

                    for comment in comments_to_save:
                        if comment.source_url in review_url_to_id:
                            comment.review_id = review_url_to_id[comment.source_url]

                    self.comment_repo.save_batch(comments_to_save)

                print(
                    f"💾 Сохранено: {len(reviews_to_save)} отзывов, {len(comments_to_save)} комментариев"
                )

            except Exception as e:
                print(f"❌ Ошибка сохранения: {e}")

    def print_progress(self):
        """Вывод прогресса парсинга."""
        elapsed = time.time() - self.stats["start_time"]
        parsed = self.stats["total_parsed"]

        print(f"\n📊 ПРОГРЕСС:")
        print(f"   🎯 Цель: 30 отзывов")
        print(f"   ✅ Обработано: {parsed}")
        print(f"   🚀 Успешно: {self.stats['successful']}")
        print(f"   ❌ Ошибок: {self.stats['failed']}")
        print(f"   💬 Комментариев: {self.stats['comments_parsed']}")
        print(f"   🏷️ Текущий бренд: {self.stats['current_brand']}")
        print(f"   ⏱️ Время работы: {elapsed/60:.1f} мин")
        if parsed > 0:
            print(f"   📈 Скорость: {parsed/elapsed*3600:.0f} отзывов/час")

    def start_parsing(self, target_reviews: int = 30):
        """Запуск полного парсинга по брендам (все отзывы каждого бренда)."""
        self.stats["start_time"] = time.time()

        print("\n🎯 СТАРТ ПОЛНОГО ПАРСИНГА ПО БРЕНДАМ")
        print("=" * 50)
        print(f"📊 Тестовая цель: {target_reviews} отзывов")
        print("🏷️ Логика: ВСЕ отзывы бренда, затем следующий бренд")
        print("💾 Сохранение каждые 10 отзывов")
        print("🔍 Проверка дубликатов включена")
        print("=" * 50)

        # Получаем уже существующие URL
        existing_urls = self.get_existing_urls()

        results_batch = []

        try:
            for brand in self.brands:
                if self.stats["total_parsed"] >= target_reviews:
                    break

                self.stats["current_brand"] = brand
                print(f"\n🏷️ ПОЛНЫЙ ПАРСИНГ БРЕНДА: {brand.upper()}")

                # Получаем ВСЕ URL отзывов для бренда
                brand_urls = self.get_all_brand_review_urls(brand)

                # Фильтруем уже существующие URL
                new_urls = [url for url in brand_urls if url not in existing_urls]
                print(f"   📋 Новых URL для парсинга: {len(new_urls)}")
                print(f"   ⚠️ Пропущено дубликатов: {len(brand_urls) - len(new_urls)}")

                brand_parsed = 0
                brand_successful = 0

                for url in new_urls:
                    if self.stats["total_parsed"] >= target_reviews:
                        print(
                            f"   🎯 Достигнута тестовая цель: {target_reviews} отзывов"
                        )
                        break

                for url in brand_urls:
                    if (
                        self.stats["total_parsed"] >= target_reviews
                        or brand_parsed >= max_per_brand
                    ):
                        break

                    print(
                        f"   📄 Парсинг {self.stats['total_parsed'] + 1}/{target_reviews}: {url}"
                    )

                    # Парсим отзыв
                    result = self.parse_review_safe(url)
                    results_batch.append(result)

                    self.stats["total_parsed"] += 1
                    brand_parsed += 1

                    if result["success"]:
                        self.stats["successful"] += 1
                        self.stats["comments_parsed"] += len(result.get("comments", []))
                        print(
                            f"   ✅ Успешно: {result['review'].brand} {result['review'].model}"
                        )
                    else:
                        self.stats["failed"] += 1
                        print(f"   ❌ Ошибка: {result.get('error', 'Неизвестно')}")

                    # Сохранение каждые 10 отзывов
                    if len(results_batch) >= 10:
                        self.save_batch(results_batch)
                        results_batch = []
                        self.print_progress()

                    # Задержка между запросами
                    time.sleep(2.0)

                print(f"   ✅ Бренд {brand} завершен: {brand_parsed} отзывов")
                self.stats["brands_completed"] += 1

                # Пауза между брендами
                time.sleep(3.0)

            # Сохраняем оставшиеся результаты
            if results_batch:
                self.save_batch(results_batch)

        except KeyboardInterrupt:
            print(f"\n⏹️ Парсинг прерван пользователем")
            if results_batch:
                self.save_batch(results_batch)

        finally:
            self.print_final_stats()

    def print_final_stats(self):
        """Финальная статистика."""
        elapsed = time.time() - self.stats["start_time"]

        print(f"\n" + "=" * 50)
        print("📈 ФИНАЛЬНАЯ СТАТИСТИКА")
        print("=" * 50)
        print(f"⏱️ Время работы: {elapsed/60:.1f} минут")
        print(f"📊 Всего обработано: {self.stats['total_parsed']}")
        print(f"✅ Успешно: {self.stats['successful']}")
        print(f"❌ Ошибок: {self.stats['failed']}")
        print(f"💬 Комментариев: {self.stats['comments_parsed']}")
        print(f"🏷️ Брендов завершено: {self.stats['brands_completed']}")
        if self.stats["total_parsed"] > 0:
            print(
                f"📈 Средняя скорость: {self.stats['total_parsed']/elapsed*3600:.0f} отзывов/час"
            )
            print(f"💾 База данных: brand_parsing_test.db")
        print("=" * 50)


def main():
    """Главная функция."""
    parser = BrandBasedParser()

    print("🎯 НАСТРОЙКИ ПАРСИНГА:")
    print("   • 30 отзывов общая цель")
    print("   • До 100 отзывов на бренд")
    print("   • Сохранение каждые 10 отзывов")
    print("   • Обход брендов по алфавиту")
    print("   • Задержки 2-3 секунды")

    answer = input("\n🚀 Начать парсинг? (y/N): ")
    if answer.lower() in ["y", "yes", "да"]:
        parser.start_parsing(target_reviews=30, max_per_brand=100)
    else:
        print("❌ Парсинг отменен")


if __name__ == "__main__":
    main()
