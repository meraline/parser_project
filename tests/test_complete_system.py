#!/usr/bin/env python3
"""
🧪 ТЕСТИРОВАНИЕ ПОЛНОЙ СИСТЕМЫ ПАРСИНГА ОТЗЫВОВ DROM.RU
====================================================

Комплексный тест системы парсинга с поддержкой длинных и коротких отзывов.

Автор: AI Assistant
Дата: 2024
"""

import logging
import time
from database_schema import DatabaseManager
from drom_reviews_parser import DromReviewsParser
from simple_catalog_initializer import SimpleCatalogInitializer


def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("test_complete_system.log", encoding="utf-8"),
        ],
    )
    return logging.getLogger(__name__)


def test_database_creation():
    """Тест создания базы данных"""
    print("🗄️ ТЕСТ: Создание базы данных")
    print("-" * 40)

    db_manager = DatabaseManager()
    db_manager.create_database()

    stats = db_manager.get_database_stats()
    print(f"✅ База данных создана")
    print(f"   Брендов: {stats.get('brands', 0)}")
    print(f"   Моделей: {stats.get('models', 0)}")
    print(f"   Отзывов: {stats.get('reviews', 0)}")
    print()

    return db_manager


def test_catalog_initialization(db_manager, logger):
    """Тест инициализации каталога брендов"""
    print("🗂️ ТЕСТ: Инициализация каталога")
    print("-" * 40)

    # Минимальный HTML для тестирования
    test_html = """
    <div class="_1ggdsc70 _1ggdsc71 css-10ib5jr" data-ftid="component_cars-list">
        <div class="_1ggdsc73">
            <div class="frg44i0">
                <img alt="" class="frg44i7 frg44ib frg44ia" src="https://c.rdrom.ru/js/bundles/media/toyota.png">
                <div class="frg44i1 frg44i2">
                    <span data-ftid="component_cars-list-item_name" class="css-1tc5ro3 e162wx9x0">Toyota</span>
                    <span class="frg44i5" data-ftid="component_cars-list-item_counter"> <!-- -->282196</span>
                </div>
                <a class="frg44i6" data-ftid="component_cars-list-item_hidden-link" href="https://www.drom.ru/reviews/toyota/">Toyota</a>
            </div>
            <div class="frg44i0">
                <img alt="" class="frg44i7 frg44ib frg44ia" src="https://c.rdrom.ru/js/bundles/media/mazda.png">
                <div class="frg44i1 frg44i2">
                    <span data-ftid="component_cars-list-item_name" class="css-1tc5ro3 e162wx9x0">Mazda</span>
                    <span class="frg44i5" data-ftid="component_cars-list-item_counter"> <!-- -->44052</span>
                </div>
                <a class="frg44i6" data-ftid="component_cars-list-item_hidden-link" href="https://www.drom.ru/reviews/mazda/">Mazda</a>
            </div>
        </div>
    </div>
    """

    try:
        initializer = SimpleCatalogInitializer()

        # Тестируем только извлечение брендов из HTML
        brands = initializer.parse_brands_from_html(test_html)

        print(f"✅ Извлечено {len(brands)} брендов из HTML")

        # Сохраняем бренды
        saved_brands = initializer.save_brands(brands)
        print(f"✅ Сохранено {saved_brands} брендов в БД")

        # Добавляем тестовые модели вручную для демонстрации
        test_models = [
            {
                "brand_slug": "toyota",
                "model_slug": "camry",
                "model_name": "Camry",
                "review_count": 1234,
                "url": "https://www.drom.ru/reviews/toyota/camry/",
            },
            {
                "brand_slug": "mazda",
                "model_slug": "familia",
                "model_name": "Familia",
                "review_count": 567,
                "url": "https://www.drom.ru/reviews/mazda/familia/",
            },
        ]

        saved_models = 0
        for model in test_models:
            result = db_manager.add_model(
                brand_slug=model["brand_slug"],
                model_slug=model["model_slug"],
                model_name=model["model_name"],
                review_count=model["review_count"],
                url=model["url"],
            )
            if result:
                saved_models += 1

        print(f"✅ Добавлено {saved_models} тестовых моделей")
        print()

        return True

    except Exception as e:
        logger.error(f"❌ Ошибка инициализации каталога: {e}")
        print(f"❌ Ошибка: {e}")
        print()
        return False


def test_reviews_parsing(db_manager, logger):
    """Тест парсинга отзывов"""
    print("📝 ТЕСТ: Парсинг отзывов")
    print("-" * 40)

    try:
        parser = DromReviewsParser(delay=2.0)

        # Получаем список моделей для тестирования
        models = db_manager.get_all_models()

        if not models:
            print("❌ Нет моделей в базе данных для тестирования")
            return False

        # Тестируем парсинг на первой модели
        test_model = models[0]
        brand_slug = test_model[1]  # brand_slug
        model_slug = test_model[2]  # model_slug
        model_name = test_model[3]  # model_name

        print(f"🚗 Тестируем парсинг: {brand_slug} {model_name}")

        # Тестируем парсинг длинных отзывов (ограничиваем 1 страницей)
        print("   📄 Длинные отзывы...")
        long_reviews = parser.parse_long_reviews(brand_slug, model_slug, max_pages=1)

        print(f"   ✅ Получено {len(long_reviews)} длинных отзывов")

        # Тестируем парсинг коротких отзывов (ограничиваем 1 страницей)
        print("   📄 Короткие отзывы...")
        short_reviews = parser.parse_short_reviews(brand_slug, model_slug, max_pages=1)

        print(f"   ✅ Получено {len(short_reviews)} коротких отзывов")

        # Показываем примеры данных
        if long_reviews:
            review = long_reviews[0]
            print(f"   📋 Пример длинного отзыва:")
            print(f"      ID: {review.get('review_id', 'N/A')}")
            print(f"      Год: {review.get('car_year', 'N/A')}")
            print(f"      Двигатель: {review.get('car_volume', 'N/A')}л")
            print(f"      Плюсы: {review.get('pros', 'N/A')[:50]}...")

        if short_reviews:
            review = short_reviews[0]
            print(f"   📋 Пример короткого отзыва:")
            print(f"      ID: {review.get('review_id', 'N/A')}")
            print(f"      Год: {review.get('car_year', 'N/A')}")
            print(f"      Город: {review.get('city', 'N/A')}")
            print(f"      Плюсы: {review.get('pros', 'N/A')[:50]}...")

        print()
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка парсинга отзывов: {e}")
        print(f"❌ Ошибка: {e}")
        print()
        return False


def test_database_operations(db_manager, logger):
    """Тест операций с базой данных"""
    print("🗄️ ТЕСТ: Операции с базой данных")
    print("-" * 40)

    try:
        # Тестируем добавление отзыва
        # Тестируем добавление отзыва
        test_review = {
            "brand_slug": "toyota",
            "model_slug": "camry",
            "review_id": "test_12345",
            "review_type": "long",
            "author": "test_user",
            "car_year": 2020,
            "car_volume": 2.0,
            "car_fuel_type": "бензин",
            "car_transmission": "автомат",
            "car_drive": "передний",
            "city": "Москва",
            "pros": "Отличная машина",
            "cons": "Дорогой сервис",
            "general_impression": "Рекомендую",
            "breakages": None,
            "photos": "[]",
            "url": "https://www.drom.ru/reviews/toyota/camry/test/",
        }

        result = db_manager.add_review(**test_review)

        if result:
            print("✅ Тестовый отзыв добавлен")
        else:
            print("❌ Ошибка добавления тестового отзыва")
            return False

        # Тестируем получение статистики
        stats = db_manager.get_database_stats()
        print(f"✅ Статистика базы данных:")
        print(f"   Брендов: {stats.get('brands', 0)}")
        print(f"   Моделей: {stats.get('models', 0)}")
        print(f"   Отзывов: {stats.get('reviews', 0)}")
        print(f"   Длинных отзывов: {stats.get('long_reviews', 0)}")
        print(f"   Коротких отзывов: {stats.get('short_reviews', 0)}")

        # Тестируем получение отзывов модели
        model_reviews = db_manager.get_model_reviews_count("toyota", "camry", "long")
        print(f"✅ Длинных отзывов Toyota Camry: {model_reviews}")

        print()
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка операций с БД: {e}")
        print(f"❌ Ошибка: {e}")
        print()
        return False


def main():
    """Основная функция тестирования"""
    print("🧪 КОМПЛЕКСНОЕ ТЕСТИРОВАНИЕ СИСТЕМЫ ПАРСИНГА")
    print("=" * 60)
    print()

    logger = setup_logging()

    # 1. Тест создания базы данных
    db_manager = test_database_creation()

    # 2. Тест инициализации каталога
    catalog_ok = test_catalog_initialization(db_manager, logger)

    # 3. Тест операций с базой данных
    db_ops_ok = test_database_operations(db_manager, logger)

    # 4. Тест парсинга отзывов (только если есть интернет)
    try:
        reviews_ok = test_reviews_parsing(db_manager, logger)
    except Exception as e:
        logger.warning(f"Пропускаем тест парсинга: {e}")
        reviews_ok = None

    # Итоговый отчет
    print("📊 ИТОГОВЫЙ ОТЧЕТ")
    print("=" * 30)
    print(f"✅ База данных: {'ОК' if db_manager else 'ОШИБКА'}")
    print(f"✅ Каталог: {'ОК' if catalog_ok else 'ОШИБКА'}")
    print(f"✅ Операции БД: {'ОК' if db_ops_ok else 'ОШИБКА'}")
    print(
        f"✅ Парсинг: {'ОК' if reviews_ok else 'ПРОПУЩЕН' if reviews_ok is None else 'ОШИБКА'}"
    )

    # Финальная статистика
    final_stats = db_manager.get_database_stats()
    print(f"\n📈 ФИНАЛЬНАЯ СТАТИСТИКА:")
    print(f"Брендов: {final_stats.get('brands', 0)}")
    print(f"Моделей: {final_stats.get('models', 0)}")
    print(f"Отзывов: {final_stats.get('reviews', 0)}")
    print(f"  - длинных: {final_stats.get('long_reviews', 0)}")
    print(f"  - коротких: {final_stats.get('short_reviews', 0)}")

    if all([db_manager, catalog_ok, db_ops_ok]):
        print("\n🎉 СИСТЕМА ГОТОВА К РАБОТЕ!")
    else:
        print("\n⚠️ ОБНАРУЖЕНЫ ПРОБЛЕМЫ - ТРЕБУЕТСЯ ДОРАБОТКА")


if __name__ == "__main__":
    main()
