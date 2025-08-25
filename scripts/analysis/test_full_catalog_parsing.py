#!/usr/bin/env python3
"""
Тестовый парсинг одного бренда из полного каталога
"""
import sqlite3
import sys
import os

# Добавляем путь к src в PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.database.repositories.brand_repository import BrandRepository


def test_parse_single_brand(brand_slug="toyota"):
    """Тестируем парсинг одного бренда"""

    print(f"=== Тестовый парсинг бренда: {brand_slug} ===")

    # Подключаемся к базе
    conn = sqlite3.connect("../../data/databases/auto_reviews.db")
    brand_repo = BrandRepository(conn)

    # Проверяем наличие бренда в базе
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM brands WHERE slug = ?", (brand_slug,))
    brand = cursor.fetchone()

    if not brand:
        print(f"❌ Бренд {brand_slug} не найден в базе данных!")
        # Показываем доступные бренды
        cursor.execute("SELECT slug, name FROM brands ORDER BY name LIMIT 20")
        available = cursor.fetchall()
        print("\nДоступные бренды (первые 20):")
        for slug, name in available:
            print(f"- {name} ({slug})")
        conn.close()
        return False

    brand_id, name, slug, url, created_at, updated_at = brand
    print(f"✅ Бренд найден: {name} ({slug})")
    print(f"URL: {url}")

    # Создаем парсер
    parser = DromParser(base_delay=1, max_delay=3, conn=conn)

    try:
        print(f"\nНачинаем парсинг моделей для бренда {name}...")

        # Парсим модели для этого бренда
        models = parser.parse_models_for_brand(brand_slug)

        print(f"✅ Найдено моделей: {len(models)}")

        if models:
            print("\nПервые 10 моделей:")
            for i, model in enumerate(models[:10]):
                print(
                    f"{i+1}. {model['name']} - {model.get('reviews_count', 0)} отзывов"
                )

            # Проверяем сохранение в базе
            cursor.execute(
                """
                SELECT COUNT(*) FROM models WHERE brand_id = ?
            """,
                (brand_id,),
            )
            saved_count = cursor.fetchone()[0]
            print(f"\nСохранено в базе: {saved_count} моделей")

            # Парсим отзывы для первой модели (тестово)
            if len(models) > 0:
                test_model = models[0]
                print(f"\nТестовый парсинг отзывов для модели: {test_model['name']}")

                # Ограничиваем количество отзывов для теста
                reviews = parser.parse_reviews_for_model(
                    test_model["slug"], max_pages=1
                )

                print(f"✅ Найдено отзывов: {len(reviews)}")

                if reviews:
                    print("\nПример отзыва:")
                    review = reviews[0]
                    print(f"Заголовок: {review.get('title', 'Не указан')}")
                    print(f"Автор: {review.get('author', 'Аноним')}")
                    print(f"Рейтинг: {review.get('rating', 'Не указан')}")
                    content = review.get("content", "")
                    if content:
                        preview = (
                            content[:200] + "..." if len(content) > 200 else content
                        )
                        print(f"Содержание: {preview}")

    except Exception as e:
        print(f"❌ Ошибка при парсинге: {e}")
        return False

    finally:
        conn.close()

    print(f"\n✅ Тестирование бренда {brand_slug} завершено!")
    return True


def show_available_brands():
    """Показываем доступные бренды"""
    conn = sqlite3.connect("../../data/databases/auto_reviews.db")
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM brands")
    total = cursor.fetchone()[0]

    print(f"Всего брендов в базе: {total}")

    # Популярные бренды
    popular_slugs = [
        "toyota",
        "nissan",
        "honda",
        "mazda",
        "mitsubishi",
        "bmw",
        "mercedes-benz",
        "audi",
        "volkswagen",
        "hyundai",
        "kia",
        "ford",
        "chevrolet",
        "lada",
        "uaz",
        "gaz",
    ]

    print("\nПопулярные бренды для тестирования:")
    cursor.execute(
        f"""
        SELECT slug, name FROM brands 
        WHERE slug IN ({','.join('?' * len(popular_slugs))})
        ORDER BY name
    """,
        popular_slugs,
    )

    for slug, name in cursor.fetchall():
        print(f"- {name} ({slug})")

    # Русские бренды
    print("\nРусские бренды:")
    cursor.execute(
        """
        SELECT slug, name FROM brands 
        WHERE name GLOB '*[а-яё]*'
        ORDER BY name
    """
    )

    for slug, name in cursor.fetchall():
        print(f"- {name} ({slug})")

    conn.close()


if __name__ == "__main__":
    print("=== Тестирование парсинга с полным каталогом ===")

    # Получаем аргумент командной строки или используем по умолчанию
    brand_slug = sys.argv[1] if len(sys.argv) > 1 else "toyota"

    if brand_slug == "--list":
        show_available_brands()
    else:
        success = test_parse_single_brand(brand_slug)

        if not success:
            print(f"\nИспользование:")
            print(f"python {sys.argv[0]} [brand_slug]")
            print(f"python {sys.argv[0]} --list  # показать доступные бренды")
            print(f"\nПример:")
            print(f"python {sys.argv[0]} toyota")
            print(f"python {sys.argv[0]} lada")
