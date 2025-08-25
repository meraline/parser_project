#!/usr/bin/env python3
"""
Скрипт для сравнения и анализа каталога брендов
"""
import json
import sqlite3
from pathlib import Path


def compare_with_old_catalog():
    """Сравниваем новый каталог со старым HTML файлом"""

    # Читаем старый HTML файл
    old_html_file = Path("data/brands_html.txt")
    if not old_html_file.exists():
        print("Старый HTML файл не найден")
        return

    with open(old_html_file, "r", encoding="utf-8") as f:
        old_html = f.read()

    # Извлекаем бренды из старого файла
    from src.auto_reviews_parser.utils.brand_extractor import BrandExtractor

    old_brands = BrandExtractor.extract_brands_from_noscript(old_html)

    # Читаем новый каталог
    with open("full_brands_catalog.json", "r", encoding="utf-8") as f:
        new_brands = json.load(f)

    print("=== Сравнение каталогов ===")
    print(f"Старый каталог: {len(old_brands)} брендов")
    print(f"Новый каталог: {len(new_brands)} брендов")
    print(f"Прирост: +{len(new_brands) - len(old_brands)} брендов")

    # Сравниваем списки
    old_slugs = {brand["slug"] for brand in old_brands}
    new_slugs = {brand["slug"] for brand in new_brands}

    # Найдем новые бренды
    new_brand_slugs = new_slugs - old_slugs
    if new_brand_slugs:
        print(f"\nНовые бренды ({len(new_brand_slugs)}):")
        new_brands_dict = {b["slug"]: b for b in new_brands}
        for slug in sorted(new_brand_slugs):
            brand = new_brands_dict[slug]
            print(f"- {brand['name']} ({slug})")

    # Найдем отсутствующие бренды
    missing_slugs = old_slugs - new_slugs
    if missing_slugs:
        print(f"\nОтсутствующие бренды ({len(missing_slugs)}):")
        old_brands_dict = {b["slug"]: b for b in old_brands}
        for slug in sorted(missing_slugs):
            brand = old_brands_dict[slug]
            print(f"- {brand['name']} ({slug})")


def analyze_brand_catalog():
    """Анализируем каталог брендов"""

    # Читаем каталог
    with open("full_brands_catalog.json", "r", encoding="utf-8") as f:
        brands = json.load(f)

    print("\n=== Анализ каталога ===")

    # Группируем по первой букве
    by_letter = {}
    for brand in brands:
        first_letter = brand["name"][0].upper()
        if first_letter not in by_letter:
            by_letter[first_letter] = []
        by_letter[first_letter].append(brand)

    print("Распределение по буквам:")
    for letter in sorted(by_letter.keys()):
        count = len(by_letter[letter])
        print(f"{letter}: {count} брендов")

    # Анализируем популярные бренды
    popular_brands = [
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

    found_popular = []
    brand_slugs = {b["slug"]: b for b in brands}

    for slug in popular_brands:
        if slug in brand_slugs:
            found_popular.append(brand_slugs[slug]["name"])
        else:
            print(f"⚠️  Популярный бренд не найден: {slug}")

    print(f"\nПопулярные бренды найдены ({len(found_popular)}):")
    for name in found_popular:
        print(f"✅ {name}")


def check_database_integrity():
    """Проверяем целостность данных в базе"""

    conn = sqlite3.connect("auto_reviews.db")
    cursor = conn.cursor()

    print("\n=== Проверка базы данных ===")

    # Проверяем дубликаты
    cursor.execute(
        """
        SELECT name, COUNT(*) as count
        FROM brands
        GROUP BY name
        HAVING count > 1
    """
    )
    duplicates = cursor.fetchall()

    if duplicates:
        print("⚠️  Найдены дубликаты:")
        for name, count in duplicates:
            print(f"- {name}: {count} записей")
    else:
        print("✅ Дубликатов не найдено")

    # Проверяем пустые значения
    cursor.execute(
        """
        SELECT COUNT(*) FROM brands 
        WHERE name IS NULL OR name = '' 
        OR slug IS NULL OR slug = ''
        OR url IS NULL OR url = ''
    """
    )
    empty_count = cursor.fetchone()[0]

    if empty_count > 0:
        print(f"⚠️  Найдено {empty_count} записей с пустыми полями")
    else:
        print("✅ Пустых полей не найдено")

    # Проверяем корректность URL
    cursor.execute(
        """
        SELECT name, url FROM brands 
        WHERE url NOT LIKE 'https://www.drom.ru/reviews/%'
    """
    )
    bad_urls = cursor.fetchall()

    if bad_urls:
        print("⚠️  Некорректные URL:")
        for name, url in bad_urls:
            print(f"- {name}: {url}")
    else:
        print("✅ Все URL корректны")

    # Показываем статистику по длине названий
    cursor.execute(
        """
        SELECT MIN(LENGTH(name)), MAX(LENGTH(name)), AVG(LENGTH(name))
        FROM brands
    """
    )
    min_len, max_len, avg_len = cursor.fetchone()
    print(f"\nСтатистика названий:")
    print(f"- Самое короткое: {min_len} символов")
    print(f"- Самое длинное: {max_len} символов")
    print(f"- Средняя длина: {avg_len:.1f} символов")

    # Найдем самые длинные и короткие названия
    cursor.execute(
        """
        SELECT name FROM brands 
        WHERE LENGTH(name) = (SELECT MIN(LENGTH(name)) FROM brands)
        LIMIT 5
    """
    )
    shortest = [row[0] for row in cursor.fetchall()]

    cursor.execute(
        """
        SELECT name FROM brands 
        WHERE LENGTH(name) = (SELECT MAX(LENGTH(name)) FROM brands)
        LIMIT 5
    """
    )
    longest = [row[0] for row in cursor.fetchall()]

    print(f"Самые короткие: {', '.join(shortest)}")
    print(f"Самые длинные: {', '.join(longest)}")

    conn.close()


if __name__ == "__main__":
    print("=== Анализ и проверка каталога брендов ===")

    compare_with_old_catalog()
    analyze_brand_catalog()
    check_database_integrity()

    print("\n✅ Анализ завершен!")
