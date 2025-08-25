#!/usr/bin/env python3
"""
Скрипт для загрузки всех моделей по каждому бренду в нормализованную БД
"""
import sqlite3
import requests
import time
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin


class ModelExtractor:
    """Класс для извлечения моделей из страниц брендов"""

    def __init__(self, delay=1.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                )
            }
        )

    def extract_models_from_brand_page(self, brand_url, brand_name):
        """Извлекаем модели с страницы бренда"""
        try:
            print(f"Парсим модели для {brand_name}: {brand_url}")
            response = self.session.get(brand_url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            models = []

            # Ищем блоки с моделями - они обычно в списке или таблице
            pattern = re.compile(r"/reviews/[^/]+/[^/]+/?$")
            model_links = soup.find_all("a", href=pattern)

            for link in model_links:
                href = link.get("href")
                if not href:
                    continue

                # Извлекаем slug модели из URL
                parts = href.strip("/").split("/")
                if len(parts) >= 3 and parts[1] == "reviews":
                    model_slug = parts[2]

                    # Получаем название модели
                    model_name = link.get_text(strip=True)
                    if not model_name:
                        continue

                    # Ищем количество отзывов
                    reviews_count = 0
                    count_pattern = re.compile(r"\d+")
                    count_elem = link.find_next("span", string=count_pattern)
                    if count_elem:
                        count_text = count_elem.get_text(strip=True)
                        numbers = re.findall(r"\d+", count_text)
                        if numbers:
                            reviews_count = int(numbers[0])

                    # Полный URL
                    full_url = urljoin(brand_url, href)

                    models.append(
                        {
                            "name": model_name,
                            "slug": model_slug,
                            "url": full_url,
                            "reviews_count": reviews_count,
                        }
                    )

            # Убираем дубликаты по slug
            unique_models = {}
            for model in models:
                if model["slug"] not in unique_models:
                    unique_models[model["slug"]] = model

            result = list(unique_models.values())
            print(f"Найдено моделей: {len(result)}")

            return result

        except Exception as e:
            print(f"Ошибка при парсинге {brand_name}: {e}")
            return []

    def extract_models_from_noscript(self, html_content):
        """Извлекаем модели из noscript блока если есть"""
        models = []

        # Ищем noscript блок
        pattern = r"<noscript>(.*?)</noscript>"
        noscript_match = re.search(pattern, html_content, re.DOTALL)
        if not noscript_match:
            return models

        noscript_content = noscript_match.group(1)

        # Паттерн для ссылок на модели
        link_pattern = (
            r'<a href="https://www\.drom\.ru/reviews/[^/]+/([^/"]+)/">' r"([^<]+)</a>"
        )

        matches = re.findall(link_pattern, noscript_content)

        for slug, name in matches:
            models.append(
                {
                    "name": name.strip(),
                    "slug": slug,
                    "url": f"https://www.drom.ru/reviews/{slug}/",
                    "reviews_count": 0,
                }
            )

        return models


def get_brands_from_db():
    """Получаем все бренды из базы данных"""
    conn = sqlite3.connect("../../data/databases/auto_reviews.db")
    cursor = conn.cursor()

    cursor.execute("SELECT id, name, slug, url FROM brands ORDER BY name")
    brands = cursor.fetchall()

    conn.close()
    return brands


def save_models_to_db(brand_id, models):
    """Сохраняем модели в базу данных"""
    if not models:
        return 0

    conn = sqlite3.connect("../../data/databases/auto_reviews.db")
    cursor = conn.cursor()

    inserted_count = 0

    for model in models:
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO models
                (brand_id, name, slug, url, reviews_count)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    brand_id,
                    model["name"],
                    model["slug"],
                    model["url"],
                    model["reviews_count"],
                ),
            )

            if cursor.rowcount > 0:
                inserted_count += 1

        except Exception as e:
            print(f"Ошибка при добавлении модели {model['name']}: {e}")

    conn.commit()
    conn.close()

    return inserted_count


def load_all_models():
    """Загружаем модели для всех брендов"""
    brands = get_brands_from_db()
    extractor = ModelExtractor(delay=1.5)  # Задержка между запросами

    total_models = 0
    processed_brands = 0

    print(f"Начинаем загрузку моделей для {len(brands)} брендов...")

    for brand_id, brand_name, brand_slug, brand_url in brands:
        processed_brands += 1

        print(f"\n[{processed_brands}/{len(brands)}] " f"Обрабатываем: {brand_name}")

        # Извлекаем модели
        models = extractor.extract_models_from_brand_page(brand_url, brand_name)

        # Сохраняем в БД
        if models:
            inserted = save_models_to_db(brand_id, models)
            total_models += inserted
            print(f"Добавлено моделей: {inserted}")
        else:
            print("Модели не найдены")

        # Задержка между запросами
        if processed_brands < len(brands):
            time.sleep(extractor.delay)

    print("\n=== Загрузка завершена ===")
    print(f"Обработано брендов: {processed_brands}")
    print(f"Добавлено моделей: {total_models}")

    return total_models


def verify_models_in_db():
    """Проверяем загруженные модели"""
    conn = sqlite3.connect("../../data/databases/auto_reviews.db")
    cursor = conn.cursor()

    # Общая статистика
    cursor.execute("SELECT COUNT(*) FROM models")
    models_count = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT b.name, COUNT(m.id) as model_count
        FROM brands b
        LEFT JOIN models m ON b.id = m.brand_id
        GROUP BY b.id, b.name
        HAVING model_count > 0
        ORDER BY model_count DESC
        LIMIT 10
    """
    )
    top_brands = cursor.fetchall()

    cursor.execute(
        """
        SELECT b.name, COUNT(m.id) as model_count
        FROM brands b
        LEFT JOIN models m ON b.id = m.brand_id
        GROUP BY b.id, b.name
        HAVING model_count = 0
        ORDER BY b.name
    """
    )
    empty_brands = cursor.fetchall()

    print("\nСтатистика моделей:")
    print(f"Всего моделей в БД: {models_count}")

    print("\nТоп-10 брендов по количеству моделей:")
    for brand_name, count in top_brands:
        print(f"- {brand_name}: {count} моделей")

    if empty_brands:
        print(f"\nБренды без моделей ({len(empty_brands)}):")
        for brand_name, _ in empty_brands[:10]:
            print(f"- {brand_name}")
        if len(empty_brands) > 10:
            print(f"... и еще {len(empty_brands) - 10} брендов")

    # Примеры моделей
    cursor.execute(
        """
        SELECT b.name as brand, m.name as model, m.reviews_count
        FROM models m
        JOIN brands b ON m.brand_id = b.id
        ORDER BY m.reviews_count DESC
        LIMIT 10
    """
    )
    top_models = cursor.fetchall()

    if top_models:
        print("\nТоп-10 моделей по количеству отзывов:")
        for brand, model, count in top_models:
            print(f"- {brand} {model}: {count} отзывов")

    conn.close()


if __name__ == "__main__":
    print("=== Загрузка моделей для всех брендов ===")

    # Проверяем что бренды загружены
    brands = get_brands_from_db()
    if not brands:
        print("❌ Сначала загрузите бренды в базу данных!")
        exit(1)

    print(f"Найдено брендов в БД: {len(brands)}")

    # Запрашиваем подтверждение для долгой операции
    response = input(
        f"Начать загрузку моделей для {len(brands)} брендов? "
        f"Это может занять длительное время. (y/N): "
    )

    if response.lower() != "y":
        print("Операция отменена")
        exit(0)

    try:
        total_models = load_all_models()
        verify_models_in_db()

        if total_models > 0:
            print(f"\n✅ Успешно загружено {total_models} моделей!")
        else:
            print(f"\n⚠️  Модели не найдены")

    except KeyboardInterrupt:
        print(f"\n⚠️  Операция прервана пользователем")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
