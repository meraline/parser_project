#!/usr/bin/env python3
"""
Отладка логики определения типа контента
"""


def debug_url_type():
    urls = [
        "https://www.drom.ru/reviews/toyota/camry/1428758/",  # основной отзыв
        "https://www.drom.ru/reviews/toyota/camry/1428758/187642/",  # дополнение
    ]

    for url in urls:
        print(f"\nURL: {url}")

        # Текущая логика
        url_parts = url.rstrip("/").split("/")
        print(f"URL parts: {url_parts}")
        print(f"Количество частей: {len(url_parts)}")

        content_type = "review"
        if len(url_parts) > 7:
            content_type = "addition"

        print(f"Тип: {content_type}")


if __name__ == "__main__":
    debug_url_type()
