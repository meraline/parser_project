#!/usr/bin/env python3
"""
Тест длинных отзывов для мастер-парсера
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser, ModelInfo, BrandInfo
from dataclasses import dataclass

@dataclass
class BrandInfoFixed:
    """Информация о бренде"""
    name: str
    url: str
    reviews_count: int
    url_name: str

def test_long_reviews():
    parser = MasterDromParser()
    
    # Тестируем на Audi 100
    audi_100 = ModelInfo(
        name="100",
        brand="audi",
        url="https://www.drom.ru/reviews/audi/100/",
        url_name="100",
        long_reviews_count=69,
        short_reviews_count=69
    )
    
    print(f"🧪 Тестируем длинные отзывы для Audi {audi_100.name}")
    print(f"📍 URL: {audi_100.url}")
    
    # Получаем длинные отзывы с лимитом
    long_reviews = parser.parse_long_reviews(audi_100, limit=3)
    
    print(f"\n📊 Результаты:")
    print(f"✅ Получено {len(long_reviews)} длинных отзывов")
    
    for i, review in enumerate(long_reviews, 1):
        print(f"\n--- Отзыв {i} ---")
        print(f"ID: {review.review_id}")
        print(f"Автор: {getattr(review, 'author', 'Неизвестно')}")
        print(f"Дата: {getattr(review, 'date', 'Неизвестно')}")
        print(f"Город: {getattr(review, 'city', 'Неизвестно')}")
        print(f"Заголовок: {getattr(review, 'title', 'Нет заголовка')}")
        print(f"Рейтинг: {getattr(review, 'rating', 'Нет рейтинга')}")
        print(f"Плюсы: {getattr(review, 'positive_text', 'Нет')[:100]}...")
        print(f"Минусы: {getattr(review, 'negative_text', 'Нет')[:100]}...")

if __name__ == "__main__":
    test_long_reviews()
