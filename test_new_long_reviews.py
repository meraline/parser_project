#!/usr/bin/env python3
"""
Тест новой логики парсинга длинных отзывов с индивидуальным подходом
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser, ModelInfo, BrandInfo

def test_new_long_reviews_logic():
    """Тестирует новую логику парсинга длинных отзывов"""
    
    print("🔍 Тестируем новую логику парсинга длинных отзывов")
    
    # Создаем парсер
    parser = MasterDromParser(delay=1.0)
    
    # Создаем тестовую модель
    brand = BrandInfo(
        name="AITO",
        url="https://www.drom.ru/reviews/aito/",
        reviews_count=85,
        url_name="aito"
    )
    
    model = ModelInfo(
        brand="AITO",
        name="M7",
        url="https://www.drom.ru/reviews/aito/m7/",
        long_reviews_count=25,
        url_name="m7"
    )
    
    print(f"📱 Тестовая модель: {model.brand} {model.name}")
    print(f"🔗 URL: {model.url}")
    
    # 1. Тестируем получение URL отзывов
    print(f"\n1️⃣ Получение URL отзывов...")
    review_urls = parser._get_review_urls_from_list_page(model)
    
    print(f"✅ Найдено {len(review_urls)} URL отзывов")
    if review_urls:
        print(f"📋 Первые 3 URL:")
        for i, url in enumerate(review_urls[:3], 1):
            print(f"  {i}. {url}")
    
    # 2. Тестируем парсинг одного отзыва
    if review_urls:
        print(f"\n2️⃣ Парсинг первого отзыва...")
        first_review_url = review_urls[0]
        print(f"🔗 URL отзыва: {first_review_url}")
        
        review_data = parser._parse_individual_review_page(first_review_url, model)
        
        if review_data:
            print(f"✅ Отзыв успешно обработан:")
            print(f"  📝 Заголовок: {review_data.get('title', 'Не найден')}")
            print(f"  👤 Автор: {review_data.get('author', 'Не найден')}")
            print(f"  ⭐ Рейтинг: {review_data.get('rating', 'Не найден')}")
            print(f"  📖 Контент (начало): {str(review_data.get('content', ''))[:100]}...")
            print(f"  📅 Дата: {review_data.get('date', 'Не найдена')}")
            print(f"  📸 Фотографий: {review_data.get('photos_count', 0)}")
        else:
            print(f"❌ Не удалось обработать отзыв")
    
    # 3. Тестируем полный процесс (ограничиваем 2 отзывами)
    print(f"\n3️⃣ Полный процесс парсинга (лимит 2 отзыва)...")
    reviews = parser.parse_long_reviews(model, limit=2)
    
    print(f"✅ Получено {len(reviews)} отзывов")
    for i, review in enumerate(reviews, 1):
        print(f"  {i}. {review.get('title', 'Без заголовка')[:50]}...")
        print(f"     Автор: {review.get('author', 'Неизвестен')}")
        print(f"     Рейтинг: {review.get('rating', 'Нет')}")
    
    print(f"\n📊 Результат тестирования:")
    print(f"  URL найдено: {len(review_urls)}")
    print(f"  Отзывов обработано: {len(reviews)}")
    print(f"  Успешность: {'✅' if reviews else '❌'}")

if __name__ == "__main__":
    test_new_long_reviews_logic()
