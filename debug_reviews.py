#!/usr/bin/env python3
"""
Детальная проверка парсинга длинных отзывов для одной модели
"""

import sys
import os

# Добавляем путь к модулям
sys.path.append('/home/analityk/Документы/projects/parser_project/src')

from auto_reviews_parser.parsers.production_drom_parser import ProductionDromParser, ModelInfo

def debug_long_reviews():
    """Детальная отладка парсинга длинных отзывов"""
    
    parser = ProductionDromParser()
    
    # Тестируем модель с большим количеством длинных отзывов
    test_model = ModelInfo(
        name='3-Series',
        brand='BMW',
        url='https://www.drom.ru/reviews/bmw/3-series/'
    )
    
    print(f"🔍 Тестируем модель: {test_model.brand} {test_model.name}")
    print(f"URL: {test_model.url}")
    print()
    
    try:
        # Получаем количество отзывов
        long_count, short_count = parser.get_review_counts_for_model(test_model)
        print(f"📊 Количество отзывов:")
        print(f"   Длинные: {long_count}")
        print(f"   Короткие: {short_count}")
        print()
        
        if long_count > 0:
            print("🔍 Пытаемся получить длинные отзывы...")
            
            # Включаем подробное логирование
            import logging
            logging.basicConfig(level=logging.DEBUG)
            
            long_reviews = parser.parse_long_reviews(test_model, limit=2)
            
            print(f"✅ Получено длинных отзывов: {len(long_reviews)}")
            
            for i, review in enumerate(long_reviews, 1):
                print(f"\n📝 Отзыв {i}:")
                print(f"   ID: {review.review_id}")
                print(f"   Автор: {review.author}")
                print(f"   Дата: {review.date}")
                print(f"   Рейтинг: {review.rating}")
                print(f"   Заголовок: {review.title}")
                print(f"   Плюсы: {review.positive[:100] if review.positive else 'Нет'}...")
                print(f"   Минусы: {review.negative[:100] if review.negative else 'Нет'}...")
        
        if short_count > 0:
            print("\n🔍 Пытаемся получить короткие отзывы...")
            
            short_reviews = parser.parse_short_reviews(test_model, limit=2)
            
            print(f"✅ Получено коротких отзывов: {len(short_reviews)}")
            
            for i, review in enumerate(short_reviews, 1):
                print(f"\n💬 Короткий отзыв {i}:")
                print(f"   ID: {review.review_id}")
                print(f"   Автор: {review.author}")
                print(f"   Дата: {review.date}")
                print(f"   Плюсы: {review.positive[:100] if review.positive else 'Нет'}...")
                print(f"   Минусы: {review.negative[:100] if review.negative else 'Нет'}...")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_long_reviews()
