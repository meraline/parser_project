#!/usr/bin/env python3
"""
🚀 ТЕСТИРОВАНИЕ РЕАЛЬНОГО ПАРСЕРА DROM.RU
=========================================

Скрипт для проверки работы парсера на реальных данных
без моков и базы данных - только парсинг и вывод результатов.
"""

from auto_reviews_parser.parsers.drom_reviews import DromReviewsParser


def test_real_parsing():
    """Тестирование реального парсинга отзывов"""
    print("🚀 Запуск реального парсинга отзывов с drom.ru")
    print("="*60)
    
    parser = DromReviewsParser(delay=0.5)
    
    # Список моделей для тестирования
    test_models = [
        ("toyota", "camry"),
        ("toyota", "land-cruiser"),
        ("bmw", "x5"),
        ("mercedes-benz", "e-class"),
        ("lada", "granta")
    ]
    
    for brand, model in test_models:
        print(f"\n📋 Тестирование: {brand.upper()} {model.upper()}")
        print("-" * 40)
        
        try:
            # Тестируем длинные отзывы (1 страница)
            print(f"🔍 Поиск длинных отзывов...")
            long_reviews = parser.parse_long_reviews(
                brand_url_name=brand,
                model_url_name=model,
                max_pages=1
            )
            
            # Тестируем короткие отзывы (1 страница)  
            print(f"🔍 Поиск коротких отзывов...")
            short_reviews = parser.parse_short_reviews(
                brand_url_name=brand,
                model_url_name=model,
                max_pages=1
            )
            
            # Выводим результаты
            total_reviews = len(long_reviews) + len(short_reviews)
            print(f"📊 РЕЗУЛЬТАТ: {total_reviews} отзывов найдено")
            print(f"   • Длинные отзывы: {len(long_reviews)}")
            print(f"   • Короткие отзывы: {len(short_reviews)}")
            
            # Показываем примеры отзывов
            if long_reviews:
                print(f"\n📝 Пример длинного отзыва:")
                review = long_reviews[0]
                print(f"   Автор: {review.get('author', 'Неизвестен')}")
                print(f"   Дата: {review.get('publish_date', 'Неизвестна')}")
                content = review.get('content', '')
                if content:
                    print(f"   Текст: {content[:100]}...")
            
            if short_reviews:
                print(f"\n💬 Пример короткого отзыва:")
                review = short_reviews[0]
                print(f"   Автор: {review.get('author', 'Неизвестен')}")
                content = review.get('content', '')
                if content:
                    print(f"   Текст: {content[:100]}...")
            
            # Если найдены отзывы - останавливаемся для демонстрации
            if total_reviews > 0:
                print(f"\n✅ УСПЕХ! Найдены отзывы для {brand}/{model}")
                return brand, model, long_reviews, short_reviews
                
        except Exception as e:
            print(f"❌ ОШИБКА при парсинге {brand}/{model}: {e}")
            continue
    
    print(f"\n⚠️  Отзывы не найдены ни для одной из тестируемых моделей")
    return None, None, [], []


if __name__ == "__main__":
    brand, model, long_reviews, short_reviews = test_real_parsing()
    
    if brand and model:
        print(f"\n🎯 ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА")
        print(f"Успешно спаршены отзывы для {brand.upper()} {model.upper()}")
        print(f"Найдено: {len(long_reviews)} длинных + {len(short_reviews)} коротких отзывов")
    else:
        print(f"\n🔍 Рекомендация: попробуйте другие марки/модели")
        print(f"Или проверьте доступность сайта drom.ru")
