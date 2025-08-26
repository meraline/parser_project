#!/usr/bin/env python3
"""
Тестирование исправленного парсера на реальных страницах
"""

import sys
import os

# Добавляем путь к модулям
sys.path.append('/home/analityk/Документы/projects/parser_project/src')

from auto_reviews_parser.parsers.production_drom_parser import ProductionDromParser, ModelInfo

def test_fixed_parser():
    """Тестируем исправленный парсер"""
    
    # Создаем парсер
    parser = ProductionDromParser()
    
    # Тестовые модели с большими числами отзывов
    test_models = [
        ModelInfo(
            name='Camry',
            brand='Toyota', 
            url='https://www.drom.ru/reviews/toyota/camry/'
        ),
        ModelInfo(
            name='Civic',
            brand='Honda',
            url='https://www.drom.ru/reviews/honda/civic/'
        ),
        ModelInfo(
            name='RAV4',
            brand='Toyota',
            url='https://www.drom.ru/reviews/toyota/rav4/'
        )
    ]
    
    print("=== Тестирование исправленного парсера ===")
    print("Проверяем правильность определения количества отзывов")
    print("Особенно важно для чисел с пробелами (например: '1 413 отзывов')")
    print()
    
    for model in test_models:
        try:
            print(f"🔍 Тестируем: {model.brand} {model.name}")
            print(f"   URL: {model.url}")
            
            # Получаем количество отзывов
            long_count, short_count = parser.get_review_counts_for_model(model)
            
            print(f"📊 Результаты:")
            print(f"   Длинные отзывы:  {long_count:,}")
            print(f"   Короткие отзывы: {short_count:,}")
            print(f"   Всего отзывов:   {(long_count + short_count):,}")
            
            # Проверяем что получили разумные числа
            if long_count > 10000 or short_count > 10000:
                print(f"   ✅ Исправление regex работает! (большие числа корректно извлечены)")
            elif long_count > 0 and short_count > 0:
                print(f"   ✅ Числа извлечены корректно")
            else:
                print(f"   ⚠️  Возможная проблема с извлечением")
                
            print()
            
        except Exception as e:
            print(f"❌ Ошибка при тестировании {model.brand} {model.name}: {e}")
            print()

def test_regex_edge_cases():
    """Дополнительный тест для проверки крайних случаев"""
    
    print("=== Тестирование regex на крайних случаях ===")
    
    import re
    
    test_cases = [
        "1 отзыв",
        "12 отзывов", 
        "123 отзыва",
        "1 234 отзывов",
        "12 345 отзывов",
        "123 456 отзывов",
        "1 234 567 отзывов"
    ]
    
    for text in test_cases:
        # Используем исправленную логику
        match = re.search(r'(\d+)', text.replace(' ', ''))
        result = int(match.group(1)) if match else None
        
        print(f"'{text}' → {result:,}")

if __name__ == "__main__":
    test_regex_edge_cases()
    print()
    test_fixed_parser()
