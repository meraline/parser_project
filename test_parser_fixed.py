#!/usr/bin/env python3
"""
Быстрый тест исправленного парсера на реальной странице
"""

import sys
import os

# Добавляем путь к модулям
sys.path.append('/home/analityk/Документы/projects/parser_project/src')

from auto_reviews_parser.parsers.production_drom_parser import ProductionDromParser
from auto_reviews_parser.models.brand import Brand
from auto_reviews_parser.models.model import Model

def test_fixed_parser():
    """Тестируем исправленный парсер"""
    
    # Создаем парсер
    parser = ProductionDromParser()
    
    # Тестовые модели с большими числами отзывов
    test_models = [
        {
            'brand': 'Toyota',
            'model': 'Camry',
            'url': 'https://www.drom.ru/reviews/toyota/camry/'
        },
        {
            'brand': 'Honda', 
            'model': 'Civic',
            'url': 'https://www.drom.ru/reviews/honda/civic/'
        }
    ]
    
    print("=== Тестирование исправленного парсера ===")
    
    for test_data in test_models:
        try:
            # Создаем объекты
            brand = Brand(name=test_data['brand'], slug=test_data['brand'].lower())
            model = Model(name=test_data['model'], slug=test_data['model'].lower(), 
                         brand=brand, url=test_data['url'])
            
            print(f"\n🔍 Тестируем: {test_data['brand']} {test_data['model']}")
            print(f"URL: {test_data['url']}")
            
            # Получаем количество отзывов
            long_count, short_count = parser._get_reviews_count(model)
            
            print(f"📊 Результат:")
            print(f"  Длинные отзывы: {long_count}")
            print(f"  Короткие отзывы: {short_count}")
            print(f"  Всего отзывов: {long_count + short_count}")
            
        except Exception as e:
            print(f"❌ Ошибка при тестировании {test_data['brand']} {test_data['model']}: {e}")

if __name__ == "__main__":
    test_fixed_parser()
