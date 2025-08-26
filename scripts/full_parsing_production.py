#!/usr/bin/env python3
"""
Демонстрационный скрипт полноценного парсинга
Извлекает из первых 3 брендов по 3 модели с по 3 длинных и 10 коротких отзывов
со всеми характеристиками и данными
"""

import json
import sys
import os
from datetime import datetime
from typing import List, Dict, Any

# Добавляем корневую директорию в PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.auto_reviews_parser.parsers.drom_reviews import DromReviewsParser


def get_test_models() -> List[Dict[str, Any]]:
    """
    Возвращает предопределенный список моделей для тестирования
    """
    return [
        {
            'brand': 'toyota',
            'model': 'camry',
            'display_name': 'Toyota Camry'
        },
        {
            'brand': 'toyota', 
            'model': '4runner',
            'display_name': 'Toyota 4Runner'
        },
        {
            'brand': 'toyota',
            'model': 'highlander', 
            'display_name': 'Toyota Highlander'
        },
        {
            'brand': 'mazda',
            'model': 'cx-5',
            'display_name': 'Mazda CX-5'
        },
        {
            'brand': 'mazda',
            'model': '3',
            'display_name': 'Mazda 3'
        },
        {
            'brand': 'mazda',
            'model': '6',
            'display_name': 'Mazda 6'
        },
        {
            'brand': 'honda',
            'model': 'civic',
            'display_name': 'Honda Civic'
        },
        {
            'brand': 'honda',
            'model': 'accord',
            'display_name': 'Honda Accord'
        },
        {
            'brand': 'honda',
            'model': 'cr-v',
            'display_name': 'Honda CR-V'
        }
    ]


def parse_model_reviews(parser: DromReviewsParser, model: Dict[str, Any], 
                       long_limit: int = 3, short_limit: int = 10) -> Dict[str, Any]:
    """
    Парсит отзывы для конкретной модели
    """
    brand_name = model['brand']
    model_name = model['model']
    display_name = model['display_name']
    
    print(f"  🚗 Парсим модель: {display_name}")
    
    result = {
        'brand': brand_name,
        'model': model_name,
        'display_name': display_name,
        'long_reviews': [],
        'short_reviews': [],
        'parsed_at': datetime.now().isoformat(),
        'stats': {
            'long_reviews_found': 0,
            'short_reviews_found': 0,
            'total_reviews': 0
        }
    }
    
    try:
        # Парсим длинные отзывы (ограничиваем количество страниц для получения нужного лимита)
        print(f"    📝 Парсим длинные отзывы (до {long_limit} отзывов)...")
        max_pages = max(1, (long_limit // 10) + 1)  # ~10 отзывов на страницу
        
        long_reviews = parser.parse_long_reviews(brand_name, model_name, max_pages=max_pages)
        
        # Ограничиваем количество отзывов
        limited_long_reviews = long_reviews[:long_limit]
        result['long_reviews'] = limited_long_reviews
        result['stats']['long_reviews_found'] = len(limited_long_reviews)
        
        for i, review in enumerate(limited_long_reviews):
            print(f"      ✅ Длинный отзыв {i+1}: {review.get('title', 'Без названия')[:50]}...")
        
        # Парсим короткие отзывы
        print(f"    💬 Парсим короткие отзывы (до {short_limit} отзывов)...")
        max_pages_short = max(1, (short_limit // 20) + 1)  # ~20 отзывов на страницу
        
        short_reviews = parser.parse_short_reviews(brand_name, model_name, max_pages=max_pages_short)
        
        # Ограничиваем количество отзывов
        limited_short_reviews = short_reviews[:short_limit]
        result['short_reviews'] = limited_short_reviews
        result['stats']['short_reviews_found'] = len(limited_short_reviews)
        result['stats']['total_reviews'] = result['stats']['long_reviews_found'] + result['stats']['short_reviews_found']
        
        for i, review in enumerate(limited_short_reviews[:3]):  # Показываем первые 3
            print(f"      ✅ Короткий отзыв {i+1}: {review.get('author', 'Аноним')} - {review.get('positive', '')[:30]}...")
        
        if len(limited_short_reviews) > 3:
            print(f"      ... и еще {len(limited_short_reviews) - 3} коротких отзывов")
        
        print(f"    📊 Результат: {result['stats']['long_reviews_found']} длинных + {result['stats']['short_reviews_found']} коротких = {result['stats']['total_reviews']} отзывов")
        
    except Exception as e:
        print(f"    ❌ Ошибка парсинга модели {display_name}: {e}")
    
    return result


def main():
    """
    Основная функция демонстрации полноценного парсинга
    """
    print("🚀 ЗАПУСК ПОЛНОЦЕННОГО ПАРСИНГА DROM.RU")
    print("=" * 60)
    
    # Получаем список моделей для тестирования (по 3 модели из 3 брендов)
    all_models = get_test_models()
    
    # Группируем по брендам
    brands_dict = {}
    for model in all_models:
        brand = model['brand']
        if brand not in brands_dict:
            brands_dict[brand] = []
        brands_dict[brand].append(model)
    
    # Берем первые 3 бренда
    selected_brands = list(brands_dict.keys())[:3]
    
    print(f"📋 Выбранные бренды для парсинга: {', '.join(selected_brands)}")
    
    # Инициализируем парсер
    parser = DromReviewsParser()
    
    # Результаты парсинга
    parsing_results = {
        'started_at': datetime.now().isoformat(),
        'brands_processed': [],
        'total_stats': {
            'brands': 0,
            'models': 0,
            'long_reviews': 0,
            'short_reviews': 0,
            'total_reviews': 0
        }
    }
    
    # Парсим каждый бренд
    for brand_idx, brand in enumerate(selected_brands):
        print(f"\n🏢 БРЕНД {brand_idx + 1}/3: {brand.upper()}")
        print("-" * 40)
        
        brand_result = {
            'name': brand,
            'models': [],
            'stats': {
                'models_processed': 0,
                'long_reviews': 0,
                'short_reviews': 0,
                'total_reviews': 0
            }
        }
        
        # Парсим модели бренда (по 3 модели)
        brand_models = brands_dict[brand][:3]
        
        for model_idx, model in enumerate(brand_models):
            print(f"\n  🚗 МОДЕЛЬ {model_idx + 1}/{len(brand_models)}: {model['display_name']}")
            
            model_result = parse_model_reviews(parser, model, long_limit=3, short_limit=10)
            brand_result['models'].append(model_result)
            
            # Обновляем статистику бренда
            brand_result['stats']['models_processed'] += 1
            brand_result['stats']['long_reviews'] += model_result['stats']['long_reviews_found']
            brand_result['stats']['short_reviews'] += model_result['stats']['short_reviews_found']
            brand_result['stats']['total_reviews'] += model_result['stats']['total_reviews']
        
        parsing_results['brands_processed'].append(brand_result)
        
        # Обновляем общую статистику
        parsing_results['total_stats']['brands'] += 1
        parsing_results['total_stats']['models'] += brand_result['stats']['models_processed']
        parsing_results['total_stats']['long_reviews'] += brand_result['stats']['long_reviews']
        parsing_results['total_stats']['short_reviews'] += brand_result['stats']['short_reviews']
        parsing_results['total_stats']['total_reviews'] += brand_result['stats']['total_reviews']
        
        print(f"\n  📊 ИТОГО ПО БРЕНДУ {brand.upper()}:")
        print(f"     Моделей: {brand_result['stats']['models_processed']}")
        print(f"     Длинных отзывов: {brand_result['stats']['long_reviews']}")
        print(f"     Коротких отзывов: {brand_result['stats']['short_reviews']}")
        print(f"     Всего отзывов: {brand_result['stats']['total_reviews']}")
    
    # Завершаем парсинг
    parsing_results['completed_at'] = datetime.now().isoformat()
    
    # Сохраняем результаты
    output_file = f"data/exports/full_parsing_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(parsing_results, f, ensure_ascii=False, indent=2)
    
    # Выводим финальную статистику
    print("\n" + "=" * 60)
    print("🎉 ПАРСИНГ ЗАВЕРШЕН! ФИНАЛЬНАЯ СТАТИСТИКА:")
    print("=" * 60)
    print(f"📊 Обработано брендов: {parsing_results['total_stats']['brands']}")
    print(f"🚗 Обработано моделей: {parsing_results['total_stats']['models']}")
    print(f"📝 Длинных отзывов: {parsing_results['total_stats']['long_reviews']}")
    print(f"💬 Коротких отзывов: {parsing_results['total_stats']['short_reviews']}")
    print(f"📋 ВСЕГО ОТЗЫВОВ: {parsing_results['total_stats']['total_reviews']}")
    print(f"💾 Результаты сохранены в: {output_file}")
    
    # Показываем примеры данных
    if parsing_results['brands_processed']:
        print("\n" + "=" * 60)
        print("📋 ПРИМЕРЫ ИЗВЛЕЧЕННЫХ ДАННЫХ:")
        print("=" * 60)
        
        for brand in parsing_results['brands_processed'][:2]:  # Показываем первые 2 бренда
            print(f"\n🏢 БРЕНД: {brand['name'].upper()}")
            
            for model in brand['models'][:2]:  # Показываем первые 2 модели
                print(f"\n  🚗 МОДЕЛЬ: {model['display_name']}")
                
                # Показываем пример длинного отзыва
                if model['long_reviews']:
                    long_review = model['long_reviews'][0]
                    print(f"    📝 ДЛИННЫЙ ОТЗЫВ:")
                    print(f"       Заголовок: {long_review.get('title', 'Н/Д')}")
                    print(f"       Автор: {long_review.get('author', 'Н/Д')}")
                    print(f"       Город: {long_review.get('city', 'Н/Д')}")
                    print(f"       Дата: {long_review.get('date', 'Н/Д')}")
                    if long_review.get('pros'):
                        print(f"       Плюсы: {long_review['pros'][:100]}...")
                    if long_review.get('cons'):
                        print(f"       Минусы: {long_review['cons'][:100]}...")
                
                # Показываем пример короткого отзыва
                if model['short_reviews']:
                    short_review = model['short_reviews'][0]
                    print(f"    💬 КОРОТКИЙ ОТЗЫВ:")
                    print(f"       Автор: {short_review.get('author', 'Н/Д')}")
                    print(f"       Город: {short_review.get('city', 'Н/Д')}")
                    print(f"       Год: {short_review.get('year', 'Н/Д')}")
                    print(f"       Объем: {short_review.get('volume', 'Н/Д')}")
                    if short_review.get('positive'):
                        print(f"       Плюсы: {short_review['positive'][:100]}...")
                    if short_review.get('negative'):
                        print(f"       Минусы: {short_review['negative'][:100]}...")
    
    print("\n✅ Полноценный парсинг успешно завершен!")


if __name__ == "__main__":
    main()
