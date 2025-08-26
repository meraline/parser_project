#!/usr/bin/env python3
"""
Тестовый запуск парсера для первых 3 брендов
По 3 длинных и 10 коротких отзывов на модель
"""

import sys
import os
import json
from datetime import datetime

# Добавляем путь к модулям
sys.path.append('/home/analityk/Документы/projects/parser_project/src')

from auto_reviews_parser.parsers.production_drom_parser import ProductionDromParser

def run_limited_parsing():
    """Запуск ограниченного парсинга для тестирования"""
    
    print("🚀 ЗАПУСК ТЕСТОВОГО ПАРСИНГА")
    print("=" * 50)
    print("Параметры:")
    print("- Первые 3 бренда")
    print("- По 3 длинных отзыва на модель")
    print("- По 10 коротких отзывов на модель")
    print("=" * 50)
    print()
    
    # Создаем парсер
    parser = ProductionDromParser()
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'parameters': {
            'brands_limit': 3,
            'long_reviews_per_model': 3,
            'short_reviews_per_model': 10
        },
        'brands': []
    }
    
    try:
        # Получаем каталог брендов
        print("📋 Получаем каталог брендов...")
        brands = parser.get_brands_catalog()
        
        if not brands:
            print("❌ Не удалось получить каталог брендов")
            return
            
        print(f"✅ Найдено {len(brands)} брендов")
        
        # Берем только первые 3 бренда
        limited_brands = brands[:3]
        print(f"🎯 Выбираем первые {len(limited_brands)} брендов:")
        
        for i, brand in enumerate(limited_brands, 1):
            print(f"   {i}. {brand.name} ({brand.reviews_count} отзывов)")
        
        print()
        
        # Парсим каждый бренд
        for brand_idx, brand in enumerate(limited_brands, 1):
            print(f"🔍 БРЕНД {brand_idx}/3: {brand.name}")
            print("-" * 40)
            
            brand_result = {
                'name': brand.name,
                'url': brand.url,
                'total_reviews_count': brand.reviews_count,
                'models': []
            }
            
            try:
                # Получаем модели бренда
                print(f"   📋 Получаем модели бренда {brand.name}...")
                models = parser.get_models_for_brand(brand)
                
                if not models:
                    print(f"   ⚠️  Модели не найдены для {brand.name}")
                    continue
                    
                print(f"   ✅ Найдено {len(models)} моделей")
                
                # Парсим каждую модель
                for model_idx, model in enumerate(models[:5], 1):  # Ограничиваем 5 моделями на бренд
                    print(f"      🚗 Модель {model_idx}: {model.name}")
                    
                    model_result = {
                        'name': model.name,
                        'url': model.url,
                        'long_reviews': [],
                        'short_reviews': []
                    }
                    
                    try:
                        # Получаем количество отзывов
                        long_count, short_count = parser.get_review_counts_for_model(model)
                        print(f"         📊 Доступно: {long_count} длинных, {short_count} коротких")
                        
                        # Парсим длинные отзывы (максимум 3)
                        if long_count > 0:
                            print(f"         📝 Парсим длинные отзывы (до 3)...")
                            long_reviews = parser.parse_long_reviews(model, limit=3)
                            model_result['long_reviews'] = [
                                {
                                    'id': review.review_id,
                                    'title': review.title,
                                    'content': (review.positive or '') + ' ' + (review.negative or '') + ' ' + (review.general or ''),
                                    'positive': review.positive,
                                    'negative': review.negative,
                                    'general': review.general,
                                    'rating': review.rating,
                                    'author': review.author,
                                    'date': review.date
                                }
                                for review in long_reviews
                            ]
                            print(f"         ✅ Получено {len(long_reviews)} длинных отзывов")
                        
                        # Парсим короткие отзывы (максимум 10)
                        if short_count > 0:
                            print(f"         💬 Парсим короткие отзывы (до 10)...")
                            short_reviews = parser.parse_short_reviews(model, limit=10)
                            model_result['short_reviews'] = [
                                {
                                    'id': review.review_id,
                                    'positive': review.positive,
                                    'negative': review.negative,
                                    'general': review.general,
                                    'rating': review.rating,
                                    'author': review.author,
                                    'date': review.date
                                }
                                for review in short_reviews
                            ]
                            print(f"         ✅ Получено {len(short_reviews)} коротких отзывов")
                        
                    except Exception as e:
                        print(f"         ❌ Ошибка при парсинге модели {model.name}: {e}")
                        continue
                    
                    brand_result['models'].append(model_result)
                    print()
                
            except Exception as e:
                print(f"   ❌ Ошибка при обработке бренда {brand.name}: {e}")
                continue
            
            results['brands'].append(brand_result)
            print(f"✅ Бренд {brand.name} завершен")
            print()
        
        # Сохраняем результаты
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'/home/analityk/Документы/projects/parser_project/data/limited_parsing_results_{timestamp}.json'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print("=" * 50)
        print("🎉 ПАРСИНГ ЗАВЕРШЕН!")
        print(f"📄 Результаты сохранены: {output_file}")
        
        # Статистика
        total_models = sum(len(brand['models']) for brand in results['brands'])
        total_long = sum(len(model['long_reviews']) for brand in results['brands'] for model in brand['models'])
        total_short = sum(len(model['short_reviews']) for brand in results['brands'] for model in brand['models'])
        
        print(f"📊 Статистика:")
        print(f"   Обработано брендов: {len(results['brands'])}")
        print(f"   Обработано моделей: {total_models}")
        print(f"   Получено длинных отзывов: {total_long}")
        print(f"   Получено коротких отзывов: {total_short}")
        print(f"   Всего отзывов: {total_long + total_short}")
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_limited_parsing()
