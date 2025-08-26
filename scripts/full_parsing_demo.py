#!/usr/bin/env python3
"""
Демонстрационный скрипт полноценного парсинга
Извлекает из первых 3 моделей по 3 длинных и 10 коротких отзывов
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


def get_first_brands_with_models(limit: int = 3) -> List[Dict[str, Any]]:
    """
    Получает первые бренды с их моделями
    """
    print(f"🔍 Получаем первые {limit} брендов с моделями...")
    
    parser = DromReviewsParser()
    
    # Получаем список брендов
    brands_url = "https://www.drom.ru/reviews/"
    try:
        response = parser.session.get(brands_url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Находим блок с брендами
        brands_container = soup.find('div', {'data-ftid': 'component_cars-list'})
        if not brands_container:
            print("❌ Не найден контейнер с брендами")
            return []
        
        brands_links = brands_container.find_all('a', {'data-ftid': 'component_cars-list-item_hidden-link'})
        
        result_brands = []
        
        for i, brand_link in enumerate(brands_links[:limit]):
            if len(result_brands) >= limit:
                break
                
            brand_url = brand_link.get('href')
            brand_name = brand_link.text.strip()
            
            if not brand_url or not brand_name:
                continue
                
            print(f"📋 Обрабатываем бренд {i+1}/{limit}: {brand_name}")
            
            # Получаем модели бренда
            try:
                models_response = parser.session.get(brand_url, timeout=30)
                models_response.raise_for_status()
                
                models_soup = BeautifulSoup(models_response.content, 'html.parser')
                
                # Находим ссылки на модели
                model_links = models_soup.find_all('a', href=True)
                models = []
                
                for model_link in model_links:
                    href = model_link.get('href', '')
                    if '/reviews/' in href and '/' in href.split('/reviews/')[-1]:
                        model_path = href.split('/reviews/')[-1]
                        if model_path.count('/') >= 1:  # brand/model/
                            parts = model_path.strip('/').split('/')
                            if len(parts) >= 2:
                                model_name = parts[1]
                                if model_name and model_name not in [m['name'] for m in models]:
                                    models.append({
                                        'name': model_name,
                                        'url': href,
                                        'brand': parts[0]
                                    })
                
                if models:
                    result_brands.append({
                        'name': brand_name,
                        'url': brand_url,
                        'models': models[:3]  # Берем первые 3 модели
                    })
                    print(f"  ✅ Найдено {len(models[:3])} моделей")
                else:
                    print(f"  ⚠️ Модели не найдены")
                    
            except Exception as e:
                print(f"  ❌ Ошибка получения моделей: {e}")
                continue
        
        return result_brands
        
    except Exception as e:
        print(f"❌ Ошибка получения брендов: {e}")
        return []


def parse_model_reviews(parser: DromReviewsParser, model: Dict[str, Any], 
                       long_limit: int = 3, short_limit: int = 10) -> Dict[str, Any]:
    """
    Парсит отзывы для конкретной модели
    """
    model_name = model['name']
    model_url = model['url']
    brand_name = model['brand']
    
    print(f"  🚗 Парсим модель: {brand_name}/{model_name}")
    
    result = {
        'brand': brand_name,
        'model': model_name,
        'url': model_url,
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
        # Парсим длинные отзывы
        print(f"    📝 Парсим длинные отзывы (лимит: {long_limit})...")
        long_reviews = parser.parse_long_reviews(model_url, limit=long_limit)
        
        # Детализируем длинные отзывы
        detailed_long_reviews = []
        for review in long_reviews[:long_limit]:
            try:
                detailed_review = parser.parse_review_details(review.get('url', ''))
                if detailed_review:
                    # Объединяем базовую информацию и детали
                    full_review = {**review, **detailed_review}
                    detailed_long_reviews.append(full_review)
                    print(f"      ✅ Длинный отзыв: {full_review.get('title', 'Без названия')[:50]}...")
            except Exception as e:
                print(f"      ⚠️ Ошибка детализации длинного отзыва: {e}")
                detailed_long_reviews.append(review)
        
        result['long_reviews'] = detailed_long_reviews
        result['stats']['long_reviews_found'] = len(detailed_long_reviews)
        
        # Парсим короткие отзывы
        print(f"    💬 Парсим короткие отзывы (лимит: {short_limit})...")
        short_reviews = parser.parse_short_reviews(model_url, limit=short_limit)
        
        result['short_reviews'] = short_reviews[:short_limit]
        result['stats']['short_reviews_found'] = len(short_reviews[:short_limit])
        result['stats']['total_reviews'] = result['stats']['long_reviews_found'] + result['stats']['short_reviews_found']
        
        print(f"    📊 Результат: {result['stats']['long_reviews_found']} длинных + {result['stats']['short_reviews_found']} коротких = {result['stats']['total_reviews']} отзывов")
        
    except Exception as e:
        print(f"    ❌ Ошибка парсинга модели {model_name}: {e}")
    
    return result


def main():
    """
    Основная функция демонстрации полноценного парсинга
    """
    print("🚀 ЗАПУСК ПОЛНОЦЕННОГО ПАРСИНГА DROM.RU")
    print("=" * 60)
    
    # Получаем первые 3 бренда с моделями
    brands = get_first_brands_with_models(3)
    
    if not brands:
        print("❌ Не удалось получить бренды")
        return
    
    print(f"\n📋 Найдено {len(brands)} брендов для парсинга")
    
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
    for brand_idx, brand in enumerate(brands):
        print(f"\n🏢 БРЕНД {brand_idx + 1}/3: {brand['name']}")
        print("-" * 40)
        
        brand_result = {
            'name': brand['name'],
            'url': brand['url'],
            'models': [],
            'stats': {
                'models_processed': 0,
                'long_reviews': 0,
                'short_reviews': 0,
                'total_reviews': 0
            }
        }
        
        # Парсим модели бренда
        for model_idx, model in enumerate(brand['models'][:3]):
            print(f"\n  🚗 МОДЕЛЬ {model_idx + 1}/{len(brand['models'][:3])}: {model['name']}")
            
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
        
        print(f"\n  📊 ИТОГО ПО БРЕНДУ {brand['name']}:")
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
            print(f"\n🏢 БРЕНД: {brand['name']}")
            
            for model in brand['models'][:2]:  # Показываем первые 2 модели
                print(f"\n  🚗 МОДЕЛЬ: {model['model']}")
                
                # Показываем пример длинного отзыва
                if model['long_reviews']:
                    long_review = model['long_reviews'][0]
                    print(f"    📝 ДЛИННЫЙ ОТЗЫВ:")
                    print(f"       Заголовок: {long_review.get('title', 'Н/Д')}")
                    print(f"       Автор: {long_review.get('author', 'Н/Д')}")
                    print(f"       Город: {long_review.get('city', 'Н/Д')}")
                    print(f"       Дата: {long_review.get('date', 'Н/Д')}")
                    print(f"       Плюсы: {long_review.get('pros', 'Н/Д')[:100]}...")
                    print(f"       Минусы: {long_review.get('cons', 'Н/Д')[:100]}...")
                
                # Показываем пример короткого отзыва
                if model['short_reviews']:
                    short_review = model['short_reviews'][0]
                    print(f"    💬 КОРОТКИЙ ОТЗЫВ:")
                    print(f"       Автор: {short_review.get('author', 'Н/Д')}")
                    print(f"       Город: {short_review.get('city', 'Н/Д')}")
                    print(f"       Год: {short_review.get('year', 'Н/Д')}")
                    print(f"       Объем: {short_review.get('volume', 'Н/Д')}")
                    print(f"       Плюсы: {short_review.get('positive', 'Н/Д')[:100]}...")
    
    print("\n✅ Полноценный парсинг успешно завершен!")


if __name__ == "__main__":
    main()
