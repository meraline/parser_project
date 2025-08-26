#!/usr/bin/env python3
"""
Финальный тест парсера с выводом всех извлеченных характеристик
"""

import sys
import os
import json
from datetime import datetime

# Добавляем путь к модулям
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, 'src'))

from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser, ModelInfo

def test_final_comprehensive_parsing():
    """Финальный тест комплексного парсинга"""
    
    print("🚀 ФИНАЛЬНЫЙ ТЕСТ КОМПЛЕКСНОГО ПАРСИНГА ДЛИННЫХ ОТЗЫВОВ")
    print("=" * 70)
    
    # Инициализируем парсер
    parser = MasterDromParser()
    
    # Тестовая модель
    test_model = ModelInfo(
        name="4Runner",
        brand="Toyota", 
        url="https://www.drom.ru/reviews/toyota/4runner/",
        url_name="4runner"
    )
    
    # Тестовый URL - работающий отзыв
    test_url = "https://www.drom.ru/reviews/toyota/4runner/1425079/"
    
    print(f"📄 Парсинг отзыва: {test_url}")
    print("-" * 70)
    
    try:
        # Парсим отзыв
        review_data = parser._parse_individual_review_page(test_url, test_model)
        
        if review_data:
            print(f"✅ УСПЕШНО ОБРАБОТАН ОТЗЫВ!")
            print()
            
            # Основная информация
            print("📊 ОСНОВНАЯ ИНФОРМАЦИЯ:")
            print(f"   🆔 ID отзыва: {review_data.review_id}")
            print(f"   🏷️  Заголовок: {review_data.title}")
            print(f"   👤 Автор: {review_data.author}")
            print(f"   ⭐ Общий рейтинг: {review_data.rating}")
            print(f"   📅 Дата: {review_data.date}")
            print(f"   🌐 URL: {review_data.url}")
            print(f"   📸 Количество фотографий: {review_data.photos_count}")
            
            if review_data.content:
                print(f"   📄 Размер контента: {len(review_data.content)} символов")
                print(f"   📝 Начало контента: {review_data.content[:150]}...")
            print()
            
            # Создаем расширенный словарь для вывода всех данных
            test_dict = {
                'review_id': review_data.review_id,
                'brand': review_data.brand,
                'model': review_data.model,
                'title': review_data.title,
                'author': review_data.author,
                'rating': review_data.rating,
                'date': review_data.date,
                'photos_count': review_data.photos_count,
                'content_length': len(review_data.content) if review_data.content else 0
            }
            
            # Теперь нужно извлечь дополнительные данные из внутренних структур парсера
            # Получим страницу еще раз для извлечения дополнительных данных
            soup = parser._make_request(test_url)
            if soup:
                # Извлекаем характеристики автомобиля из таблицы
                car_characteristics = {}
                tables = soup.select('table')
                for table in tables:
                    rows = table.select('tr')
                    for row in rows:
                        cells = row.select('td, th')
                        if len(cells) >= 2:
                            key = cells[0].get_text(strip=True).replace(':', '').lower()
                            value = cells[1].get_text(strip=True)
                            
                            # Мапим ключи на стандартные названия
                            key_mapping = {
                                'год выпуска': 'year',
                                'тип кузова': 'body_type', 
                                'трансмиссия': 'transmission',
                                'привод': 'drive_type',
                                'двигатель': 'engine',
                                'объем двигателя': 'engine_volume',
                                'мощность': 'engine_power',
                                'топливо': 'fuel_type',
                                'расход топлива': 'fuel_consumption',
                                'разгон до 100': 'acceleration',
                                'максимальная скорость': 'max_speed'
                            }
                            
                            mapped_key = key_mapping.get(key, key)
                            if value and value != '-':
                                car_characteristics[mapped_key] = value
                
                test_dict['car_characteristics'] = car_characteristics
                
                # Извлекаем Schema.org данные
                schema_data = {}
                brand_elem = soup.select_one('[itemprop="brand"]')
                if brand_elem:
                    schema_data['brand'] = brand_elem.get_text(strip=True)
                
                model_elem = soup.select_one('[itemprop="model"]')
                if model_elem:
                    schema_data['model'] = model_elem.get_text(strip=True)
                
                year_elem = soup.select_one('[itemprop="vehicleModelDate"]')
                if year_elem:
                    schema_data['year'] = year_elem.get_text(strip=True)
                
                test_dict['schema_org_data'] = schema_data
                
                # Извлекаем все рейтинги
                ratings_found = {}
                rating_elements = soup.select('[itemprop="ratingValue"]')
                for i, elem in enumerate(rating_elements):
                    try:
                        rating_value = elem.get('content') or elem.get_text(strip=True)
                        if isinstance(rating_value, str) and rating_value.replace('.', '').replace(',', '').isdigit():
                            rating_float = float(rating_value.replace(',', '.'))
                            ratings_found[f'rating_{i+1}'] = rating_float
                    except (ValueError, TypeError):
                        continue
                
                test_dict['all_ratings'] = ratings_found
            
            # Выводим характеристики автомобиля
            if test_dict.get('car_characteristics'):
                print("🚗 ХАРАКТЕРИСТИКИ АВТОМОБИЛЯ:")
                for key, value in test_dict['car_characteristics'].items():
                    print(f"   {key}: {value}")
                print()
            
            # Выводим Schema.org данные
            if test_dict.get('schema_org_data'):
                print("🏷️  SCHEMA.ORG ДАННЫЕ:")
                for key, value in test_dict['schema_org_data'].items():
                    print(f"   {key}: {value}")
                print()
            
            # Выводим все найденные рейтинги
            if test_dict.get('all_ratings'):
                print("⭐ ВСЕ НАЙДЕННЫЕ РЕЙТИНГИ:")
                for key, value in test_dict['all_ratings'].items():
                    print(f"   {key}: {value}")
                print()
            
            # Выводим первые несколько фотографий
            if review_data.photos:
                print("📸 ФОТОГРАФИИ (первые 5):")
                for i, photo in enumerate(review_data.photos[:5], 1):
                    print(f"   {i}. {photo}")
                print()
            
            # Сохраняем полные результаты
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"data/final_comprehensive_test_{timestamp}.json"
            
            os.makedirs("data", exist_ok=True)
            
            # Полный результат для сохранения
            full_result = {
                'test_info': {
                    'timestamp': timestamp,
                    'test_url': test_url,
                    'parser_version': 'enhanced_comprehensive'
                },
                'basic_data': test_dict,
                'full_content_sample': review_data.content[:1000] if review_data.content else None,
                'photos_sample': review_data.photos[:10] if review_data.photos else []
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(full_result, f, ensure_ascii=False, indent=2)
            
            print(f"💾 РЕЗУЛЬТАТЫ СОХРАНЕНЫ: {output_file}")
            print(f"🎯 ИТОГО ИЗВЛЕЧЕНО:")
            print(f"   • Основных параметров: {len([k for k in test_dict.keys() if not k.endswith('_characteristics') and not k.endswith('_data') and not k.endswith('_ratings')])}")
            print(f"   • Характеристик автомобиля: {len(test_dict.get('car_characteristics', {}))}")
            print(f"   • Schema.org параметров: {len(test_dict.get('schema_org_data', {}))}")
            print(f"   • Рейтингов: {len(test_dict.get('all_ratings', {}))}")
            print(f"   • Фотографий: {review_data.photos_count}")
            
        else:
            print("❌ Не удалось обработать отзыв")
            
    except Exception as e:
        print(f"💥 Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_final_comprehensive_parsing()
