#!/usr/bin/env python3
"""
Тест комплексного парсинга длинных отзывов с полными характеристиками
"""

import sys
import os
import json
from datetime import datetime

# Добавляем путь к модулям
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, 'src'))

from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser, ModelInfo

def test_comprehensive_parsing():
    """Тестирует комплексный парсинг длинных отзывов"""
    
    print("🚀 Запуск теста комплексного парсинга длинных отзывов...")
    
    # Инициализируем парсер
    parser = MasterDromParser()
    
    # Тестовая модель (Toyota 4Runner)
    test_model = ModelInfo(
        name="4Runner",
        brand="Toyota", 
        url="https://www.drom.ru/reviews/toyota/4runner/",
        url_name="4runner"
    )
    
    # Тестовые URL длинных отзывов
    test_urls = [
        "https://www.drom.ru/reviews/toyota/4runner/1425079/",
        "https://www.drom.ru/reviews/toyota/4runner/1384584/",
        "https://www.drom.ru/reviews/toyota/4runner/1298473/"
    ]
    
    results = []
    
    for i, url in enumerate(test_urls, 1):
        print(f"\n📄 Парсинг отзыва {i}/{len(test_urls)}: {url}")
        
        try:
            # Парсим отдельную страницу отзыва
            review_data = parser._parse_individual_review_page(url, test_model)
            
            if review_data:
                print(f"✅ Успешно обработан отзыв: {review_data.title[:80] if review_data.title else 'Без заголовка'}...")
                print(f"   📊 Рейтинг: {review_data.rating}")
                print(f"   👤 Автор: {review_data.author}")
                print(f"   📅 Дата: {review_data.date}")
                print(f"   📸 Фото: {review_data.photos_count}")
                print(f"   📄 Контент: {len(review_data.content) if review_data.content else 0} символов")
                
                # Конвертируем в словарь для сохранения
                review_dict = {
                    'review_id': review_data.review_id,
                    'brand': review_data.brand,
                    'model': review_data.model,
                    'review_type': review_data.review_type,
                    'url': review_data.url,
                    'title': review_data.title,
                    'author': review_data.author,
                    'rating': review_data.rating,
                    'content': review_data.content[:500] if review_data.content else None,  # Ограничим для вывода
                    'date': review_data.date,
                    'photos_count': review_data.photos_count,
                    'photos': review_data.photos[:3] if review_data.photos else [],  # Первые 3 фото
                    'parsed_at': review_data.parsed_at.isoformat() if review_data.parsed_at else None
                }
                
                results.append(review_dict)
                
            else:
                print(f"❌ Не удалось обработать отзыв: {url}")
                
        except Exception as e:
            print(f"💥 Ошибка при обработке {url}: {e}")
    
    # Сохраняем результаты
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"data/comprehensive_long_reviews_test_{timestamp}.json"
    
    os.makedirs("data", exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'test_info': {
                'timestamp': timestamp,
                'total_urls': len(test_urls),
                'successful_parses': len(results),
                'test_model': {
                    'brand': test_model.brand,
                    'name': test_model.name,
                    'url_name': test_model.url_name
                }
            },
            'results': results
        }, f, ensure_ascii=False, indent=2)
    
    print(f"\n📊 Результаты теста:")
    print(f"   🎯 Обработано URL: {len(test_urls)}")
    print(f"   ✅ Успешно спарсено: {len(results)}")
    print(f"   💾 Результаты сохранены в: {output_file}")
    
    if results:
        print(f"\n📈 Статистика первого отзыва:")
        first_review = results[0]
        for key, value in first_review.items():
            if key != 'content':  # Не выводим весь контент
                print(f"   {key}: {value}")

if __name__ == "__main__":
    test_comprehensive_parsing()
