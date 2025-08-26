#!/usr/bin/env python3
"""
Детальный тест парсинга одного длинного отзыва с выводом всех характеристик
"""

import sys
import os
import json
from datetime import datetime

# Добавляем путь к модулям
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, 'src'))

from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser, ModelInfo

def detailed_single_review_test():
    """Детальный тест одного отзыва"""
    
    print("🔍 Детальный тест парсинга длинного отзыва...")
    
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
    
    print(f"\n📄 Парсинг отзыва: {test_url}")
    
    try:
        # Сначала получим полную страницу для анализа
        soup = parser._make_request(test_url)
        if not soup:
            print("❌ Не удалось получить страницу")
            return
        
        print("✅ Страница получена успешно")
        
        # Теперь парсим с помощью нашего метода
        review_data = parser._parse_individual_review_page(test_url, test_model)
        
        if review_data:
            print(f"\n📊 ОСНОВНАЯ ИНФОРМАЦИЯ:")
            print(f"   ID: {review_data.review_id}")
            print(f"   Заголовок: {review_data.title}")
            print(f"   Автор: {review_data.author}")
            print(f"   Рейтинг: {review_data.rating}")
            print(f"   Дата: {review_data.date}")
            print(f"   Фотографий: {review_data.photos_count}")
            
            if review_data.content:
                print(f"   Контент: {len(review_data.content)} символов")
                print(f"   Первые 200 символов: {review_data.content[:200]}...")
            
            # Проверим что еще есть на странице - найдем все Schema.org элементы
            print(f"\n🔍 АНАЛИЗ SCHEMA.ORG ЭЛЕМЕНТОВ:")
            schema_elements = soup.select('[itemprop]')
            if schema_elements:
                schema_props = {}
                for elem in schema_elements:
                    prop = elem.get('itemprop')
                    value = elem.get('content') or elem.get_text(strip=True)[:100]
                    if prop in schema_props:
                        if isinstance(schema_props[prop], list):
                            schema_props[prop].append(value)
                        else:
                            schema_props[prop] = [schema_props[prop], value]
                    else:
                        schema_props[prop] = value
                
                for prop, value in sorted(schema_props.items()):
                    print(f"   {prop}: {value}")
            
            # Поиск всех data-ftid элементов
            print(f"\n🏷️  АНАЛИЗ DATA-FTID ЭЛЕМЕНТОВ:")
            ftid_elements = soup.select('[data-ftid]')
            if ftid_elements:
                ftid_types = {}
                for elem in ftid_elements:
                    ftid = elem.get('data-ftid')
                    if ftid:
                        text = elem.get_text(strip=True)
                        if len(text) > 100:
                            text = text[:100] + "..."
                        if ftid in ftid_types:
                            ftid_types[ftid] += 1
                        else:
                            ftid_types[ftid] = 1
                            if text:
                                print(f"   {ftid}: {text}")
                
                print(f"\n📈 Статистика data-ftid:")
                for ftid, count in sorted(ftid_types.items()):
                    print(f"   {ftid}: {count} элементов")
            
            # Поиск рейтингов и оценок
            print(f"\n⭐ ПОИСК РЕЙТИНГОВ:")
            rating_patterns = [
                ('.rating', 'CSS класс .rating'),
                ('[class*="rating"]', 'CSS классы содержащие "rating"'),
                ('.stars', 'CSS класс .stars'),
                ('[class*="star"]', 'CSS классы содержащие "star"'),
                ('.score', 'CSS класс .score'),
                ('[class*="score"]', 'CSS классы содержащие "score"')
            ]
            
            for selector, description in rating_patterns:
                elements = soup.select(selector)
                if elements:
                    print(f"   {description}: найдено {len(elements)} элементов")
                    for elem in elements[:3]:  # Первые 3
                        text = elem.get_text(strip=True)
                        if text:
                            print(f"     - {text}")
            
            # Поиск таблиц с характеристиками
            print(f"\n📋 ПОИСК ТАБЛИЦ И ХАРАКТЕРИСТИК:")
            tables = soup.select('table')
            if tables:
                print(f"   Найдено {len(tables)} таблиц")
                for i, table in enumerate(tables[:2]):  # Первые 2 таблицы
                    rows = table.select('tr')
                    print(f"   Таблица {i+1}: {len(rows)} строк")
                    for row in rows[:3]:  # Первые 3 строки
                        cells = row.select('td, th')
                        if len(cells) >= 2:
                            key = cells[0].get_text(strip=True)
                            value = cells[1].get_text(strip=True)
                            print(f"     {key}: {value}")
            
        else:
            print("❌ Не удалось обработать отзыв")
            
    except Exception as e:
        print(f"💥 Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    detailed_single_review_test()
