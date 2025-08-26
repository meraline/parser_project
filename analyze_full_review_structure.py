#!/usr/bin/env python3
"""
Анализ полной структуры страницы отзыва для извлечения всех параметров
"""

import requests
from bs4 import BeautifulSoup
import json
from typing import Dict, List, Any

def analyze_full_review_structure():
    """Анализ полной структуры страницы отзыва"""
    
    # URL страницы с отзывами AITO
    url = "https://www.drom.ru/reviews/aito/m7/1425079/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        print(f"Запрос к: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        print("\n=== АНАЛИЗ СТРУКТУРЫ ОТЗЫВА ===")
        
        # 1. Основная информация
        print("\n1. ОСНОВНАЯ ИНФОРМАЦИЯ:")
        title = soup.find('h1')
        if title:
            print(f"Заголовок: {title.get_text(strip=True)}")
        
        # 2. Рейтинги и оценки
        print("\n2. РЕЙТИНГИ И ОЦЕНКИ:")
        
        # Общий рейтинг
        rating_elements = soup.find_all(['span', 'div'], attrs={'itemprop': 'ratingValue'})
        for elem in rating_elements:
            print(f"Общий рейтинг: {elem.get_text(strip=True)}")
        
        # Детальные оценки
        rating_sections = soup.find_all(['div', 'section'])
        for section in rating_sections:
            classes = section.get('class', [])
            if classes and any('rating' in str(cls).lower() or 'score' in str(cls).lower() or 'grade' in str(cls).lower() for cls in classes):
                print(f"Секция рейтингов: {classes} - {section.get_text(strip=True)[:100]}...")
        
        # Поиск звездочек и оценок
        stars = soup.find_all(['span', 'div'])
        for star in stars:
            classes = star.get('class', [])
            if classes and any('star' in str(cls).lower() or 'rating' in str(cls).lower() for cls in classes):
        for star in stars[:5]:  # Первые 5
            print(f"Звезды/рейтинг: {star.get('class', [])} - {star.get_text(strip=True)}")
        
        # 3. Характеристики автомобиля
        print("\n3. ХАРАКТЕРИСТИКИ АВТОМОБИЛЯ:")
        
        # Поиск технических характеристик
        spec_sections = soup.find_all(['div', 'section', 'dl', 'table'], class_=lambda x: x and (
            'spec' in ' '.join(x).lower() or 
            'param' in ' '.join(x).lower() or 
            'tech' in ' '.join(x).lower() or
            'character' in ' '.join(x).lower()
        ))
        
        for section in spec_sections:
            print(f"Характеристики: {section.get('class', [])} - {section.get_text(strip=True)[:150]}...")
        
        # 4. Плюсы и минусы
        print("\n4. ПЛЮСЫ И МИНУСЫ:")
        
        # Поиск позитивных отзывов
        positive = soup.find_all(['div', 'section'], class_=lambda x: x and (
            'positive' in ' '.join(x).lower() or 
            'plus' in ' '.join(x).lower() or
            'good' in ' '.join(x).lower() or
            'pro' in ' '.join(x).lower()
        ))
        
        for pos in positive:
            print(f"Плюсы: {pos.get('class', [])} - {pos.get_text(strip=True)[:100]}...")
        
        # Поиск негативных отзывов
        negative = soup.find_all(['div', 'section'], class_=lambda x: x and (
            'negative' in ' '.join(x).lower() or 
            'minus' in ' '.join(x).lower() or
            'bad' in ' '.join(x).lower() or
            'con' in ' '.join(x).lower()
        ))
        
        for neg in negative:
            print(f"Минусы: {neg.get('class', [])} - {neg.get_text(strip=True)[:100]}...")
        
        # 5. Schema.org разметка
        print("\n5. SCHEMA.ORG РАЗМЕТКА:")
        
        schema_elements = soup.find_all(attrs={'itemprop': True})
        schema_data = {}
        for elem in schema_elements:
            prop = elem.get('itemprop')
            value = elem.get_text(strip=True) if elem.get_text(strip=True) else elem.get('content', '')
            if prop not in schema_data:
                schema_data[prop] = []
            schema_data[prop].append(value)
        
        for prop, values in schema_data.items():
            print(f"itemprop='{prop}': {values[:3]}")  # Первые 3 значения
        
        # 6. Поиск по data-ftid атрибутам
        print("\n6. DATA-FTID АТРИБУТЫ:")
        
        ftid_elements = soup.find_all(attrs={'data-ftid': True})
        ftid_data = {}
        for elem in ftid_elements:
            ftid = elem.get('data-ftid')
            if 'review' in ftid.lower() or 'rating' in ftid.lower() or 'score' in ftid.lower():
                text = elem.get_text(strip=True)
                if text and len(text) < 200:  # Короткие значимые тексты
                    ftid_data[ftid] = text
        
        for ftid, text in list(ftid_data.items())[:10]:  # Первые 10
            print(f"data-ftid='{ftid}': {text}")
        
        # 7. Сохранение полной страницы для анализа
        with open('full_review_page_sample.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"\n✅ Полная страница сохранена в full_review_page_sample.html")
        
        # 8. Поиск конкретных паттернов
        print("\n7. СПЕЦИФИЧЕСКИЕ ПАТТЕРНЫ:")
        
        # Год, объем, тип топлива
        year_volume_pattern = soup.find_all(text=lambda text: text and any(
            word in text.lower() for word in ['год', 'л,', 'бензин', 'дизель', 'электро', 'гибрид']
        ))
        
        for pattern in year_volume_pattern[:5]:
            if pattern.strip() and len(pattern.strip()) < 100:
                print(f"Паттерн авто: {pattern.strip()}")
        
        # Поиск числовых оценок
        numeric_ratings = soup.find_all(text=lambda text: text and (
            text.strip().replace('.', '').replace(',', '').isdigit() and 
            len(text.strip()) <= 3
        ))
        
        for rating in numeric_ratings[:10]:
            print(f"Числовая оценка: '{rating.strip()}'")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    analyze_full_review_structure()
