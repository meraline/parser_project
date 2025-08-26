#!/usr/bin/env python3
"""
Анализ полной структуры страницы отзыва для извлечения всех параметров
"""

import requests
from bs4 import BeautifulSoup
import json
import re

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
        
        # 2. Schema.org разметка - ГЛАВНОЕ!
        print("\n2. SCHEMA.ORG РАЗМЕТКА (ГЛАВНЫЕ ДАННЫЕ):")
        
        schema_elements = soup.find_all(attrs={'itemprop': True})
        schema_data = {}
        for elem in schema_elements:
            prop = elem.get('itemprop')
            value = elem.get_text(strip=True) if elem.get_text(strip=True) else elem.get('content', '')
            if prop not in schema_data:
                schema_data[prop] = []
            if value:
                schema_data[prop].append(value)
        
        for prop, values in schema_data.items():
            print(f"itemprop='{prop}': {values[:3] if len(values) > 3 else values}")
        
        # 3. Поиск по data-ftid атрибутам
        print("\n3. DATA-FTID АТРИБУТЫ:")
        
        ftid_elements = soup.find_all(attrs={'data-ftid': True})
        ftid_data = {}
        for elem in ftid_elements:
            ftid = elem.get('data-ftid')
            if any(keyword in ftid.lower() for keyword in ['review', 'rating', 'score', 'param', 'spec', 'characteristic']):
                text = elem.get_text(strip=True)
                if text and len(text) < 300:
                    ftid_data[ftid] = text
        
        for ftid, text in list(ftid_data.items())[:15]:
            print(f"data-ftid='{ftid}': {text[:100]}")
        
        # 4. Поиск рейтингов и оценок
        print("\n4. РЕЙТИНГИ И ОЦЕНКИ:")
        
        # Общий рейтинг
        rating_elements = soup.find_all(['span', 'div'], attrs={'itemprop': 'ratingValue'})
        for elem in rating_elements:
            print(f"Общий рейтинг: {elem.get_text(strip=True)}")
        
        # Поиск числовых значений похожих на рейтинги
        numbers = soup.find_all(text=re.compile(r'^[0-5]\.[0-9]$|^[0-5]$'))
        for num in numbers[:10]:
            parent = num.parent
            if parent:
                classes = parent.get('class', [])
                print(f"Возможный рейтинг: '{num.strip()}' в элементе с классами: {classes}")
        
        # 5. Характеристики автомобиля
        print("\n5. ХАРАКТЕРИСТИКИ АВТОМОБИЛЯ:")
        
        # Поиск по тексту характеристик
        all_text_elements = soup.find_all(text=True)
        characteristics = []
        for text in all_text_elements:
            if text and isinstance(text, str):
                text_clean = text.strip()
                if text_clean and any(keyword in text_clean.lower() for keyword in ['год', 'объем', 'литр', 'бензин', 'дизель', 'автомат', 'механик', 'привод']):
                    characteristics.append(text_clean)
        
        for char in characteristics[:10]:
            if len(char) < 200:
                print(f"Характеристика: '{char}'")
        
        # 6. Детальные оценки
        print("\n6. ДЕТАЛЬНЫЕ ОЦЕНКИ:")
        
        # Поиск элементов с оценками
        all_divs = soup.find_all(['div', 'span', 'td'])
        for div in all_divs:
            classes = div.get('class', [])
            text = div.get_text(strip=True)
            
            # Ищем элементы с классами содержащими rating, score, grade
            if classes and any(any(keyword in str(cls).lower() for keyword in ['rating', 'score', 'grade', 'star']) for cls in classes):
                if text and len(text) < 100:
                    print(f"Оценка: '{text}' в классах: {classes}")
        
        # 7. Плюсы и минусы
        print("\n7. ПЛЮСЫ И МИНУСЫ:")
        
        # Поиск элементов с плюсами
        all_elements = soup.find_all(['div', 'section', 'p'])
        for elem in all_elements:
            classes = elem.get('class', [])
            text = elem.get_text(strip=True)
            
            if classes and any(any(keyword in str(cls).lower() for keyword in ['positive', 'plus', 'good', 'pro']) for cls in classes):
                if text and len(text) < 200:
                    print(f"Плюсы: '{text[:100]}...' в классах: {classes}")
            
            if classes and any(any(keyword in str(cls).lower() for keyword in ['negative', 'minus', 'bad', 'con']) for cls in classes):
                if text and len(text) < 200:
                    print(f"Минусы: '{text[:100]}...' в классах: {classes}")
        
        # 8. Сохранение полной страницы
        with open('full_review_page_sample.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"\n✅ Полная страница сохранена в full_review_page_sample.html")
        
        # 9. Поиск таблиц и списков
        print("\n8. ТАБЛИЦЫ И СПИСКИ:")
        
        tables = soup.find_all('table')
        for i, table in enumerate(tables[:3]):
            print(f"Таблица {i+1}: {table.get_text(strip=True)[:200]}...")
        
        lists = soup.find_all(['ul', 'ol', 'dl'])
        for i, lst in enumerate(lists[:3]):
            print(f"Список {i+1}: {lst.get_text(strip=True)[:200]}...")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    analyze_full_review_structure()
