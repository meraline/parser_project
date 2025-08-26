#!/usr/bin/env python3
"""
Тестирование исправленной логики парсинга количества отзывов
"""

import re
from bs4 import BeautifulSoup

def test_regex_fix():
    """Тестируем исправленную логику извлечения чисел"""
    
    # Тестовые случаи
    test_cases = [
        "212 отзывов",
        "1 413 отзывов", 
        "2 525 коротких отзывов",
        "63 отзыва",
        "283 коротких отзыва",
        "12554 отзывов",
        "28 421 отзывов"
    ]
    
    print("=== Тестирование исправленной логики regex ===")
    
    for text in test_cases:
        # Старая логика (только для длинных отзывов)
        old_match = re.search(r'(\d+)', text)
        old_result = int(old_match.group(1)) if old_match else None
        
        # Новая исправленная логика
        new_match = re.search(r'(\d+)', text.replace(' ', ''))
        new_result = int(new_match.group(1)) if new_match else None
        
        print(f"Текст: '{text}'")
        print(f"  Старая логика: {old_result}")
        print(f"  Новая логика:  {new_result}")
        print(f"  Исправлено:    {'✅' if old_result != new_result else '⚪'}")
        print()

def test_with_real_html():
    """Тестируем с реальным HTML"""
    
    # HTML пример с большими числами
    html_sample = '''
    <div class="_65ykvx0">
        <a class="_65ykvx1 w6f4f60 w6f4f63 w6f4f65" data-ftid="reviews_tab_button_long_reviews" href="#">
            <span>1 413 отзывов</span>
        </a>
        <a class="_65ykvx1 w6f4f60 w6f4f63 w6f4f66" data-ftid="reviews_tab_button_short_reviews" href="#">
            <span>28 525 коротких отзывов</span>
        </a>
    </div>
    '''
    
    soup = BeautifulSoup(html_sample, 'html.parser')
    tabs_block = soup.find('div', class_='_65ykvx0')
    
    print("=== Тестирование с реальным HTML ===")
    
    if tabs_block:
        # Длинные отзывы
        long_reviews_tab = tabs_block.find('a', {'data-ftid': 'reviews_tab_button_long_reviews'})
        if long_reviews_tab:
            text = long_reviews_tab.get_text(strip=True)
            match = re.search(r'(\d+)', text.replace(' ', ''))
            long_count = int(match.group(1)) if match else 0
            print(f"Длинные отзывы: текст='{text}' → число={long_count}")
        
        # Короткие отзывы  
        short_reviews_tab = tabs_block.find('a', {'data-ftid': 'reviews_tab_button_short_reviews'})
        if short_reviews_tab:
            text = short_reviews_tab.get_text(strip=True)
            match = re.search(r'(\d+)', text.replace(' ', ''))
            short_count = int(match.group(1)) if match else 0
            print(f"Короткие отзывы: текст='{text}' → число={short_count}")

if __name__ == "__main__":
    test_regex_fix()
    test_with_real_html()
