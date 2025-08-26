#!/usr/bin/env python3
"""
Тест получения URL длинных отзывов
"""

import requests
from bs4 import BeautifulSoup
import re

def test_long_review_urls():
    """Тестирует получение URL конкретных длинных отзывов"""
    
    # URL страницы со списком длинных отзывов
    list_url = "https://www.drom.ru/reviews/aito/m7/?order=relevance"
    
    print(f"🔍 Тестируем получение URL длинных отзывов")
    print(f"📍 Страница списка: {list_url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(list_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        print(f"✅ Страница загружена, размер: {len(response.content)} байт")
        
        # Ищем все ссылки на отзывы
        all_links = soup.find_all('a', href=True)
        review_urls = []
        
        print(f"🔍 Найдено всего ссылок: {len(all_links)}")
        
        for link in all_links:
            href = link.get('href', '')
            
            # Проверяем, что это ссылка на конкретный отзыв (с ID в конце)
            if '/reviews/' in href and href.count('/') >= 4:
                parts = href.strip('/').split('/')
                if len(parts) >= 4 and parts[-1].isdigit():
                    full_url = href if href.startswith('http') else f"https://www.drom.ru{href}"
                    review_urls.append(full_url)
                    print(f"  ✅ Найден отзыв: {full_url}")
        
        print(f"\n📊 Результаты:")
        print(f"✅ Найдено URL отзывов: {len(review_urls)}")
        
        # Показываем первые 5 URL
        for i, url in enumerate(review_urls[:5], 1):
            print(f"  {i}. {url}")
        
        if len(review_urls) > 5:
            print(f"  ... и еще {len(review_urls) - 5} отзывов")
            
        return review_urls
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return []

if __name__ == "__main__":
    test_long_review_urls()
