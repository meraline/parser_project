#!/usr/bin/env python3
"""
Тест парсинга конкретного длинного отзыва
"""

import requests
from bs4 import BeautifulSoup
import json

def test_single_long_review():
    """Тестирует парсинг конкретного длинного отзыва"""
    
    # URL конкретного отзыва
    review_url = "https://www.drom.ru/reviews/aito/m7/1455586/"
    
    print(f"🔍 Тестируем парсинг конкретного длинного отзыва")
    print(f"📍 URL отзыва: {review_url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(review_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        print(f"✅ Страница загружена, размер: {len(response.content)} байт")
        
        # Анализируем структуру страницы отзыва
        
        # 1. Ищем заголовок
        title_selectors = [
            'h1',
            'h2', 
            'h3',
            '[data-ftid="component_review_title"]',
            '.review-title'
        ]
        
        title = ""
        for selector in title_selectors:
            elem = soup.select_one(selector)
            if elem:
                title = elem.get_text(strip=True)
                print(f"✅ Заголовок найден ({selector}): {title}")
                break
        
        if not title:
            print("❌ Заголовок не найден")
        
        # 2. Ищем контент отзыва
        content_selectors = [
            '[data-ftid="review-content__positive"]',
            '[data-ftid="review-content__negative"]',
            '[data-ftid="review-content__breakages"]',
            '.css-6hj46s',
            '.hxiweg0',
            '.review-content'
        ]
        
        content_parts = []
        for selector in content_selectors:
            elements = soup.select(selector)
            for elem in elements:
                text = elem.get_text(strip=True)
                if text and len(text) > 10:  # Минимальная длина
                    content_parts.append(f"[{selector}]: {text[:100]}...")
        
        if content_parts:
            print(f"✅ Найдено {len(content_parts)} секций контента:")
            for i, part in enumerate(content_parts[:5], 1):
                print(f"  {i}. {part}")
        else:
            print("❌ Контент отзыва не найден")
        
        # 3. Ищем рейтинг
        rating_selectors = [
            '[data-ftid="component_rating"]',
            '.rating',
            '.stars'
        ]
        
        rating = 0
        for selector in rating_selectors:
            elem = soup.select_one(selector)
            if elem:
                rating_text = elem.get_text(strip=True)
                print(f"✅ Рейтинг найден ({selector}): {rating_text}")
                break
        
        # 4. Ищем автора
        author_selectors = [
            '.author',
            '.user-name',
            '[data-ftid="component_review_descrption"] span'
        ]
        
        author = ""
        for selector in author_selectors:
            elem = soup.select_one(selector)
            if elem:
                author = elem.get_text(strip=True)
                print(f"✅ Автор найден ({selector}): {author}")
                break
        
        # 5. Ищем фотографии
        photos = soup.find_all('img')
        photo_count = 0
        for img in photos:
            src = img.get('src') or img.get('data-src')
            if src and ('photo' in src or 'auto.drom.ru' in src):
                photo_count += 1
        
        print(f"✅ Найдено фотографий: {photo_count}")
        
        print(f"\n📊 Результат парсинга:")
        print(f"  Заголовок: {title}")
        print(f"  Автор: {author}")
        print(f"  Секций контента: {len(content_parts)}")
        print(f"  Фотографий: {photo_count}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    test_single_long_review()
