#!/usr/bin/env python3
"""
Отладка страницы длинных отзывов
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser
import requests
from bs4 import BeautifulSoup

def debug_long_reviews_page():
    parser = MasterDromParser()
    url = "https://www.drom.ru/reviews/audi/100/"
    
    print(f"🔍 Отладка страницы: {url}")
    
    # Делаем запрос
    soup = parser._make_request(url)
    if not soup:
        print("❌ Не удалось получить страницу")
        return
    
    print("✅ Страница получена")
    
    # Ищем селекторы длинных отзывов
    review_blocks = soup.find_all("div", {"data-ftid": "review-item"})
    print(f"🔎 Найдено блоков с data-ftid='review-item': {len(review_blocks)}")
    
    # Ищем альтернативные селекторы
    alt_selectors = [
        "article",
        "div[class*='review']", 
        "div[id*='review']",
        "div[data-ftid*='review']",
        ".review-block",
        ".review-item"
    ]
    
    for selector in alt_selectors:
        try:
            elements = soup.select(selector)
            print(f"🔎 Селектор '{selector}': {len(elements)} элементов")
        except:
            pass
    
    # Ищем все data-ftid атрибуты
    all_ftids = set()
    for elem in soup.find_all(attrs={"data-ftid": True}):
        all_ftids.add(elem.get("data-ftid"))
    
    print(f"\n📋 Все data-ftid на странице:")
    for ftid in sorted(all_ftids):
        if 'review' in ftid.lower():
            print(f"  ⭐ {ftid}")
        else:
            print(f"     {ftid}")
    
    # Проверяем заголовок страницы
    title = soup.find("title")
    if title:
        print(f"\n📄 Заголовок страницы: {title.get_text()}")
    
    # Ищем элементы с текстом "отзыв"
    print(f"\n🔍 Элементы с текстом 'отзыв':")
    for elem in soup.find_all(text=lambda t: t and 'отзыв' in t.lower()):
        parent = elem.parent
        if parent:
            print(f"  📝 {elem.strip()[:50]}... в {parent.name} class='{parent.get('class')}'")

if __name__ == "__main__":
    debug_long_reviews_page()
