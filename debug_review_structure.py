#!/usr/bin/env python3
"""
Отладка структуры длинных отзывов
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser

def debug_long_review_structure():
    parser = MasterDromParser()
    url = "https://www.drom.ru/reviews/audi/100/"
    
    print(f"🔍 Отладка структуры длинных отзывов: {url}")
    
    soup = parser._make_request(url)
    if not soup:
        print("❌ Не удалось получить страницу")
        return
    
    # Проверяем селекторы
    component_reviews = soup.find_all("div", {"data-ftid": "component_review"})
    review_items = soup.find_all("div", {"data-ftid": "review-item"})
    
    print(f"🔎 component_review: {len(component_reviews)}")
    print(f"🔎 review-item: {len(review_items)}")
    
    # Анализируем первый элемент component_review
    if component_reviews:
        print(f"\n📝 Первый component_review:")
        first_review = component_reviews[0]
        print(f"  ID: {first_review.get('id', 'НЕТ')}")
        print(f"  Классы: {first_review.get('class', [])}")
        
        # Поиск заголовка
        title_h3 = first_review.find("h3")
        title_div = first_review.find("div", {"data-ftid": "component_review_title"})
        print(f"  H3 заголовок: {title_h3.get_text(strip=True) if title_h3 else 'НЕТ'}")
        print(f"  Title div: {title_div.get_text(strip=True) if title_div else 'НЕТ'}")
        
        # Поиск автора
        author_spans = first_review.find_all("span", class_="css-1u4ddp")
        print(f"  Найдено span.css-1u4ddp: {len(author_spans)}")
        if author_spans:
            print(f"  Автор: {author_spans[0].get_text(strip=True)}")
        
        # Поиск даты
        date_spans = first_review.find_all("span", class_="css-1tc5ro3")
        time_elems = first_review.find_all("time")
        print(f"  Найдено span.css-1tc5ro3: {len(date_spans)}")
        print(f"  Найдено time: {len(time_elems)}")
        
        # Поиск плюсов/минусов
        positive_divs = first_review.find_all("div", {"data-ftid": "review-content__positive"})
        negative_divs = first_review.find_all("div", {"data-ftid": "review-content__negative"})
        print(f"  Плюсы: {len(positive_divs)}")
        print(f"  Минусы: {len(negative_divs)}")
        
        # Полная структура
        print(f"\n🏗️ Полная структура первого отзыва:")
        print(f"HTML: {str(first_review)[:500]}...")

if __name__ == "__main__":
    debug_long_review_structure()
