#!/usr/bin/env python3
"""
Полная структура одного длинного отзыва
"""

import sys
import os
import json
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser

def get_full_review_structure():
    parser = MasterDromParser()
    url = "https://www.drom.ru/reviews/audi/100/"
    
    soup = parser._make_request(url)
    if not soup:
        return
    
    component_reviews = soup.find_all("div", {"data-ftid": "component_review"})
    
    if component_reviews:
        first_review = component_reviews[0]
        
        # Извлекаем review_id из data-ga-stats-va-payload
        payload = first_review.get('data-ga-stats-va-payload', '{}')
        try:
            payload_data = json.loads(payload)
            review_id = payload_data.get('review_id', 'НЕТ')
            print(f"📝 Review ID: {review_id}")
        except:
            print("❌ Не удалось извлечь review_id")
        
        # Ищем все элементы с data-ftid внутри отзыва
        print(f"\n🔍 Все data-ftid элементы внутри отзыва:")
        ftid_elements = first_review.find_all(attrs={"data-ftid": True})
        for elem in ftid_elements:
            ftid = elem.get("data-ftid")
            text = elem.get_text(strip=True)[:50] if elem.get_text(strip=True) else ""
            print(f"  📌 {ftid}: {text}...")
        
        # Ищем по классам
        print(f"\n🎨 Элементы с интересными классами:")
        interesting_classes = [
            "css-1u4ddp",  # автор
            "css-1tc5ro3", # дата  
            "css-6hj46s",  # контент
            "hxiweg0"      # основной текст (из debug)
        ]
        
        for class_name in interesting_classes:
            elements = first_review.find_all(class_=class_name)
            print(f"  🎯 .{class_name}: {len(elements)} элементов")
            for i, elem in enumerate(elements[:2]):  # Показываем первые 2
                text = elem.get_text(strip=True)[:100]
                print(f"    [{i+1}] {text}...")
        
        # Выводим читаемую структуру
        print(f"\n📋 Readable HTML структура:")
        print(first_review.prettify()[:2000])

if __name__ == "__main__":
    get_full_review_structure()
