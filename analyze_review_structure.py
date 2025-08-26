#!/usr/bin/env python3
"""
Анализ структуры страницы отзыва
"""

import requests
from bs4 import BeautifulSoup
import re

def analyze_review_page_structure():
    """Анализирует структуру страницы длинного отзыва"""
    
    review_url = "https://www.drom.ru/reviews/aito/m7/1455586/"
    
    print(f"🔍 Анализируем структуру страницы отзыва")
    print(f"📍 URL: {review_url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(review_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        print(f"✅ Страница загружена, размер: {len(response.content)} байт")
        
        # 1. Ищем все элементы с data-ftid
        ftid_elements = soup.find_all(attrs={"data-ftid": True})
        print(f"\n🏷️ Найдено {len(ftid_elements)} элементов с data-ftid:")
        
        ftid_dict = {}
        for elem in ftid_elements:
            ftid = elem.get('data-ftid')
            if ftid not in ftid_dict:
                ftid_dict[ftid] = []
            ftid_dict[ftid].append(elem.get_text(strip=True)[:50])
        
        # Показываем самые важные ftid
        important_ftids = [key for key in ftid_dict.keys() if 'review' in key.lower()]
        for ftid in sorted(important_ftids):
            print(f"  📌 {ftid}: {ftid_dict[ftid][0]}...")
        
        # 2. Ищем классы связанные с контентом
        print(f"\n🎨 CSS классы с 'content' или 'review':")
        all_elements = soup.find_all()
        content_classes = set()
        
        for elem in all_elements:
            classes = elem.get('class', [])
            for cls in classes:
                if 'content' in cls.lower() or 'review' in cls.lower():
                    content_classes.add(cls)
        
        for cls in sorted(content_classes)[:10]:
            elements = soup.find_all(class_=cls)
            if elements:
                text = elements[0].get_text(strip=True)[:50]
                print(f"  🎯 .{cls}: {text}...")
        
        # 3. Ищем основной блок отзыва
        main_content_selectors = [
            'article',
            'main',
            '.review',
            '.content',
            '[role="main"]'
        ]
        
        print(f"\n📝 Поиск основного контента:")
        for selector in main_content_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                print(f"  ✅ {selector}: найден, длина {len(text)} символов")
                if len(text) > 100:
                    print(f"      Начало: {text[:100]}...")
        
        # 4. Ищем большие текстовые блоки
        print(f"\n📖 Крупные текстовые блоки (>100 символов):")
        all_texts = soup.find_all(text=True)
        large_texts = []
        
        for text in all_texts:
            cleaned = text.strip()
            if len(cleaned) > 100 and not re.match(r'^[\s\n\r]*$', cleaned):
                parent = text.parent
                if parent:
                    tag_info = f"{parent.name}"
                    if parent.get('class'):
                        tag_info += f".{'.'.join(parent.get('class'))}"
                    if parent.get('data-ftid'):
                        tag_info += f"[data-ftid='{parent.get('data-ftid')}']"
                    
                    large_texts.append((tag_info, cleaned[:100]))
        
        for i, (tag_info, text) in enumerate(large_texts[:5], 1):
            print(f"  {i}. {tag_info}: {text}...")
        
        # 5. Сохраняем часть HTML для анализа
        print(f"\n💾 Сохраняем часть HTML для детального анализа...")
        
        with open('/home/analityk/Документы/projects/parser_project/review_page_sample.html', 'w', encoding='utf-8') as f:
            f.write(str(soup.prettify())[:50000])  # Первые 50KB
        
        print(f"✅ Сохранено в review_page_sample.html")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    analyze_review_page_structure()
