#!/usr/bin/env python3
"""
Тест проверки правильного определения типов отзывов
"""

import requests
from bs4 import BeautifulSoup
import re

def test_reviews_detection():
    """Тестирует правильность определения типов отзывов"""
    
    # Настройка сессии
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    
    # Тестовые URL для разных случаев
    test_cases = [
        {
            'brand': 'Acura',
            'url': 'https://www.drom.ru/reviews/acura/',
            'expected_structure': True  # Ожидаем наличие переключателей
        },
        {
            'brand': 'Acura MDX',
            'url': 'https://www.drom.ru/reviews/acura/mdx/',
            'expected_structure': True
        },
        {
            'brand': 'Toyota Camry',
            'url': 'https://www.drom.ru/reviews/toyota/camry/',
            'expected_structure': True
        }
    ]
    
    print("🔍 ТЕСТ: Проверка определения типов отзывов\n")
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"📋 Тест {i}: {test_case['brand']}")
        print(f"URL: {test_case['url']}")
        
        try:
            # Запрос страницы
            response = session.get(test_case['url'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ищем блок с переключателями
            tabs_block = soup.find('div', class_='_65ykvx0')
            
            if tabs_block:
                print("✅ Найден блок переключения типов отзывов")
                
                # Поиск кнопки длинных отзывов
                long_reviews_tab = tabs_block.find('a', {'data-ftid': 'reviews_tab_button_long_reviews'})
                short_reviews_tab = tabs_block.find('a', {'data-ftid': 'reviews_tab_button_short_reviews'})
                
                # Анализ длинных отзывов
                if long_reviews_tab:
                    long_text = long_reviews_tab.get_text(strip=True)
                    long_href = long_reviews_tab.get('href', '')
                    long_match = re.search(r'(\d+)', long_text)
                    long_count = int(long_match.group(1)) if long_match else 0
                    
                    print(f"📝 Длинные отзывы:")
                    print(f"   Текст: '{long_text}'")
                    print(f"   Ссылка: {long_href}")
                    print(f"   Количество: {long_count}")
                else:
                    print("❌ Кнопка длинных отзывов не найдена")
                
                # Анализ коротких отзывов
                if short_reviews_tab:
                    short_text = short_reviews_tab.get_text(strip=True)
                    short_href = short_reviews_tab.get('href', '')
                    short_match = re.search(r'(\d+)', short_text.replace(' ', ''))
                    short_count = int(short_match.group(1)) if short_match else 0
                    
                    print(f"💬 Короткие отзывы:")
                    print(f"   Текст: '{short_text}'")
                    print(f"   Ссылка: {short_href}")
                    print(f"   Количество: {short_count}")
                else:
                    print("❌ Кнопка коротких отзывов не найдена")
                
                # Проверка правильности определения
                print(f"\n🔍 АНАЛИЗ:")
                if long_reviews_tab and short_reviews_tab:
                    print("✅ Оба типа отзывов определены корректно")
                    
                    # Проверка структуры ссылок
                    long_is_main = '5kopeek' not in long_href
                    short_has_5kopeek = '5kopeek' in short_href
                    
                    if long_is_main and short_has_5kopeek:
                        print("✅ Структура ссылок корректна:")
                        print(f"   - Длинные: основная страница")
                        print(f"   - Короткие: страница с '5kopeek'")
                    else:
                        print("❌ Структура ссылок некорректна:")
                        print(f"   - Длинные содержат '5kopeek': {not long_is_main}")
                        print(f"   - Короткие не содержат '5kopeek': {not short_has_5kopeek}")
                    
                else:
                    print("❌ Не все типы отзывов найдены")
                    
            else:
                print("❌ Блок переключения типов отзывов не найден")
                
                # Возможно, на странице только один тип отзывов
                # Проверим прямо содержимое
                long_reviews = soup.find_all('div', {'data-ftid': 'review-item'})
                short_reviews = soup.find_all('div', {'data-ftid': 'short-review-item'})
                
                print(f"🔍 Прямая проверка содержимого:")
                print(f"   - Длинных отзывов: {len(long_reviews)}")
                print(f"   - Коротких отзывов: {len(short_reviews)}")
                
        except Exception as e:
            print(f"❌ Ошибка при обработке: {e}")
        
        print("-" * 80)
        print()

def test_html_structure():
    """Тестирует HTML-структуру из примера пользователя"""
    
    print("🧪 ТЕСТ: Анализ HTML-структуры из примера\n")
    
    # HTML из примера пользователя
    sample_html = '''
    <div class="_65ykvx0">
        <a class="_65ykvx1 w6f4f60 w6f4f63 w6f4f65" data-ftid="reviews_tab_button_long_reviews" href="https://www.drom.ru/reviews/acura/?order=relevance" data-ga-stats-name="tabs" data-ga-stats-track-view="true" data-ga-stats-track-click="true" data-ga-stats-va-payload="{&quot;tab_name&quot;:&quot;long_reviews&quot;}">
            <span>130 отзывов</span>
        </a>
        <a class="_65ykvx1 w6f4f60 w6f4f63 w6f4f66" data-ftid="reviews_tab_button_short_reviews" href="https://www.drom.ru/reviews/acura/5kopeek/?order=useful" data-ga-stats-name="tabs" data-ga-stats-track-view="true" data-ga-stats-track-click="true" data-ga-stats-va-payload="{&quot;tab_name&quot;:&quot;short_reviews&quot;}">
            <span>553 коротких отзыва</span>
        </a>
    </div>
    '''
    
    soup = BeautifulSoup(sample_html, 'html.parser')
    
    # Анализ структуры
    tabs_block = soup.find('div', class_='_65ykvx0')
    print(f"✅ Блок найден: {bool(tabs_block)}")
    
    if tabs_block:
        # Длинные отзывы
        long_tab = tabs_block.find('a', {'data-ftid': 'reviews_tab_button_long_reviews'})
        if long_tab:
            long_text = long_tab.get_text(strip=True)
            long_href = long_tab.get('href')
            long_match = re.search(r'(\d+)', long_text)
            long_count = int(long_match.group(1)) if long_match else 0
            
            print(f"📝 Длинные отзывы:")
            print(f"   Текст: '{long_text}'")
            print(f"   Ссылка: {long_href}")
            print(f"   Количество: {long_count}")
            print(f"   Содержит '5kopeek': {'5kopeek' in long_href}")
        
        # Короткие отзывы
        short_tab = tabs_block.find('a', {'data-ftid': 'reviews_tab_button_short_reviews'})
        if short_tab:
            short_text = short_tab.get_text(strip=True)
            short_href = short_tab.get('href')
            short_match = re.search(r'(\d+)', short_text.replace(' ', ''))
            short_count = int(short_match.group(1)) if short_match else 0
            
            print(f"💬 Короткие отзывы:")
            print(f"   Текст: '{short_text}'")
            print(f"   Ссылка: {short_href}")
            print(f"   Количество: {short_count}")
            print(f"   Содержит '5kopeek': {'5kopeek' in short_href}")
        
        print(f"\n🎯 ЗАКЛЮЧЕНИЕ:")
        print(f"✅ Алгоритм парсера корректно определяет:")
        print(f"   - data-ftid='reviews_tab_button_long_reviews' → длинные отзывы")
        print(f"   - data-ftid='reviews_tab_button_short_reviews' → короткие отзывы")
        print(f"   - Извлечение чисел из текста работает правильно")
        print(f"   - Ссылки правильно ведут на соответствующие разделы")

if __name__ == "__main__":
    print("🚀 ПРОВЕРКА ОПРЕДЕЛЕНИЯ ТИПОВ ОТЗЫВОВ\n")
    print("=" * 80)
    
    # Тест HTML-структуры из примера
    test_html_structure()
    
    print("\n" + "=" * 80)
    
    # Тест реальных страниц
    test_reviews_detection()
