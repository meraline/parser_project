#!/usr/bin/env python3
"""
Скрипт для обновления SQL-запросов в simple_menu.py для работы с схемой auto_reviews
"""

import re

def update_simple_menu():
    """Обновляет SQL-запросы в simple_menu.py"""
    
    # Читаем файл
    with open('/home/analityk/Документы/projects/parser_project/simple_menu.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Список замен для таблиц
    table_replacements = {
        'FROM бренды': 'FROM auto_reviews.бренды',
        'FROM модели': 'FROM auto_reviews.модели', 
        'FROM отзывы': 'FROM auto_reviews.отзывы',
        'FROM короткие_отзывы': 'FROM auto_reviews.короткие_отзывы',
        'FROM авторы': 'FROM auto_reviews.авторы',
        'FROM сессии_парсинга': 'FROM auto_reviews.сессии_парсинга',
        'FROM города': 'FROM auto_reviews.города',
        'FROM комментарии': 'FROM auto_reviews.комментарии',
        'FROM расход_топлива': 'FROM auto_reviews.расход_топлива',
        'FROM рейтинги_деталей': 'FROM auto_reviews.рейтинги_деталей',
        'FROM характеристики': 'FROM auto_reviews.характеристики',
        
        # JOIN
        'JOIN бренды': 'JOIN auto_reviews.бренды',
        'JOIN модели': 'JOIN auto_reviews.модели',
        'JOIN отзывы': 'JOIN auto_reviews.отзывы',
        'JOIN короткие_отзывы': 'JOIN auto_reviews.короткие_отзывы',
        'JOIN авторы': 'JOIN auto_reviews.авторы',
        'JOIN сессии_парсинга': 'JOIN auto_reviews.сессии_парсинга',
        'JOIN города': 'JOIN auto_reviews.города',
        'JOIN комментарии': 'JOIN auto_reviews.комментарии',
        
        # INSERT INTO
        'INSERT INTO бренды': 'INSERT INTO auto_reviews.бренды',
        'INSERT INTO модели': 'INSERT INTO auto_reviews.модели',
        'INSERT INTO отзывы': 'INSERT INTO auto_reviews.отзывы',
        'INSERT INTO короткие_отзывы': 'INSERT INTO auto_reviews.короткие_отзывы',
        'INSERT INTO авторы': 'INSERT INTO auto_reviews.авторы',
        'INSERT INTO сессии_парсинга': 'INSERT INTO auto_reviews.сессии_парсинга',
        
        # UPDATE
        'UPDATE бренды': 'UPDATE auto_reviews.бренды',
        'UPDATE модели': 'UPDATE auto_reviews.модели',
        'UPDATE отзывы': 'UPDATE auto_reviews.отзывы',
        'UPDATE короткие_отзывы': 'UPDATE auto_reviews.короткие_отзывы',
        'UPDATE авторы': 'UPDATE auto_reviews.авторы',
        'UPDATE сессии_парсинга': 'UPDATE auto_reviews.сессии_парсинга',
        
        # DELETE FROM
        'DELETE FROM бренды': 'DELETE FROM auto_reviews.бренды',
        'DELETE FROM модели': 'DELETE FROM auto_reviews.модели',
        'DELETE FROM отзывы': 'DELETE FROM auto_reviews.отзывы',
        'DELETE FROM короткие_отзывы': 'DELETE FROM auto_reviews.короткие_отзывы',
        'DELETE FROM авторы': 'DELETE FROM auto_reviews.авторы',
        'DELETE FROM сессии_парсинга': 'DELETE FROM auto_reviews.сессии_парсинга'
    }
    
    # Также нужно исправить названия колонок
    column_replacements = {
        'название_бренда': 'название',  # в таблице бренды колонка называется просто 'название'
        'время_начала': 'начало_сессии',  # исправляем названия колонок
        'время_окончания': 'конец_сессии'
    }
    
    # Применяем замены
    original_content = content
    
    for old, new in table_replacements.items():
        content = content.replace(old, new)
    
    for old, new in column_replacements.items():
        content = content.replace(old, new)
    
    # Записываем обновленный файл
    if content != original_content:
        with open('/home/analityk/Документы/projects/parser_project/simple_menu.py', 'w', encoding='utf-8') as f:
            f.write(content)
        print("✅ Файл simple_menu.py обновлен для работы с схемой auto_reviews")
        
        # Показываем количество изменений
        changes = sum(1 for old, new in table_replacements.items() if old in original_content)
        print(f"📊 Выполнено изменений: {changes}")
    else:
        print("ℹ️  Файл simple_menu.py уже обновлен")

if __name__ == "__main__":
    update_simple_menu()
