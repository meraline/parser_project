#!/usr/bin/env python3
"""
Скрипт для показа детальных данных из базы PostgreSQL
"""

import asyncio
import asyncpg

async def show_detailed_data():
    """Показать детальные данные из базы"""
    try:
        conn = await asyncpg.connect(
            host='localhost',
            port=5432,
            database='auto_reviews',
            user='parser',
            password='parser'
        )
        
        print("📋 ДЕТАЛЬНЫЙ ПРОСМОТР ДАННЫХ")
        print("=" * 60)
        
        # Показываем бренды
        print("\n🏢 БРЕНДЫ:")
        print("-" * 40)
        brands = await conn.fetch("""
            SELECT ід, название, название_в_url, количество_отзывов
            FROM auto_reviews.бренды
            ORDER BY количество_отзывов DESC NULLS LAST
        """)
        
        for brand in brands:
            print(f"  ID {brand['ід']:2}: {brand['название']:15} | URL: {brand['название_в_url'] or 'None':15} | Отзывов: {brand['количество_отзывов'] or 0}")
        
        # Показываем модели
        print("\n🚗 МОДЕЛИ:")
        print("-" * 40)
        models = await conn.fetch("""
            SELECT m.ід, b.название as бренд, m.название, m.название_в_url, m.количество_отзывов
            FROM auto_reviews.модели m
            JOIN auto_reviews.бренды b ON m.ід_бренда = b.ід
            ORDER BY b.название, m.название
        """)
        
        for model in models:
            print(f"  ID {model['ід']:2}: {model['бренд']:10} {model['название']:20} | URL: {model['название_в_url'] or 'None':15} | Отзывов: {model['количество_отзывов'] or 0}")
        
        # Показываем короткие отзывы
        print("\n📝 КОРОТКИЕ ОТЗЫВЫ:")
        print("-" * 40)
        short_reviews = await conn.fetch("""
            SELECT sr.ід, b.название as бренд, m.название as модель, 
                   sr.год_автомобиля, sr.объем_двигателя, sr.тип_топлива,
                   sr.город_автора, sr.дата_публикации,
                   SUBSTRING(sr.плюсы, 1, 50) as плюсы_короткие
            FROM auto_reviews.короткие_отзывы sr
            JOIN auto_reviews.модели m ON sr.модель_ід = m.ід
            JOIN auto_reviews.бренды b ON m.ід_бренда = b.ід
            ORDER BY sr.дата_публикации DESC NULLS LAST
        """)
        
        for review in short_reviews:
            print(f"  ID {review['ід']:2}: {review['бренд']} {review['модель']}")
            print(f"        {review['год_автомобиля'] or '?'} год, {review['объем_двигателя'] or '?'}л, {review['тип_топлива'] or '?'}")
            print(f"        Город: {review['город_автора'] or '?'}")
            print(f"        Плюсы: {review['плюсы_короткие'] or 'Не указаны'}...")
            print(f"        Дата: {review['дата_публикации'] or 'Не указана'}")
            print()
        
        # Показываем сессии парсинга
        print("\n🔄 СЕССИИ ПАРСИНГА:")
        print("-" * 40)
        sessions = await conn.fetch("""
            SELECT ід, тип_парсинга, статус, начало_сессии, конец_сессии, 
                   всего_найдено, успешно_спарсено, ошибок
            FROM auto_reviews.сессии_парсинга
            ORDER BY начало_сессии DESC
        """)
        
        for session in sessions:
            print(f"  ID {session['ід']:2}: {session['тип_парсинга']:15} | Статус: {session['статус']:10}")
            print(f"        Период: {session['начало_сессии']} - {session['конец_сессии'] or 'в процессе'}")
            print(f"        Найдено: {session['всего_найдено'] or 0}, Спарсено: {session['успешно_спарсено'] or 0}, Ошибок: {session['ошибок'] or 0}")
            print()
        
        await conn.close()
        print("✅ Детальный просмотр завершен")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(show_detailed_data())
