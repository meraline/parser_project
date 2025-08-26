"""
Простой тест для проверки расширенной схемы PostgreSQL
"""
import asyncio
import asyncpg

async def test_schema():
    """Простой тест схемы"""
    try:
        # Подключаемся к базе данных
        conn = await asyncpg.connect(
            host="localhost",
            port=5432,
            user="parser",
            password="parser",
            database="auto_reviews"
        )
        
        print("✅ Подключение к PostgreSQL установлено")
        
        # Проверяем таблицы в схеме auto_reviews
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'auto_reviews'
            ORDER BY table_name
        """)
        
        print("📊 Таблицы в схеме auto_reviews:")
        for table in tables:
            print(f"  - {table['table_name']}")
        
        # Тестируем вставку бренда
        await conn.execute("""
            INSERT INTO auto_reviews.бренды (название, название_в_url, полная_ссылка)
            VALUES ($1, $2, $3)
            ON CONFLICT (название) DO NOTHING
        """, "Toyota", "toyota", "https://auto.drom.ru/toyota/")
        
        # Проверяем что бренд добавился
        brand = await conn.fetchrow("""
            SELECT * FROM auto_reviews.бренды WHERE название = $1
        """, "Toyota")
        
        if brand:
            print(f"✅ Бренд добавлен: {brand['название']} (ID: {brand['ід']})")
            
            # Тестируем вставку модели
            await conn.execute("""
                INSERT INTO auto_reviews.модели (ід_бренда, название, название_в_url, полная_ссылка)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (ід_бренда, название) DO NOTHING
            """, brand['ід'], "Camry", "camry", "https://auto.drom.ru/toyota/camry/")
            
            # Проверяем модель
            model = await conn.fetchrow("""
                SELECT * FROM auto_reviews.модели WHERE ід_бренда = $1 AND название = $2
            """, brand['ід'], "Camry")
            
            if model:
                print(f"✅ Модель добавлена: {model['название']} (ID: {model['ід']})")
                
                # Тестируем короткий отзыв
                await conn.execute("""
                    INSERT INTO auto_reviews.короткие_отзывы 
                    (модель_ід, внешний_id, плюсы, минусы, год_автомобиля)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (модель_ід, внешний_id) DO NOTHING
                """, model['ід'], "test_review_1", "Отличная машина", "Дорогие запчасти", 2020)
                
                print("✅ Короткий отзыв добавлен")
        
        # Проверяем статистику
        stats = await conn.fetchrow("""
            SELECT 
                (SELECT COUNT(*) FROM auto_reviews.бренды) as brands_count,
                (SELECT COUNT(*) FROM auto_reviews.модели) as models_count,
                (SELECT COUNT(*) FROM auto_reviews.короткие_отзывы) as short_reviews_count
        """)
        
        print(f"📊 Статистика:")
        print(f"  - Брендов: {stats['brands_count']}")
        print(f"  - Моделей: {stats['models_count']}")
        print(f"  - Коротких отзывов: {stats['short_reviews_count']}")
        
        await conn.close()
        print("🎉 Тест успешно завершен!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(test_schema())
