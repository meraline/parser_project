"""
🧪 ИНТЕГРАЦИОННЫЙ ТЕСТ ПАРСЕРОВ С POSTGRESQL
=============================================

Тест интеграции существующих парсеров с новой расширенной PostgreSQL схемой
для работы с брендами, моделями и короткими отзывами.
"""

import asyncio
import asyncpg
import json
import logging
from pathlib import Path

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParserIntegrationTest:
    """Тест интеграции парсеров с PostgreSQL"""
    
    def __init__(self):
        self.connection = None
        
    async def connect_to_db(self):
        """Подключение к PostgreSQL"""
        try:
            self.connection = await asyncpg.connect(
                host="localhost",
                port=5432,
                user="parser",
                password="parser",
                database="auto_reviews"
            )
            logger.info("✅ Подключение к PostgreSQL установлено")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
            raise
    
    async def test_brand_catalog_integration(self):
        """Тест интеграции каталога брендов"""
        logger.info("🔍 Тестирование интеграции каталога брендов...")
        
        # Симулируем данные из brand_catalog_extractor.py
        sample_brands = [
            {
                "name": "Toyota",
                "slug": "toyota", 
                "url": "https://www.drom.ru/reviews/toyota/",
                "review_count": 15423,
                "models": ["camry", "corolla", "rav4", "highlander"]
            },
            {
                "name": "Mazda",
                "slug": "mazda",
                "url": "https://www.drom.ru/reviews/mazda/", 
                "review_count": 8945,
                "models": ["cx-5", "mazda3", "mazda6", "cx-3"]
            },
            {
                "name": "Honda",
                "slug": "honda",
                "url": "https://www.drom.ru/reviews/honda/",
                "review_count": 12567,
                "models": ["accord", "civic", "cr-v", "pilot"]
            }
        ]
        
        # Вставляем бренды в PostgreSQL
        for brand_data in sample_brands:
            brand_id = await self.connection.fetchval("""
                INSERT INTO auto_reviews.бренды (название, название_в_url, полная_ссылка, количество_отзывов)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (название) DO UPDATE SET
                    название_в_url = EXCLUDED.название_в_url,
                    полная_ссылка = EXCLUDED.полная_ссылка,
                    количество_отзывов = EXCLUDED.количество_отзывов
                RETURNING ід
            """, brand_data["name"], brand_data["slug"], brand_data["url"], brand_data["review_count"])
            
            logger.info(f"✅ Бренд {brand_data['name']} добавлен (ID: {brand_id})")
            
            # Добавляем модели для каждого бренда
            for model_slug in brand_data["models"]:
                model_name = model_slug.replace("-", " ").title()
                model_url = f"{brand_data['url']}{model_slug}/"
                
                model_id = await self.connection.fetchval("""
                    INSERT INTO auto_reviews.модели (ід_бренда, название, название_в_url, полная_ссылка)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (ід_бренда, название) DO UPDATE SET
                        название_в_url = EXCLUDED.название_в_url,
                        полная_ссылка = EXCLUDED.полная_ссылка
                    RETURNING ід
                """, brand_id, model_name, model_slug, model_url)
                
                logger.info(f"  ├─ Модель {model_name} добавлена (ID: {model_id})")
        
        logger.info("✅ Интеграция каталога брендов завершена")
    
    async def test_short_reviews_integration(self):
        """Тест интеграции коротких отзывов"""
        logger.info("💬 Тестирование интеграции коротких отзывов...")
        
        # Получаем тестовую модель
        model = await self.connection.fetchrow("""
            SELECT m.ід, m.название, b.название as бренд
            FROM auto_reviews.модели m
            JOIN auto_reviews.бренды b ON m.ід_бренда = b.ід
            WHERE m.название_в_url = 'camry'
            LIMIT 1
        """)
        
        if not model:
            logger.warning("⚠️ Модель Camry не найдена, пропускаем тест коротких отзывов")
            return
        
        # Симулируем данные коротких отзывов из drom_reviews.py
        sample_short_reviews = [
            {
                "внешний_id": "short_review_001",
                "плюсы": "Надёжная машина, экономичная, комфортная",
                "минусы": "Дорогие запчасти, шумная в салоне",
                "поломки": "Замена тормозных колодок на 80 тыс км",
                "год_автомобиля": 2019,
                "объем_двигателя": 2.0,
                "тип_топлива": "Бензин",
                "тип_трансмиссии": "Автомат", 
                "тип_привода": "Передний",
                "количество_фото": 3,
                "город_автора": "Москва"
            },
            {
                "внешний_id": "short_review_002", 
                "плюсы": "Отличная управляемость, мягкая подвеска",
                "минусы": "Маленький багажник, высокий расход в городе",
                "поломки": None,
                "год_автомобиля": 2021,
                "объем_двигателя": 2.5,
                "тип_топлива": "Бензин",
                "тип_трансмиссии": "Автомат",
                "тип_привода": "Полный",
                "количество_фото": 0,
                "город_автора": "Санкт-Петербург"
            },
            {
                "внешний_id": "short_review_003",
                "плюсы": "Хорошая сборка, много места в салоне", 
                "минусы": "Слабая динамика, дорогое обслуживание",
                "поломки": "Замена генератора на 100 тыс км",
                "год_автомобиля": 2018,
                "объем_двигателя": 1.8,
                "тип_топлива": "Гибрид",
                "тип_трансмиссии": "Вариатор",
                "тип_привода": "Передний", 
                "количество_фото": 5,
                "город_автора": "Екатеринбург"
            }
        ]
        
        # Вставляем короткие отзывы
        for review_data in sample_short_reviews:
            review_id = await self.connection.fetchval("""
                INSERT INTO auto_reviews.короткие_отзывы (
                    модель_ід, внешний_id, плюсы, минусы, поломки,
                    год_автомобиля, объем_двигателя, тип_топлива,
                    тип_трансмиссии, тип_привода, количество_фото, город_автора
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (модель_ід, внешний_id) DO UPDATE SET
                    плюсы = EXCLUDED.плюсы,
                    минусы = EXCLUDED.минусы,
                    поломки = EXCLUDED.поломки
                RETURNING ід
            """, 
                model['ід'], review_data["внешний_id"], review_data["плюсы"], review_data["минусы"],
                review_data["поломки"], review_data["год_автомобиля"], review_data["объем_двигателя"],
                review_data["тип_топлива"], review_data["тип_трансмиссии"], review_data["тип_привода"],
                review_data["количество_фото"], review_data["город_автора"]
            )
            
            logger.info(f"✅ Короткий отзыв {review_data['внешний_id']} добавлен (ID: {review_id})")
        
        logger.info(f"✅ Интеграция коротких отзывов для {model['бренд']} {model['название']} завершена")
    
    async def test_parse_session_tracking(self):
        """Тест отслеживания сессий парсинга"""
        logger.info("📊 Тестирование отслеживания сессий парсинга...")
        
        # Получаем тестовый бренд
        brand = await self.connection.fetchrow("""
            SELECT ід, название FROM auto_reviews.бренды WHERE название_в_url = 'toyota' LIMIT 1
        """)
        
        if not brand:
            logger.warning("⚠️ Бренд Toyota не найден, пропускаем тест сессий")
            return
        
        # Создаем сессию парсинга каталога
        catalog_session_id = await self.connection.fetchval("""
            INSERT INTO auto_reviews.сессии_парсинга (
                тип_парсинга, бренд_ід, статус, всего_найдено, успешно_спарсено, 
                параметры_парсинга
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING ід
        """, 
            "catalog", brand['ід'], "завершена", 25, 23,
            json.dumps({"deep_scan": True, "include_models": True, "max_pages": 10})
        )
        
        logger.info(f"✅ Сессия парсинга каталога создана (ID: {catalog_session_id})")
        
        # Создаем сессию парсинга коротких отзывов
        short_reviews_session_id = await self.connection.fetchval("""
            INSERT INTO auto_reviews.сессии_парсинга (
                тип_парсинга, бренд_ід, статус, всего_найдено, успешно_спарсено,
                ошибок, параметры_парсинга
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING ід  
        """,
            "short_reviews", brand['ід'], "завершена", 156, 142, 14,
            json.dumps({"max_reviews_per_model": 50, "skip_duplicates": True})
        )
        
        logger.info(f"✅ Сессия парсинга коротких отзывов создана (ID: {short_reviews_session_id})")
        
        # Завершаем сессии
        await self.connection.execute("""
            UPDATE auto_reviews.сессии_парсинга 
            SET конец_сессии = CURRENT_TIMESTAMP
            WHERE ід IN ($1, $2)
        """, catalog_session_id, short_reviews_session_id)
        
        logger.info("✅ Отслеживание сессий парсинга завершено")
    
    async def generate_integration_report(self):
        """Генерация отчета об интеграции"""
        logger.info("📈 Генерация отчета об интеграции...")
        
        # Собираем статистику
        stats = await self.connection.fetchrow("""
            SELECT 
                (SELECT COUNT(*) FROM auto_reviews.бренды) as brands_count,
                (SELECT COUNT(*) FROM auto_reviews.модели) as models_count,
                (SELECT COUNT(*) FROM auto_reviews.короткие_отзывы) as short_reviews_count,
                (SELECT COUNT(*) FROM auto_reviews.сессии_парсинга) as sessions_count,
                (SELECT SUM(количество_отзывов) FROM auto_reviews.бренды) as total_reviews_expected
        """)
        
        # Детализация по брендам
        brands_detail = await self.connection.fetch("""
            SELECT 
                b.название,
                b.количество_отзывов as ожидаемо_отзывов,
                COUNT(m.ід) as моделей,
                COUNT(ko.ід) as коротких_отзывов
            FROM auto_reviews.бренды b
            LEFT JOIN auto_reviews.модели m ON b.ід = m.ід_бренда
            LEFT JOIN auto_reviews.короткие_отзывы ko ON m.ід = ko.модель_ід
            GROUP BY b.ід, b.название, b.количество_отзывов
            ORDER BY b.название
        """)
        
        print("\n" + "="*70)
        print("🏁 ОТЧЕТ ОБ ИНТЕГРАЦИИ ПАРСЕРОВ С POSTGRESQL")
        print("="*70)
        print(f"📊 Общая статистика:")
        print(f"  ├─ Брендов в базе: {stats['brands_count']}")
        print(f"  ├─ Моделей в базе: {stats['models_count']}")
        print(f"  ├─ Коротких отзывов: {stats['short_reviews_count']}")
        print(f"  ├─ Сессий парсинга: {stats['sessions_count']}")
        print(f"  └─ Ожидается отзывов: {stats['total_reviews_expected']}")
        
        print(f"\n📋 Детализация по брендам:")
        for brand in brands_detail:
            print(f"  📁 {brand['название']}:")
            print(f"    ├─ Моделей: {brand['моделей']}")
            print(f"    ├─ Коротких отзывов: {brand['коротких_отзывов']}")
            print(f"    └─ Ожидается отзывов: {brand['ожидаемо_отзывов']}")
        
        # Проверка готовности для парсинга
        print(f"\n🔍 Готовность к парсингу:")
        
        integration_ready = True
        if stats['brands_count'] == 0:
            print(f"  ❌ Нет брендов в базе")
            integration_ready = False
        else:
            print(f"  ✅ Бренды загружены")
        
        if stats['models_count'] == 0:
            print(f"  ❌ Нет моделей в базе") 
            integration_ready = False
        else:
            print(f"  ✅ Модели загружены")
        
        if stats['short_reviews_count'] > 0:
            print(f"  ✅ Короткие отзывы работают")
        else:
            print(f"  ⚠️ Нет коротких отзывов (нормально для начального состояния)")
        
        if stats['sessions_count'] > 0:
            print(f"  ✅ Отслеживание сессий работает")
        else:
            print(f"  ⚠️ Нет сессий парсинга")
        
        print(f"\n🎯 Статус интеграции: {'✅ ГОТОВ' if integration_ready else '❌ НЕ ГОТОВ'}")
        
        if integration_ready:
            print(f"\n🚀 Следующие шаги:")
            print(f"  1. Запустить catalog_parser.py для обновления каталога")
            print(f"  2. Запустить drom_reviews.py для парсинга коротких отзывов")
            print(f"  3. Интегрировать с extended_postgres_manager.py")
        
        print("="*70)
    
    async def close_connection(self):
        """Закрытие соединения"""
        if self.connection:
            await self.connection.close()
            logger.info("🔌 Соединение с PostgreSQL закрыто")

async def main():
    """Основная функция"""
    test = ParserIntegrationTest()
    
    try:
        await test.connect_to_db()
        await test.test_brand_catalog_integration()
        await test.test_short_reviews_integration()
        await test.test_parse_session_tracking()
        await test.generate_integration_report()
        
    except Exception as e:
        logger.error(f"💥 Критическая ошибка тестирования: {e}")
        raise
    finally:
        await test.close_connection()

if __name__ == "__main__":
    asyncio.run(main())
