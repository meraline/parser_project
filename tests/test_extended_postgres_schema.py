"""
Тестовый скрипт для проверки расширенной PostgreSQL схемы
Тестирует работу с брендами, моделями и короткими отзывами
"""

import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime

# Добавляем путь к проекту
sys.path.append('/home/analityk/Документы/projects/parser_project')

try:
    from src.auto_reviews_parser.database.extended_postgres_manager import (
        ExtendedPostgresManager, BrandData, ModelData, ShortReviewData, ParseSessionData
    )
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Проверьте установку зависимостей: pip install asyncpg")
    sys.exit(1)


class ExtendedSchemaTest:
    """Тестирование расширенной схемы PostgreSQL"""
    
    def __init__(self):
        self.connection_params = {
            'host': 'localhost',
            'port': 5432,
            'database': 'auto_reviews',
            'user': 'parser',
            'password': 'parser'
        }
        self.manager = ExtendedPostgresManager(self.connection_params)
        self.logger = logging.getLogger(__name__)
    
    async def run_all_tests(self):
        """Запуск всех тестов"""
        try:
            # Инициализируем подключение (БЕЗ создания схемы, т.к. она уже есть)
            await self.manager.initialize_connection_only()
            
            # Запускаем тесты
            await self.test_brands_operations()
            await self.test_models_operations()
            await self.test_authors_operations()
            await self.test_full_reviews_operations()
            await self.test_short_reviews_operations()
            await self.test_parse_sessions_operations()
            await self.test_statistics()
            
            logger.info("✅ Все тесты прошли успешно!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка тестирования: {e}")
            raise
        finally:
            await self.manager.close()
    
    async def test_brands(self):
        """Тестирование работы с брендами"""
        self.logger.info("🔧 Тестируем работу с брендами...")
        
        # Тестовые данные брендов
        test_brands = [
            BrandData(
                название="Toyota",
                название_в_url="toyota", 
                url="https://www.drom.ru/reviews/toyota/",
                количество_отзывов=50000
            ),
            BrandData(
                название="BMW",
                название_в_url="bmw",
                url="https://www.drom.ru/reviews/bmw/",
                количество_отзывов=15000
            ),
            BrandData(
                название="Mercedes-Benz",
                название_в_url="mercedes-benz",
                url="https://www.drom.ru/reviews/mercedes-benz/",
                количество_отзывов=12000
            )
        ]
        
        # Добавляем бренды
        brand_ids = []
        for brand in test_brands:
            brand_id = await self.manager.insert_brand(brand)
            brand_ids.append(brand_id)
            self.logger.debug(f"Добавлен бренд: {brand.название} (ID: {brand_id})")
        
        # Проверяем получение бренда по слагу
        toyota = await self.manager.get_brand_by_slug("toyota")
        assert toyota is not None, "Бренд Toyota не найден"
        assert toyota['название'] == "Toyota", "Неверное название бренда"
        
        # Проверяем список всех брендов
        all_brands = await self.manager.get_all_brands()
        assert len(all_brands) >= 3, "Недостаточно брендов в базе"
        
        self.logger.info("✅ Тесты брендов пройдены")
        return brand_ids
    
    async def test_models(self):
        """Тестирование работы с моделями"""
        self.logger.info("🔧 Тестируем работу с моделями...")
        
        # Сначала получаем Toyota
        toyota = await self.manager.get_brand_by_slug("toyota")
        toyota_id = toyota['id']
        
        # Тестовые модели
        test_models = [
            ModelData(
                бренд_id=toyota_id,
                название="Camry",
                название_в_url="camry",
                поколение="XV70",
                тип_кузова="седан",
                тип_топлива="бензин",
                url="https://www.drom.ru/reviews/toyota/camry/"
            ),
            ModelData(
                бренд_id=toyota_id,
                название="RAV4",
                название_в_url="rav4",
                поколение="XA50",
                тип_кузова="кроссовер",
                тип_топлива="бензин",
                url="https://www.drom.ru/reviews/toyota/rav4/"
            ),
            ModelData(
                бренд_id=toyota_id,
                название="Land Cruiser",
                название_в_url="land_cruiser",
                поколение="J200",
                тип_кузова="внедорожник",
                тип_топлива="бензин",
                url="https://www.drom.ru/reviews/toyota/land_cruiser/"
            )
        ]
        
        # Добавляем модели
        model_ids = []
        for model in test_models:
            model_id = await self.manager.insert_model(model)
            model_ids.append(model_id)
            self.logger.debug(f"Добавлена модель: {model.название} (ID: {model_id})")
        
        # Проверяем получение модели
        camry = await self.manager.get_model_by_brand_and_slug(toyota_id, "camry")
        assert camry is not None, "Модель Camry не найдена"
        assert camry['название'] == "Camry", "Неверное название модели"
        
        # Проверяем модели бренда
        toyota_models = await self.manager.get_models_by_brand(toyota_id)
        assert len(toyota_models) >= 3, "Недостаточно моделей у Toyota"
        
        self.logger.info("✅ Тесты моделей пройдены")
        return model_ids
    
    async def test_authors_and_cities(self):
        """Тестирование авторов и городов"""
        self.logger.info("🔧 Тестируем авторы и города...")
        
        # Создаем тестовых авторов
        author1_id = await self.manager.get_or_create_author(
            псевдоним="driver123",
            настоящее_имя="Иван Петров",
            город="Москва"
        )
        
        author2_id = await self.manager.get_or_create_author(
            псевдоним="auto_lover",
            город="Санкт-Петербург"
        )
        
        # Проверяем что авторы созданы
        assert author1_id > 0, "Автор 1 не создан"
        assert author2_id > 0, "Автор 2 не создан"
        
        # Проверяем что повторное создание возвращает тот же ID
        same_author_id = await self.manager.get_or_create_author(псевдоним="driver123")
        assert same_author_id == author1_id, "Автор создался дважды"
        
        self.logger.info("✅ Тесты авторов и городов пройдены")
        return [author1_id, author2_id]
    
    async def test_full_reviews(self):
        """Тестирование полных отзывов"""
        self.logger.info("🔧 Тестируем полные отзывы...")
        
        # Получаем модель и автора
        toyota = await self.manager.get_brand_by_slug("toyota")
        camry = await self.manager.get_model_by_brand_and_slug(toyota['id'], "camry")
        author_id = await self.manager.get_or_create_author("test_reviewer")
        
        # Тестовый отзыв
        from src.auto_reviews_parser.database.extended_postgres_manager import FullReviewData
        
        review_data = FullReviewData(
            модель_id=camry['id'],
            ссылка="https://www.drom.ru/reviews/toyota/camry/test123/",
            заголовок="Отличная машина для семьи",
            содержание="Подробный отзыв о Toyota Camry...",
            плюсы="Надежность, комфорт, экономичность",
            минусы="Высокая цена, дорогое обслуживание", 
            общий_рейтинг=4.5,
            год_приобретения=2020,
            пробег_км=50000,
            цвет_кузова="белый",
            количество_просмотров=150,
            количество_лайков=12,
            дата_публикации=datetime.now()
        )
        
        # Добавляем отзыв
        review_id = await self.manager.insert_full_review(review_data, author_id)
        assert review_id > 0, "Отзыв не добавлен"
        
        self.logger.info("✅ Тесты полных отзывов пройдены")
        return review_id
    
    async def test_short_reviews(self):
        """Тестирование коротких отзывов"""
        self.logger.info("🔧 Тестируем короткие отзывы...")
        
        # Получаем модель и автора
        toyota = await self.manager.get_brand_by_slug("toyota")
        rav4 = await self.manager.get_model_by_brand_and_slug(toyota['id'], "rav4")
        author_id = await self.manager.get_or_create_author("short_reviewer")
        
        # Тестовые короткие отзывы
        short_reviews = [
            ShortReviewData(
                модель_id=rav4['id'],
                внешний_id="short001",
                плюсы="Отличная проходимость",
                минусы="Расход топлива больше ожидаемого",
                поломки="Замена тормозных колодок",
                год_автомобиля=2019,
                объем_двигателя=2.0,
                тип_топлива="бензин",
                количество_фото=3,
                город_автора="Екатеринбург"
            ),
            ShortReviewData(
                модель_id=rav4['id'],
                внешний_id="short002",
                плюсы="Просторный салон",
                минусы="Жесткая подвеска",
                год_автомобиля=2020,
                объем_двигателя=2.5,
                тип_топлива="гибрид",
                количество_фото=0,
                город_автора="Новосибирск"
            )
        ]
        
        # Добавляем короткие отзывы
        short_review_ids = []
        for review in short_reviews:
            review_id = await self.manager.insert_short_review(review, author_id)
            short_review_ids.append(review_id)
        
        assert all(rid > 0 for rid in short_review_ids), "Не все короткие отзывы добавлены"
        
        self.logger.info("✅ Тесты коротких отзывов пройдены")
        return short_review_ids
    
    async def test_parse_sessions(self):
        """Тестирование сессий парсинга"""
        self.logger.info("🔧 Тестируем сессии парсинга...")
        
        # Создаем тестовую сессию
        session_data = ParseSessionData(
            тип_парсинга='test_session',
            версия_парсера='2.0_test'
        )
        
        session_id = await self.manager.start_parse_session(session_data)
        assert session_id > 0, "Сессия не создана"
        
        # Обновляем сессию
        await self.manager.update_parse_session(session_id, {
            'обработано_страниц': 5,
            'найдено_отзывов': 50,
            'сохранено_отзывов': 45,
            'ошибок': 5
        })
        
        # Завершаем сессию
        await self.manager.finish_parse_session(session_id, 'завершен')
        
        self.logger.info("✅ Тесты сессий парсинга пройдены")
        return session_id
    
    async def test_statistics(self):
        """Тестирование статистики"""
        self.logger.info("🔧 Тестируем статистику...")
        
        stats = await self.manager.get_database_stats()
        
        # Проверяем что статистика содержит нужные поля
        required_fields = [
            'бренды', 'модели', 'полные_отзывы', 'короткие_отзывы',
            'авторы', 'города', 'топ_бренды', 'последние_сессии'
        ]
        
        for field in required_fields:
            assert field in stats, f"Отсутствует поле в статистике: {field}"
        
        # Проверяем что данные есть
        assert stats['бренды'] >= 3, "Недостаточно брендов в статистике"
        assert stats['модели'] >= 3, "Недостаточно моделей в статистике"
        
        self.logger.info("📊 Статистика базы данных:")
        for key, value in stats.items():
            if key not in ['топ_бренды', 'последние_сессии']:
                self.logger.info(f"  {key}: {value}")
        
        if stats['топ_бренды']:
            self.logger.info("🏆 Топ брендов:")
            for brand in stats['топ_бренды'][:3]:
                self.logger.info(f"  {brand['название']}: {brand['количество_отзывов']} отзывов")
        
        self.logger.info("✅ Тесты статистики пройдены")


async def main():
    """Главная функция тестирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🚀 Запуск тестирования расширенной PostgreSQL схемы")
        
        # Проверяем подключение к PostgreSQL
        try:
            import asyncpg
        except ImportError:
            logger.error("❌ Модуль asyncpg не установлен. Установите: pip install asyncpg")
            return
        
        # Запускаем тесты
        test_runner = ExtendedSchemaTest()
        await test_runner.run_all_tests()
        
        logger.info("🎉 Все тесты расширенной схемы пройдены успешно!")
        
    except Exception as e:
        logger.error(f"💥 Критическая ошибка тестирования: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
