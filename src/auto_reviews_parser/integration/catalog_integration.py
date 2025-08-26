"""
Интеграционный адаптер для работы с существующими парсерами брендов и моделей
Связывает каталогные парсеры с расширенной PostgreSQL схемой
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

# Импорты для работы с существующими парсерами
import sys
sys.path.append('/home/analityk/Документы/projects/parser_project')

try:
    from scripts.parsing.brand_catalog_extractor import BrandCatalogExtractor, BrandInfo, CatalogData
    from src.auto_reviews_parser.database.extended_postgres_manager import (
        ExtendedPostgresManager, BrandData, ModelData, ShortReviewData, ParseSessionData
    )
    from src.auto_reviews_parser.parsers.drom_reviews import DromReviewsParser
except ImportError as e:
    logging.warning(f"Импорт парсеров не удался: {e}")


@dataclass
class CatalogIntegrationResult:
    """Результат интеграции каталога"""
    новых_брендов: int
    обновленных_брендов: int
    новых_моделей: int
    обновленных_моделей: int
    ошибок: int
    детали_ошибок: List[str]


class CatalogIntegrator:
    """Интегратор для работы с каталогом брендов и моделей"""
    
    def __init__(self, postgres_manager: ExtendedPostgresManager):
        self.postgres_manager = postgres_manager
        self.logger = logging.getLogger(__name__)
        self.catalog_extractor = BrandCatalogExtractor()
    
    async def extract_and_import_catalog(self, force_update: bool = False) -> CatalogIntegrationResult:
        """
        Извлечение каталога с drom.ru и импорт в PostgreSQL
        
        Args:
            force_update: Принудительное обновление даже если каталог существует
        """
        result = CatalogIntegrationResult(
            новых_брендов=0, обновленных_брендов=0,
            новых_моделей=0, обновленных_моделей=0,
            ошибок=0, детали_ошибок=[]
        )
        
        # Начинаем сессию парсинга
        session_data = ParseSessionData(
            тип_парсинга='catalog',
            версия_парсера='2.0'
        )
        session_id = await self.postgres_manager.start_parse_session(session_data)
        
        try:
            self.logger.info("🚀 Начинаем извлечение и импорт каталога брендов")
            
            # Извлекаем каталог
            catalog_data = await self._extract_catalog_data(force_update)
            if not catalog_data:
                raise ValueError("Не удалось извлечь данные каталога")
            
            self.logger.info(f"📦 Извлечен каталог: {catalog_data.total_brands} брендов")
            
            # Импортируем бренды и модели
            await self._import_brands_and_models(catalog_data, result)
            
            # Обновляем сессию
            await self.postgres_manager.update_parse_session(session_id, {
                'обработано_страниц': 1,
                'найдено_отзывов': catalog_data.total_brands,
                'сохранено_отзывов': result.новых_брендов + result.обновленных_брендов,
                'ошибок': result.ошибок
            })
            
            await self.postgres_manager.finish_parse_session(session_id, 'завершен')
            
            self.logger.info("✅ Импорт каталога завершен успешно")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка импорта каталога: {e}")
            result.ошибок += 1
            result.детали_ошибок.append(str(e))
            
            await self.postgres_manager.finish_parse_session(
                session_id, 'ошибка', str(e)
            )
        
        return result
    
    async def _extract_catalog_data(self, force_update: bool) -> Optional[CatalogData]:
        """Извлечение данных каталога"""
        # Проверяем наличие кешированного каталога
        cache_file = Path('data/brand_catalog.json')
        
        if cache_file.exists() and not force_update:
            self.logger.info("📂 Используем кешированный каталог")
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return CatalogData(
                        extraction_date=datetime.fromisoformat(data['extraction_date']),
                        total_brands=data['total_brands'],
                        brands=[BrandInfo(**brand) for brand in data['brands']]
                    )
            except Exception as e:
                self.logger.warning(f"Ошибка чтения кеша: {e}")
        
        # Извлекаем новый каталог
        self.logger.info("🌐 Извлекаем свежий каталог с drom.ru")
        try:
            # Используем существующий экстрактор
            catalog_data = self.catalog_extractor.extract_full_catalog()
            
            # Сохраняем в кеш
            if catalog_data:
                self._save_catalog_to_cache(catalog_data, cache_file)
            
            return catalog_data
            
        except Exception as e:
            self.logger.error(f"Ошибка извлечения каталога: {e}")
            return None
    
    def _save_catalog_to_cache(self, catalog_data: CatalogData, cache_file: Path):
        """Сохранение каталога в кеш"""
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            
            data = {
                'extraction_date': catalog_data.extraction_date.isoformat(),
                'total_brands': catalog_data.total_brands,
                'brands': [
                    {
                        'name': brand.name,
                        'slug': brand.slug,
                        'url': brand.url,
                        'review_count': brand.review_count,
                        'models': brand.models
                    }
                    for brand in catalog_data.brands
                ]
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"💾 Каталог сохранен в кеш: {cache_file}")
            
        except Exception as e:
            self.logger.warning(f"Ошибка сохранения кеша: {e}")
    
    async def _import_brands_and_models(self, catalog_data: CatalogData, result: CatalogIntegrationResult):
        """Импорт брендов и моделей в PostgreSQL"""
        
        for brand_info in catalog_data.brands:
            try:
                # Импортируем бренд
                brand_data = BrandData(
                    название=brand_info.name,
                    название_в_url=brand_info.slug,
                    url=brand_info.url,
                    количество_отзывов=brand_info.review_count
                )
                
                # Проверяем существование бренда
                existing_brand = await self.postgres_manager.get_brand_by_slug(brand_info.slug)
                
                brand_id = await self.postgres_manager.insert_brand(brand_data)
                
                if existing_brand:
                    result.обновленных_брендов += 1
                    self.logger.debug(f"🔄 Обновлен бренд: {brand_info.name}")
                else:
                    result.новых_брендов += 1
                    self.logger.debug(f"➕ Добавлен бренд: {brand_info.name}")
                
                # Импортируем модели бренда
                await self._import_brand_models(brand_id, brand_info, result)
                
            except Exception as e:
                self.logger.error(f"Ошибка импорта бренда {brand_info.name}: {e}")
                result.ошибок += 1
                result.детали_ошибок.append(f"Бренд {brand_info.name}: {e}")
    
    async def _import_brand_models(self, brand_id: int, brand_info: BrandInfo, result: CatalogIntegrationResult):
        """Импорт моделей конкретного бренда"""
        
        for model_slug in brand_info.models:
            try:
                # Создаем данные модели
                model_data = ModelData(
                    бренд_id=brand_id,
                    название=model_slug.replace('_', ' ').title(),  # Примерное название
                    название_в_url=model_slug,
                    url=f"{brand_info.url.rstrip('/')}/{model_slug}/"
                )
                
                # Проверяем существование модели
                existing_model = await self.postgres_manager.get_model_by_brand_and_slug(
                    brand_id, model_slug
                )
                
                model_id = await self.postgres_manager.insert_model(model_data)
                
                if existing_model:
                    result.обновленных_моделей += 1
                else:
                    result.новых_моделей += 1
                
            except Exception as e:
                self.logger.error(f"Ошибка импорта модели {model_slug}: {e}")
                result.ошибок += 1
                result.детали_ошибок.append(f"Модель {model_slug}: {e}")


class ShortReviewsIntegrator:
    """Интегратор для работы с короткими отзывами"""
    
    def __init__(self, postgres_manager: ExtendedPostgresManager):
        self.postgres_manager = postgres_manager
        self.logger = logging.getLogger(__name__)
        self.drom_parser = DromReviewsParser()
    
    async def parse_and_import_short_reviews(self, model_id: int, model_url: str, 
                                           max_pages: int = 10) -> Dict[str, Any]:
        """
        Парсинг и импорт коротких отзывов для модели
        
        Args:
            model_id: ID модели в PostgreSQL
            model_url: URL модели на drom.ru
            max_pages: Максимальное количество страниц для парсинга
        """
        result = {
            'обработано_страниц': 0,
            'найдено_отзывов': 0,
            'сохранено_отзывов': 0,
            'ошибок': 0,
            'детали_ошибок': []
        }
        
        # Начинаем сессию парсинга
        session_data = ParseSessionData(
            тип_парсинга='short_reviews',
            модель_id=model_id,
            версия_парсера='2.0'
        )
        session_id = await self.postgres_manager.start_parse_session(session_data)
        
        try:
            self.logger.info(f"🚀 Начинаем парсинг коротких отзывов для модели {model_id}")
            
            # Формируем URL для коротких отзывов
            short_reviews_url = f"{model_url.rstrip('/')}/5kopeek/"
            
            # Парсим короткие отзывы
            for page in range(1, max_pages + 1):
                try:
                    page_url = f"{short_reviews_url}?page={page}"
                    self.logger.debug(f"📄 Обрабатываем страницу {page}: {page_url}")
                    
                    # Используем существующий парсер
                    short_reviews = self.drom_parser.parse_short_reviews(page_url)
                    
                    if not short_reviews:
                        self.logger.info(f"Страница {page} пуста, завершаем")
                        break
                    
                    result['обработано_страниц'] += 1
                    result['найдено_отзывов'] += len(short_reviews)
                    
                    # Сохраняем отзывы
                    for review_data in short_reviews:
                        try:
                            # Преобразуем в нашу структуру
                            short_review = await self._convert_review_data(
                                review_data, model_id
                            )
                            
                            # Обрабатываем автора
                            author_id = None
                            if review_data.get('author_name'):
                                author_id = await self.postgres_manager.get_or_create_author(
                                    псевдоним=review_data['author_name'],
                                    город=review_data.get('author_city')
                                )
                            
                            # Сохраняем отзыв
                            review_id = await self.postgres_manager.insert_short_review(
                                short_review, author_id
                            )
                            
                            if review_id:
                                result['сохранено_отзывов'] += 1
                            
                        except Exception as e:
                            result['ошибок'] += 1
                            result['детали_ошибок'].append(f"Отзыв на странице {page}: {e}")
                    
                    # Обновляем прогресс сессии
                    await self.postgres_manager.update_parse_session(session_id, {
                        'обработано_страниц': result['обработано_страниц'],
                        'найдено_отзывов': result['найдено_отзывов'],
                        'сохранено_отзывов': result['сохранено_отзывов'],
                        'ошибок': result['ошибок']
                    })
                    
                except Exception as e:
                    self.logger.error(f"Ошибка обработки страницы {page}: {e}")
                    result['ошибок'] += 1
                    result['детали_ошибок'].append(f"Страница {page}: {e}")
            
            await self.postgres_manager.finish_parse_session(session_id, 'завершен')
            self.logger.info("✅ Парсинг коротких отзывов завершен")
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка парсинга: {e}")
            result['ошибок'] += 1
            result['детали_ошибок'].append(str(e))
            
            await self.postgres_manager.finish_parse_session(
                session_id, 'ошибка', str(e)
            )
        
        return result
    
    async def _convert_review_data(self, review_data: Dict, model_id: int) -> ShortReviewData:
        """Преобразование данных отзыва в нашу структуру"""
        return ShortReviewData(
            модель_id=model_id,
            внешний_id=str(review_data.get('review_id', '')),
            плюсы=review_data.get('positive_text'),
            минусы=review_data.get('negative_text'),
            поломки=review_data.get('breakages_text'),
            год_автомобиля=review_data.get('car_year'),
            объем_двигателя=review_data.get('car_engine_volume'),
            количество_фото=review_data.get('photos_count', 0),
            город_автора=review_data.get('author_city'),
            дата_публикации=review_data.get('publication_date'),
            статус_парсинга='успех'
        )


# Основной класс интеграции
class ParserIntegrationManager:
    """Главный менеджер интеграции парсеров с PostgreSQL"""
    
    def __init__(self, postgres_connection_params: Dict[str, Any]):
        self.postgres_manager = ExtendedPostgresManager(postgres_connection_params)
        self.catalog_integrator = CatalogIntegrator(self.postgres_manager)
        self.short_reviews_integrator = ShortReviewsIntegrator(self.postgres_manager)
        self.logger = logging.getLogger(__name__)
    
    async def initialize(self):
        """Инициализация интеграционного менеджера"""
        await self.postgres_manager.initialize()
        self.logger.info("✅ Интеграционный менеджер инициализирован")
    
    async def close(self):
        """Закрытие интеграционного менеджера"""
        await self.postgres_manager.close()
    
    async def full_catalog_integration(self, force_update: bool = False) -> CatalogIntegrationResult:
        """Полная интеграция каталога брендов и моделей"""
        return await self.catalog_integrator.extract_and_import_catalog(force_update)
    
    async def integrate_short_reviews_for_model(self, brand_slug: str, model_slug: str, 
                                              max_pages: int = 10) -> Dict[str, Any]:
        """Интеграция коротких отзывов для конкретной модели"""
        # Получаем бренд и модель
        brand = await self.postgres_manager.get_brand_by_slug(brand_slug)
        if not brand:
            raise ValueError(f"Бренд {brand_slug} не найден")
        
        model = await self.postgres_manager.get_model_by_brand_and_slug(
            brand['id'], model_slug
        )
        if not model:
            raise ValueError(f"Модель {model_slug} не найдена")
        
        # Парсим короткие отзывы
        return await self.short_reviews_integrator.parse_and_import_short_reviews(
            model['id'], model['url'], max_pages
        )
    
    async def get_integration_stats(self) -> Dict[str, Any]:
        """Получение статистики интеграции"""
        return await self.postgres_manager.get_database_stats()


# Пример использования
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    async def test_integration():
        connection_params = {
            'host': 'localhost',
            'port': 5432,
            'database': 'auto_reviews',
            'user': 'parser',
            'password': 'parser'
        }
        
        manager = ParserIntegrationManager(connection_params)
        
        try:
            await manager.initialize()
            
            # Интеграция каталога
            catalog_result = await manager.full_catalog_integration()
            print("Результат интеграции каталога:", catalog_result)
            
            # Статистика
            stats = await manager.get_integration_stats()
            print("Статистика:", stats)
            
        finally:
            await manager.close()
    
    asyncio.run(test_integration())
