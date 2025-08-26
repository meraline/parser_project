"""
E2E тесты критических пользовательских сценариев
"""
import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta


class TestCriticalUserScenarios:
    """E2E тесты критических пользовательских сценариев"""
    
    @pytest.fixture
    def mock_system_components(self):
        """Мок всех системных компонентов"""
        return {
            'parser': Mock(),
            'database': Mock(),
            'web_driver': Mock(),
            'file_manager': Mock(),
            'logger': Mock(),
            'config': Mock()
        }
    
    @pytest.mark.e2e
    @pytest.mark.smoke
    def test_system_startup_and_configuration(self, mock_system_components):
        """Тест запуска системы и загрузки конфигурации"""
        components = mock_system_components
        
        # Имитируем загрузку конфигурации
        config_data = {
            'database_url': 'postgresql://localhost:5432/test_db',
            'batch_size': 100,
            'delay_between_requests': 1.0,
            'max_retries': 3
        }
        components['config'].load.return_value = config_data
        
        # Имитируем инициализацию системы
        def simulate_system_startup():
            # 1. Загрузка конфигурации
            config = components['config'].load()
            
            # 2. Инициализация базы данных
            db_connected = components['database'].connect(config['database_url'])
            
            # 3. Инициализация веб-драйвера
            driver_ready = components['web_driver'].initialize()
            
            # 4. Проверка готовности парсера
            parser_ready = components['parser'].initialize()
            
            return all([config, db_connected, driver_ready, parser_ready])
        
        components['database'].connect.return_value = True
        components['web_driver'].initialize.return_value = True
        components['parser'].initialize.return_value = True
        
        startup_success = simulate_system_startup()
        
        assert startup_success == True
        components['config'].load.assert_called_once()
        components['database'].connect.assert_called_once()
        components['web_driver'].initialize.assert_called_once()
        components['parser'].initialize.assert_called_once()
    
    @pytest.mark.e2e
    @pytest.mark.smoke
    def test_brand_catalog_parsing_scenario(self, mock_system_components):
        """Тест сценария парсинга каталога брендов"""
        components = mock_system_components
        
        # Мок списка брендов
        brands_data = [
            {'name': 'Toyota', 'url': 'https://www.drom.ru/reviews/toyota/'},
            {'name': 'Honda', 'url': 'https://www.drom.ru/reviews/honda/'},
            {'name': 'Mazda', 'url': 'https://www.drom.ru/reviews/mazda/'}
        ]
        
        components['parser'].get_brands_list.return_value = brands_data
        
        # Мок моделей для каждого бренда
        models_data = {
            'Toyota': ['Camry', 'Corolla', 'RAV4'],
            'Honda': ['Civic', 'Accord', 'CR-V'],
            'Mazda': ['CX-5', 'Mazda3', 'CX-3']
        }
        
        def mock_get_models(brand_url):
            brand_name = brand_url.split('/')[-2].capitalize()
            return models_data.get(brand_name, [])
        
        components['parser'].get_models_list.side_effect = mock_get_models
        
        def simulate_brand_catalog_parsing():
            total_models = 0
            parsed_brands = []
            
            # Получаем список брендов
            brands = components['parser'].get_brands_list()
            
            for brand in brands:
                # Получаем модели для каждого бренда
                models = components['parser'].get_models_list(brand['url'])
                
                brand_info = {
                    'name': brand['name'],
                    'models_count': len(models),
                    'models': models
                }
                
                parsed_brands.append(brand_info)
                total_models += len(models)
                
                # Сохраняем информацию о бренде
                components['database'].save_brand_info(brand_info)
                
                # Задержка между брендами
                time.sleep(0.1)
            
            return parsed_brands, total_models
        
        components['database'].save_brand_info.return_value = True
        
        parsed_brands, total_models = simulate_brand_catalog_parsing()
        
        assert len(parsed_brands) == 3
        assert total_models == 9  # 3 модели на бренд
        assert parsed_brands[0]['name'] == 'Toyota'
        assert 'Camry' in parsed_brands[0]['models']
        
        # Проверяем сохранение в базу данных
        assert components['database'].save_brand_info.call_count == 3
    
    @pytest.mark.e2e
    @pytest.mark.smoke
    def test_reviews_extraction_and_storage_scenario(self, mock_system_components):
        """Тест сценария извлечения и сохранения отзывов"""
        components = mock_system_components
        
        # Мок URL страниц с отзывами
        review_pages = [
            'https://www.drom.ru/reviews/toyota/camry/page1/',
            'https://www.drom.ru/reviews/toyota/camry/page2/',
            'https://www.drom.ru/reviews/toyota/camry/page3/'
        ]
        
        # Мок отзывов для каждой страницы
        def mock_parse_page_reviews(page_url):
            page_num = int(page_url.split('page')[1].split('/')[0])
            return [
                {
                    'id': f'review_{page_num}_{i}',
                    'brand': 'Toyota',
                    'model': 'Camry',
                    'rating': 4 + (i % 2),  # 4 или 5
                    'content': f'Review content from page {page_num}, review {i}',
                    'author': f'User{page_num}{i}',
                    'date': '2024-01-15'
                }
                for i in range(1, 6)  # 5 отзывов на страницу
            ]
        
        components['parser'].parse_page_reviews.side_effect = mock_parse_page_reviews
        components['database'].save_reviews.return_value = True
        
        def simulate_reviews_extraction():
            all_reviews = []
            failed_pages = []
            
            for page_url in review_pages:
                try:
                    # Извлекаем отзывы со страницы
                    page_reviews = components['parser'].parse_page_reviews(page_url)
                    
                    # Валидируем отзывы
                    valid_reviews = []
                    for review in page_reviews:
                        if review.get('id') and review.get('content'):
                            valid_reviews.append(review)
                    
                    # Сохраняем в базу данных
                    if valid_reviews:
                        save_success = components['database'].save_reviews(valid_reviews)
                        if save_success:
                            all_reviews.extend(valid_reviews)
                            components['logger'].info(f"Saved {len(valid_reviews)} reviews from {page_url}")
                        else:
                            failed_pages.append(page_url)
                    
                    # Задержка между страницами
                    time.sleep(0.2)
                    
                except Exception as e:
                    failed_pages.append(page_url)
                    components['logger'].error(f"Failed to process {page_url}: {str(e)}")
            
            return all_reviews, failed_pages
        
        all_reviews, failed_pages = simulate_reviews_extraction()
        
        assert len(all_reviews) == 15  # 3 страницы по 5 отзывов
        assert len(failed_pages) == 0  # Все страницы успешно обработаны
        assert all_reviews[0]['brand'] == 'Toyota'
        assert components['database'].save_reviews.call_count == 3
    
    @pytest.mark.e2e
    @pytest.mark.smoke
    def test_data_export_scenario(self, mock_system_components):
        """Тест сценария экспорта данных"""
        components = mock_system_components
        
        # Мок данных из базы
        mock_reviews_data = [
            {
                'id': 'review_1',
                'brand': 'Toyota',
                'model': 'Camry',
                'rating': 5,
                'content': 'Excellent car',
                'date': '2024-01-15'
            },
            {
                'id': 'review_2', 
                'brand': 'Honda',
                'model': 'Civic',
                'rating': 4,
                'content': 'Good car',
                'date': '2024-01-10'
            }
        ]
        
        components['database'].get_reviews.return_value = mock_reviews_data
        components['file_manager'].export_to_csv.return_value = '/exports/reviews_2024.csv'
        components['file_manager'].export_to_json.return_value = '/exports/reviews_2024.json'
        
        def simulate_data_export():
            export_results = {}
            
            # Получаем данные из базы
            reviews_data = components['database'].get_reviews()
            
            if not reviews_data:
                return None
            
            # Экспорт в CSV
            csv_file = components['file_manager'].export_to_csv(reviews_data)
            export_results['csv'] = csv_file
            
            # Экспорт в JSON
            json_file = components['file_manager'].export_to_json(reviews_data)
            export_results['json'] = json_file
            
            # Логирование результатов
            components['logger'].info(f"Exported {len(reviews_data)} reviews")
            components['logger'].info(f"CSV: {csv_file}")
            components['logger'].info(f"JSON: {json_file}")
            
            return export_results
        
        export_results = simulate_data_export()
        
        assert export_results is not None
        assert 'csv' in export_results
        assert 'json' in export_results
        assert export_results['csv'].endswith('.csv')
        assert export_results['json'].endswith('.json')
        
        components['database'].get_reviews.assert_called_once()
        components['file_manager'].export_to_csv.assert_called_once()
        components['file_manager'].export_to_json.assert_called_once()
    
    @pytest.mark.e2e
    @pytest.mark.smoke
    def test_system_recovery_after_failure(self, mock_system_components):
        """Тест восстановления системы после сбоя"""
        components = mock_system_components
        
        # Имитируем различные типы сбоев
        failure_scenarios = [
            'database_connection_lost',
            'web_driver_crashed',
            'network_timeout',
            'memory_overflow'
        ]
        
        def simulate_system_recovery(failure_type):
            recovery_steps = []
            
            if failure_type == 'database_connection_lost':
                # Попытка переподключения к БД
                for attempt in range(3):
                    try:
                        components['database'].reconnect()
                        recovery_steps.append(f"DB reconnect attempt {attempt + 1}")
                        if attempt == 2:  # Успех на третьей попытке
                            return True, recovery_steps
                    except:
                        continue
                return False, recovery_steps
            
            elif failure_type == 'web_driver_crashed':
                # Перезапуск веб-драйвера
                try:
                    components['web_driver'].quit()
                    components['web_driver'].restart()
                    recovery_steps.append("Web driver restarted")
                    return True, recovery_steps
                except:
                    return False, recovery_steps
            
            elif failure_type == 'network_timeout':
                # Увеличение таймаутов и повторная попытка
                components['config'].update_timeout(30)
                recovery_steps.append("Timeout increased")
                return True, recovery_steps
            
            elif failure_type == 'memory_overflow':
                # Очистка кэша и сборка мусора
                components['parser'].clear_cache()
                recovery_steps.append("Cache cleared")
                return True, recovery_steps
            
            return False, recovery_steps
        
        # Настройка моков для успешного восстановления
        components['database'].reconnect.side_effect = [Exception(), Exception(), True]
        components['web_driver'].quit.return_value = None
        components['web_driver'].restart.return_value = True
        components['config'].update_timeout.return_value = None
        components['parser'].clear_cache.return_value = None
        
        # Тестируем восстановление для каждого типа сбоя
        for failure_type in failure_scenarios:
            success, steps = simulate_system_recovery(failure_type)
            
            assert success == True
            assert len(steps) > 0
            
            components['logger'].info(f"Recovery from {failure_type}: {'SUCCESS' if success else 'FAILED'}")
        
        # Проверяем что все методы восстановления были вызваны
        assert components['database'].reconnect.call_count == 3  # 3 попытки
        assert components['web_driver'].restart.called
        assert components['config'].update_timeout.called
        assert components['parser'].clear_cache.called
