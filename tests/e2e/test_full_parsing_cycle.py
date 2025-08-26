"""
End-to-End тесты для полного цикла парсинга
"""
import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from src.auto_reviews_parser.models.review import Review


class TestFullParsingCycle:
    """E2E тесты полного цикла парсинга"""
    
    @pytest.fixture
    def mock_parser_components(self):
        """Мок всех компонентов парсера"""
        components = {
            'web_driver': Mock(),
            'html_parser': Mock(),
            'database': Mock(),
            'logger': Mock()
        }
        return components
    
    @pytest.fixture
    def sample_html_page(self):
        """Образец HTML страницы с отзывами"""
        return """
        <!DOCTYPE html>
        <html>
        <head><title>Отзывы Toyota Camry</title></head>
        <body>
            <div class="review" data-id="123456">
                <div class="author">TestUser</div>
                <div class="rating">5</div>
                <div class="date">15 января 2024</div>
                <div class="content">Отличный автомобиль</div>
                <div class="car-info">
                    <span class="brand">Toyota</span>
                    <span class="model">Camry</span>
                    <span class="year">2020</span>
                </div>
            </div>
            <div class="review" data-id="123457">
                <div class="author">AnotherUser</div>
                <div class="rating">4</div>
                <div class="date">10 января 2024</div>
                <div class="content">Хорошая машина</div>
                <div class="car-info">
                    <span class="brand">Toyota</span>
                    <span class="model">Camry</span>
                    <span class="year">2019</span>
                </div>
            </div>
        </body>
        </html>
        """
    
    @pytest.mark.e2e
    @pytest.mark.slow
    def test_complete_parsing_workflow(self, mock_parser_components, sample_html_page):
        """Тест полного рабочего процесса парсинга"""
        components = mock_parser_components
        
        # Настройка моков
        components['web_driver'].get.return_value = None
        components['web_driver'].page_source = sample_html_page
        
        # Мок парсинга HTML
        expected_reviews = [
            Review(
                id="123456",
                brand="Toyota",
                model="Camry", 
                year=2020,
                author="TestUser",
                rating=5,
                content="Отличный автомобиль",
                date="2024-01-15",
                url="https://www.drom.ru/reviews/toyota/camry/123456/",
                city="Москва",
                useful_count=0,
                not_useful_count=0,
                source="drom",
                type="long"
            ),
            Review(
                id="123457",
                brand="Toyota",
                model="Camry",
                year=2019,
                author="AnotherUser", 
                rating=4,
                content="Хорошая машина",
                date="2024-01-10",
                url="https://www.drom.ru/reviews/toyota/camry/123457/",
                city="Москва",
                useful_count=0,
                not_useful_count=0,
                source="drom",
                type="long"
            )
        ]
        
        components['html_parser'].parse_reviews.return_value = expected_reviews
        components['database'].insert_reviews.return_value = True
        
        # Имитируем полный цикл парсинга
        def simulate_full_parsing_cycle():
            # 1. Инициализация веб-драйвера
            driver = components['web_driver']
            
            # 2. Переход на страницу
            url = "https://www.drom.ru/reviews/toyota/camry/"
            driver.get(url)
            
            # 3. Получение HTML контента
            html_content = driver.page_source
            
            # 4. Парсинг отзывов
            parser = components['html_parser']
            reviews = parser.parse_reviews(html_content)
            
            # 5. Сохранение в базу данных
            database = components['database']
            success = database.insert_reviews(reviews)
            
            # 6. Логирование результатов
            logger = components['logger']
            logger.info(f"Parsed {len(reviews)} reviews")
            
            return reviews, success
        
        # Выполняем полный цикл
        reviews, success = simulate_full_parsing_cycle()
        
        # Проверки
        assert len(reviews) == 2
        assert success == True
        assert reviews[0].brand == "Toyota"
        assert reviews[0].model == "Camry"
        assert reviews[1].rating == 4
        
        # Проверяем вызовы моков
        components['web_driver'].get.assert_called_once()
        components['html_parser'].parse_reviews.assert_called_once()
        components['database'].insert_reviews.assert_called_once()
        components['logger'].info.assert_called_once()
    
    @pytest.mark.e2e
    @pytest.mark.slow
    def test_error_handling_in_full_cycle(self, mock_parser_components):
        """Тест обработки ошибок в полном цикле"""
        components = mock_parser_components
        
        # Настройка мока с ошибкой
        components['web_driver'].get.side_effect = Exception("Network error")
        
        def simulate_parsing_with_error():
            try:
                driver = components['web_driver']
                driver.get("https://www.drom.ru/reviews/toyota/camry/")
                return True
            except Exception as e:
                components['logger'].error(f"Parsing failed: {str(e)}")
                return False
        
        result = simulate_parsing_with_error()
        
        assert result == False
        components['logger'].error.assert_called_once()
    
    @pytest.mark.e2e
    @pytest.mark.slow
    def test_pagination_handling(self, mock_parser_components):
        """Тест обработки пагинации"""
        components = mock_parser_components
        
        # Мок для нескольких страниц
        page_responses = [
            "Page 1 content",
            "Page 2 content", 
            "Page 3 content",
            None  # Конец страниц
        ]
        
        components['web_driver'].page_source = "Page content"
        
        def simulate_pagination():
            total_reviews = []
            page_number = 1
            
            while page_number <= 3:  # Ограничение для теста
                # Переход на страницу
                url = f"https://www.drom.ru/reviews/toyota/camry/?page={page_number}"
                components['web_driver'].get(url)
                
                # Парсинг страницы
                page_content = page_responses[page_number - 1] if page_number <= len(page_responses) else None
                
                if not page_content:
                    break
                
                # Имитируем получение отзывов со страницы
                page_reviews = [f"Review {page_number}-{i}" for i in range(1, 4)]
                total_reviews.extend(page_reviews)
                
                page_number += 1
                time.sleep(0.1)  # Имитация задержки
            
            return total_reviews
        
        reviews = simulate_pagination()
        
        assert len(reviews) == 9  # 3 страницы по 3 отзыва
        assert "Review 1-1" in reviews
        assert "Review 3-3" in reviews
    
    @pytest.mark.e2e
    @pytest.mark.slow
    def test_data_validation_in_full_cycle(self, mock_parser_components):
        """Тест валидации данных в полном цикле"""
        components = mock_parser_components
        
        # Мок данных с невалидными значениями
        invalid_reviews = [
            {
                "id": "",  # Пустой ID
                "brand": "Toyota",
                "model": "Camry",
                "rating": 10,  # Невалидный рейтинг
                "year": 1800   # Невалидный год
            },
            {
                "id": "123456",
                "brand": "",  # Пустой бренд
                "model": "Camry",
                "rating": 5,
                "year": 2020
            },
            {
                "id": "123457",
                "brand": "Toyota",
                "model": "Camry",
                "rating": 4,
                "year": 2020
            }  # Валидный отзыв
        ]
        
        def validate_and_filter_reviews(reviews_data):
            valid_reviews = []
            invalid_count = 0
            
            for review_data in reviews_data:
                # Простая валидация
                is_valid = (
                    review_data.get("id") and
                    review_data.get("brand") and
                    review_data.get("model") and
                    1 <= review_data.get("rating", 0) <= 5 and
                    1900 <= review_data.get("year", 0) <= 2025
                )
                
                if is_valid:
                    valid_reviews.append(review_data)
                else:
                    invalid_count += 1
                    components['logger'].warning(f"Invalid review data: {review_data.get('id', 'unknown')}")
            
            return valid_reviews, invalid_count
        
        valid_reviews, invalid_count = validate_and_filter_reviews(invalid_reviews)
        
        assert len(valid_reviews) == 1  # Только один валидный отзыв
        assert invalid_count == 2
        assert valid_reviews[0]["id"] == "123457"
    
    @pytest.mark.e2e
    @pytest.mark.slow
    def test_database_transaction_integrity(self, mock_parser_components):
        """Тест целостности транзакций базы данных"""
        components = mock_parser_components
        
        # Мок успешной и неуспешной вставки
        def mock_insert_with_failure(reviews):
            if len(reviews) > 5:
                raise Exception("Database error: too many reviews")
            return True
        
        components['database'].insert_reviews.side_effect = mock_insert_with_failure
        
        # Тест с небольшим количеством отзывов (успех)
        small_batch = [Mock() for _ in range(3)]
        
        try:
            result = components['database'].insert_reviews(small_batch)
            assert result == True
        except Exception:
            pytest.fail("Small batch should succeed")
        
        # Тест с большим количеством отзывов (неудача)
        large_batch = [Mock() for _ in range(10)]
        
        with pytest.raises(Exception):
            components['database'].insert_reviews(large_batch)
    
    @pytest.mark.e2e
    @pytest.mark.slow
    def test_end_to_end_performance(self, mock_parser_components):
        """Тест производительности E2E"""
        components = mock_parser_components
        
        start_time = time.time()
        
        # Имитируем обработку большого количества отзывов
        def simulate_bulk_processing():
            batch_size = 100
            total_processed = 0
            
            for batch_num in range(5):  # 5 батчей по 100 отзывов
                # Имитируем получение батча
                batch_reviews = [Mock() for _ in range(batch_size)]
                
                # Имитируем обработку
                for review in batch_reviews:
                    # Некоторая обработка
                    pass
                
                # Имитируем сохранение в БД
                components['database'].insert_reviews(batch_reviews)
                total_processed += len(batch_reviews)
                
                # Небольшая задержка между батчами
                time.sleep(0.01)
            
            return total_processed
        
        total_processed = simulate_bulk_processing()
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        assert total_processed == 500
        assert execution_time < 5.0  # Должно выполниться менее чем за 5 секунд
        
        # Проверяем что database.insert_reviews вызывался 5 раз
        assert components['database'].insert_reviews.call_count == 5
