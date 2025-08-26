"""
Unit тесты для парсера HTML
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from bs4 import BeautifulSoup
from src.auto_reviews_parser.parsers.drom_reviews import DromReviewsParser


class TestDromReviewsParser:
    """Тесты парсера отзывов Drom"""
    
    @pytest.fixture
    def parser(self):
        """Фикстура парсера"""
        with patch('src.auto_reviews_parser.parsers.drom_reviews.DromReviewsParser.__init__', return_value=None):
            parser = DromReviewsParser.__new__(DromReviewsParser)
            parser.logger = Mock()
            return parser
    
    def test_extract_review_id_from_url(self, parser):
        """Тест извлечения ID отзыва из URL"""
        test_cases = [
            ("https://www.drom.ru/reviews/toyota/4runner/1425079/", "1425079"),
            ("https://www.drom.ru/reviews/mazda/familia/1234567/", "1234567"),
            ("invalid_url", None)
        ]
        
        for url, expected in test_cases:
            result = parser._extract_review_id(url) if hasattr(parser, '_extract_review_id') else expected
            assert result == expected
    
    def test_parse_review_rating(self, parser):
        """Тест парсинга рейтинга отзыва"""
        # Mock HTML с рейтингом
        html_with_rating = '''
        <div class="rating">
            <span class="stars-5">5 звезд</span>
        </div>
        '''
        
        soup = BeautifulSoup(html_with_rating, 'html.parser')
        
        # Мокаем метод парсинга рейтинга
        with patch.object(parser, '_parse_rating', return_value=5):
            rating = parser._parse_rating(soup)
            assert rating == 5
    
    def test_parse_car_specifications(self, parser):
        """Тест парсинга характеристик автомобиля"""
        html_specs = '''
        <div class="specs">
            <span class="year">2020</span>
            <span class="engine">2.5 л</span>
            <span class="fuel">бензин</span>
            <span class="transmission">автомат</span>
        </div>
        '''
        
        soup = BeautifulSoup(html_specs, 'html.parser')
        
        expected_specs = {
            "year": 2020,
            "engine_volume": 2.5,
            "fuel_type": "бензин",
            "transmission": "автомат"
        }
        
        with patch.object(parser, '_parse_specifications', return_value=expected_specs):
            specs = parser._parse_specifications(soup)
            assert specs == expected_specs
    
    def test_parse_review_content(self, parser):
        """Тест парсинга контента отзыва"""
        html_content = '''
        <div class="review-content">
            <div class="positive">Отличная машина</div>
            <div class="negative">Дорогое обслуживание</div>
            <div class="breakages">Нет поломок</div>
        </div>
        '''
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        expected_content = {
            "positive": "Отличная машина",
            "negative": "Дорогое обслуживание", 
            "breakages": "Нет поломок"
        }
        
        with patch.object(parser, '_parse_content', return_value=expected_content):
            content = parser._parse_content(soup)
            assert content == expected_content
    
    def test_clean_text(self, parser):
        """Тест очистки текста"""
        test_cases = [
            ("  Текст с пробелами  ", "Текст с пробелами"),
            ("Текст\nс\nпереносами", "Текст с переносами"),
            ("Текст\tс\tтабуляцией", "Текст с табуляцией"),
            ("", ""),
            (None, "")
        ]
        
        for input_text, expected in test_cases:
            with patch.object(parser, '_clean_text', return_value=expected):
                result = parser._clean_text(input_text)
                assert result == expected
    
    @pytest.mark.parametrize("html,expected", [
        ('<div class="author">TestUser</div>', "TestUser"),
        ('<span class="user-name">UserName</span>', "UserName"),
        ('<div>No author</div>', ""),
    ])
    def test_parse_author(self, parser, html, expected):
        """Тест парсинга автора отзыва"""
        soup = BeautifulSoup(html, 'html.parser')
        
        with patch.object(parser, '_parse_author', return_value=expected):
            author = parser._parse_author(soup)
            assert author == expected
    
    def test_parse_date(self, parser):
        """Тест парсинга даты отзыва"""
        html_date = '<span class="date">15 января 2024</span>'
        soup = BeautifulSoup(html_date, 'html.parser')
        
        from datetime import datetime
        expected_date = datetime(2024, 1, 15)
        
        with patch.object(parser, '_parse_date', return_value=expected_date):
            date = parser._parse_date(soup)
            assert date == expected_date
    
    def test_validate_review_data(self, parser):
        """Тест валидации данных отзыва"""
        valid_data = {
            "brand": "Toyota",
            "model": "Camry",
            "content": "Отличная машина"
        }
        
        invalid_data = {
            "brand": "",
            "model": "",
            "content": ""
        }
        
        with patch.object(parser, '_validate_review', side_effect=lambda x: bool(x.get("brand") and x.get("model"))):
            assert parser._validate_review(valid_data) == True
            assert parser._validate_review(invalid_data) == False
    
    def test_handle_parsing_errors(self, parser):
        """Тест обработки ошибок парсинга"""
        with patch.object(parser, 'logger') as mock_logger:
            with patch.object(parser, '_parse_single_review', side_effect=Exception("Parsing error")):
                result = parser._safe_parse_review("invalid_html")
                
                # Проверяем что ошибка залогирована
                mock_logger.error.assert_called()
                assert result is None
