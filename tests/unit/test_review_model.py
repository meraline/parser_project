"""
Unit тесты для модели Review
"""
import pytest
from datetime import datetime
from src.auto_reviews_parser.models.review import Review


class TestReviewModel:
    """Тесты модели Review"""
    
    def test_review_creation_with_valid_data(self, sample_review_data):
        """Тест создания отзыва с валидными данными"""
        review = Review(**sample_review_data)
        
        assert review.brand == "Toyota"
        assert review.model == "Camry"
        assert review.year == 2020
        assert review.rating == 5
        assert review.source == "drom"
        assert review.type == "review"  # Исправляем ожидаемый тип
    
    def test_review_validation_invalid_rating(self, sample_review_data):
        """Тест валидации неверного рейтинга"""
        review_data = {
            "source": "drom.ru",
            "type": "review", 
            "brand": "Toyota",
            "model": "Camry",
            "rating": 6  # Неверный рейтинг
        }
        
        with pytest.raises(ValueError, match="Рейтинг должен быть от 1 до 5"):
            Review(**review_data)
    
    def test_review_validation_invalid_year(self, sample_review_data):
        """Тест валидации неверного года"""
        review_data = {
            "source": "drom.ru",
            "type": "review",
            "brand": "Toyota", 
            "model": "Camry",
            "year": 1800  # Слишком старый год
        }
        
        with pytest.raises(ValueError, match="Год должен быть разумным"):
            Review(**review_data)
    
    def test_review_validation_empty_content(self, sample_review_data):
        """Тест валидации пустого контента"""
        review_data = {
            "source": "drom.ru",
            "type": "review",
            "brand": "Toyota",
            "model": "Camry", 
            "content": {}  # Пустой контент
        }
        
        with pytest.raises(ValueError, match="Контент не может быть пустым"):
            Review(**review_data)
    
    def test_review_to_dict(self, sample_review_data):
        """Тест преобразования в словарь"""
        review = Review(**sample_review_data)
        result = review.to_dict()
        
        assert isinstance(result, dict)
        assert result["id"] == "1234567"
        assert result["brand"] == "Toyota"
        assert "content" in result
    
    def test_review_from_dict(self, sample_review_data):
        """Тест создания из словаря"""
        review = Review.from_dict(sample_review_data)
        
        assert review.id == "1234567"
        assert review.brand == "Toyota"
    
    def test_review_str_representation(self, sample_review_data):
        """Тест строкового представления"""
        review = Review(**sample_review_data)
        str_repr = str(review)
        
        assert "Toyota Camry" in str_repr
        assert "2020" in str_repr
    
    def test_review_hash(self, sample_review_data):
        """Тест хеширования отзыва"""
        review1 = Review(**sample_review_data)
        review2 = Review(**sample_review_data)
        
        assert hash(review1) == hash(review2)
    
    def test_review_equality(self, sample_review_data):
        """Тест сравнения отзывов"""
        import copy
        from datetime import datetime
        
        # Используем одинаковый parsed_at для корректного сравнения
        fixed_time = datetime.now()
        sample_review_data["parsed_at"] = fixed_time
        
        review1 = Review(**sample_review_data)
        
        # Создаем копию данных для второго отзыва
        sample_review_data2 = copy.deepcopy(sample_review_data)
        review2 = Review(**sample_review_data2)
        
        assert review1 == review2
        
        # Изменяем один отзыв
        sample_review_data3 = copy.deepcopy(sample_review_data)
        sample_review_data3["rating"] = 3
        review3 = Review(**sample_review_data3)
        
        assert review1 != review3
    
    @pytest.mark.parametrize("rating,expected", [
        (1, "очень плохо"),
        (2, "плохо"),
        (3, "средне"),
        (4, "хорошо"),
        (5, "отлично")
    ])
    def test_rating_description(self, sample_review_data, rating, expected):
        """Тест описания рейтинга"""
        sample_review_data["rating"] = rating
        review = Review(**sample_review_data)
        
        assert review.get_rating_description() == expected
    
    def test_review_age_calculation(self, sample_review_data):
        """Тест расчета возраста отзыва"""
        review = Review(**sample_review_data)
        age = review.get_age_days()
        
        assert isinstance(age, int)
        assert age >= 0
    
    def test_content_summary(self, sample_review_data):
        """Тест создания краткого содержания"""
        review = Review(**sample_review_data)
        summary = review.get_content_summary(max_length=50)
        
        assert len(summary) <= 50
        assert isinstance(summary, str)
    
    def test_is_useful_threshold(self, sample_review_data):
        """Тест определения полезности отзыва"""
        review = Review(**sample_review_data)
        
        # Отзыв полезен если ratio > 0.7
        assert review.is_useful(threshold=0.7) == True
        assert review.is_useful(threshold=0.9) == False
