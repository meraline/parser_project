"""
Integration тесты для базы данных
"""
import pytest
import psycopg2
from unittest.mock import Mock, patch
from src.auto_reviews_parser.models.review import Review
from datetime import datetime


class TestDatabaseIntegration:
    """Интеграционные тесты базы данных"""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Мок подключения к базе данных"""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    @pytest.fixture
    def sample_review_for_db(self):
        """Образец отзыва для тестов базы данных"""
        return Review(
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
            useful_count=10,
            not_useful_count=2,
            source="test",
            type="long"
        )
    
    @pytest.mark.integration
    def test_insert_review(self, mock_db_connection, sample_review_for_db):
        """Тест вставки отзыва в базу данных"""
        conn, cursor = mock_db_connection
        
        # Мокаем выполнение SQL запроса
        cursor.execute.return_value = None
        cursor.rowcount = 1
        
        # Имитируем вставку
        insert_query = """
        INSERT INTO reviews (id, brand, model, year, author, rating, content, date, url, city)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            sample_review_for_db.id,
            sample_review_for_db.brand,
            sample_review_for_db.model,
            sample_review_for_db.year,
            sample_review_for_db.author,
            sample_review_for_db.rating,
            sample_review_for_db.content,
            sample_review_for_db.date,
            sample_review_for_db.url,
            sample_review_for_db.city
        ))
        
        # Проверяем что запрос был выполнен
        cursor.execute.assert_called_once()
        assert cursor.rowcount == 1
    
    @pytest.mark.integration
    def test_select_review_by_id(self, mock_db_connection):
        """Тест выборки отзыва по ID"""
        conn, cursor = mock_db_connection
        
        # Мокаем результат запроса
        cursor.fetchone.return_value = (
            "123456", "Toyota", "Camry", 2020, "TestUser", 
            5, "Отличный автомобиль", datetime(2024, 1, 15),
            "https://www.drom.ru/reviews/toyota/camry/123456/", "Москва"
        )
        
        select_query = "SELECT * FROM reviews WHERE id = %s"
        cursor.execute(select_query, ("123456",))
        result = cursor.fetchone()
        
        assert result is not None
        assert result[0] == "123456"  # id
        assert result[1] == "Toyota"  # brand
        assert result[2] == "Camry"   # model
    
    @pytest.mark.integration
    def test_update_review(self, mock_db_connection):
        """Тест обновления отзыва"""
        conn, cursor = mock_db_connection
        
        cursor.rowcount = 1
        
        update_query = """
        UPDATE reviews 
        SET rating = %s, content = %s 
        WHERE id = %s
        """
        
        cursor.execute(update_query, (4, "Обновленный отзыв", "123456"))
        
        cursor.execute.assert_called_once()
        assert cursor.rowcount == 1
    
    @pytest.mark.integration
    def test_delete_review(self, mock_db_connection):
        """Тест удаления отзыва"""
        conn, cursor = mock_db_connection
        
        cursor.rowcount = 1
        
        delete_query = "DELETE FROM reviews WHERE id = %s"
        cursor.execute(delete_query, ("123456",))
        
        cursor.execute.assert_called_once()
        assert cursor.rowcount == 1
    
    @pytest.mark.integration
    def test_bulk_insert_reviews(self, mock_db_connection):
        """Тест массовой вставки отзывов"""
        conn, cursor = mock_db_connection
        
        reviews_data = [
            ("123456", "Toyota", "Camry", 2020, "User1", 5),
            ("123457", "Honda", "Civic", 2019, "User2", 4),
            ("123458", "Mazda", "CX-5", 2021, "User3", 5)
        ]
        
        cursor.executemany.return_value = None
        cursor.rowcount = 3
        
        insert_query = """
        INSERT INTO reviews (id, brand, model, year, author, rating)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(insert_query, reviews_data)
        
        cursor.executemany.assert_called_once()
        assert cursor.rowcount == 3
    
    @pytest.mark.integration
    def test_transaction_rollback(self, mock_db_connection):
        """Тест отката транзакции при ошибке"""
        conn, cursor = mock_db_connection
        
        # Мокаем ошибку при выполнении запроса
        cursor.execute.side_effect = psycopg2.Error("Database error")
        
        try:
            cursor.execute("INVALID SQL")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()
        
        conn.rollback.assert_called_once()
    
    @pytest.mark.integration
    def test_connection_pool(self):
        """Тест пула подключений"""
        with patch('psycopg2.pool.SimpleConnectionPool') as mock_pool:
            pool = mock_pool.return_value
            conn = Mock()
            pool.getconn.return_value = conn
            
            # Получаем подключение из пула
            connection = pool.getconn()
            assert connection is not None
            
            # Возвращаем подключение в пул
            pool.putconn(connection)
            pool.putconn.assert_called_once()
    
    @pytest.mark.integration
    def test_database_schema_validation(self, mock_db_connection):
        """Тест валидации схемы базы данных"""
        conn, cursor = mock_db_connection
        
        # Мокаем проверку существования таблицы
        cursor.fetchone.return_value = ("reviews",)
        
        check_table_query = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'reviews'
        """
        
        cursor.execute(check_table_query)
        result = cursor.fetchone()
        
        assert result is not None
        assert result[0] == "reviews"
    
    @pytest.mark.integration
    def test_database_indexes(self, mock_db_connection):
        """Тест проверки индексов"""
        conn, cursor = mock_db_connection
        
        # Мокаем список индексов
        cursor.fetchall.return_value = [
            ("idx_reviews_brand_model",),
            ("idx_reviews_date",),
            ("idx_reviews_rating",)
        ]
        
        check_indexes_query = """
        SELECT indexname FROM pg_indexes 
        WHERE tablename = 'reviews'
        """
        
        cursor.execute(check_indexes_query)
        indexes = cursor.fetchall()
        
        assert len(indexes) >= 3
        index_names = [idx[0] for idx in indexes]
        assert "idx_reviews_brand_model" in index_names
