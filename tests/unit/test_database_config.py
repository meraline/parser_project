"""
Unit тесты для конфигурации базы данных
"""
import pytest
import os
from unittest.mock import patch, Mock
from src.auto_reviews_parser.config.database import DatabaseConfig


class TestDatabaseConfig:
    """Тесты конфигурации базы данных"""
    
    def test_default_config_values(self):
        """Тест значений конфигурации по умолчанию"""
        config = DatabaseConfig()
        
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.user == "postgres"
        assert config.database == "drom_reviews"
        assert config.pool_size == 10
        assert config.max_overflow == 20
    
    def test_config_from_environment(self):
        """Тест загрузки конфигурации из переменных окружения"""
        env_vars = {
            'DB_HOST': 'test_host',
            'DB_PORT': '5433',
            'DB_USER': 'test_user',
            'DB_PASSWORD': 'test_pass',
            'DB_NAME': 'test_db'
        }
        
        with patch.dict(os.environ, env_vars):
            config = DatabaseConfig.from_env()
            
            assert config.host == 'test_host'
            assert config.port == 5433
            assert config.user == 'test_user'
            assert config.password == 'test_pass'
            assert config.database == 'test_db'
    
    def test_connection_url_generation(self):
        """Тест генерации URL подключения"""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            user="postgres",
            password="secret",
            database="test_db"
        )
        
        expected_url = "postgresql://postgres:secret@localhost:5432/test_db"
        assert config.get_connection_url() == expected_url
    
    def test_connection_url_without_password(self):
        """Тест генерации URL без пароля"""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            user="postgres",
            database="test_db"
        )
        
        expected_url = "postgresql://postgres@localhost:5432/test_db"
        assert config.get_connection_url() == expected_url
    
    @pytest.mark.parametrize("port,expected", [
        ("5432", 5432),
        (5433, 5433),
        ("invalid", 5432),  # должен вернуться к default
    ])
    def test_port_validation(self, port, expected):
        """Тест валидации порта"""
        with patch.dict(os.environ, {'DB_PORT': str(port)}):
            config = DatabaseConfig.from_env()
            assert config.port == expected
    
    def test_config_validation(self):
        """Тест валидации конфигурации"""
        # Валидная конфигурация
        valid_config = DatabaseConfig(
            host="localhost",
            user="postgres",
            database="test_db"
        )
        assert valid_config.is_valid() == True
        
        # Невалидная конфигурация
        invalid_config = DatabaseConfig(
            host="",
            user="",
            database=""
        )
        assert invalid_config.is_valid() == False
    
    def test_config_to_dict(self):
        """Тест преобразования конфигурации в словарь"""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            user="postgres",
            database="test_db"
        )
        
        config_dict = config.to_dict()
        
        assert config_dict['host'] == "localhost"
        assert config_dict['port'] == 5432
        assert config_dict['user'] == "postgres"
        assert config_dict['database'] == "test_db"
        assert 'password' not in config_dict or config_dict['password'] is None
    
    def test_config_from_dict(self):
        """Тест создания конфигурации из словаря"""
        config_data = {
            'host': 'test_host',
            'port': 5433,
            'user': 'test_user',
            'password': 'test_pass',
            'database': 'test_db'
        }
        
        config = DatabaseConfig.from_dict(config_data)
        
        assert config.host == 'test_host'
        assert config.port == 5433
        assert config.user == 'test_user'
        assert config.password == 'test_pass'
        assert config.database == 'test_db'
    
    def test_ssl_configuration(self):
        """Тест SSL конфигурации"""
        config = DatabaseConfig(
            host="localhost",
            user="postgres",
            database="test_db",
            ssl_mode="require"
        )
        
        connection_url = config.get_connection_url()
        assert "sslmode=require" in connection_url
    
    def test_connection_parameters(self):
        """Тест параметров подключения"""
        config = DatabaseConfig()
        
        params = config.get_connection_params()
        
        assert 'host' in params
        assert 'port' in params
        assert 'user' in params
        assert 'database' in params
        assert params['pool_size'] == 10
        assert params['max_overflow'] == 20
