from typing import Any, Optional
import redis
import json

from ...utils.logger import get_logger

logger = get_logger(__name__)


class CacheRepository:
    """Реализация кэширования с использованием Redis."""

    def __init__(self, redis_url: str, ttl_seconds: int = 3600):
        """
        Инициализация репозитория кэша.

        Args:
            redis_url: URL подключения к Redis
            ttl_seconds: Время жизни кэша в секундах
        """
        self.redis = redis.from_url(redis_url)
        self.ttl = ttl_seconds

    def get(self, key: str) -> Optional[Any]:
        """
        Получение значения из кэша.

        Args:
            key: Ключ для поиска

        Returns:
            Any: Закэшированное значение или None
        """
        try:
            value = self.redis.get(key)
            if isinstance(value, bytes):
                return json.loads(value.decode("utf-8"))
            return None
        except redis.RedisError as e:
            logger.error(f"Ошибка получения из кэша: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON из кэша: {e}")
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Сохранение значения в кэш.

        Args:
            key: Ключ
            value: Значение для сохранения
            ttl: Время жизни в секундах (опционально)

        Returns:
            bool: True если значение успешно сохранено
        """
        try:
            json_value = json.dumps(value)
            expiration = ttl or self.ttl
            return bool(self.redis.set(key, json_value, ex=expiration))
        except (redis.RedisError, TypeError) as e:
            logger.error(f"Ошибка сохранения в кэш: {e}")
            return False

    def delete(self, key: str) -> bool:
        """
        Удаление значения из кэша.

        Args:
            key: Ключ для удаления

        Returns:
            bool: True если значение успешно удалено
        """
        try:
            return bool(self.redis.delete(key))
        except redis.RedisError as e:
            logger.error(f"Ошибка удаления из кэша: {e}")
            return False

    def exists(self, key: str) -> bool:
        """
        Проверка существования ключа в кэше.

        Args:
            key: Ключ для проверки

        Returns:
            bool: True если ключ существует
        """
        try:
            return bool(self.redis.exists(key))
        except redis.RedisError as e:
            logger.error(f"Ошибка проверки существования в кэше: {e}")
            return False

    def clear(self, pattern: str = "*") -> bool:
        """
        Очистка кэша по шаблону.

        Args:
            pattern: Шаблон для поиска ключей

        Returns:
            bool: True если очистка прошла успешно
        """
        try:
            pipeline = self.redis.pipeline()
            for key in self.redis.scan_iter(pattern):
                pipeline.delete(key)
            pipeline.execute()
            return True
        except redis.RedisError as e:
            logger.error(f"Ошибка очистки кэша: {e}")
            return False

    def get_many(self, keys: list[str]) -> dict[str, Any]:
        """
        Получение нескольких значений из кэша.

        Args:
            keys: Список ключей

        Returns:
            dict: Словарь {ключ: значение}
        """
        try:
            pipe = self.redis.pipeline()
            for key in keys:
                pipe.get(key)
            values = pipe.execute()

            if not values:
                return {}

            result = {}
            for key, value in zip(keys, values):
                if isinstance(value, bytes):
                    try:
                        result[key] = json.loads(value.decode("utf-8"))
                    except json.JSONDecodeError:
                        logger.error(f"Ошибка декодирования JSON " f"для ключа {key}")
            return result
        except redis.RedisError as e:
            logger.error(f"Ошибка получения значений из кэша: {e}")
            return {}
