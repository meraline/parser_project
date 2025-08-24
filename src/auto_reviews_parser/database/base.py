import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, Optional, Tuple


class Database:
    """Абстракция подключения к базе данных SQLite."""

    def __init__(self, path: str):
        self.path = path
        # Ensure directory exists
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        """Контекстный менеджер для соединения с базой."""
        conn = sqlite3.connect(self.path)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Контекстный менеджер для транзакций."""
        with self.connection() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def execute(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Выполняет SQL запрос и возвращает результат.

        Args:
            query: SQL запрос
            params: Параметры запроса

        Returns:
            Результат запроса в виде списка кортежей
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()

    def execute_many(self, query: str, params_list: List[Tuple]) -> None:
        """Выполняет массовую вставку данных.

        Args:
            query: SQL запрос для вставки
            params_list: Список кортежей с параметрами
        """
        with self.transaction() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
