import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


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
