from typing import Dict, List, Optional, Tuple

from ..base import Database


class QueueRepository:
    """CRUD-операции для очереди источников."""

    def __init__(self, db: Database):
        self.db = db
        self._init_table()

    def _init_table(self) -> None:
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS sources_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    brand TEXT,
                    model TEXT,
                    source TEXT,
                    base_url TEXT,
                    priority INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'pending',
                    last_parsed DATETIME,
                    total_pages INTEGER DEFAULT 0,
                    parsed_pages INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()

    def initialize(self, target_brands: Dict[str, List[str]]):
        """Инициализация очереди источников."""
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM sources_queue")
            for brand, models in target_brands.items():
                for model in models:
                    for source in ["drom.ru", "drive2.ru"]:
                        cursor.execute(
                            "INSERT INTO sources_queue (brand, model, source, priority) VALUES (?, ?, ?, ?)",
                            (brand, model, source, 1),
                        )
            conn.commit()

    def get_next(self) -> Optional[Tuple[str, str, str]]:
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, brand, model, source FROM sources_queue
                WHERE status = 'pending'
                ORDER BY priority DESC, RANDOM()
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            if not row:
                return None
            source_id, brand, model, source = row
            cursor.execute(
                """
                UPDATE sources_queue
                SET status = 'processing', last_parsed = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (source_id,)
            )
            conn.commit()
            return brand, model, source

    def mark_completed(
        self, brand: str, model: str, source: str, pages_parsed: int, reviews_found: int
    ) -> None:
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE sources_queue
                SET status = 'completed', parsed_pages = ?, total_pages = ?
                WHERE brand = ? AND model = ? AND source = ?
                """,
                (pages_parsed, pages_parsed, brand, model, source),
            )
            conn.commit()

    def get_stats(self) -> Dict[str, int]:
        """Статистика по статусам очереди."""
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status, COUNT(*) FROM sources_queue GROUP BY status"
            )
            return dict(cursor.fetchall())
