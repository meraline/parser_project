import sqlite3
from typing import Dict, Optional, Tuple, List


class QueueService:
    """Сервис управления очередью источников"""

    def __init__(self, db_path: str, target_brands: Dict[str, List[str]]):
        self.db_path = db_path
        self.target_brands = target_brands

    def initialize_queue(self) -> None:
        """Инициализация очереди источников для парсинга"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM sources_queue")

        for brand, models in self.target_brands.items():
            for model in models:
                for source in ["drom.ru", "drive2.ru"]:
                    cursor.execute(
                        """
                        INSERT INTO sources_queue (brand, model, source, priority)
                        VALUES (?, ?, ?, ?)
                        """,
                        (brand, model, source, 1),
                    )
        conn.commit()
        conn.close()

        total_sources = sum(len(models) for models in self.target_brands.values()) * 2
        print(f"✅ Инициализирована очередь из {total_sources} источников")

    def get_next_source(self) -> Optional[Tuple[str, str, str]]:
        """Получение следующего источника для парсинга"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, brand, model, source FROM sources_queue
            WHERE status = 'pending'
            ORDER BY priority DESC, RANDOM()
            LIMIT 1
            """
        )
        result = cursor.fetchone()
        if result:
            source_id, brand, model, source = result
            cursor.execute(
                """
                UPDATE sources_queue
                SET status = 'processing', last_parsed = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (source_id,),
            )
            conn.commit()
            conn.close()
            return brand, model, source
        conn.close()
        return None

    def mark_source_completed(
        self, brand: str, model: str, source: str, pages_parsed: int, reviews_found: int
    ) -> None:
        """Отметка источника как завершенного"""
        conn = sqlite3.connect(self.db_path)
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
        conn.close()

    def get_queue_stats(self) -> Dict[str, int]:
        """Получение статистики очереди"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status, COUNT(*) FROM sources_queue GROUP BY status")
        stats = dict(cursor.fetchall())
        conn.close()
        return stats
