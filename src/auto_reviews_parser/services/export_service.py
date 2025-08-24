import sqlite3
from datetime import datetime
from typing import List, Dict

from botasaurus import bt
import logging


logger = logging.getLogger(__name__)


class ExportService:
    """Сервис экспорта данных из базы"""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def export_data(self, output_format: str = "xlsx") -> None:
        """Экспорт данных из базы"""
        logger.info(f"📤 Экспорт данных в формате {output_format}...")

        conn = sqlite3.connect(self.db_path)
        query = """
            SELECT
                source, type, brand, model, year, title, author, rating,
                content, pros, cons, mileage, engine_volume, fuel_type,
                transmission, body_type, drive_type, publish_date,
                views_count, likes_count, comments_count, url, parsed_at
            FROM reviews
            ORDER BY brand, model, parsed_at DESC
        """
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [description[0] for description in cursor.description]
        df_data: List[Dict] = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()

        if not df_data:
            logger.error("❌ Нет данных для экспорта")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if output_format.lower() == "xlsx":
            filename = f"auto_reviews_export_{timestamp}.xlsx"
            bt.write_excel(df_data, filename.replace(".xlsx", ""))
            logger.info(f"✅ Данные экспортированы в {filename}")
        elif output_format.lower() == "json":
            filename = f"auto_reviews_export_{timestamp}.json"
            bt.write_json(df_data, filename.replace(".json", ""))
            logger.info(f"✅ Данные экспортированы в {filename}")
        else:
            logger.error(f"❌ Неподдерживаемый формат: {output_format}")
