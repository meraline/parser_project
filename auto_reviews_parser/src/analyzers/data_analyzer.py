#!/usr/bin/env python3
"""
Утилиты для анализа собранных данных отзывов
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from collections import Counter
import re
from typing import Dict, List, Tuple
from pathlib import Path
import json

from src.database.base import Database
from src.utils.logger import get_logger
from src.utils.validators import validate_non_empty_string

logger = get_logger(__name__)


class ReviewsAnalyzer:
    """Анализатор собранных отзывов"""

    def __init__(self, db_path: str = "auto_reviews.db"):
        db_path = validate_non_empty_string(db_path, "db_path")
        self.db = Database(db_path)
        self.ensure_db_exists()

    def ensure_db_exists(self):
        """Проверка существования базы данных"""
        if not Path(self.db.path).exists():
            raise FileNotFoundError(f"База данных не найдена: {self.db.path}")

    def get_basic_stats(self) -> Dict:
        """Базовая статистика по собранным данным"""
        with self.db.connection() as conn:
            stats = {}

            # Общее количество
            stats["total_reviews"] = pd.read_sql_query(
                "SELECT COUNT(*) as count FROM reviews", conn
            ).iloc[0]["count"]

            # По источникам
            stats["by_source"] = (
                pd.read_sql_query(
                    "SELECT source, COUNT(*) as count FROM reviews GROUP BY source",
                    conn,
                )
                .set_index("source")["count"]
                .to_dict()
            )

            # По типам
            stats["by_type"] = (
                pd.read_sql_query(
                    "SELECT type, COUNT(*) as count FROM reviews GROUP BY type",
                    conn,
                )
                .set_index("type")["count"]
                .to_dict()
            )

            # По брендам (топ 10)
            stats["top_brands"] = (
                pd.read_sql_query(
                    """SELECT brand, COUNT(*) as count FROM reviews
               GROUP BY brand ORDER BY count DESC LIMIT 10""",
                    conn,
                )
                .set_index("brand")["count"]
                .to_dict()
            )

            # По моделям (топ 15)
            stats["top_models"] = (
                pd.read_sql_query(
                    """SELECT brand || ' ' || model as model, COUNT(*) as count
               FROM reviews GROUP BY brand, model ORDER BY count DESC LIMIT 15""",
                    conn,
                )
                .set_index("model")["count"]
                .to_dict()
            )

        return stats

        # Временная статистика
        stats["parsing_timeline"] = pd.read_sql_query(
            """SELECT DATE(parsed_at) as date, COUNT(*) as count 
               FROM reviews GROUP BY DATE(parsed_at) ORDER BY date""",
            conn,
        )

        # Средние рейтинги по брендам
        stats["avg_ratings"] = pd.read_sql_query(
            """SELECT brand, AVG(rating) as avg_rating, COUNT(*) as reviews_count
               FROM reviews WHERE rating IS NOT NULL 
               GROUP BY brand HAVING reviews_count >= 10
               ORDER BY avg_rating DESC""",
            conn,
        )

        return stats

    def generate_report(self, output_dir: str = "analysis_reports") -> str:
        """Генерация подробного отчета"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = output_path / f"analysis_report_{timestamp}.html"

        stats = self.get_basic_stats()

        # Создаем HTML отчет
        html_content = self._generate_html_report(stats)

        with open(report_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        logger.info(f"Отчет сохранен: {report_file}")
        return str(report_file)

    def _generate_html_report(self, stats: Dict) -> str:
        """Генерация HTML отчета"""

        # Топ моделей для таблицы
        top_models_table = ""
        for model, count in list(stats["top_models"].items())[:10]:
            top_models_table += f"<tr><td>{model}</td><td>{count:,}</td></tr>\n"

        # Рейтинги брендов
        ratings_table = ""
        for _, row in stats["avg_ratings"].head(10).iterrows():
            ratings_table += f"<tr><td>{row['brand']}</td><td>{row['avg_rating']:.2f}</td><td>{int(row['reviews_count']):,}</td></tr>\n"

        # Временная динамика (последние 30 дней)
        timeline_data = stats["parsing_timeline"].tail(30)
        timeline_chart = ""
        for _, row in timeline_data.iterrows():
            timeline_chart += f"['{row['date']}', {row['count']}],\n"

        html = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Анализ Автомобильных Отзывов</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f7fa;
            color: #333;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 15px;
            margin-bottom: 30px;
            text-align: center;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .stat-number {{
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 5px;
        }}
        .stat-label {{
            color: #666;
            font-size: 0.9em;
        }}
        .section {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 25px;
            box-shadow: 0 2px 15px rgba(0,0,0,0.1);
        }}
        .section h2 {{
            color: #333;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
            margin-bottom: 20px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background: #f8f9fa;
            font-weight: 600;
            color: #555;
        }}
        .chart-container {{
            position: relative;
            height: 400px;
            margin: 20px 0;
        }}
        .progress-bar {{
            background: #e9ecef;
            border-radius: 10px;
            overflow: hidden;
            margin: 5px 0;
        }}
        .progress-fill {{
            height: 25px;
            background: linear-gradient(90deg, #667eea, #764ba2);
            border-radius: 10px;
            display: flex;
            align-items: center;
            padding: 0 10px;
            color: white;
            font-weight: bold;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Анализ Автомобильных Отзывов</h1>
            <p>Отчет создан: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}</p>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-number">{stats['total_reviews']:,}</div>
                <div class="stat-label">Всего отзывов</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{len(stats['top_brands'])}</div>
                <div class="stat-label">Брендов</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{len(stats['top_models'])}</div>
                <div class="stat-label">Моделей</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{stats['by_source'].get('drom.ru', 0) + stats['by_source'].get('drive2.ru', 0):,}</div>
                <div class="stat-label">Источников</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📈 Распределение по источникам</h2>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                <div>
                    <h4>Drom.ru: {stats['by_source'].get('drom.ru', 0):,} отзывов</h4>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: {(stats['by_source'].get('drom.ru', 0) / stats['total_reviews'] * 100):.1f}%">
                            {(stats['by_source'].get('drom.ru', 0) / stats['total_reviews'] * 100):.1f}%
                        </div>
                    </div>
                </div>
                <div>
                    <h4>Drive2.ru: {stats['by_source'].get('drive2.ru', 0):,} отзывов</h4>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: {(stats['by_source'].get('drive2.ru', 0) / stats['total_reviews'] * 100):.1f}%">
                            {(stats['by_source'].get('drive2.ru', 0) / stats['total_reviews'] * 100):.1f}%
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>🏆 Топ-10 моделей по количеству отзывов</h2>
            <table>
                <thead>
                    <tr>
                        <th>Модель</th>
                        <th>Количество отзывов</th>
                    </tr>
                </thead>
                <tbody>
                    {top_models_table}
                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>⭐ Рейтинги брендов</h2>
            <table>
                <thead>
                    <tr>
                        <th>Бренд</th>
                        <th>Средний рейтинг</th>
                        <th>Количество отзывов</th>
                    </tr>
                </thead>
                <tbody>
                    {ratings_table}
                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>📅 Динамика сбора данных (последние 30 дней)</h2>
            <div class="chart-container">
                <canvas id="timelineChart"></canvas>
            </div>
        </div>
    </div>
    
    <script>
        // График временной динамики
        const ctx = document.getElementById('timelineChart').getContext('2d');
        new Chart(ctx, {{
            type: 'line',
            data: {{
                datasets: [{{
                    label: 'Отзывов в день',
                    data: [
                        {timeline_chart}
                    ],
                    borderColor: '#667eea',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                parsing: {{
                    xAxisKey: '0',
                    yAxisKey: '1'
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        title: {{
                            display: true,
                            text: 'Количество отзывов'
                        }}
                    }},
                    x: {{
                        title: {{
                            display: true,
                            text: 'Дата'
                        }}
                    }}
                }},
                plugins: {{
                    legend: {{
                        display: true,
                        position: 'top'
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
        """

        return html

    def get_brand_analysis(self, brand: str) -> Dict:
        """Детальный анализ конкретного бренда"""
        analysis = {}
        with self.db.connection() as conn:
            # Основная статистика
            analysis["total_reviews"] = pd.read_sql_query(
                "SELECT COUNT(*) as count FROM reviews WHERE brand = ?",
                conn,
                params=[brand],
            ).iloc[0]["count"]

            # По моделям
            analysis["models"] = pd.read_sql_query(
                """SELECT model, COUNT(*) as count, AVG(rating) as avg_rating
               FROM reviews WHERE brand = ?
               GROUP BY model ORDER BY count DESC""",
                conn,
                params=[brand],
            )

            # Средний рейтинг
            analysis["avg_rating"] = pd.read_sql_query(
                "SELECT AVG(rating) as avg_rating FROM reviews WHERE brand = ? AND rating IS NOT NULL",
                conn,
                params=[brand],
            ).iloc[0]["avg_rating"]

            # Распределение по годам
            analysis["by_year"] = pd.read_sql_query(
                """SELECT year, COUNT(*) as count FROM reviews
               WHERE brand = ? AND year IS NOT NULL
               GROUP BY year ORDER BY year""",
                conn,
                params=[brand],
            )

            # Частые проблемы (анализ текста)
            reviews_text = pd.read_sql_query(
                "SELECT content, cons FROM reviews WHERE brand = ? AND (content != '' OR cons != '')",
                conn,
                params=[brand],
            )

            if not reviews_text.empty:
                all_text = " ".join(
                    reviews_text["content"].fillna("")
                    + " "
                    + reviews_text["cons"].fillna("")
                )
                analysis["common_issues"] = self._extract_common_issues(all_text)

        return analysis

    def _extract_common_issues(self, text: str) -> List[Tuple[str, int]]:
        """Извлечение частых проблем из текста отзывов"""
        # Ключевые слова проблем
        problem_keywords = [
            "ломается",
            "не работает",
            "проблема",
            "дефект",
            "поломка",
            "скрипит",
            "течет",
            "ржавеет",
            "износ",
            "замена",
            "дорогой ремонт",
            "часто меняю",
            "вышел из строя",
            "коррозия",
            "трещина",
            "стук",
            "вибрация",
            "шум",
        ]

        # Автомобильные узлы
        car_parts = [
            "двигатель",
            "коробка передач",
            "кпп",
            "сцепление",
            "тормоза",
            "подвеска",
            "стойки",
            "амортизаторы",
            "рулевое",
            "электрика",
            "кондиционер",
            "печка",
            "радиатор",
            "генератор",
            "стартер",
            "топливная система",
            "выхлопная система",
            "кузов",
            "салон",
        ]

        text_lower = text.lower()
        issues = []

        for part in car_parts:
            for problem in problem_keywords:
                pattern = f"{part}.*?{problem}|{problem}.*?{part}"
                matches = re.findall(pattern, text_lower)
                if matches:
                    issues.append((f"{part} - {problem}", len(matches)))

        # Возвращаем топ-10 проблем
        return Counter(dict(issues)).most_common(10)

    def export_analytics_data(self, output_file: str = None) -> str:
        """Экспорт аналитических данных"""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"analytics_export_{timestamp}.json"

        analytics = {
            "export_timestamp": datetime.now().isoformat(),
            "basic_stats": self.get_basic_stats(),
            "brand_ratings": {},
            "model_popularity": {},
            "technical_stats": {},
        }

        with self.db.connection() as conn:
            # Рейтинги брендов
            brand_ratings = pd.read_sql_query(
                """SELECT brand, AVG(rating) as avg_rating, COUNT(*) as review_count
               FROM reviews WHERE rating IS NOT NULL
               GROUP BY brand ORDER BY avg_rating DESC""",
                conn,
            )

            for _, row in brand_ratings.iterrows():
                analytics["brand_ratings"][row["brand"]] = {
                    "avg_rating": round(row["avg_rating"], 2),
                    "review_count": int(row["review_count"]),
                }

            # Популярность моделей
            model_popularity = pd.read_sql_query(
                """SELECT brand, model, COUNT(*) as review_count, AVG(rating) as avg_rating
               FROM reviews GROUP BY brand, model
               ORDER BY review_count DESC LIMIT 50""",
                conn,
            )

            for _, row in model_popularity.iterrows():
                key = f"{row['brand']} {row['model']}"
                analytics["model_popularity"][key] = {
                    "review_count": int(row["review_count"]),
                    "avg_rating": round(row["avg_rating"] or 0, 2),
                }

            # Технические характеристики
            tech_stats = pd.read_sql_query(
                """SELECT
                fuel_type, COUNT(*) as count,
                transmission,
                AVG(engine_volume) as avg_engine,
                AVG(mileage) as avg_mileage
               FROM reviews
               WHERE fuel_type IS NOT NULL OR transmission IS NOT NULL
               GROUP BY fuel_type, transmission""",
                conn,
            )

            for _, row in tech_stats.iterrows():
                key = f"{row['fuel_type'] or 'unknown'}_{row['transmission'] or 'unknown'}"
                analytics["technical_stats"][key] = {
                    "count": int(row["count"]),
                    "avg_engine": round(row["avg_engine"] or 0, 1),
                    "avg_mileage": int(row["avg_mileage"] or 0),
                }

        # Сохраняем в JSON
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(analytics, f, ensure_ascii=False, indent=2)

        print(f"📊 Аналитические данные экспортированы: {output_file}")
        return output_file

    def get_recommendations(self) -> Dict:
        """Рекомендации на основе анализа отзывов"""
        recommendations = {}
        with self.db.connection() as conn:
            # Самые надежные модели (высокий рейтинг + много отзывов)
            reliable_models = pd.read_sql_query(
                """SELECT brand, model, AVG(rating) as avg_rating, COUNT(*) as review_count
               FROM reviews WHERE rating IS NOT NULL
               GROUP BY brand, model
               HAVING review_count >= 10 AND avg_rating >= 4.0
               ORDER BY avg_rating DESC, review_count DESC
               LIMIT 10""",
                conn,
            )

            recommendations["most_reliable"] = []
            for _, row in reliable_models.iterrows():
                recommendations["most_reliable"].append(
                    {
                        "model": f"{row['brand']} {row['model']}",
                        "rating": round(row["avg_rating"], 2),
                        "reviews": int(row["review_count"]),
                    }
                )

            # Модели с наибольшим количеством отзывов (популярные)
            popular_models = pd.read_sql_query(
                """SELECT brand, model, COUNT(*) as review_count, AVG(rating) as avg_rating
               FROM reviews GROUP BY brand, model
               ORDER BY review_count DESC
               LIMIT 10""",
                conn,
            )

            recommendations["most_popular"] = []
            for _, row in popular_models.iterrows():
                recommendations["most_popular"].append(
                    {
                        "model": f"{row['brand']} {row['model']}",
                        "reviews": int(row["review_count"]),
                        "rating": round(row["avg_rating"] or 0, 2),
                    }
                )

            # Бренды с лучшими рейтингами
            top_brands = pd.read_sql_query(
                """SELECT brand, AVG(rating) as avg_rating, COUNT(*) as review_count
               FROM reviews WHERE rating IS NOT NULL
               GROUP BY brand HAVING review_count >= 20
               ORDER BY avg_rating DESC
               LIMIT 5""",
                conn,
            )

            recommendations["top_brands"] = []
            for _, row in top_brands.iterrows():
                recommendations["top_brands"].append(
                    {
                        "brand": row["brand"],
                        "rating": round(row["avg_rating"], 2),
                        "reviews": int(row["review_count"]),
                    }
                )

            # Модели с низкими рейтингами (избегать)
            avoid_models = pd.read_sql_query(
                """SELECT brand, model, AVG(rating) as avg_rating, COUNT(*) as review_count
               FROM reviews WHERE rating IS NOT NULL
               GROUP BY brand, model
               HAVING review_count >= 5 AND avg_rating <= 2.5
               ORDER BY avg_rating ASC
               LIMIT 5""",
                conn,
            )

            recommendations["to_avoid"] = []
            for _, row in avoid_models.iterrows():
                recommendations["to_avoid"].append(
                    {
                        "model": f"{row['brand']} {row['model']}",
                        "rating": round(row["avg_rating"], 2),
                        "reviews": int(row["review_count"]),
                    }
                )

        return recommendations


def main():
    """Главная функция для анализа данных"""
    import argparse

    parser = argparse.ArgumentParser(description="Анализ собранных отзывов")
    parser.add_argument(
        "command",
        choices=["stats", "report", "brand", "export", "recommendations"],
        help="Команда для выполнения",
    )
    parser.add_argument("--brand", help="Бренд для детального анализа")
    parser.add_argument("--output", help="Файл для экспорта данных")

    args = parser.parse_args()

    analyzer = ReviewsAnalyzer()

    if args.command == "stats":
        print("📊 БАЗОВАЯ СТАТИСТИКА")
        print("=" * 50)

        stats = analyzer.get_basic_stats()

        print(f"Всего отзывов: {stats['total_reviews']:,}")
        print(f"Уникальных брендов: {len(stats['top_brands'])}")
        print(f"Уникальных моделей: {len(stats['top_models'])}")

        print("\nПо источникам:")
        for source, count in stats["by_source"].items():
            print(f"  {source}: {count:,}")

        print("\nПо типам:")
        for type_name, count in stats["by_type"].items():
            print(f"  {type_name}: {count:,}")

        print("\nТоп-5 брендов:")
        for brand, count in list(stats["top_brands"].items())[:5]:
            print(f"  {brand}: {count:,}")

        print("\nТоп-5 моделей:")
        for model, count in list(stats["top_models"].items())[:5]:
            print(f"  {model}: {count:,}")

    elif args.command == "report":
        print("📋 Генерация подробного отчета...")
        report_file = analyzer.generate_report()
        print(f"✅ Отчет готов: {report_file}")

    elif args.command == "brand":
        if not args.brand:
            print("❌ Укажите бренд с помощью --brand")
            return

        print(f"🔍 АНАЛИЗ БРЕНДА: {args.brand.upper()}")
        print("=" * 50)

        analysis = analyzer.get_brand_analysis(args.brand.lower())

        print(f"Всего отзывов: {analysis['total_reviews']:,}")

        if analysis.get("avg_rating"):
            print(f"Средний рейтинг: {analysis['avg_rating']:.2f}")

        print(f"\nМодели ({len(analysis['models'])} шт.):")
        for _, row in analysis["models"].head(10).iterrows():
            rating_str = (
                f"({row['avg_rating']:.1f}★)" if pd.notna(row["avg_rating"]) else ""
            )
            print(f"  {row['model']}: {int(row['count']):,} отзывов {rating_str}")

        if not analysis["by_year"].empty:
            print(f"\nРаспределение по годам:")
            for _, row in analysis["by_year"].tail(10).iterrows():
                print(f"  {int(row['year'])}: {int(row['count']):,} отзывов")

        if analysis.get("common_issues"):
            print(f"\nЧастые проблемы:")
            for issue, count in analysis["common_issues"][:5]:
                print(f"  {issue}: {count} упоминаний")

    elif args.command == "export":
        print("💾 Экспорт аналитических данных...")
        export_file = analyzer.export_analytics_data(args.output)
        print(f"✅ Данные экспортированы: {export_file}")

    elif args.command == "recommendations":
        print("🎯 РЕКОМЕНДАЦИИ НА ОСНОВЕ ОТЗЫВОВ")
        print("=" * 50)

        recommendations = analyzer.get_recommendations()

        print("🏆 Самые надежные модели:")
        for model_info in recommendations["most_reliable"]:
            print(
                f"  {model_info['model']}: {model_info['rating']}★ ({model_info['reviews']} отзывов)"
            )

        print(f"\n📈 Самые популярные модели:")
        for model_info in recommendations["most_popular"]:
            print(
                f"  {model_info['model']}: {model_info['reviews']} отзывов ({model_info['rating']}★)"
            )

        print(f"\n⭐ Лучшие бренды:")
        for brand_info in recommendations["top_brands"]:
            print(
                f"  {brand_info['brand']}: {brand_info['rating']}★ ({brand_info['reviews']} отзывов)"
            )

        if recommendations["to_avoid"]:
            print(f"\n⚠️ Модели с низкими рейтингами:")
            for model_info in recommendations["to_avoid"]:
                print(
                    f"  {model_info['model']}: {model_info['rating']}★ ({model_info['reviews']} отзывов)"
                )


if __name__ == "__main__":
    main()
