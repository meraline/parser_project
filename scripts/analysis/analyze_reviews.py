#!/usr/bin/env python3
"""Анализ структурированных данных отзывов для ML."""

import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


def analyze_reviews_data():
    """Анализирует структурированные данные отзывов."""

    # Загружаем данные
    try:
        with open("toyota_camry_reviews.json", "r", encoding="utf-8") as f:
            reviews_data = json.load(f)
    except FileNotFoundError:
        print("Файл toyota_camry_reviews.json не найден.")
        print("Сначала запустите parse_structured_reviews.py")
        return

    # Создаем DataFrame
    df = pd.DataFrame(reviews_data)

    print("=== АНАЛИЗ ОТЗЫВОВ TOYOTA CAMRY ===")
    print(f"Общее количество отзывов: {len(df)}")
    print()

    # Основная статистика
    print("=== ОСНОВНАЯ СТАТИСТИКА ===")
    print(f"Средний рейтинг: {df['rating'].mean():.2f}")
    print(f"Медианный рейтинг: {df['rating'].median():.2f}")
    print(f"Средние просмотры: {df['views_count'].mean():.0f}")
    print(f"Средние комментарии: {df['comments_count'].mean():.0f}")
    print(f"Средние лайки: {df['likes_count'].mean():.0f}")
    print(f"Средняя длина отзыва: {df['content_length'].mean():.0f} символов")
    print()

    # Статистика по рейтингам
    print("=== РАСПРЕДЕЛЕНИЕ РЕЙТИНГОВ ===")
    rating_counts = df["rating"].value_counts().sort_index()
    for rating, count in rating_counts.items():
        print(f"Рейтинг {rating}: {count} отзывов")
    print()

    # Корреляции
    print("=== КОРРЕЛЯЦИИ ===")
    numeric_cols = [
        "rating",
        "views_count",
        "comments_count",
        "likes_count",
        "content_length",
    ]
    correlation_matrix = df[numeric_cols].corr()
    print("Корреляция между метриками:")
    print(correlation_matrix.round(3))
    print()

    # Топ отзывы по просмотрам
    print("=== ТОП-3 ОТЗЫВА ПО ПРОСМОТРАМ ===")
    top_views = df.nlargest(3, "views_count")
    for i, row in top_views.iterrows():
        print(
            f"{i+1}. Просмотры: {row['views_count']}, "
            f"Рейтинг: {row['rating']}, "
            f"Автор: {row['author']}"
        )
    print()

    # Отзывы с самым длинным текстом
    print("=== САМЫЕ ПОДРОБНЫЕ ОТЗЫВЫ ===")
    longest_reviews = df.nlargest(3, "content_length")
    for i, row in longest_reviews.iterrows():
        print(
            f"{i+1}. Длина: {row['content_length']} символов, "
            f"Рейтинг: {row['rating']}, "
            f"Просмотры: {row['views_count']}"
        )
    print()

    # Данные для ML
    print("=== ПРИЗНАКИ ДЛЯ МАШИННОГО ОБУЧЕНИЯ ===")
    ml_features = []
    for _, row in df.iterrows():
        features = {
            "content_length": row["content_length"],
            "views_count": row["views_count"],
            "comments_count": row["comments_count"],
            "likes_count": row["likes_count"],
            "rating": row["rating"] or 0,
            "has_content": 1 if row["content_length"] > 0 else 0,
            "high_engagement": 1 if row["comments_count"] > 10 else 0,
            "popular": 1 if row["views_count"] > 1000 else 0,
        }
        ml_features.append(features)

    # Сохраняем признаки для ML
    ml_df = pd.DataFrame(ml_features)
    ml_df.to_csv("toyota_camry_ml_features.csv", index=False)
    print("Признаки для ML сохранены в: toyota_camry_ml_features.csv")
    print("Размерность данных для ML:", ml_df.shape)
    print("Описание признаков:")
    print(ml_df.describe().round(2))
    print()

    # Рекомендации для улучшения парсинга
    print("=== РЕКОМЕНДАЦИИ ===")
    empty_content = len(df[df["content_length"] == 0])
    if empty_content > 0:
        print(f"• {empty_content} отзывов имеют пустой контент - проверить парсинг")

    no_rating = len(df[df["rating"].isna()])
    if no_rating > 0:
        print(f"• {no_rating} отзывов без рейтинга - улучшить извлечение оценок")

    low_engagement = len(df[df["views_count"] == 0])
    if low_engagement > 0:
        print(f"• {low_engagement} отзывов с 0 просмотров - возможно дубли или ошибки")

    print("\n=== ГОТОВЫЕ ДАННЫЕ ДЛЯ РЕКОМЕНДАТЕЛЬНЫХ СИСТЕМ ===")
    print("✓ Структурированный JSON с полными данными")
    print("✓ CSV с признаками для машинного обучения")
    print("✓ Метрики вовлеченности (просмотры, комментарии, лайки)")
    print("✓ Рейтинги пользователей и владельцев")
    print("✓ Характеристики контента (длина, наличие)")


if __name__ == "__main__":
    analyze_reviews_data()
