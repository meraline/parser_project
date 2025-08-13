import sqlite3
import pytest

from src.models.review import Review


def test_init_database_creates_tables(test_db):
    conn = sqlite3.connect(test_db.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    conn.close()
    assert {"reviews", "parsing_stats", "sources_queue"} <= tables


def test_insert_and_duplicate_review(test_db):
    review = Review(
        source="drom.ru",
        type="review",
        brand="Toyota",
        model="Camry",
        url="http://example.com/review1",
        title="Great car",
        content="It is good",
        author="Alice",
    )
    assert test_db.save_review(review) is True

    conn = sqlite3.connect(test_db.db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT source, type, brand, model, url, title, content, author FROM reviews WHERE url=?",
        (review.url,),
    )
    row = cursor.fetchone()
    assert row == (
        review.source,
        review.type,
        review.brand,
        review.model,
        review.url,
        review.title,
        review.content,
        review.author,
    )

    with pytest.raises(sqlite3.IntegrityError):
        cursor.execute(
            "INSERT INTO reviews (source, type, brand, model, url, title, content, author, content_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                review.source,
                review.type,
                review.brand,
                review.model,
                review.url,
                review.title,
                review.content,
                review.author,
                review.content_hash,
            ),
        )
    conn.close()
