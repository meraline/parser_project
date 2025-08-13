import sqlite3
import types
import pathlib
import pytest


def load_db_module():
    path = pathlib.Path(__file__).resolve().parents[1] / "auto_reviews_parser.py"
    lines = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            if line.startswith("class BaseParser"):
                break
            if "botasaurus" in line:
                continue
            lines.append(line)
    module = types.ModuleType("db_module")
    exec("".join(lines), module.__dict__)
    return module


db_module = load_db_module()
Review = db_module.Review
ReviewsDatabase = db_module.ReviewsDatabase


def test_init_database_creates_tables(tmp_path):
    db_path = tmp_path / "test.db"
    db = ReviewsDatabase(str(db_path))
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    conn.close()
    assert {"reviews", "parsing_stats", "sources_queue"} <= tables


def test_insert_and_duplicate_review(tmp_path):
    db_path = tmp_path / "test.db"
    db = ReviewsDatabase(str(db_path))
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
    assert db.save_review(review) is True

    conn = sqlite3.connect(db.db_path)
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
