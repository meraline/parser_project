import sqlite3
import pytest

<<<<<<<< HEAD:auto_reviews_parser/tests/unit/integration/test_reviews_database.py
from parsers.models import ReviewData
from auto_reviews_parser import ReviewsDatabase


<<<<<<< HEAD:tests/integration/test_reviews_database.py
def test_init_database_creates_tables(test_db):
    conn = sqlite3.connect(test_db.db_path)
=======
db_module = load_db_module()
Review = db_module.Review
ReviewsDatabase = db_module.ReviewsDatabase
========
from src.database import ReviewsDatabase
from src.models import ReviewData
>>>>>>>> origin/codex/restructure-project-directory-and-update-imports:auto_reviews_parser/tests/unit/test_reviews_database.py


def test_init_database_creates_tables(tmp_path):
    db_path = tmp_path / "test.db"
    db = ReviewsDatabase(str(db_path))
    conn = sqlite3.connect(db.db_path)
>>>>>>> origin/codex/create-review-model-and-update-parsers:tests/test_reviews_database.py
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    conn.close()
    assert {"reviews", "parsing_stats", "sources_queue"} <= tables


<<<<<<< HEAD:tests/integration/test_reviews_database.py
def test_insert_and_duplicate_review(test_db):
    review = ReviewData(
=======
def test_insert_and_duplicate_review(tmp_path):
    db_path = tmp_path / "test.db"
    db = ReviewsDatabase(str(db_path))
    review = Review(
>>>>>>> origin/codex/create-review-model-and-update-parsers:tests/test_reviews_database.py
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
