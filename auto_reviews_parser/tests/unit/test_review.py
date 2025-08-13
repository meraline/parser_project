import hashlib
from datetime import datetime

from parsers.models import ReviewData


def test_parsed_at_auto_set():
    review = Review(
        source="drom.ru",
        type="review",
        brand="toyota",
        model="corolla",
    )
    assert review.parsed_at is not None
    assert isinstance(review.parsed_at, datetime)
    assert (datetime.now() - review.parsed_at).total_seconds() < 5


def test_content_hash_md5_known_values():
    url = "http://example.com/review1"
    title = "Great car"
    content = "a" * 150
    review = Review(
        source="drom.ru",
        type="review",
        brand="toyota",
        model="corolla",
        url=url,
        title=title,
        content=content,
    )
    expected_hash = hashlib.md5(f"{url}_{title}_{content[:100]}".encode()).hexdigest()
    assert review.content_hash == expected_hash
    assert review.content_hash == review.generate_hash()
