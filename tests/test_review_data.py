import hashlib
from datetime import datetime
import pathlib
import re


def get_review_data_class():
    source_path = pathlib.Path(__file__).resolve().parents[1] / "auto_reviews_parser.py"
    text = source_path.read_text()
    match = re.search(
        r"@dataclass\nclass ReviewData:[\s\S]+?def __post_init__\(self\):[\s\S]+?self.content_hash = hashlib.md5\(.+?\)\n",
        text,
    )
    snippet = match.group(0)
    namespace = {}
    exec(
        "from dataclasses import dataclass\nfrom datetime import datetime\nfrom typing import Optional\nimport hashlib\n"
        + snippet,
        namespace,
    )
    return namespace["ReviewData"]


ReviewData = get_review_data_class()


def test_parsed_at_auto_set():
    review = ReviewData(
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
    review = ReviewData(
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
