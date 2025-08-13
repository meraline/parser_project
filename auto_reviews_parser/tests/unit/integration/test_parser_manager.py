import logging
import pytest

from services.auto_reviews_parser import AutoReviewsParser


@pytest.mark.parametrize("source", ["drom.ru", "drive2.ru"])
def test_parse_single_source_returns_zero_and_logs_warning(auto_parser, source, caplog):
    auto_parser.drom_parser.parse_brand_model_reviews.return_value = None
    auto_parser.drive2_parser.parse_brand_model_reviews.return_value = None

    with caplog.at_level(logging.WARNING):
        result = AutoReviewsParser.parse_single_source(auto_parser, "Brand", "Model", source)

    assert result == 0
    assert any(record.levelno == logging.WARNING for record in caplog.records)
    auto_parser.db.save_review.assert_not_called()
    auto_parser.mark_source_completed.assert_called_once()

    if source == "drom.ru":
        auto_parser.drom_parser.parse_brand_model_reviews.assert_called_once()
        auto_parser.drive2_parser.parse_brand_model_reviews.assert_not_called()
    else:
        auto_parser.drive2_parser.parse_brand_model_reviews.assert_called_once()
        auto_parser.drom_parser.parse_brand_model_reviews.assert_not_called()
