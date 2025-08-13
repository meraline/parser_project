import os
import sys
import types
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

# Ensure project root is on the Python path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# Stub external 'botasaurus' dependencies before importing the module under test
botasaurus_pkg = types.ModuleType("botasaurus")

browser_module = types.ModuleType("botasaurus.browser")

def browser(*args, **kwargs):
    def decorator(func):
        return func
    return decorator

class Driver:
    pass

browser_module.browser = browser
browser_module.Driver = Driver

request_module = types.ModuleType("botasaurus.request")

def request(*args, **kwargs):
    def decorator(func):
        return func
    return decorator

class Request:
    pass

request_module.request = request
request_module.Request = Request

soupify_module = types.ModuleType("botasaurus.soupify")

def soupify(*args, **kwargs):
    return None

soupify_module.soupify = soupify

bt_module = types.ModuleType("botasaurus.bt")

# Register stub modules
sys.modules["botasaurus"] = botasaurus_pkg
sys.modules["botasaurus.browser"] = browser_module
sys.modules["botasaurus.request"] = request_module
sys.modules["botasaurus.soupify"] = soupify_module
sys.modules["botasaurus.bt"] = bt_module

botasaurus_pkg.browser = browser_module
botasaurus_pkg.request = request_module
botasaurus_pkg.soupify = soupify_module
botasaurus_pkg.bt = bt_module

from services.auto_reviews_parser import AutoReviewsParser


@pytest.mark.parametrize("source", ["drom.ru", "drive2.ru"])
def test_parse_single_source_returns_false_and_logs_warning(source, caplog):
    parser = AutoReviewsParser.__new__(AutoReviewsParser)
    parser.drom_parser = SimpleNamespace(parse_brand_model_reviews=lambda *args, **kwargs: None)
    parser.drive2_parser = SimpleNamespace(parse_brand_model_reviews=lambda *args, **kwargs: None)
    parser.db = Mock()
    parser.db.save_review.return_value = True
    parser.mark_source_completed = Mock()

    with caplog.at_level(logging.WARNING):
        result = AutoReviewsParser.parse_single_source(parser, "Brand", "Model", source)

    assert not result
    assert any(record.levelno == logging.WARNING for record in caplog.records)
    parser.db.save_review.assert_not_called()
    parser.mark_source_completed.assert_called_once()
