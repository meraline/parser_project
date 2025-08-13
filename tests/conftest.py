import sys
import types
from pathlib import Path
from unittest.mock import MagicMock
import pytest

# Stub external 'botasaurus' dependencies before importing project modules
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

class Request:
    pass

def request(*args, **kwargs):
    def decorator(func):
        return func
    return decorator

request_module.request = request
request_module.Request = Request

soupify_module = types.ModuleType("botasaurus.soupify")

def soupify(*args, **kwargs):
    return None

soupify_module.soupify = soupify

bt_module = types.ModuleType("botasaurus.bt")

def write_excel(*args, **kwargs):
    return None

def write_json(*args, **kwargs):
    return None

bt_module.write_excel = write_excel
bt_module.write_json = write_json

botasaurus_pkg.browser = browser_module
botasaurus_pkg.request = request_module
botasaurus_pkg.soupify = soupify_module
botasaurus_pkg.bt = bt_module

sys.modules.setdefault("botasaurus", botasaurus_pkg)
sys.modules.setdefault("botasaurus.browser", browser_module)
sys.modules.setdefault("botasaurus.request", request_module)
sys.modules.setdefault("botasaurus.soupify", soupify_module)
sys.modules.setdefault("botasaurus.bt", bt_module)

# Ensure project root is on the Python path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


@pytest.fixture
def test_db(tmp_path):
    """Provide a temporary database for tests."""
    from auto_reviews_parser import ReviewsDatabase

    db_path = tmp_path / "test.db"
    return ReviewsDatabase(str(db_path))


@pytest.fixture
def parser_mocks():
    """Mocks for individual site parsers."""
    from parsers.drom_parser import DromParser
    from parsers.drive2_parser import Drive2Parser

    drom_mock = MagicMock(spec=DromParser)
    drive2_mock = MagicMock(spec=Drive2Parser)
    return drom_mock, drive2_mock


@pytest.fixture
def auto_parser(parser_mocks):
    """AutoReviewsParser with mocked dependencies."""
    from auto_reviews_parser import AutoReviewsParser

    parser = AutoReviewsParser.__new__(AutoReviewsParser)
    parser.db = MagicMock()
    parser.drom_parser, parser.drive2_parser = parser_mocks
    parser.sources_queue = MagicMock()
    parser.mark_source_completed = MagicMock()
    return parser
