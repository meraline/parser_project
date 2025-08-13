import sys
import types
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock
from bs4 import BeautifulSoup

# Stub out botasaurus modules to avoid heavy dependencies during import
botsaurus = types.ModuleType("botasaurus")

browser_mod = types.ModuleType("botasaurus.browser")

def browser(*args, **kwargs):
    def decorator(func):
        return func
    return decorator

class Driver:  # placeholder for type hints
    pass

browser_mod.browser = browser
browser_mod.Driver = Driver

request_mod = types.ModuleType("botasaurus.request")
class Request:  # minimal placeholder
    pass

def request(*args, **kwargs):
    return None
request_mod.Request = Request
request_mod.request = request

soupify_mod = types.ModuleType("botasaurus.soupify")

def soupify(*args, **kwargs):
    return None
soupify_mod.soupify = soupify

bt_mod = types.ModuleType("botasaurus.bt")

def write_excel(*args, **kwargs):
    return None

def write_json(*args, **kwargs):
    return None
bt_mod.write_excel = write_excel
bt_mod.write_json = write_json

# Register stub modules
sys.modules.setdefault("botasaurus", botsaurus)
sys.modules.setdefault("botasaurus.browser", browser_mod)
sys.modules.setdefault("botasaurus.request", request_mod)
sys.modules.setdefault("botasaurus.soupify", soupify_mod)
sys.modules.setdefault("botasaurus.bt", bt_mod)

# Import parser classes from the project module (test.py)
spec = importlib.util.spec_from_file_location("parser_module", Path(__file__).resolve().parent.parent / "test.py")
parser_module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(parser_module)
DromParser = parser_module.DromParser
Drive2Parser = parser_module.Drive2Parser


class MockElement:
    """Wrapper around BeautifulSoup elements providing required interface."""

    def __init__(self, element):
        self.element = element

    def select(self, selector):
        found = self.element.select_one(selector)
        return MockElement(found) if found else None

    def select_all(self, selector):
        return [MockElement(el) for el in self.element.select(selector)]

    def get_text(self):
        return self.element.get_text()

    def get_attribute(self, attr, default=None):
        return self.element.attrs.get(attr, default)


def create_driver(html):
    """Create a mocked driver for a single HTML page."""
    soup = BeautifulSoup(html, "html.parser")
    driver = MagicMock()
    driver.title = "Test"
    driver.google_get.side_effect = lambda url, bypass_cloudflare=True: None
    driver.get_via_this_page.side_effect = lambda url: None
    driver.select_all.side_effect = lambda selector: [
        MockElement(e) for e in soup.select(selector)
    ]
    driver.select.side_effect = lambda selector: (
        MockElement(soup.select_one(selector)) if soup.select_one(selector) else None
    )
    return driver


def create_driver_sequence(html_pages):
    """Create a mocked driver cycling through provided HTML pages for sequential calls."""
    soups = [BeautifulSoup(h, "html.parser") for h in html_pages]
    driver = MagicMock()
    driver.title = "Test"
    state = {"index": -1}

    def google_get(url, bypass_cloudflare=True):
        state["index"] += 1
        driver.current_soup = soups[state["index"]]

    driver.google_get.side_effect = google_get
    driver.get_via_this_page.side_effect = lambda url: None
    driver.select_all.side_effect = lambda selector: [
        MockElement(e) for e in driver.current_soup.select(selector)
    ]
    driver.select.side_effect = lambda selector: (
        MockElement(driver.current_soup.select_one(selector))
        if driver.current_soup.select_one(selector)
        else None
    )
    return driver


def test_drom_parser_parses_expected_fields():
    html = """
    <div data-ftid="component_reviews-item">
      <h3><a href="/review1">Great Car</a></h3>
      <div class="css-kxziuu">4.5</div>
      <div class="css-username">John Doe</div>
      <div class="css-1x4jntm">2015, 2.0 л, 50 000 км, бензин, автомат</div>
      <div class="css-1wdvlz0">It is a great car.</div>
      <div class="css-date">01.01.2023</div>
    </div>
    """

    driver = create_driver(html)
    db = MagicMock()
    db.is_url_parsed.return_value = False

    parser = DromParser(db)
    parser.random_delay = lambda *a, **k: None

    reviews = parser.parse_brand_model_reviews(
        driver, {"brand": "toyota", "model": "camry", "max_pages": 1}
    )

    assert len(reviews) == 1
    review = reviews[0]
    assert review.title == "Great Car"
    assert review.rating == 4.5
    assert review.author == "John Doe"
    assert review.year == 2015
    assert review.engine_volume == 2.0
    assert review.mileage == 50000


def test_drive2_parser_parses_expected_fields():
    html_experience = """
    <div class="c-car-card">
      <a class="c-car-card__caption" href="/exp1">Exp Title</a>
      <div class="c-username__link">Alice</div>
      <div class="c-car-card__info">2018, 1.6 л, 30 000 км, бензин, механика, полный</div>
      <div class="c-car-card__param_mileage">30 000 км</div>
      <div class="c-car-card__preview">Loved it.</div>
      <div class="c-post-card__views">123</div>
      <div class="c-post-card__likes">45</div>
      <div class="c-car-card__date">02.02.2024</div>
    </div>
    """

    html_logbook = """
    <div class="c-post-card">
      <a class="c-post-card__title" href="/log1">Log Entry</a>
      <div class="c-username__link">Bob</div>
      <div class="c-post-card__car-info">2019, 2.5 л, 40 000 км, дизель, автомат, передний</div>
      <div class="c-post-card__preview">Diary text</div>
      <div class="c-post-card__views">200</div>
      <div class="c-post-card__likes">10</div>
      <div class="c-post-card__date">03.03.2024</div>
    </div>
    """

    driver = create_driver_sequence([html_experience, html_logbook])
    db = MagicMock()
    db.is_url_parsed.return_value = False

    parser = Drive2Parser(db)
    parser.random_delay = lambda *a, **k: None

    reviews = parser.parse_brand_model_reviews(
        driver, {"brand": "toyota", "model": "camry", "max_pages": 2}
    )

    assert len(reviews) == 2
    review_exp, review_log = reviews

    assert review_exp.type == "review"
    assert review_exp.title == "Exp Title"
    assert review_exp.author == "Alice"
    assert review_exp.year == 2018
    assert review_exp.engine_volume == 1.6
    assert review_exp.mileage == 30000

    assert review_log.type == "board_journal"
    assert review_log.title == "Log Entry"
    assert review_log.author == "Bob"
    assert review_log.year == 2019
    assert review_log.engine_volume == 2.5
    assert review_log.mileage == 40000
