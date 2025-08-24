import unittest
from src.auto_reviews_parser.parsers.drom import DromParser


class TestDromParser(unittest.TestCase):
    def setUp(self):
        self.parser = DromParser()
        self.test_url = "https://www.drom.ru/reviews/toyota/camry/1700000/"

    def test_brand_model_extraction(self):
        """Тест извлечения марки и модели из URL"""
        brand, model = self.parser.extract_brand_model(self.test_url)
        self.assertEqual(brand, "toyota")
        self.assertEqual(model, "camry")

    def test_review_parsing(self):
        """Тест парсинга отзыва"""
        reviews = self.parser.parse_reviews(self.test_url)
        self.assertGreater(len(reviews), 0)

        review = reviews[0]
        self.assertEqual(review.source, "drom.ru")
        self.assertEqual(review.type, "review")
        self.assertEqual(review.brand, "toyota")
        self.assertEqual(review.model, "camry")
        self.assertIsNotNone(review.content)
        self.assertIsNotNone(review.publish_date)
        self.assertIsNotNone(review.author)


if __name__ == "__main__":
    unittest.main()
