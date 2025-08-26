import unittest
from auto_reviimport unittest
from auto_reviews_parser.parsers.drom_reviews import DromReviewsParser


class TestDromReviewsParser(unittest.TestCase):
    def setUp(self):
        self.parser = DromReviewsParser()
        # URL для тестирования - конкретная модель с отзывами
        self.test_brand = "toyota"
        self.test_model = "camry"

    def test_parse_long_reviews(self):
        """Тест парсинга длинных отзывов"""
        try:
            reviews = self.parser.parse_long_reviews(
                brand=self.test_brand, 
                model=self.test_model,
                limit=3  # Ограничиваем для быстрого тестирования
            )
            
            self.assertIsInstance(reviews, list)
            self.assertGreater(len(reviews), 0)
            
            # Проверяем структуру первого отзыва
            if reviews:
                review = reviews[0]
                self.assertIn('brand', review)
                self.assertIn('model', review)
                self.assertIn('content', review)
                self.assertEqual(review['brand'], self.test_brand)
                self.assertEqual(review['model'], self.test_model)
                print(f"✅ Успешно спаршено {len(reviews)} длинных отзывов")
                
        except Exception as e:
            self.fail(f"Ошибка при парсинге длинных отзывов: {e}")

    def test_parse_short_reviews(self):
        """Тест парсинга коротких отзывов"""
        try:
            reviews = self.parser.parse_short_reviews(
                brand=self.test_brand,
                model=self.test_model,
                limit=5  # Ограничиваем для быстрого тестирования
            )
            
            self.assertIsInstance(reviews, list)
            self.assertGreater(len(reviews), 0)
            
            # Проверяем структуру первого отзыва
            if reviews:
                review = reviews[0]
                self.assertIn('brand', review)
                self.assertIn('model', review)
                self.assertEqual(review['brand'], self.test_brand)
                self.assertEqual(review['model'], self.test_model)
                print(f"✅ Успешно спаршено {len(reviews)} коротких отзывов")
                
        except Exception as e:
            self.fail(f"Ошибка при парсинге коротких отзывов: {e}")

    def test_parse_model_reviews_combined(self):
        """Тест комбинированного парсинга отзывов модели"""
        try:
            result = self.parser.parse_model_reviews(
                brand=self.test_brand,
                model=self.test_model,
                max_long_reviews=2,
                max_short_reviews=3
            )
            
            self.assertIsInstance(result, dict)
            self.assertIn('long_reviews', result)
            self.assertIn('short_reviews', result)
            
            long_reviews = result['long_reviews']
            short_reviews = result['short_reviews']
            
            self.assertIsInstance(long_reviews, list)
            self.assertIsInstance(short_reviews, list)
            
            total_reviews = len(long_reviews) + len(short_reviews)
            self.assertGreater(total_reviews, 0)
            
            print(f"✅ Комбинированный парсинг: {len(long_reviews)} длинных + {len(short_reviews)} коротких отзывов")
            
        except Exception as e:
            self.fail(f"Ошибка при комбинированном парсинге: {e}")


if __name__ == '__main__':
    unittest.main(verbosity=2)s_parser.parsers.drom_reviews import DromReviewsParser


class TestDromParser(unittest.TestCase):
    def setUp(self):
        self.parser = DromReviewsParser()
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
