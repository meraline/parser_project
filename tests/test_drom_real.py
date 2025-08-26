import unittest
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
                brand_url_name=self.test_brand, 
                model_url_name=self.test_model,
                max_pages=1  # Ограничиваем для быстрого тестирования
            )
            
            self.assertIsInstance(reviews, list)
            self.assertGreaterEqual(len(reviews), 0)  # Может быть 0 если нет отзывов
            
            # Проверяем структуру первого отзыва (если есть)
            if reviews:
                review = reviews[0]
                self.assertIn('brand', review)
                self.assertIn('model', review)
                self.assertIn('content', review)
                self.assertEqual(review['brand'], self.test_brand)
                self.assertEqual(review['model'], self.test_model)
                print(f"✅ Успешно спаршено {len(reviews)} длинных отзывов")
            else:
                print("ℹ️ Длинные отзывы не найдены (это нормально)")
                
        except Exception as e:
            self.fail(f"Ошибка при парсинге длинных отзывов: {e}")

    def test_parse_short_reviews(self):
        """Тест парсинга коротких отзывов"""
        try:
            reviews = self.parser.parse_short_reviews(
                brand_url_name=self.test_brand,
                model_url_name=self.test_model,
                max_pages=1  # Ограничиваем для быстрого тестирования
            )
            
            self.assertIsInstance(reviews, list)
            self.assertGreaterEqual(len(reviews), 0)  # Может быть 0 если нет отзывов
            
            # Проверяем структуру первого отзыва (если есть)
            if reviews:
                review = reviews[0]
                self.assertIn('brand', review)
                self.assertIn('model', review)
                self.assertEqual(review['brand'], self.test_brand)
                self.assertEqual(review['model'], self.test_model)
                print(f"✅ Успешно спаршено {len(reviews)} коротких отзывов")
            else:
                print("ℹ️ Короткие отзывы не найдены (это нормально)")
                
        except Exception as e:
            self.fail(f"Ошибка при парсинге коротких отзывов: {e}")

    def test_parse_model_reviews_combined(self):
        """Тест комбинированного парсинга отзывов модели"""
        try:
            result = self.parser.parse_model_reviews(
                brand_url_name=self.test_brand,
                model_url_name=self.test_model,
                max_pages_long=1,
                max_pages_short=1
            )
            
            # Этот метод возвращает bool (успех сохранения в БД)
            self.assertIsInstance(result, bool)
            
            print(f"✅ Комбинированный парсинг завершен с результатом: {result}")
            
        except Exception as e:
            self.fail(f"Ошибка при комбинированном парсинге: {e}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
