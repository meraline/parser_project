"""
Производственный парсер отзывов Drom.ru

Архитектура:
1. Режим "все бренды" - парсит все бренды согласно блока со страницы
2. Режим "проверка новых брендов" - проверяет и добавляет новые бренды
3. Для каждой модели собирает длинные и короткие отзывы
4. Структура URL: https://www.drom.ru/reviews/BRAND/MODEL/ID/
"""

import json
import time
import re
from typing import List, Dict, Optional, Tuple, Union
from dataclasses import dataclass, asdict, field
from pathlib import Path
import logging

try:
    import requests
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin, urlparse
except ImportError:
    print("Требуется установка: pip install requests beautifulsoup4")
    raise

@dataclass
class ReviewData:
    """Структура данных отзыва"""
    review_id: str
    brand: str
    model: str
    review_type: str  # 'long' или 'short'
    
    # Характеристики автомобиля
    year: Optional[int] = None
    engine_volume: Optional[float] = None
    fuel_type: Optional[str] = None
    transmission: Optional[str] = None
    drive_type: Optional[str] = None
    
    # Данные отзыва
    author: Optional[str] = None
    city: Optional[str] = None
    date: Optional[str] = None
    rating: Optional[float] = None
    title: Optional[str] = None
    
    # Содержание отзыва
    positive: Optional[str] = None
    negative: Optional[str] = None
    general: Optional[str] = None
    breakages: Optional[str] = None
    
    # Дополнительные данные
    photos: List[str] = field(default_factory=list)
    url: Optional[str] = None
    
    def __post_init__(self):
        pass  # photos уже инициализированы как пустой список

@dataclass
class BrandInfo:
    """Информация о бренде"""
    name: str
    url: str
    reviews_count: int
    url_name: str  # имя в URL, например 'alfa_romeo' для 'Alfa Romeo'

@dataclass
class ModelInfo:
    """Информация о модели"""
    name: str
    brand: str
    url: str
    long_reviews_count: int = 0
    short_reviews_count: int = 0
    url_name: str = ""

class BaseParser:
    """Базовый класс для парсеров"""
    def __init__(self):
        pass

class ProductionDromParser(BaseParser):
    """
    Производственный парсер отзывов с Drom.ru
    
    БОЛЬШЕ НЕ СОЗДАЕМ НОВЫХ ПАРСЕРОВ! Развиваем только этот!
    """
    
    def __init__(self, cache_dir: str = "data/cache", delay: float = 1.0):
        super().__init__()
        self.base_url = "https://www.drom.ru"
        self.reviews_url = f"{self.base_url}/reviews/"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.delay = delay
        
        # Настройка логирования
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Настройка сессии
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
    
    def get_brands_catalog(self) -> List[BrandInfo]:
        """
        Получает каталог всех брендов из блока на главной странице отзывов
        
        Returns:
            List[BrandInfo]: Список брендов с информацией
        """
        cache_file = self.cache_dir / "brands_catalog.json"
        
        # Проверяем кэш
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                    brands = [BrandInfo(**brand) for brand in cached_data]
                    self.logger.info(f"Загружен каталог брендов из кэша: {len(brands)} брендов")
                    return brands
            except Exception as e:
                self.logger.warning(f"Ошибка при загрузке кэша брендов: {e}")
        
        # Парсим главную страницу отзывов
        self.logger.info("Парсинг каталога брендов...")
        response = self.session.get(self.reviews_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Ищем блок с брендами
        brands_block = soup.find('div', {'data-ftid': 'component_cars-list'})
        if not brands_block:
            raise ValueError("Не найден блок с брендами")
        
        brands = []
        
        # Парсим каждый бренд
        brand_items = brands_block.find_all('div', class_='frg44i0')
        
        for item in brand_items:
            try:
                # Получаем ссылку
                link = item.find('a', {'data-ftid': 'component_cars-list-item_hidden-link'})
                if not link:
                    continue
                
                brand_url = link.get('href')
                if not brand_url:
                    continue
                
                # Извлекаем имя бренда из URL
                url_name = brand_url.rstrip('/').split('/')[-1]
                
                # Получаем отображаемое имя
                name_span = item.find('span', {'data-ftid': 'component_cars-list-item_name'})
                if not name_span:
                    continue
                
                brand_name = name_span.get_text(strip=True)
                
                # Получаем количество отзывов
                counter_span = item.find('span', {'data-ftid': 'component_cars-list-item_counter'})
                reviews_count = 0
                if counter_span:
                    counter_text = counter_span.get_text(strip=True)
                    # Извлекаем число из текста
                    count_match = re.search(r'\d+', counter_text.replace(' ', ''))
                    if count_match:
                        reviews_count = int(count_match.group())
                
                brand_info = BrandInfo(
                    name=brand_name,
                    url=brand_url,
                    reviews_count=reviews_count,
                    url_name=url_name
                )
                brands.append(brand_info)
                
                self.logger.debug(f"Найден бренд: {brand_name} ({reviews_count} отзывов)")
                
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге бренда: {e}")
                continue
        
        self.logger.info(f"Найдено брендов: {len(brands)}")
        
        # Сохраняем в кэш
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump([asdict(brand) for brand in brands], f, ensure_ascii=False, indent=2)
            self.logger.info(f"Каталог брендов сохранен в кэш: {cache_file}")
        except Exception as e:
            self.logger.warning(f"Ошибка при сохранении кэша брендов: {e}")
        
        return brands
    
    def check_new_brands(self, known_brands: List[str]) -> List[BrandInfo]:
        """
        Проверяет и возвращает новые бренды, которых нет в списке известных
        
        Args:
            known_brands: Список известных брендов (url_name)
            
        Returns:
            List[BrandInfo]: Список новых брендов
        """
        all_brands = self.get_brands_catalog()
        new_brands = [brand for brand in all_brands if brand.url_name not in known_brands]
        
        if new_brands:
            self.logger.info(f"Найдено новых брендов: {len(new_brands)}")
            for brand in new_brands:
                self.logger.info(f"  - {brand.name} ({brand.url_name})")
        else:
            self.logger.info("Новых брендов не найдено")
        
        return new_brands
    
    def get_models_for_brand(self, brand: BrandInfo) -> List[ModelInfo]:
        """
        Получает список моделей для бренда
        
        Args:
            brand: Информация о бренде
            
        Returns:
            List[ModelInfo]: Список моделей
        """
        cache_file = self.cache_dir / f"models_{brand.url_name}.json"
        
        # Проверяем кэш
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                    models = [ModelInfo(**model) for model in cached_data]
                    self.logger.info(f"Загружены модели {brand.name} из кэша: {len(models)} моделей")
                    return models
            except Exception as e:
                self.logger.warning(f"Ошибка при загрузке кэша моделей {brand.name}: {e}")
        
        # Парсим страницу бренда
        self.logger.info(f"Парсинг моделей бренда {brand.name}...")
        
        time.sleep(self.delay)
        response = self.session.get(brand.url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        models = []
        
        # Ищем блок с моделями
        model_items = soup.find_all('a', href=re.compile(rf'/reviews/{brand.url_name}/[^/]+/?$'))
        
        for item in model_items:
            try:
                model_url = item.get('href')
                if not model_url:
                    continue
                
                # Извлекаем имя модели из URL
                url_parts = model_url.rstrip('/').split('/')
                if len(url_parts) < 4:
                    continue
                
                model_url_name = url_parts[-1]
                
                # Получаем отображаемое имя модели
                model_name = item.get_text(strip=True)
                if not model_name:
                    continue
                
                # Получаем полный URL
                full_url = urljoin(self.base_url, model_url)
                
                model_info = ModelInfo(
                    name=model_name,
                    brand=brand.name,
                    url=full_url,
                    url_name=model_url_name
                )
                models.append(model_info)
                
            except Exception as e:
                self.logger.error(f"Ошибка при парсинге модели: {e}")
                continue
        
        self.logger.info(f"Найдено моделей для {brand.name}: {len(models)}")
        
        # Сохраняем в кэш
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump([asdict(model) for model in models], f, ensure_ascii=False, indent=2)
            self.logger.info(f"Модели {brand.name} сохранены в кэш: {cache_file}")
        except Exception as e:
            self.logger.warning(f"Ошибка при сохранении кэша моделей {brand.name}: {e}")
        
        return models
    
    def get_review_counts_for_model(self, model: ModelInfo) -> Tuple[int, int]:
        """
        Получает количество длинных и коротких отзывов для модели
        
        Args:
            model: Информация о модели
            
        Returns:
            Tuple[int, int]: (количество длинных отзывов, количество коротких отзывов)
        """
        try:
            time.sleep(self.delay)
            response = self.session.get(model.url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            long_reviews_count = 0
            short_reviews_count = 0
            
            # Ищем кнопки переключения между длинными и короткими отзывами
            tabs_block = soup.find('div', class_='_65ykvx0')
            if tabs_block:
                # Кнопка длинных отзывов
                long_reviews_tab = tabs_block.find('a', {'data-ftid': 'reviews_tab_button_long_reviews'})
                if long_reviews_tab:
                    text = long_reviews_tab.get_text(strip=True)
                    # Удаляем пробелы из числа для корректного извлечения
                    match = re.search(r'(\d+)', text.replace(' ', ''))
                    if match:
                        long_reviews_count = int(match.group(1))
                
                # Кнопка коротких отзывов
                short_reviews_tab = tabs_block.find('a', {'data-ftid': 'reviews_tab_button_short_reviews'})
                if short_reviews_tab:
                    text = short_reviews_tab.get_text(strip=True)
                    # Удаляем пробелы из числа для корректного извлечения
                    match = re.search(r'(\d+)', text.replace(' ', ''))
                    if match:
                        short_reviews_count = int(match.group(1))
            
            self.logger.debug(f"Модель {model.name}: {long_reviews_count} длинных, {short_reviews_count} коротких отзывов")
            
            return long_reviews_count, short_reviews_count
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении количества отзывов для {model.name}: {e}")
            return 0, 0
    
    def parse_long_reviews(self, model: ModelInfo, limit: Optional[int] = None) -> List[ReviewData]:
        """
        Парсит длинные отзывы для модели
        
        Args:
            model: Информация о модели
            limit: Максимальное количество отзывов
            
        Returns:
            List[ReviewData]: Список длинных отзывов
        """
        reviews = []
        
        try:
            # URL длинных отзывов
            long_reviews_url = f"{model.url}?order=relevance"
            
            time.sleep(self.delay)
            response = self.session.get(long_reviews_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ищем блоки с длинными отзывами
            review_blocks = soup.find_all('div', {'data-ftid': 'review-item'})
            
            for block in review_blocks:
                if limit and len(reviews) >= limit:
                    break
                
                try:
                    review = self._parse_long_review_block(block, model)
                    if review:
                        reviews.append(review)
                        
                except Exception as e:
                    self.logger.error(f"Ошибка при парсинге длинного отзыва: {e}")
                    continue
            
            self.logger.info(f"Получено длинных отзывов для {model.name}: {len(reviews)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге длинных отзывов для {model.name}: {e}")
        
        return reviews
    
    def parse_short_reviews(self, model: ModelInfo, limit: Optional[int] = None) -> List[ReviewData]:
        """
        Парсит короткие отзывы для модели
        
        Args:
            model: Информация о модели
            limit: Максимальное количество отзывов
            
        Returns:
            List[ReviewData]: Список коротких отзывов
        """
        reviews = []
        
        try:
            # URL коротких отзывов
            short_reviews_url = f"{model.url}5kopeek/?order=useful"
            
            time.sleep(self.delay)
            response = self.session.get(short_reviews_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ищем блоки с короткими отзывами
            review_blocks = soup.find_all('div', {'data-ftid': 'short-review-item'})
            
            for block in review_blocks:
                if limit and len(reviews) >= limit:
                    break
                
                try:
                    review = self._parse_short_review_block(block, model)
                    if review:
                        reviews.append(review)
                        
                except Exception as e:
                    self.logger.error(f"Ошибка при парсинге короткого отзыва: {e}")
                    continue
            
            self.logger.info(f"Получено коротких отзывов для {model.name}: {len(reviews)}")
            
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге коротких отзывов для {model.name}: {e}")
        
        return reviews
    
    def _parse_long_review_block(self, block, model: ModelInfo) -> Optional[ReviewData]:
        """Парсит блок длинного отзыва"""
        try:
            # Получаем ID отзыва
            review_id = block.get('id', '')
            
            # URL отзыва
            review_url = f"{model.url}{review_id}/"
            
            # Извлекаем данные отзыва
            review_data = {
                'review_id': review_id,
                'brand': model.brand.lower(),
                'model': model.url_name,
                'review_type': 'long',
                'url': review_url
            }
            
            # Заголовок
            title_elem = block.find("h3")
            if title_elem:
                review_data["title"] = title_elem.get_text(strip=True)
            
            # Плюсы
            positive_elem = block.find("div", {"data-ftid": "review-content__positive"})
            if positive_elem:
                review_data["positive_text"] = positive_elem.get_text(strip=True)
            
            # Минусы
            negative_elem = block.find("div", {"data-ftid": "review-content__negative"})
            if negative_elem:
                review_data["negative_text"] = negative_elem.get_text(strip=True)
                
            # Поломки
            breakages_elem = block.find("div", {"data-ftid": "review-content__breakages"})
            if breakages_elem:
                review_data["breakages_text"] = breakages_elem.get_text(strip=True)
            
            # Основной контент из секций
            content_parts = []
            content_sections = block.find_all("div", class_="css-6hj46s")
            for section in content_sections:
                text = section.get_text(strip=True)
                if text:
                    content_parts.append(text)
            
            if content_parts:
                review_data["content"] = "\n".join(content_parts)
            
            # Количество фотографий
            photos = block.find_all("img")
            review_data["photos_count"] = len(photos) if photos else 0
            
            # Создаем ReviewData
            review = ReviewData(**review_data)
            return review
            
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге длинного отзыва: {e}")
            return None
    
    def _parse_short_review_block(self, block, model: ModelInfo) -> Optional[ReviewData]:
        """Парсит блок короткого отзыва"""
        try:
            # Получаем ID отзыва
            review_id = block.get('id', '')
            
            review = ReviewData(
                review_id=review_id,
                brand=model.brand.lower(),
                model=model.url_name,
                review_type='short',
                url=f"{model.url}5kopeek/"
            )
            
            # Парсим заголовок с характеристиками
            title_div = block.find('div', {'data-ftid': 'short-review-item__title'})
            if title_div:
                self._extract_car_specs_from_title(title_div, review)
            
            # Парсим автора и дату
            author_info = block.find('div', class_='css-987tv1')
            if author_info:
                spans = author_info.find_all('span')
                if len(spans) >= 1:
                    review.author = spans[0].get_text(strip=True)
                if len(spans) >= 2:
                    review.date = spans[1].get_text(strip=True)
                if len(spans) >= 3:
                    review.city = spans[2].get_text(strip=True)
            
            # Парсим позитивные отзывы
            positive_div = block.find('div', {'data-ftid': 'short-review-content__positive'})
            if positive_div:
                review.positive = positive_div.get_text(strip=True)
            
            # Парсим негативные отзывы
            negative_div = block.find('div', {'data-ftid': 'short-review-content__negative'})
            if negative_div:
                review.negative = negative_div.get_text(strip=True)
            
            # Парсим поломки
            breakages_div = block.find('div', {'data-ftid': 'short-review-content__breakages'})
            if breakages_div:
                review.breakages = breakages_div.get_text(strip=True)
            
            # Парсим фотографии
            photo_divs = block.find_all('div', class_='_1gzw4372')
            photos = []
            for photo_div in photo_divs:
                img = photo_div.find('img')
                if img and img.get('src'):
                    photos.append(img['src'])
            review.photos = photos
            
            return review
            
        except Exception as e:
            self.logger.error(f"Ошибка при парсинге короткого отзыва: {e}")
            return None
    
    def _extract_car_specs_from_title(self, title_div, review: ReviewData):
        """Извлекает характеристики автомобиля из заголовка"""
        try:
            text = title_div.get_text(strip=True)
            
            # Год
            year_span = title_div.find('span', {'data-ftid': 'short-review-item__year'})
            if year_span:
                year_text = year_span.get_text(strip=True)
                try:
                    review.year = int(year_text)
                except ValueError:
                    pass
            
            # Объем двигателя
            volume_span = title_div.find('span', {'data-ftid': 'short-review-item__volume'})
            if volume_span:
                volume_text = volume_span.get_text(strip=True)
                try:
                    review.engine_volume = float(volume_text)
                except ValueError:
                    pass
            
            # Тип топлива, коробка и привод из текста
            if 'бензин' in text:
                review.fuel_type = 'бензин'
            elif 'дизель' in text:
                review.fuel_type = 'дизель'
            elif 'гибрид' in text:
                review.fuel_type = 'гибрид'
            elif 'электро' in text:
                review.fuel_type = 'электро'
            
            if 'автомат' in text:
                review.transmission = 'автомат'
            elif 'механика' in text:
                review.transmission = 'механика'
                
            if 'передний' in text:
                review.drive_type = 'передний'
            elif 'задний' in text:
                review.drive_type = 'задний'
            elif 'полный' in text:
                review.drive_type = 'полный'
            
            if 'автомат' in text:
                review.transmission = 'автомат'
            elif 'механика' in text:
                review.transmission = 'механика'
            elif 'вариатор' in text:
                review.transmission = 'вариатор'
            
            if 'передний' in text:
                review.drive_type = 'передний'
            elif 'задний' in text:
                review.drive_type = 'задний'
            elif 'полный' in text:
                review.drive_type = 'полный'
                
        except Exception as e:
            self.logger.error(f"Ошибка при извлечении характеристик: {e}")
    
    def parse_all_brands(self, long_reviews_per_model: int = 3, short_reviews_per_model: int = 10) -> Dict:
        """
        Режим 1: Парсит все бренды
        
        Args:
            long_reviews_per_model: Количество длинных отзывов на модель
            short_reviews_per_model: Количество коротких отзывов на модель
            
        Returns:
            Dict: Результаты парсинга
        """
        self.logger.info("=== РЕЖИМ: Парсинг всех брендов ===")
        
        brands = self.get_brands_catalog()
        results = {
            'mode': 'all_brands',
            'total_brands': len(brands),
            'processed_brands': 0,
            'total_models': 0,
            'total_long_reviews': 0,
            'total_short_reviews': 0,
            'brands_data': []
        }
        
        for i, brand in enumerate(brands):
            self.logger.info(f"[{i+1}/{len(brands)}] Обрабатываем бренд: {brand.name}")
            
            try:
                # Получаем модели бренда
                models = self.get_models_for_brand(brand)
                
                brand_data = {
                    'brand': brand.name,
                    'brand_url_name': brand.url_name,
                    'models_count': len(models),
                    'models': []
                }
                
                for model in models[:5]:  # Ограничиваем первыми 5 моделями для теста
                    self.logger.info(f"  Модель: {model.name}")
                    
                    # Парсим отзывы
                    long_reviews = self.parse_long_reviews(model, long_reviews_per_model)
                    short_reviews = self.parse_short_reviews(model, short_reviews_per_model)
                    
                    model_data = {
                        'model': model.name,
                        'model_url_name': model.url_name,
                        'long_reviews_count': len(long_reviews),
                        'short_reviews_count': len(short_reviews),
                        'long_reviews': [asdict(review) for review in long_reviews],
                        'short_reviews': [asdict(review) for review in short_reviews]
                    }
                    
                    brand_data['models'].append(model_data)
                    results['total_long_reviews'] += len(long_reviews)
                    results['total_short_reviews'] += len(short_reviews)
                
                results['brands_data'].append(brand_data)
                results['processed_brands'] += 1
                results['total_models'] += len(models)
                
            except Exception as e:
                self.logger.error(f"Ошибка при обработке бренда {brand.name}: {e}")
                continue
        
        return results
    
    def check_and_parse_new_brands(self, known_brands_file: str, long_reviews_per_model: int = 3, short_reviews_per_model: int = 10) -> Dict:
        """
        Режим 2: Проверяет новые бренды и парсит их
        
        Args:
            known_brands_file: Файл с известными брендами
            long_reviews_per_model: Количество длинных отзывов на модель
            short_reviews_per_model: Количество коротких отзывов на модель
            
        Returns:
            Dict: Результаты парсинга новых брендов
        """
        self.logger.info("=== РЕЖИМ: Проверка и парсинг новых брендов ===")
        
        # Загружаем известные бренды
        known_brands = []
        try:
            with open(known_brands_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                known_brands = [item.get('url_name', '') for item in data]
        except FileNotFoundError:
            self.logger.warning(f"Файл известных брендов не найден: {known_brands_file}")
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке известных брендов: {e}")
        
        # Проверяем новые бренды
        new_brands = self.check_new_brands(known_brands)
        
        if not new_brands:
            return {
                'mode': 'new_brands_check',
                'new_brands_found': 0,
                'message': 'Новых брендов не найдено'
            }
        
        # Парсим новые бренды
        results = {
            'mode': 'new_brands_check',
            'new_brands_found': len(new_brands),
            'processed_brands': 0,
            'total_models': 0,
            'total_long_reviews': 0,
            'total_short_reviews': 0,
            'new_brands_data': []
        }
        
        for new_brand in new_brands:
            self.logger.info(f"Обрабатываем новый бренд: {new_brand.name}")
            
            try:
                # Получаем модели бренда
                models = self.get_models_for_brand(new_brand)
                
                brand_data = {
                    'brand': new_brand.name,
                    'brand_url_name': new_brand.url_name,
                    'models_count': len(models),
                    'models': []
                }
                
                for model in models[:3]:  # Ограничиваем первыми 3 моделями для новых брендов
                    self.logger.info(f"  Модель: {model.name}")
                    
                    # Парсим отзывы
                    long_reviews = self.parse_long_reviews(model, long_reviews_per_model)
                    short_reviews = self.parse_short_reviews(model, short_reviews_per_model)
                    
                    model_data = {
                        'model': model.name,
                        'model_url_name': model.url_name,
                        'long_reviews_count': len(long_reviews),
                        'short_reviews_count': len(short_reviews),
                        'long_reviews': [asdict(review) for review in long_reviews],
                        'short_reviews': [asdict(review) for review in short_reviews]
                    }
                    
                    brand_data['models'].append(model_data)
                    results['total_long_reviews'] += len(long_reviews)
                    results['total_short_reviews'] += len(short_reviews)
                
                results['new_brands_data'].append(brand_data)
                results['processed_brands'] += 1
                results['total_models'] += len(models)
                
            except Exception as e:
                self.logger.error(f"Ошибка при обработке нового бренда {new_brand.name}: {e}")
                continue
        
        return results
