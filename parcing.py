import re
import time
import json
import requests
import boto3
from botocore.exceptions import ClientError
from playwright.sync_api import sync_playwright
from math import radians, sin, cos, sqrt, atan2

# Функция для расчёта расстояния между кремлем и квартирой по координатам
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))




#ПАРСЕР

class CianParser:

    def __init__(self, keyword: str):
        self.keyword = keyword
        self.results = {}
        self.playwright = None
        self.browser = None
        self.page = None
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123',
            region_name='us-east-1'
        )
        self.BUCKET_NAME = 'cian-photos'
    def _ensure_bucket(self):
        """Создаёт бакет, если он ещё не существует."""
        try:
            self.s3_client.head_bucket(Bucket=self.BUCKET_NAME)
            print(f"Бакет '{self.BUCKET_NAME}' уже существует.")
        except ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchBucket'):
                self.s3_client.create_bucket(Bucket=self.BUCKET_NAME)
                print(f"Бакет '{self.BUCKET_NAME}' создан.")
            else:
                raise

    def _page_down(self, times: int = 5):
        for _ in range(times):
            self.page.keyboard.press('PageDown')
            time.sleep(0.4)

    @staticmethod
    def extract_id(url: str) -> str:
        parts = [p for p in url.split('/') if p.isdigit()]
        return parts[-1] if parts else url

    def save_to_json(self, path: str = "cian_results.json"):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        print(f"Сохранено {len(self.results)} объявлений → {path}")

    def upload_photos(self, offer_id: str) -> list:
        """Собирает URL фотографий с открытой страницы, скачивает каждую
        и загружает в MinIO. Возвращает список S3 URI."""
        s3_uris = []

        try:
            self.page.wait_for_selector(
                'div[data-name="GalleryInnerComponent"] img',
                timeout=5000
            )
            img_elements = self.page.query_selector_all(
                'div[data-name="GalleryInnerComponent"] img'
            )
            photo_urls = []
            for img in img_elements:
                src = img.get_attribute('src')
                if src and src.startswith('http'):
                    photo_urls.append(src)

            seen = set()
            photo_urls = [u for u in photo_urls if not (u in seen or seen.add(u))]

        except Exception as e:
            print(f"Не удалось получить фото для {offer_id}: {e}")
            return s3_uris

        # [:10] — лимит 10 фото на объявление
        for idx, photo_url in enumerate(photo_urls[:10]):
            filename = f"{offer_id}/{idx + 1}.jpg"
            try:
                response = requests.get(photo_url, timeout=15)
                response.raise_for_status()

                self.s3_client.put_object(
                    Bucket=self.BUCKET_NAME,
                    Key=filename,
                    Body=response.content,
                    ContentType='image/jpeg'
                )
                s3_uri = f"s3://{self.BUCKET_NAME}/{filename}"
                s3_uris.append(s3_uri)
                print(f"  Загружено фото {idx + 1}/{min(len(photo_urls), 10)}: {s3_uri}")

            except Exception as e:
                print(f"  Ошибка при загрузке фото {filename}: {e}")

        return s3_uris



#АЛГОРИТМ ПОИСКА
##################################################

    def parcing_announcement(self, url: str):
        try:
            self.page.goto(url, timeout=20000, wait_until="domcontentloaded")
            self.page.wait_for_selector('[data-testid="price-amount"]', timeout=7000)
            price_text = self.page.query_selector('[data-testid="price-amount"]').inner_text()
            price = price_text.replace("\xa0", "").replace(" ", "").strip()
        except Exception:
            price = None
# описание
        description = None
        try:
            el = self.page.query_selector('[data-id="content"]')
            description = el.inner_text() if el else None
        except Exception:
            pass
# адрес
        adress = None
        try:
            el = self.page.query_selector('[data-name="AddressContainer"]')
            adress = el.inner_text() if el else None
        except Exception:
            pass
        
        
#класс дома
        house_class = None
        try:
            HOUSE_CLASS = {
        "деревянный":          0.0,
        "панельный":           0.2,
        "блочный":             0.3,
        "сталинский":          0.5,
        "кирпичный":           0.6,
        "кирпично-монолитный": 0.85,
        "монолитный":          1.0,
    }
            cl_locator = self.page.locator(
                '//div[@data-name="OfferSummaryInfoItem"][.//p[text()="Тип дома"]]/p[2]'
            )
            if cl_locator.count() > 0:
                cl_text = cl_locator.inner_text().strip().lower()
                house_class = next(
                    (score for key, score in HOUSE_CLASS.items() if key in cl_text),
                    None
                )
        except Exception:
            pass


# этаж
        floor, floors_total = None, None
        try:
            floor_locator = self.page.locator('[data-name="ObjectFactoidsItem"]').filter(
                has_text="Этаж"
            ).locator('span[style*="letter-spacing"]')
            
            if floor_locator.count() > 0:
                floor_text = floor_locator.inner_text().strip()  # "2 из 8"
                match = re.search(r'(\d+)\s+из\s+(\d+)', floor_text)
                if match:
                    floor = int(match.group(1))
                    floors_total = int(match.group(2))
                else:
                    # если формат просто "2" без "из N"
                    if floor_text.isdigit():
                        floor = int(floor_text)
        except Exception as e:
            print(f"  Ошибка парсинга этажа: {e}")



# новизна дома
        new_building = False
        try:
            container = self.page.locator('div[class*="--group"][class*="--right"]')
            if container.is_visible():
                sub_element = container.locator('[data-name="OfferSummaryInfoItem"]')
                if sub_element.count() > 0:
                    year_locator = self.page.locator(
                        '//div[@data-name="OfferSummaryInfoItem"][.//p[text()="Год постройки"]]/p[2]'
                    )
                    if year_locator.count() > 0:
                        year_text = year_locator.inner_text().strip()
                        new_building = int(year_text) >= 2020
        except Exception as e:
            print(f"  Ошибка парсинга года: {e}")

# ремонт
        repair = None

        REPAIR_RATING = {
        # Вторичка
        "без ремонта":  0.0,
        "косметический": 0.3,
        "евроремонт":   0.7,
        "дизайнерский": 1.0,
        # Новостройка
        "без отделки":        0.0,
        "предчистовая":       0.3,
        "чистовая":           0.7,
        "чистовая с мебелью": 1.0,
        }
        
        try:
            repair_locator = self.page.locator(
                '//div[@data-name="OfferSummaryInfoItem"][.//p[text()="Ремонт"]]/p[2]'
            )
            if repair_locator.count() > 0:
                repair_text = repair_locator.inner_text().strip().lower()
                repair = next(
                    (score for key, score in REPAIR_RATING.items() if key in repair_text),
                    None
                )
        except Exception as e:
            print(f"  Ошибка парсинга ремонта: {e}")
# Площадь
        square = None
        try:
            sq_locator = self.page.locator('[data-name="ObjectFactoidsItem"]').filter(
                has_text="Общая площадь"
            ).locator('span[style*="letter-spacing"]')
            if sq_locator.count() > 0:
                square = sq_locator.inner_text().replace("\xa0", "").replace("м²", "").strip()
        except Exception as e:
            print(f"  Ошибка парсинга площади: {e}")

        lat, lon = None, None
        try:
            # Ждём пока JS отрисует карту и запишет координаты
            self.page.wait_for_timeout(2000)
            content = self.page.content()
            
            # Пробуем разные паттерны которые использует Циан
            patterns = [
                r'"coordinates":\{"lat":([\d.]+),"lng":([\d.]+)',
                r'"geo":\{"lat":([\d.]+),"lng":([\d.]+)',
                r'"point":\{"lat":([\d.]+),"lng":([\d.]+)',
                r'"location":\{"lat":([\d.]+),"lng":([\d.]+)',
                r'"center":\{"lat":([\d.]+),"lng":([\d.]+)',
                r'"lat":(5[5-6]\.\d+),"lng":(3[7-8]\.\d+)',  # московские координаты
            ]
            for pattern in patterns:
                match = re.search(pattern, content)
                if match:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    break

        except Exception as e:
            print(f"  Ошибка парсинга координат: {e}")


        offer_id = self.extract_id(url)
        photo_s3_uris = self.upload_photos(offer_id)

    
        title_element = self.page.query_selector('[data-name="OfferTitleNew"]')
        if title_element:
            title_text = title_element.inner_text()
            rooms_match = re.search(r'(\d+)-комн', title_text)
            if rooms_match:
                rooms = int(rooms_match.group(1))
            else:
                rooms = None
        else:
            rooms = None

        return {
            "url": url,
            "price": price,
            "description": description,
            "address": adress,
            "new_building": new_building,
            "square": square,
            "repair": repair,
            "lat": lat,
            "lon": lon,
            "rooms": rooms,
            "house_class": house_class,
            "floor": floor,
            "floors_total": floors_total,
            "dist_to_center": haversine(lat, lon, 55.7520, 37.6175) if lat and lon else None,
            "photos": photo_s3_uris,

        }


    def parse(self):
        self.page.get_by_placeholder(
            "Купить квартиру с большой кухней рядом с метро"
        ).type(text=self.keyword, delay=0.1)
        self.page.query_selector('button[data-name="search-submit"]').click()
        self.page.wait_for_selector('[data-name="CardComponent"]', timeout=15000)

        base_search_url = self.page.url
        all_unique_links = set()
        target_count = 5000
        current_page = 1


        while len(all_unique_links) < target_count:
            print(f"Сбор ссылок со страницы {current_page} (собрано: {len(all_unique_links)})")

            page_url = (
                f"{base_search_url}&p={current_page}"
                if "?" in base_search_url
                else f"{base_search_url}?p={current_page}"
            )
            self.page.goto(page_url, wait_until="domcontentloaded")
            self._page_down()
            time.sleep(1)

            elements = self.page.query_selector_all('[data-name="CardComponent"] a[href*="/flat/"]')

            before_count = len(all_unique_links)
            for el in elements:
                href = el.get_attribute('href')
                if href:
                    all_unique_links.add(href.split('?')[0])

            if len(all_unique_links) == before_count or current_page >= 54:
                break

            current_page += 1

        unique_links = list(all_unique_links)[:target_count]
        print(f"Итого зафиксировано для парсинга: {len(unique_links)}")


        for i, url in enumerate(unique_links):
            print(f"[{i+1}/{len(unique_links)}] Парсим: {url}")
            offer_id = self.extract_id(url)
            result = self.parcing_announcement(url)
            if result:
                self.results[offer_id] = result
                if (i + 1) % 10 == 0:
                    self.save_to_json()
            time.sleep(1.5)

        self.save_to_json()


    def run_parser(self, headless: bool = False):
        self._ensure_bucket()
        with sync_playwright() as pw:
            self.playwright = pw
            self.browser = pw.chromium.launch(headless=headless)
            self.page = self.browser.new_page()
            self.page.goto("https://www.cian.ru/", wait_until="domcontentloaded")
            try:
                self.parse()
            finally:
                self.browser.close()


if __name__ == "__main__":
    app = CianParser('Квартира Москва')
    app.run_parser(headless=False)