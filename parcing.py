import re
import time
import json
import requests
import boto3
from botocore.exceptions import ClientError
from playwright.sync_api import sync_playwright


class CianParser:

    REPAIR_RATING = {
        "без ремонта": 0.0,
        "требует ремонта": 0.05,
        "в удовлетворительном": 0.1,
        "рабочее состояние": 0.1,
        "косметический": 0.3,
        "сделан косметический": 0.3,
        "стандартный": 0.4,
        "современный": 0.5,
        "хороший": 0.5,
        "черновой": 0.0,
        "чистовая отделка": 0.4,
        "от застройщика": 0.4,
        "евро": 0.7,
        "евроремонт": 0.7,
        "евростандарт": 0.7,
        "под ключ": 0.6,
        "качественный": 0.6,
        "хороший ремонт": 0.6,
        "дизайнерский": 0.85,
        "премиум": 0.85,
        "авторский": 0.9,
        "элитный": 0.9,
        "идеальный": 1.0,
        "шоу-рум": 1.0,
        "с элементами отделки": 0.2,
        "частичный": 0.15,
    }

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

    def parcing_announcement(self, url: str):
        try:
            self.page.goto(url, timeout=20000, wait_until="domcontentloaded")
            self.page.wait_for_selector('[data-testid="price-amount"]', timeout=7000)
            price_text = self.page.query_selector('[data-testid="price-amount"]').inner_text()
            price = price_text.replace("\xa0", "").replace(" ", "").strip()
        except Exception:
            price = None

        description = None
        try:
            el = self.page.query_selector('[data-id="content"]')
            description = el.inner_text() if el else None
        except Exception:
            pass

        adress = None
        try:
            el = self.page.query_selector('[data-name="AddressContainer"]')
            adress = el.inner_text() if el else None
        except Exception:
            pass

        station = None
        try:
            el = self.page.query_selector('[rel="noreferrer"]')
            station = el.inner_text() if el else None
        except Exception:
            pass

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

        repair = None
        try:
            repair_locator = self.page.locator(
                '//div[@data-name="OfferSummaryInfoItem"][.//p[text()="Ремонт"]]/p[2]'
            )
            if repair_locator.count() > 0:
                repair_text = repair_locator.inner_text().strip().lower()
                repair = next(
                    (score for key, score in self.REPAIR_RATING.items() if key in repair_text),
                    None
                )
        except Exception as e:
            print(f"  Ошибка парсинга ремонта: {e}")

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

        return {
            "url": url,
            "price": price,
            "description": description,
            "address": adress,
            "station": station,
            "new_building": new_building,
            "square": square,
            "repair": repair,
            "lat": lat,
            "lon": lon,
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
        target_count = 1

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
            time.sleep(1.5)

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