import calendar
import json
import sqlite3
from datetime import datetime, timezone
from typing import List

import feedparser
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from config import (DB_PATH, NIKTA_BASE_URL, DEFAULT_REQUEST_TIMEOUT_SECONDS, get_logger)
from utils import APIError, retry_on_exception

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
logger = get_logger(__name__)


class NewsFetcher:
    """
    Класс, инкапсулирующий логику сбора, фильтрации и сохранения новостей ТОЛЬКО из RSS.
    """

    def __init__(self):
        self.db_path = DB_PATH
        # Сессия для внешних запросов больше не нужна здесь
        self._setup_database()

    def _setup_database(self):
        # ... (код метода без изменений)
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS news (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        country_code TEXT NOT NULL,
                        title TEXT NOT NULL,
                        summary TEXT,
                        link TEXT NOT NULL UNIQUE,
                        published_dt TIMESTAMP NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                ''')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_published_dt ON news (published_dt);')
                conn.commit()
        except sqlite3.Error as e:
            logger.critical("Критическая ошибка при настройке базы данных", exc_info=True)
            raise


    def _fetch_from_rss(self, country_code: str, feed_urls: List[str], start_dt_utc: datetime,
                        end_dt_utc: datetime) -> List[tuple]:
        news_candidates = []
        for url in feed_urls:
            log_ctx = {'source': 'RSS', 'country_code': country_code, 'url': url}
            try:
                feed = feedparser.parse(url, request_headers={'User-Agent': 'MyNewsBot/1.0'})
                if feed.bozo:
                    bozo_reason = feed.get('bozo_exception', 'Неизвестная ошибка парсинга')
                    logger.warning("Ошибка парсинга RSS-ленты: %s", bozo_reason, extra={'context': log_ctx})
                    continue

                for entry in feed.entries:
                    if not hasattr(entry, 'published_parsed') or not entry.published_parsed:
                        continue

                    pub_dt = datetime.fromtimestamp(calendar.timegm(entry.published_parsed), tz=timezone.utc)

                    if start_dt_utc <= pub_dt <= end_dt_utc:
                        news_candidates.append((
                            country_code, entry.title.strip(),
                            getattr(entry, 'summary', '').strip(), entry.link.strip(), pub_dt
                        ))
            except Exception:
                logger.error("Не удалось обработать RSS-ленту", extra={'context': log_ctx}, exc_info=True)
                continue
        return news_candidates

    def _get_new_unique_news(self, news_candidates: List[tuple], country_code: str) -> List[tuple]:
        log_ctx = {'country_code': country_code}
        if not news_candidates:
            return []
        links_to_check = [item[3] for item in news_candidates]
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                placeholders = ','.join('?' for _ in links_to_check)
                query = f'SELECT link FROM news WHERE link IN ({placeholders})'
                cursor.execute(query, links_to_check)
                existing_links = {row[0] for row in cursor.fetchall()}

            new_news = [item for item in news_candidates if item[3] not in existing_links]
            logger.info(f"Фильтрация дубликатов: {len(news_candidates)} -> {len(new_news)} новых новостей.",
                        extra={'context': log_ctx})
            return new_news
        except sqlite3.Error:
            logger.error("Ошибка при проверке дубликатов в БД. Обработка продолжится без фильтрации.",
                         extra={'context': log_ctx}, exc_info=True)
            return news_candidates

    def _store_news(self, news_to_store: List[tuple], country_code: str) -> int:
        log_ctx = {'country_code': country_code}
        if not news_to_store:
            return 0
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    'INSERT OR IGNORE INTO news (country_code, title, summary, link, published_dt) VALUES (?, ?, ?, ?, ?)',
                    news_to_store
                )
                conn.commit()
                return cursor.rowcount
        except sqlite3.Error:
            logger.error("Ошибка при записи новостей в базу данных.", extra={'context': log_ctx}, exc_info=True)
            return 0

    def fetch_and_process_news(self, country_code: str, rss_links: List[str], start_dt_utc, end_dt_utc) -> str:
        """
        Основной метод: выполняет цикл сбора из RSS, фильтрации и сохранения.
        """
        log_ctx = {'country_code': country_code}
        logger.info("Начинаем сбор новостей из RSS.", extra={'context': log_ctx})

        rss_news = self._fetch_from_rss(country_code, rss_links, start_dt_utc, end_dt_utc)
        logger.info(f"Найдено {len(rss_news)} новостей-кандидатов в RSS.", extra={'context': log_ctx})

        if not rss_news:
            logger.info("Новостей-кандидатов не найдено. Завершаем обработку страны.", extra={'context': log_ctx})
            return ""

        new_unique_news = self._get_new_unique_news(rss_news, country_code)
        if not new_unique_news:
            logger.info("Все найденные новости уже есть в базе.", extra={'context': log_ctx})
            return ""

        added_count = self._store_news(new_unique_news, country_code)
        logger.info(f"Успешно добавлено {added_count} новых записей в БД.", extra={'context': log_ctx})

        report_lines = [
            f"- {title}\n  (Published: {pub_dt.strftime('%Y-%m-%d')})\n  {summary}\n  ({link})\n"
            for _, title, summary, link, pub_dt in new_unique_news
        ]
        return "\n".join(report_lines)


class NiktaAPIClient:
    """Клиент для взаимодействия с Nikta LLM API с retry-логикой."""

    def __init__(self, email: str, password: str):
        self.base_url = NIKTA_BASE_URL
        self._email = email
        self._password = password
        self.session = requests.Session()
        self.session.verify = False
        self.session.timeout = DEFAULT_REQUEST_TIMEOUT_SECONDS

    @retry_on_exception(exceptions=(APIError, requests.RequestException))
    def authenticate(self):
        log_ctx = {'api': 'Nikta', 'operation': 'authenticate'}
        logger.info("Аутентификация...", extra={'context': log_ctx})
        payload = {"email": self._email, "password": self._password}
        try:
            response = self.session.post(f"{self.base_url}/login", json=payload)
            response.raise_for_status()
            token = response.json().get("token")
            if not token:
                raise APIError("Токен не найден в ответе сервера.")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            logger.info("Аутентификация прошла успешно.", extra={'context': log_ctx})
        except requests.RequestException as e:
            raise APIError(f"Сетевая ошибка при аутентификации: {e}")
        except (json.JSONDecodeError, KeyError) as e:
            raise APIError(f"Ошибка парсинга ответа при аутентификации: {e}")

    @retry_on_exception(exceptions=(APIError, requests.RequestException))
    def run_scenario(self, scenario_id: int, message: str, info: dict) -> dict:
        log_ctx = {'api': 'Nikta', 'scenario_id': scenario_id}
        if "Authorization" not in self.session.headers:
            raise APIError("Клиент не аутентифицирован.")

        logger.info("Запуск сценария...", extra={'context': log_ctx})
        payload = {
            "scenario_id": scenario_id, "channel_id": '1', "dialog_id": '1', "user_id": 1,
            "state": {"messages": [{"role": "human", "content": message}], "info": info}
        }
        try:
            response = self.session.post(f"{self.base_url}/run", json=payload)
            response.raise_for_status()
            logger.info("Сценарий успешно выполнен.", extra={'context': log_ctx})
            return response.json()
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Ошибка 401. JWT токен, вероятно, истек. Требуется повторная аутентификация.",
                             extra={'context': log_ctx})
                self.authenticate()
            raise APIError(f"HTTP ошибка: {e.response.status_code} {e.response.text}")
        except requests.RequestException as e:
            raise APIError(f"Сетевая ошибка: {e}")
        except json.JSONDecodeError:
            raise APIError(f"Не удалось декодировать JSON из ответа: {response.text}")