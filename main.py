import sqlite3
from datetime import datetime, timedelta, timezone

from config import (ConfigError, NIKTA_USER_EMAIL, NIKTA_USER_PASSWORD,
                    get_logger, load_feed_sources, NIKTA_FILTER_SCENARIO_ID,
                    NIKTA_FINAL_REPORT_SCENARIO_ID, N_HOURS)
from services import NiktaAPIClient, NewsFetcher
from utils import APIError

# Настройка адаптеров для корректной работы SQLite с aware datetime
sqlite3.register_adapter(datetime, lambda ts: ts.isoformat())
sqlite3.register_converter("timestamp", lambda val: datetime.fromisoformat(val.decode()))

logger = get_logger(__name__)


def main_cycle() -> str:
    """
    Основная функция, выполняющая один полный цикл обработки.
    Возвращает строку (final_digest_status)
    - final_digest_status: Сообщение со статусом.
    """
    logger.info("--- Начало нового цикла обработки ---")

    # Шаг 1: Загрузка и валидация конфигурации
    try:
        feed_sources = load_feed_sources()
    except ConfigError as e:
        logger.critical("Критическая ошибка конфигурации: %s.", e, exc_info=True)
        error_msg = f"❌ **Ошибка конфигурации:**\n`{e}`\n\nОбработка не может быть запущена."
        return error_msg

    if not feed_sources:
        logger.warning("Источники новостей не настроены. Пропуск цикла.")
        status_msg = "⚠️ **Источники новостей (RSS) не настроены.**\nДобавьте их в `config.xlsx` и попробуйте снова."
        return status_msg

    # Шаг 2: Инициализация клиентов
    try:
        nikta_client = NiktaAPIClient(NIKTA_USER_EMAIL, NIKTA_USER_PASSWORD)
        nikta_client.authenticate()
        news_fetcher = NewsFetcher()
    except (APIError, sqlite3.Error) as e:
        logger.critical("Не удалось инициализировать клиенты или БД: %s.", e, exc_info=True)
        error_msg = f"❌ **Критическая ошибка инициализации:**\n`{e}`\n\nПроверьте доступность API и БД."
        return error_msg

    # Шаг 3: Сбор и обработка новостей по странам
    end_dt_utc = datetime.now(timezone.utc)
    start_dt_utc = end_dt_utc - timedelta(hours=N_HOURS)
    logger.info("Временное окно для поиска: %d часов (с %s по %s)", N_HOURS, start_dt_utc.isoformat(),
                end_dt_utc.isoformat())

    filtered_reports = {}
    total_nikta_tokens = 0
    total_nikta_price = 0.0

    for country, rss_links in feed_sources.items():
        log_ctx = {'context': {'country_code': country}}
        try:
            report_text = news_fetcher.fetch_and_process_news(
                country_code=country, rss_links=rss_links,
                start_dt_utc=start_dt_utc, end_dt_utc=end_dt_utc
            )

            if not report_text:
                logger.info("Нет новых новостей для обработки Nikta.", extra=log_ctx)
                continue

            result = nikta_client.run_scenario(NIKTA_FILTER_SCENARIO_ID, report_text, {})
            nikta_tokens = result.get('tokens', 0)
            nikta_price = result.get('logs', {}).get('total_price', 0.0)

            total_nikta_tokens += nikta_tokens
            total_nikta_price += nikta_price

            filtered_reports[country] = result.get('result', "")
            logger.info(
                "Новости отфильтрованы. Потрачено токенов Nikta: %d, цена: $%.4f",
                nikta_tokens, nikta_price, extra=log_ctx
            )

        except APIError as e:
            logger.error("Ошибка API при обработке страны: %s", e, extra=log_ctx)
        except Exception:
            logger.error("Непредвиденная ошибка при обработке страны.", extra=log_ctx, exc_info=True)

    # Шаг 4: Формирование итогового отчета
    if not filtered_reports:
        logger.info("Не найдено новостей для итогового отчета. Завершение.")
        logger.info("--- Цикл обработки завершен ---")
        return "No news"

    final_report_text = "\n\n".join(
        [f"News for {country}:\n{report}" for country, report in filtered_reports.items() if report.strip()]
    )

    final_digest_status = ""

    if not final_report_text:
        logger.warning("Все отчеты по странам оказались пустыми после фильтрации. Итоговый дайджест не будет создан.")
    else:
        logger.info("Все отчеты собраны. Отправка для создания итогового дайджеста в Nikta...")
        try:
            result = nikta_client.run_scenario(NIKTA_FINAL_REPORT_SCENARIO_ID, final_report_text, {})
            final_tokens = result.get('tokens', 0)
            final_price = result.get('logs', {}).get('total_price', 0.0)
            total_nikta_tokens += final_tokens
            total_nikta_price += final_price

            final_digest_status = result.get('result', "")

            logger.info(
                "Итоговый дайджест успешно создан. Потрачено токенов Nikta: %d, цена: $%.4f",
                final_tokens, final_price
            )
        except APIError as e:
            logger.error("Не удалось создать итоговый дайджест через Nikta.", exc_info=True)
            final_digest_status = f"\n\n⚠️ Не удалось сгенерировать финальный дайджест: `{e}`"

    nikta_stats = f"Nikta (всего): {total_nikta_tokens} токенов, ${total_nikta_price:.5f}"
    logger.info("Итоговая статистика цикла: %s", nikta_stats)

    logger.info("--- Цикл обработки успешно завершен ---")

    return final_digest_status