import asyncio
from datetime import time, timezone

from telegram import Update, constants
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler, # Не используется напрямую, но JobQueue его использует под капотом
)

import config
from main import main_cycle

# Настройка логирования
config.setup_logging()
logger = config.get_logger(__name__)

# Глобальная блокировка для предотвращения одновременного запуска нескольких циклов обработки
processing_lock = asyncio.Lock()


async def run_processing_job(context: ContextTypes.DEFAULT_TYPE, chat_id: int, trigger_type: str):
    """
    Универсальная функция для запуска одного цикла обработки новостей.
    Она управляет блокировкой, чтобы избежать параллельных запусков.

    :param context: Контекст бота.
    :param chat_id: ID чата для отправки сообщений.
    :param trigger_type: Тип запуска ('manual' или 'scheduled') для логирования.
    """
    if processing_lock.locked():
        logger.warning(
            "Попытка запуска обработки (%s), когда она уже активна. Запуск отменен.",
            trigger_type
        )
        # Формируем сообщение в зависимости от типа запуска
        if trigger_type == 'manual':
            message_text = "⏳ Обработка уже запущена. Ваш ручной запуск отменен. Дождитесь завершения."
        else: # 'scheduled'
            message_text = f"⚠️ Обработка уже была активна, когда подошло время для запуска по расписанию ({context.job.name}). Запланированный запуск пропущен."

        # Отправляем сообщение в любом случае
        await context.bot.send_message(
            chat_id=chat_id,
            text=message_text
        )
        return

    async with processing_lock:
        logger.info("Блокировка установлена. Триггер: %s.", trigger_type)
        proc_msg = None
        try:
            proc_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🚀 Начинаю обработку новостей... (Запуск: {trigger_type})"
            )
            logger.info("Запуск main_cycle для чата %d", chat_id)

            # Запускаем тяжелую, синхронную функцию в отдельном потоке
            final_digest_status = await asyncio.to_thread(main_cycle)

            if final_digest_status == "Digest sent":
                logger.info("Дайджест или статусное сообщение отправлено в чат %d.", chat_id)
            elif final_digest_status == 'No news':
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"No news for the past period"
                )
                logger.info("Сообщение о том, что новых новостей не найдено отправлено в чат %d.", chat_id)
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"Произошла ошибка при обработке дайджеста, {final_digest_status}"
                )
                logger.info("Новых новостей не найдено, цикл завершен.")

        except Exception as e:
            logger.critical("Критическая ошибка в run_processing_job", exc_info=True)
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"❌ Произошла критическая ошибка при выполнении: `{e}`",
                    parse_mode=constants.ParseMode.MARKDOWN_V2
                )
            except Exception as send_e:
                logger.error("Не удалось даже отправить сообщение об ошибке: %s", send_e)

        finally:
            if proc_msg:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=proc_msg.message_id)
                    logger.info("Сообщение о старте обработки удалено.")
                except Exception:
                    logger.warning("Не удалось удалить сообщение о старте.")
            logger.info("Блокировка снята.")


async def start_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает команду /start, запущенную вручную в целевом канале.
    """
    chat = update.effective_chat
    if chat.id != config.TELEGRAM_CHAT_ID:
        logger.warning("Получена команда /start в непредусмотренном чате ID: %d", chat.id)
        return

    logger.info("Получена команда /start в чате %d", chat.id)
    # Запускаем задачу в фоне, чтобы бот мог продолжать отвечать на другие команды
    asyncio.create_task(run_processing_job(context, chat.id, trigger_type='manual'))


async def scheduled_run(context: ContextTypes.DEFAULT_TYPE):
    """
    Функция-обертка для запуска по расписанию через JobQueue.
    """
    job = context.job
    chat_id = job.chat_id
    logger.info("Сработал запланированный запуск (job: %s)", job.name)
    await run_processing_job(context, chat_id, trigger_type='scheduled')


def main():
    """Основная функция запуска бота и настройки расписания."""
    logger.info("--- Запуск бота ---")

    # Валидация базовой конфигурации перед запуском
    try:
        config.load_feed_sources()
    except config.ConfigError as e:
        logger.critical("Критическая ошибка конфигурации, бот не может быть запущен: %s", e)
        return

    if not config.TELEGRAM_BOT_TOKEN:
        logger.critical("Не указан TELEGRAM_BOT_TOKEN. Бот не может быть запущен.")
        return
    if not config.TELEGRAM_CHAT_ID:
        logger.critical("Не указан TELEGRAM_CHAT_ID. Бот не может быть запущен.")
        return


    app = ApplicationBuilder().token(config.TELEGRAM_BOT_TOKEN).build()

    # 1. Добавляем обработчик ручного запуска
    app.add_handler(
        MessageHandler(
            filters=filters.ChatType.CHANNEL & filters.Command("start"),
            callback=start_command_handler
        )
    )

    # 2. Настраиваем и запускаем задачи по расписанию
    schedule_times = config.load_schedule()
    if schedule_times:
        job_queue = app.job_queue
        for t_str in schedule_times:
            hour, minute = map(int, t_str.split(':'))
            run_time = time(hour, minute, tzinfo=timezone.utc)
            job_queue.run_daily(
                callback=scheduled_run,
                time=run_time,
                chat_id=config.TELEGRAM_CHAT_ID,
                name=f"Daily digest at {t_str} UTC"
            )
        logger.info("Всего задач по расписанию добавлено: %d", len(schedule_times))
    else:
        logger.info("Задачи по расписанию не добавлены.")


    logger.info("Бот запущен и готов к работе.")
    app.run_polling()


if __name__ == "__main__":
    main()