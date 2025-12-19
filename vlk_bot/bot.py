"""
Головний модуль бота: ініціалізація, обробники, main().
"""

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    CallbackQueryHandler,
)

from vlk_bot.config import (
    initialize_bot, JOIN_GETTING_ID, JOIN_GETTING_DATE,
    CANCEL_GETTING_ID,
    SHOW_GETTING_OPTION, SHOW_GETTING_DATE,
    STATUS_GETTING_ID,
    POLL_CONFIRM, POLL_RESCHEDULE, POLL_CANCEL, POLL_DATE,
    POLL_CANCEL_CONFIRM, POLL_CANCEL_ABORT, POLL_CANCEL_RESCHEDULE,
)
from vlk_bot.handlers_admin import (
    perform_queue_cleanup,
    open_sheet_command, grant_admin, drop_admin, ban, unban, test_poll,
    run_cleanup_command, run_notify_command, run_reminder_command,
    run_check_sheet_command, run_poll_command, show_environment_command
)
from vlk_bot.handlers_cancel import cancel_record_start, cancel_record_get_id
from vlk_bot.handlers_common import (
    start, help_command, prediction_command,
    cancel_conversation, fallback, error_handler
)
from vlk_bot.handlers_join import join_start, join_get_id, join_get_date
from vlk_bot.handlers_poll import handle_poll_response, handle_poll_cancel_actions, handle_poll_date_selection, \
    handle_poll_custom_date
from vlk_bot.handlers_show import show_start, show_get_option, show_get_date
from vlk_bot.handlers_status import status_start, status_get_id
from vlk_bot.keyboards import BUTTON_TEXT_JOIN, BUTTON_TEXT_SHOW, BUTTON_TEXT_CANCEL_RECORD, BUTTON_TEXT_PREDICTION, \
    BUTTON_TEXT_CANCEL_OP, BUTTON_TEXT_STATUS
from vlk_bot.scheduler import notify_status, date_reminder, check_new_daily_sheet

logger = logging.getLogger(__name__)


def main() -> None:
    """Головна функція для запуску бота."""
    initialize_bot()
    
    from vlk_bot.config import TOKEN, ENVIRONMENT
    
    application = Application.builder().token(TOKEN).build()

    join_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(f"^{BUTTON_TEXT_JOIN}$"), join_start)],
        states={
            JOIN_GETTING_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(f"^{BUTTON_TEXT_CANCEL_OP}$"), join_get_id)
            ],
            JOIN_GETTING_DATE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(f"^{BUTTON_TEXT_CANCEL_OP}$"), join_get_date)
            ],
        },
        fallbacks=[
            MessageHandler(filters.Regex(f"^{BUTTON_TEXT_CANCEL_OP}$"), cancel_conversation),
            CommandHandler("cancel", cancel_conversation),
        ],
    )

    cancel_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(f"^{BUTTON_TEXT_CANCEL_RECORD}$"), cancel_record_start)],
        states={
            CANCEL_GETTING_ID[0]: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(f"^{BUTTON_TEXT_CANCEL_OP}$"), cancel_record_get_id)
            ],
        },
        fallbacks=[
            MessageHandler(filters.Regex(f"^{BUTTON_TEXT_CANCEL_OP}$"), cancel_conversation),
            CommandHandler("cancel", cancel_conversation),
        ],
    )

    show_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(f"^{BUTTON_TEXT_SHOW}$"), show_start)],
        states={
            SHOW_GETTING_OPTION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(f"^{BUTTON_TEXT_CANCEL_OP}$"), show_get_option)
            ],
            SHOW_GETTING_DATE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(f"^{BUTTON_TEXT_CANCEL_OP}$"), show_get_date)
            ],
        },
        fallbacks=[
            MessageHandler(filters.Regex(f"^{BUTTON_TEXT_CANCEL_OP}$"), cancel_conversation),
            CommandHandler("cancel", cancel_conversation),
        ],
    )

    status_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(f"^{BUTTON_TEXT_STATUS}$"), status_start)],
        states={
            STATUS_GETTING_ID[0]: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(f"^{BUTTON_TEXT_CANCEL_OP}$"), status_get_id)
            ],
        },
        fallbacks=[
            MessageHandler(filters.Regex(f"^{BUTTON_TEXT_CANCEL_OP}$"), cancel_conversation),
            CommandHandler("cancel", cancel_conversation),
        ],
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("sheet", open_sheet_command))
    application.add_handler(CommandHandler("grant_admin", grant_admin))
    application.add_handler(CommandHandler("drop_admin", drop_admin))
    application.add_handler(CommandHandler("ban", ban))
    application.add_handler(CommandHandler("unban", unban))
    application.add_handler(CommandHandler("test_poll", test_poll))
    application.add_handler(CommandHandler("run_cleanup", run_cleanup_command))
    application.add_handler(CommandHandler("run_notify", run_notify_command))
    application.add_handler(CommandHandler("run_reminder", run_reminder_command))
    application.add_handler(CommandHandler("run_check_sheet", run_check_sheet_command))
    application.add_handler(CommandHandler("run_poll", run_poll_command))
    application.add_handler(CommandHandler("env", show_environment_command))

    application.add_handler(join_conv_handler)
    application.add_handler(cancel_conv_handler)
    application.add_handler(show_conv_handler)
    application.add_handler(status_conv_handler)

    application.add_handler(MessageHandler(filters.Regex(f"^{BUTTON_TEXT_PREDICTION}$"), prediction_command))

    application.add_handler(CallbackQueryHandler(handle_poll_cancel_actions, pattern=f"^({POLL_CANCEL_CONFIRM}|{POLL_CANCEL_ABORT}|{POLL_CANCEL_RESCHEDULE})_"))
    application.add_handler(CallbackQueryHandler(handle_poll_response, pattern=f"^({POLL_CONFIRM}|{POLL_RESCHEDULE}|{POLL_CANCEL})_"))
    application.add_handler(CallbackQueryHandler(handle_poll_date_selection, pattern=f"^{POLL_DATE}_"))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_poll_custom_date), group=-1)
    
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fallback))
    application.add_error_handler(error_handler)

    if ENVIRONMENT != "test":
        kyiv_tz = timezone('Europe/Kyiv')
        scheduler = AsyncIOScheduler(timezone=kyiv_tz)
        
        scheduler.add_job(
            notify_status,
            'cron',
            hour='8-17',
            minute='*/5',
            args=[application],
            id='notify_status',
            replace_existing=True
        )
        
        scheduler.add_job(
            date_reminder,
            'cron',
            hour=8,
            minute=0,
            args=[application],
            id='date_reminder',
            replace_existing=True
        )
        
        scheduler.add_job(
            perform_queue_cleanup,
            'cron',
            hour=1,
            minute=0,
            args=["Нічне очищення"],
            id='queue_cleanup',
            replace_existing=True
        )
        
        scheduler.add_job(
            check_new_daily_sheet,
            'cron',
            hour='8-17',
            minute='*/5',
            args=[application],
            id='check_new_daily_sheet',
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("Заплановані завдання увімкнено")
    else:
        logger.info("Тестовий режим: заплановані завдання вимкнено")

    logger.info("Бот запущено")
    application.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    main()

