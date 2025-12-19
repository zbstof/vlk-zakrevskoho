"""
Спільні обробники: start, help, cancel, fallback, error.
"""

import logging
import os

from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from vlk_bot.keyboards import MAIN_KEYBOARD
from vlk_bot.utils import get_user_log_info, is_admin

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INFOGRAPHIC_PATH = os.path.join(PROJECT_ROOT, 'infographic.jpg')


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробник команди /start."""
    user = update.effective_user
    logger.info(f"Користувач {get_user_log_info(user)} розпочав розмову.")
    
    caption_text = (
        f"Вітаю, {user.mention_html()}\n"
        "Я бот для запису в електронну чергу ВЛК на Закревського, 81/1\n"
        "1. Ознайомтеся з інфографікою 👆\n"
        "2. Оберайте потрібну команду за допомогою кнопок:\n"
        "* <code>Записатися / Перенести</code> - записатися або перенести дату відвідання\n"
        "* <code>Скасувати запис</code> - скасувати свій запис\n"
        "* <code>Переглянути чергу</code> - переглянути поточну чергу повністю або на обраний день\n"
        "* <code>Прогноз черги</code> - графік ймовірності проходження черги\n"
        "* <code>Відкрити таблицю</code> - перейти до таблиці Google Sheets з даними черги (тільки для адміністраторів)\n"
        "* <code>Скасувати ввід</code> - скасувати ввід під час діалогу"
    )

    try:
        with open(INFOGRAPHIC_PATH, 'rb') as photo:
            await update.message.reply_photo(
                photo=photo,
                caption=caption_text,
                parse_mode='HTML',
                reply_markup=MAIN_KEYBOARD
            )
    except Exception as e:
        logger.error(f"Не вдалося надіслати фото (infographic.jpg): {e}")
        await update.message.reply_html(
            caption_text,
            reply_markup=MAIN_KEYBOARD,
        )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показує список доступних команд."""
    user = update.effective_user
    
    user_commands = (
        "<b>Команди для всіх користувачів:</b>\n"
        "/start - Почати роботу з ботом\n"
        "/help - Показати цю довідку\n\n"
        "<b>Основні дії (кнопки):</b>\n"
        "<code>Записатися / Перенести</code> - записатися або перенести дату\n"
        "<code>Скасувати запис</code> - скасувати свій запис\n"
        "<code>Переглянути статус</code> - статус вашої заявки\n"
        "<code>Переглянути чергу</code> - переглянути чергу\n"
        "<code>Прогноз черги</code> - графік ймовірності\n"
        "<code>Скасувати ввід</code> - скасувати поточну дію"
    )
    
    admin_commands = ""
    if is_admin(user.id):
        admin_commands = (
            "\n\n<b>Команди адміністратора:</b>\n"
            "/env - Показати оточення та команди запуску\n"
            "/run_cleanup - Запустити очищення черги\n"
            "/run_notify - Запустити перевірку статусів\n"
            "/run_reminder - Запустити нагадування\n"
            "/run_check_sheet - Перевірити новий аркуш\n"
            "/run_poll - Надіслати опитування\n"
            "/test_poll [ID] - Тестове опитування\n"
            "/grant_admin ID - Додати адміністратора\n"
            "/drop_admin ID - Видалити адміністратора\n"
            "/ban ID - Заблокувати користувача\n"
            "/unban ID - Розблокувати користувача\n"
            "/sheet - Посилання на Google Sheets"
        )
    
    await update.message.reply_text(
        user_commands + admin_commands,
        parse_mode="HTML",
        reply_markup=MAIN_KEYBOARD
    )


async def prediction_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Відправляє користувачу посилання на сайт з прогнозом."""
    site_url = "https://zbstof.github.io/vlk-zakrevskoho/"
    await update.message.reply_text(
        f"Графік прогнозу черги доступний за посиланням:\n{site_url}",
        reply_markup=MAIN_KEYBOARD
    )


async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обробляє команду скасування діалогу."""
    user = update.effective_user
    logger.info(f"Користувач {get_user_log_info(user)} скасував діалог.")
    context.user_data.clear()
    await update.message.reply_text(
        "Дію скасовано. Ви можете обрати нову команду.",
        reply_markup=MAIN_KEYBOARD
    )
    return ConversationHandler.END


async def fallback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробляє невідомі команди."""
    await update.message.reply_text(
        "Не розумію цю команду. Будь ласка, використовуйте кнопки або /help.",
        reply_markup=MAIN_KEYBOARD
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробляє помилки."""
    from telegram.error import NetworkError
    from httpx import ConnectError, RemoteProtocolError
    
    error = context.error
    error_message = str(error)
    
    # Мережеві помилки - логуємо без повного traceback
    if isinstance(error, (NetworkError, ConnectError, RemoteProtocolError)):
        logger.critical(f"Мережева помилка: {error_message}")
        return
    
    # Перевіряємо чи це обгорнута мережева помилка
    if error.__cause__ and isinstance(error.__cause__, (NetworkError, ConnectError, RemoteProtocolError)):
        logger.critical(f"Мережева помилка: {error_message}")
        return
    
    # Інші помилки - логуємо з повним traceback
    logger.error(f"Помилка: {error_message}", exc_info=error)
    
    if update and hasattr(update, 'effective_message') and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "Виникла помилка. Спробуйте ще раз пізніше.",
                reply_markup=MAIN_KEYBOARD
            )
        except Exception:
            pass

