"""
ConversationHandler для відображення черги.
"""

import datetime
import logging
import re

import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from vlk_bot.config import SHOW_GETTING_OPTION, SHOW_GETTING_DATE, days_ahead
from vlk_bot.formatters import display_queue_data
from vlk_bot.keyboards import (
    MAIN_KEYBOARD, SHOW_OPTION_KEYBOARD, date_keyboard,
    BUTTON_TEXT_SHOW_ALL, BUTTON_TEXT_SHOW_DATE
)
from vlk_bot.sheets import load_queue_data
from vlk_bot.utils import get_user_log_info

logger = logging.getLogger(__name__)


async def show_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запускає процес відображення черги."""
    import vlk_bot.config as config_module
    config_module.queue_df = load_queue_data()
    
    if config_module.queue_df is None:
        logger.error(f"Помилка завантаження даних для перегляду черги користувача {get_user_log_info(update.effective_user)}.")
        await update.message.reply_text(
            "Сталася помилка при завантаженні даних. Будь ласка, спробуйте повторити спробу пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear()
        return ConversationHandler.END
    
    logger.info(f"Користувач {get_user_log_info(update.effective_user)} розпочав перегляд черги.")
    await update.message.reply_text(
        "Як ви хочете переглянути записи?",
        reply_markup=SHOW_OPTION_KEYBOARD
    )
    return SHOW_GETTING_OPTION


async def show_get_option(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує опцію відображення."""
    import vlk_bot.config as config_module
    queue_df = config_module.queue_df
    
    choice = update.message.text.strip()

    if choice == BUTTON_TEXT_SHOW_ALL:
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} обрав перегляд усіх записів.")
        await display_queue_data(update, queue_df, title="Усі записи в черзі зі статусом \"Ухвалено\":", reply_markup=MAIN_KEYBOARD)
        context.user_data.clear()
        return ConversationHandler.END
    elif choice == BUTTON_TEXT_SHOW_DATE:
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} обрав перегляд записів на конкретну дату.")

        today = datetime.date.today()
        DATE_KEYBOARD = date_keyboard(today, 0, days_ahead)

        await update.message.reply_text(
            "Будь ласка, введіть дату, на яку ви хочете переглянути записи, у форматі `ДД.ММ.РРРР`.\n"
            f"Ви можете обрати дату зі списку на {days_ahead} днів або ввести з клавіатури.",
            parse_mode='Markdown',
            reply_markup=DATE_KEYBOARD
        )
        return SHOW_GETTING_DATE
    else:
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів невідому опцію перегляду: '{choice}'")
        await update.message.reply_text(
            "Невірна опція. Будь ласка, оберіть `Показати всі записи` або `Показати записи на конкретну дату`, або скасуйте дію.",
            parse_mode='Markdown',
            reply_markup=SHOW_OPTION_KEYBOARD
        )
        return SHOW_GETTING_OPTION


async def show_get_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує дату для відображення записів."""
    import vlk_bot.config as config_module
    queue_df = config_module.queue_df
    
    date_input = update.message.text.strip()
    
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{2,4})', date_input)
    if match:
        date_text = match.group(0)
        try:
            if len(match.group(3)) == 2:
                 chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%y").date()
            else:
                 chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%Y").date()
        except ValueError:
             chosen_date = None
    else:
        date_text = date_input
        chosen_date = None

    try:
        if not chosen_date:
            chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%Y").date()
    except ValueError:
        try:
            chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%y").date()
        except ValueError:
            logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний формат дати для перегляду: '{date_input}'")
            today = datetime.date.today()
            DATE_KEYBOARD = date_keyboard(today, 0, days_ahead)
            await update.message.reply_html(
                "Невірний формат дати. Будь ласка, введіть дату у форматі <code>ДД.ММ.РРРР</code> (наприклад, 25.12.2025) або скасуйте дію.",
                reply_markup=DATE_KEYBOARD
            )
            return SHOW_GETTING_DATE

    try:
        current_date_obj = datetime.date.today()
        
        if chosen_date < current_date_obj:
            DATE_KEYBOARD = date_keyboard(current_date_obj, 0, days_ahead)
            logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів дату ранішу за поточну: '{chosen_date.strftime('%d.%m.%Y')}'")
            await update.message.reply_text(
                f"Дата повинна бути не раніше за поточну (`{current_date_obj.strftime('%d.%m.%Y')}`). Будь ласка, спробуйте ще раз або скасуйте дію.",
                parse_mode='Markdown',
                reply_markup=DATE_KEYBOARD
            )
            return SHOW_GETTING_DATE
        
        if chosen_date.weekday() >= 5:
            today = datetime.date.today()
            DATE_KEYBOARD = date_keyboard(today, 0, days_ahead)
            logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів дату що припадає на вихідний: '{chosen_date}'")
            await update.message.reply_text(
                "Ви обрали вихідний день. Записи на вихідні дні не створюються. Будь ласка, оберіть робочий день або скасуйте дію.",
                reply_markup=DATE_KEYBOARD
            )
            return SHOW_GETTING_DATE

        temp_df = queue_df.copy()
        temp_df['Змінено_dt'] = pd.to_datetime(temp_df['Змінено'].astype(str), format="%d.%m.%Y %H:%M:%S", dayfirst=True, errors='coerce')
        temp_df['Змінено_dt'] = temp_df['Змінено_dt'].fillna("01.01.2025 00:00:00")
        actual_records = temp_df.sort_values(by=['ID', 'Змінено_dt'], ascending=[True, True]).drop_duplicates(subset='ID', keep='last')
        actual_queue = actual_records[actual_records['Дата'].astype(str).str.strip() != '']
        
        filtered_df = actual_queue[
            (actual_queue['Дата'] == chosen_date.strftime("%d.%m.%Y")) &
            (actual_queue['Статус'].astype(str).str.strip().str.lower() == 'ухвалено')
        ]
        
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} переглянув записи на дату: {chosen_date.strftime('%d.%m.%Y')}")
        next_working_day = current_date_obj + datetime.timedelta(days=1)
        while next_working_day.weekday() >= 5:
            next_working_day += datetime.timedelta(days=1)
        
        await display_queue_data(update, filtered_df, title=f"Поточна черга зі статусом \"Ухвалено\" на `{chosen_date.strftime('%d.%m.%Y')}`:\n", reply_markup=MAIN_KEYBOARD)
        context.user_data.clear()
        return ConversationHandler.END

    except ValueError:
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний формат дати для перегляду: '{date_input}'")
        today = datetime.date.today()
        DATE_KEYBOARD = date_keyboard(today, 0, days_ahead)
        await update.message.reply_html(
            "Невірний формат дати. Будь ласка, введіть дату у форматі <code>ДД.ММ.РРРР</code> (наприклад, 25.12.2025) або скасуйте дію.",
            reply_markup=DATE_KEYBOARD
        )
        return SHOW_GETTING_DATE

