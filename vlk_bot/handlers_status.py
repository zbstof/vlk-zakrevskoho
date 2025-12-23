"""
ConversationHandler для перегляду статусу.
"""

import datetime
import logging
import re

import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from vlk_bot.config import STATUS_GETTING_ID
from vlk_bot.keyboards import MAIN_KEYBOARD, CANCEL_KEYBOARD
from vlk_bot.prediction import calculate_prediction, calculate_date_probability
from vlk_bot.sheets import load_queue_data, get_stats_data
from vlk_bot.utils import get_user_log_info, extract_main_id

logger = logging.getLogger(__name__)


async def status_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запускає процес перегляду статусу."""
    import vlk_bot.config as config_module
    config_module.queue_df = load_queue_data()
    
    if config_module.queue_df is None:
        logger.error(f"Помилка завантаження даних для перегляду статусу користувача {get_user_log_info(update.effective_user)}.")
        await update.message.reply_text(
            "Сталася помилка при завантаженні даних. Будь ласка, спробуйте повторити спробу пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear()
        return ConversationHandler.END
    
    logger.info(f"Користувач {get_user_log_info(update.effective_user)} розпочав перегляд статусу.")
    await update.message.reply_text(
        "Будь ласка, введіть номер зі списку первинної черги, статус якого ви хочете перевірити. "
        "Це може бути ціле число або два цілих числа, розділені слешем (наприклад, `9999` або `9999/1`).",
        parse_mode='Markdown',
        reply_markup=CANCEL_KEYBOARD
    )
    return STATUS_GETTING_ID[0]


async def status_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує ID від користувача та відображає статус."""
    import vlk_bot.config as config_module
    queue_df = config_module.queue_df
    
    id_to_check = update.message.text.strip()
    id_pattern = r"^(\d+|\d+\/\d+)$"

    if not re.match(id_pattern, id_to_check):
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний ID для перевірки статусу: '{id_to_check}'")
        await update.message.reply_text(
            "Невірний формат номеру. Будь ласка, введіть ціле число або два цілих числа, розділені слешем (наприклад, `9999` або `9999/1`).",
            parse_mode='Markdown',
            reply_markup=CANCEL_KEYBOARD
        )
        return STATUS_GETTING_ID[0]

    id_records = queue_df[queue_df['ID'] == id_to_check].copy() 
    
    if id_records.empty:
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} запитав статус для ID '{id_to_check}'.")
        await update.message.reply_text(
            f"Запис з номером `{id_to_check}` не знайдено.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear()
        return ConversationHandler.END

    id_records['Змінено_dt'] = pd.to_datetime(
        id_records['Змінено'].astype(str),
        format="%d.%m.%Y %H:%M:%S",
        dayfirst=True,
        errors='coerce'
    )
    id_records['Змінено_dt'] = id_records['Змінено_dt'].fillna(datetime.datetime(2025, 1, 1, 0, 0, 0))

    latest_record = id_records.sort_values(by='Змінено_dt', ascending=False).iloc[0]
    is_actual_record = (latest_record['Дата'].strip() != '')

    status_message = f"**Статус запису для номеру:** `{latest_record['ID']}`\n"

    if is_actual_record:
        status_message += f"**Дата запису:** `{latest_record['Дата']}`\n"
        status_message += f"**Поточний статус:** `{latest_record['Статус'] if latest_record['Статус'].strip() else 'Невизначений'}`\n"
        
        try:
            stats_df = await get_stats_data()
            if stats_df is not None and not stats_df.empty:
                main_id = extract_main_id(latest_record['ID'])
                prediction = calculate_prediction(main_id, stats_df)
                
                if prediction:
                    record_date = datetime.datetime.strptime(latest_record['Дата'], "%d.%m.%Y").date()
                    dist = prediction['dist']
                    prob = calculate_date_probability(record_date, dist)
                    status_message += f"*Орієнтовна ймовірність зайти в 252 кабінет і розпочати ВЛК:* `{prob:.0f}%`\n"
        except Exception as e:
            logger.error(f"Помилка при розрахунку ймовірності в status_get_id: {e}")

        if latest_record['Попередня дата'].strip():
            status_message += f"**Перенесено з дати:** `{latest_record['Попередня дата']}`\n"
    else:
        status_message += "**Дата:** `скасування запису`\n"
        status_message += f"**Поточний статус:** `{latest_record['Статус'] if latest_record['Статус'].strip() else 'Невизначений'}`\n"
        if latest_record['Попередня дата'].strip():
            status_message += f"**Скасовано запис від:** `{latest_record['Попередня дата']}`\n"

    if latest_record['Статус'].strip().lower() == 'ухвалено':
        status_message += "Вашу заявку ухвалено.\nВона вже або через деякий час з'явиться в жовтій таблиці 🟡TODO."
    elif latest_record['Статус'].strip().lower() == 'на розгляді':
        status_message += "Ваша заявка на розгляді у адміністраторів.\nЯкщо вона буде \"Ухвалена\", то через деякий час з'явиться в жовтій таблиці 🟡TODO."
    else:
        status_message += "Примітка:\nСхоже з вашою заявкою виникли проблеми.\nЗверніться до адміністраторів в групі [ВЛК Закревського 81](https://t.me/vlkzakrevskogo81) за роз'ясненнями."

    logger.info(f"Користувач {get_user_log_info(update.effective_user)} переглянув статус для ID: {id_to_check}.")
    await update.message.reply_text(status_message, parse_mode='Markdown', reply_markup=MAIN_KEYBOARD)
    context.user_data.clear()
    return ConversationHandler.END

