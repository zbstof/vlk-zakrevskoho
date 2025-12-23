"""
ConversationHandler для скасування запису.
"""

import datetime
import logging
import re

import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from vlk_bot.config import CANCEL_GETTING_ID
from vlk_bot.keyboards import MAIN_KEYBOARD, CANCEL_KEYBOARD
from vlk_bot.sheets import load_queue_data, save_queue_data
from vlk_bot.utils import get_user_log_info, get_user_telegram_data, is_banned, send_group_notification

logger = logging.getLogger(__name__)


async def cancel_record_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запускає процес скасування запису."""
    if is_banned(update.effective_user.id):
        logger.warning(f"Заблокований користувач {get_user_log_info(update.effective_user)} намагався скасувати запис.")
        await update.message.reply_text(
            "Ваш обліковий запис заблоковано. Зверніться до адміністраторів щоб розблокувати.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear()
        return ConversationHandler.END
    
    import vlk_bot.config as config_module
    config_module.queue_df = load_queue_data()

    if config_module.queue_df is None:
        logger.error(f"Помилка завантаження даних для скасування запису користувача {get_user_log_info(update.effective_user)}.")
        await update.message.reply_text(
            "Сталася помилка при завантаженні даних. Будь ласка, спробуйте повторити спробу пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear()
        return ConversationHandler.END
    
    logger.info(f"Користувач {get_user_log_info(update.effective_user)} розпочав скасування запису.")
    context.user_data['telegram_user_data'] = get_user_telegram_data(update.effective_user)

    await update.message.reply_text(
        "Будь ласка, введіть номер зі списку первинної черзи для запису, який ви хочете скасувати. "
        "Це може бути ціле число або два цілих числа, розділені слешем (наприклад, `9999` або `9999/1`).",
        parse_mode='Markdown',
        reply_markup=CANCEL_KEYBOARD
    )
    return CANCEL_GETTING_ID[0]


async def cancel_record_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує ID для скасування."""
    import vlk_bot.config as config_module
    queue_df = config_module.queue_df
    
    id_to_cancel = update.message.text.strip()
    telegram_user_data = context.user_data.get('telegram_user_data')

    id_pattern = r"^(\d+|\d+\/\d+)$"
    
    if not re.match(id_pattern, id_to_cancel):
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний ID для скасування: '{id_to_cancel}'")
        await update.message.reply_text(
            "Невірний формат номеру. Будь ласка, введіть ціле число або два цілих числа, розділені слешем (наприклад, `9999` або `9999/1`).",
            parse_mode='Markdown',
            reply_markup=CANCEL_KEYBOARD
        )
        return CANCEL_GETTING_ID[0]

    temp_df_for_prev = queue_df.copy()
    temp_df_for_prev['Змінено_dt'] = pd.to_datetime(temp_df_for_prev['Змінено'].astype(str), format="%d.%m.%Y %H:%M:%S", dayfirst=True, errors='coerce').fillna("01.01.2025 00:00:00")

    last_record_for_id = temp_df_for_prev[temp_df_for_prev['ID'] == id_to_cancel].sort_values(by='Змінено_dt', ascending=False)
    
    if (not last_record_for_id.empty and last_record_for_id.iloc[0]['Дата'] != '') or (not last_record_for_id.empty and last_record_for_id.iloc[0]['Дата'] == '' and last_record_for_id.iloc[0]['Статус'] == 'Відхилено'):
        previous_date = last_record_for_id.iloc[0]['Дата']
        
        new_entry = {
            'ID': id_to_cancel,
            'Дата': '',
            'Примітки': '',
            'Статус': 'На розгляді',
            'Змінено': datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
            'Попередня дата': previous_date,
            **telegram_user_data
        }
        
        new_entry_df = pd.DataFrame([new_entry])
        if save_queue_data(new_entry_df):
            config_module.queue_df = pd.concat([queue_df, new_entry_df], ignore_index=True)
            logger.info(f"Запис з ID '{id_to_cancel}' на `{previous_date}` успішно скасовано користувачем {get_user_log_info(update.effective_user)}.")
            notification_text = f"❎ Користувач {update.effective_user.mention_html()} скасував запис для\nID <code>{id_to_cancel}</code> на <code>{previous_date}</code>" 
            await send_group_notification(context, notification_text)
            await update.message.reply_text(
                f"Ви успішно створили заявку на скасування дати в черзі!\nВаш ID: `{id_to_cancel}` попередній запис на `{previous_date}`\nСтатус заявки: `На розгляді`\nВаша заявка на розгляді у адміністраторів.\nЯкщо вона буде \"Ухвалена\", то через деякий час зникне з жовтої таблиці 🟡TODO.",
                parse_mode='Markdown',
                reply_markup=MAIN_KEYBOARD
            )
        else:
            logger.error(f"Не вдалося зберегти скасування запису для ID '{id_to_cancel}' користувачем {get_user_log_info(update.effective_user)}.")
            await update.message.reply_text(
                "Сталася помилка при скасуванні вашого запису. Будь ласка, спробуйте повторити спробу пізніше.",
                reply_markup=MAIN_KEYBOARD
            )
    elif not last_record_for_id.empty and last_record_for_id.iloc[0]['Дата'] == '' and last_record_for_id.iloc[0]['Статус'] != 'Відхилено':
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} спробував повторно скасувати запис з ID '{id_to_cancel}'.")
        await update.message.reply_text(
            f"Запит на скасування номеру `{id_to_cancel}` вже прийнято.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
    else:
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} спробував скасувати неіснуючий або вже скасований запис з ID '{id_to_cancel}'.")
        await update.message.reply_text(
            f"Запис з номером `{id_to_cancel}` не знайдено в черзі або він вже скасований.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
    context.user_data.clear()
    return ConversationHandler.END

