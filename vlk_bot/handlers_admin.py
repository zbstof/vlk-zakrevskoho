"""
Обробники адмін-команд.
"""

import datetime
import logging
from functools import wraps

import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes

from vlk_bot.formatters import get_poll_text
from vlk_bot.keyboards import MAIN_KEYBOARD, get_poll_keyboard
from vlk_bot.sheets import (
    load_queue_data, save_queue_data_full, get_users_for_date_from_active_sheet
)
from vlk_bot.utils import get_user_log_info, is_admin, get_next_working_days

logger = logging.getLogger(__name__)


def admin_only(func):
    """Декоратор для команд, доступних тільки адміністраторам."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not is_admin(user.id):
            logger.warning(f"Користувач {get_user_log_info(user)} без прав адміністратора спробував виконати {func.__name__}")
            await update.message.reply_text("У вас недостатньо прав для виконання цієї команди.", reply_markup=MAIN_KEYBOARD)
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


async def perform_queue_cleanup(logger_info_prefix: str = "Очищення за розкладом"):
    """Виконує логіку очищення черги."""
    import vlk_bot.config as config_module
    
    logger.info(f"{logger_info_prefix}: Розпочато розумне очищення черги.")

    queue_df = load_queue_data()
    if queue_df is None:
        logger.error(f"{logger_info_prefix}: Не вдалося завантажити чергу для очищення.")
        return -1
        
    sort_df = queue_df.copy()
    if sort_df.empty:
        logger.info(f"{logger_info_prefix}: Черга вже порожня.")
        return 0

    initial_records_count = len(sort_df)

    sort_df['Статус_clean'] = sort_df['Статус'].astype(str).str.strip().str.lower()
    sort_df['Дата_clean'] = sort_df['Дата'].astype(str).str.strip()
    sort_df['Дата_dt'] = pd.to_datetime(sort_df['Дата_clean'], format="%d.%m.%Y", dayfirst=True, errors='coerce')
    sort_df['Змінено_clean'] = sort_df['Змінено'].astype(str).str.strip()
    sort_df['Змінено_dt'] = pd.to_datetime(sort_df['Змінено_clean'], format="%d.%m.%Y %H:%M:%S", dayfirst=True, errors='coerce')
    
    current_date_obj = datetime.date.today()
    unique_ids = sort_df['ID'].unique()
    index_to_drop = []
    index_to_drop.extend(sort_df.loc[(sort_df['Дата_dt'].dt.date < current_date_obj) & (sort_df['Дата_dt'].notna())].index.tolist())
  
    for cur_id in unique_ids:
        max_mod_idx = sort_df[sort_df['ID'] == cur_id]['Змінено_dt'].idxmax()
        TG_ID = sort_df['TG ID'][max_mod_idx].strip()
        index_to_drop.extend(sort_df.loc[(sort_df['ID'] == cur_id) & (sort_df['Змінено_dt'] < sort_df['Змінено_dt'][max_mod_idx]) & ((sort_df['Дата_dt'].dt.date >= current_date_obj) | (sort_df['Дата_dt'].isna())) & (sort_df['Статус_clean'].isin(['відхилено']))].index.tolist())
        if sort_df['Статус_clean'][max_mod_idx] == 'ухвалено':
            index_to_drop.extend(sort_df.loc[(sort_df['ID'] == cur_id) & (sort_df['Змінено_dt'] < sort_df['Змінено_dt'][max_mod_idx]) & (sort_df['Статус_clean'].isin(['на розгляді', 'ухвалено'])) & (sort_df['TG ID'] == TG_ID)].index.tolist())
            index_to_drop.extend(sort_df.loc[(sort_df['ID'] == cur_id) & (sort_df['Змінено_dt'] < sort_df['Змінено_dt'][max_mod_idx]) & (sort_df['Статус_clean'].isin(['на розгляді', 'ухвалено'])) & (sort_df['TG ID'] != TG_ID) & (sort_df['Дата_dt'].isna())].index.tolist())
            if pd.notna(sort_df['Дата_dt'][max_mod_idx]):
                if sort_df['Дата_dt'].dt.date[max_mod_idx] < current_date_obj:
                    index_to_drop.extend(sort_df.loc[(sort_df['ID'] == cur_id) & (sort_df['Змінено_dt'] < sort_df['Змінено_dt'][max_mod_idx]) & (sort_df['Статус_clean'].isin(['на розгляді', 'ухвалено'])) & (sort_df['TG ID'] != TG_ID) & (sort_df['Дата_dt'].dt.date >= current_date_obj)].index.tolist())
                    
    unique_index_to_drop = list(set(index_to_drop))
    records_to_keep = sort_df.drop(index=unique_index_to_drop).copy()
    
    for col in ['Статус_clean', 'Дата_clean', 'Дата_dt', 'Змінено_dt', 'Змінено_clean']:
        if col in records_to_keep.columns:
            records_to_keep = records_to_keep.drop(columns=[col])

    config_module.queue_df = records_to_keep
    
    if not save_queue_data_full(records_to_keep):
        logger.error(f"{logger_info_prefix}: Помилка при збереженні очищеної черги в Google Sheet.")
        return -1

    records_removed_count = initial_records_count - len(records_to_keep)
    logger.info(f"{logger_info_prefix}: Очищено {records_removed_count} записів. Залишилось {len(records_to_keep)} записів.")
    return records_removed_count


@admin_only
async def open_sheet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Відправляє користувачу посилання на Google Sheet."""
    from vlk_bot.config import SHEETS_SERVICE, SPREADSHEET_ID
    
    user = update.effective_user
    if SHEETS_SERVICE is None:
        logger.error(f"Адміністратор {get_user_log_info(user)} спробував отримати посилання, але Google Sheets API не ініціалізовано.")
        await update.message.reply_text(
            "Не вдалося отримати посилання на таблицю, оскільки сервіс Google Sheets не ініціалізовано.",
            reply_markup=MAIN_KEYBOARD
        )
        return
        
    sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit"
    logger.info(f"Користувач {get_user_log_info(user)} отримав посилання на Google Sheet.")
    await update.message.reply_text(
        f"Ось посилання на Google Таблицю з даними черги:\n{sheet_url}",
        reply_markup=MAIN_KEYBOARD
    )


@admin_only
async def grant_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Додає користувача до списку адміністраторів."""
    from vlk_bot.config import ADMIN_IDS, config, save_config
    
    user = update.effective_user
    
    if not context.args:
        await update.message.reply_text(
            "Будь ласка, вкажіть ID користувача. Наприклад: `/grant_admin 123456789`",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
        return

    try:
        new_admin_id = int(context.args[0])
        if new_admin_id in ADMIN_IDS:
            await update.message.reply_text(
                f"Користувач з ID `{new_admin_id}` вже є адміністратором.",
                parse_mode='Markdown',
                reply_markup=MAIN_KEYBOARD
            )
            return

        ADMIN_IDS.append(new_admin_id)
        config['BOT_SETTINGS']['ADMIN_IDS'] = ','.join(map(str, ADMIN_IDS))
        save_config()

        logger.info(f"Адміністратор {get_user_log_info(user)} додав нового адміністратора: ID {new_admin_id}.")
        await update.message.reply_text(
            f"Користувач з ID `{new_admin_id}` успішно доданий до списку адміністраторів.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
    except ValueError:
        await update.message.reply_text(
            "Невірний формат ID. Будь ласка, введіть ціле число.",
            reply_markup=MAIN_KEYBOARD
        )


@admin_only
async def drop_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Видаляє користувача зі списку адміністраторів."""
    from vlk_bot.config import ADMIN_IDS, config, save_config
    
    user = update.effective_user
    
    if not context.args:
        await update.message.reply_text(
            "Будь ласка, вкажіть ID користувача. Наприклад: `/drop_admin 123456789`",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
        return

    try:
        admin_to_remove_id = int(context.args[0])
        
        if admin_to_remove_id == user.id:
            await update.message.reply_text(
                "Ви не можете видалити самого себе з адміністраторів.",
                reply_markup=MAIN_KEYBOARD
            )
            return

        if admin_to_remove_id not in ADMIN_IDS:
            await update.message.reply_text(
                f"Користувач з ID `{admin_to_remove_id}` не є адміністратором.",
                parse_mode='Markdown',
                reply_markup=MAIN_KEYBOARD
            )
            return

        ADMIN_IDS.remove(admin_to_remove_id)
        config['BOT_SETTINGS']['ADMIN_IDS'] = ','.join(map(str, ADMIN_IDS))
        save_config()

        logger.info(f"Адміністратор {get_user_log_info(user)} видалив адміністратора: ID {admin_to_remove_id}.")
        await update.message.reply_text(
            f"Користувач з ID `{admin_to_remove_id}` успішно видалений зі списку адміністраторів.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
    except ValueError:
        await update.message.reply_text(
            "Невірний формат ID. Будь ласка, введіть ціле число.",
            reply_markup=MAIN_KEYBOARD
        )


@admin_only
async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Додає користувача до списку заблокованих."""
    from vlk_bot.config import BANLIST, config, save_config
    
    user = update.effective_user
    
    if not context.args:
        await update.message.reply_text(
            "Будь ласка, вкажіть ID користувача. Наприклад: `/ban 123456789`",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
        return

    try:
        new_ban_id = int(context.args[0])
        if new_ban_id in BANLIST:
            await update.message.reply_text(
                f"Користувач з ID `{new_ban_id}` вже є в списку заблокованих.",
                parse_mode='Markdown',
                reply_markup=MAIN_KEYBOARD
            )
            return

        BANLIST.append(new_ban_id)
        config['BOT_SETTINGS']['BANLIST'] = ','.join(map(str, BANLIST))
        save_config()

        logger.info(f"Адміністратор {get_user_log_info(user)} заблокував користувача: ID {new_ban_id}.")
        await update.message.reply_text(
            f"Користувач з ID `{new_ban_id}` успішно доданий до списку заблокованих.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
    except ValueError:
        await update.message.reply_text(
            "Невірний формат ID. Будь ласка, введіть ідентифікатор користувача телеграм (TG ID).",
            reply_markup=MAIN_KEYBOARD
        )


@admin_only
async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Видаляє користувача зі списку заблокованих."""
    from vlk_bot.config import BANLIST, config, save_config
    
    user = update.effective_user
    
    if not context.args:
        await update.message.reply_text(
            "Будь ласка, вкажіть ID користувача. Наприклад: `/unban 123456789`",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
        return

    try:
        unban_id = int(context.args[0])

        if unban_id not in BANLIST:
            await update.message.reply_text(
                f"Користувач з ID `{unban_id}` відсутній в списку заблокованих.",
                parse_mode='Markdown',
                reply_markup=MAIN_KEYBOARD
            )
            return

        BANLIST.remove(unban_id)
        config['BOT_SETTINGS']['BANLIST'] = ','.join(map(str, BANLIST))
        save_config()

        logger.info(f"Адміністратор {get_user_log_info(user)} видалив користувача зі списку заблокованих: ID {unban_id}.")
        await update.message.reply_text(
            f"Користувач з ID `{unban_id}` успішно видалений зі списку заблокованих.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
    except ValueError:
        await update.message.reply_text(
            "Невірний формат ID. Будь ласка, введіть ідентифікатор користувача телеграм (TG ID).",
            reply_markup=MAIN_KEYBOARD
        )


@admin_only
async def test_poll(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Надсилає тестове опитування."""
    user = update.effective_user
    requester_id = user.id
    
    user_id = None
    
    if context.args:
        user_id = context.args[0]
    else:
        users = get_users_for_date_from_active_sheet('')
        for u in users:
            if u.get('tg_id') == str(requester_id):
                user_id = u['id']
                break
    
    if not user_id:
        await update.message.reply_text(
            "ID не знайдено в Active sheet. Вкажіть ID явно: /test_poll 1234",
            parse_mode="HTML"
        )
        return
    
    next_working_days = get_next_working_days(1)
    test_date = next_working_days[0].strftime("%d.%m.%Y") if next_working_days else "Тестова дата"
    
    context.bot_data['next_reception_sheet'] = test_date
    context.bot_data['next_reception_date'] = next_working_days[0] if next_working_days else datetime.date.today()
    
    await context.bot.send_message(
        chat_id=requester_id,
        text=f"<b>ТЕСТОВЕ</b> {get_poll_text(user_id, test_date)}",
        reply_markup=get_poll_keyboard(user_id),
        parse_mode="HTML"
    )
    
    logger.info(f"Тестове опитування надіслано адміністратору {get_user_log_info(user)} з ID: {user_id}")


@admin_only
async def run_cleanup_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ручний запуск очищення черги."""
    user = update.effective_user
    logger.info(f"Адміністратор {get_user_log_info(user)} запустив: очищення черги")
    await update.message.reply_text("Запускаю очищення черги...")
    
    removed_count = await perform_queue_cleanup(logger_info_prefix=f"Ручний запуск (адмін {user.id})")
    
    if removed_count == -1:
        await update.message.reply_text("Помилка під час очищення черги.", reply_markup=MAIN_KEYBOARD)
    else:
        await update.message.reply_text(f"Очищення завершено. Видалено {removed_count} записів.", reply_markup=MAIN_KEYBOARD)


@admin_only
async def run_notify_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ручний запуск перевірки статусів."""
    from vlk_bot.scheduler import notify_status
    
    user = update.effective_user
    logger.info(f"Адміністратор {get_user_log_info(user)} запустив: перевірку статусів")
    await update.message.reply_text("Запускаю перевірку статусів...")
    await notify_status(context)
    await update.message.reply_text("Перевірку статусів завершено.", reply_markup=MAIN_KEYBOARD)


@admin_only
async def run_reminder_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ручний запуск нагадувань."""
    from vlk_bot.scheduler import date_reminder
    
    user = update.effective_user
    logger.info(f"Адміністратор {get_user_log_info(user)} запустив: нагадування")
    await update.message.reply_text("Запускаю нагадування...")
    await date_reminder(context)
    await update.message.reply_text("Нагадування завершено.", reply_markup=MAIN_KEYBOARD)


@admin_only
async def run_check_sheet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ручний запуск перевірки нового аркуша."""
    from vlk_bot.scheduler import check_new_daily_sheet
    
    user = update.effective_user
    logger.info(f"Адміністратор {get_user_log_info(user)} запустив: перевірку аркуша")
    await update.message.reply_text("Запускаю перевірку нового аркуша...")
    
    await check_new_daily_sheet(context)
    
    next_sheet = context.bot_data.get('next_reception_sheet', 'не знайдено')
    detected_at = context.bot_data.get('sheet_detected_at')
    poll_sent = context.bot_data.get('poll_sent_for_date')
    
    status_msg = f"Перевірку завершено.\nНаступний аркуш: {next_sheet}"
    if detected_at:
        status_msg += f"\nВиявлено о: {detected_at.strftime('%H:%M:%S')}"
    if poll_sent:
        status_msg += f"\nОпитування надіслано: {poll_sent}"
    
    await update.message.reply_text(status_msg, reply_markup=MAIN_KEYBOARD)


@admin_only
async def run_poll_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ручний запуск опитування."""
    from vlk_bot.scheduler import send_visit_poll
    
    next_sheet = context.bot_data.get('next_reception_sheet')
    if not next_sheet:
        await update.message.reply_text(
            "Аркуш наступного прийомного дня не виявлено. Спочатку запустіть /run_check_sheet", 
            reply_markup=MAIN_KEYBOARD
        )
        return
    
    user = update.effective_user
    logger.info(f"Адміністратор {get_user_log_info(user)} запустив: опитування для дати {next_sheet}")
    await update.message.reply_text(f"Запускаю опитування для дати {next_sheet}...")
    await send_visit_poll(context)
    await update.message.reply_text("Опитування завершено.", reply_markup=MAIN_KEYBOARD)


@admin_only
async def show_environment_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показує поточне оточення бота."""
    from vlk_bot.config import ENVIRONMENT
    
    scheduled_status = "вимкнено" if ENVIRONMENT == "test" else "увімкнено"
    
    await update.message.reply_text(
        f"<b>Оточення:</b> <code>{ENVIRONMENT}</code>\n"
        f"<b>Заплановані завдання:</b> {scheduled_status}\n\n"
        f"<b>Команди для ручного запуску:</b>\n"
        f"/run_cleanup - очищення черги\n"
        f"/run_notify - перевірка статусів\n"
        f"/run_reminder - нагадування\n"
        f"/run_check_sheet - перевірка аркуша\n"
        f"/run_poll - надіслати опитування",
        parse_mode="HTML",
        reply_markup=MAIN_KEYBOARD
    )

