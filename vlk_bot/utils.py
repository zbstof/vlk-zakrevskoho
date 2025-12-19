"""
Допоміжні функції.
"""

import datetime
import json
import logging
import os
import re

from telegram import User

logger = logging.getLogger(__name__)


def is_admin(user_id: int) -> bool:
    """Перевіряє, чи є користувач адміністратором."""
    from vlk_bot.config import ADMIN_IDS
    return user_id in ADMIN_IDS


def is_banned(user_id: int) -> bool:
    """Перевіряє, чи забанений користувач."""
    from vlk_bot.config import BANLIST
    return user_id in BANLIST


def get_user_log_info(user: User) -> str:
    """
    Повертає рядок з інформацією про користувача для журналу.
    """
    user_info = f"ID: {user.id}"
    if user.username:
        user_info += f", @{user.username}"
    elif user.full_name:
        user_info += f", Ім'я: {user.full_name}"
    else:
        user_info += ", [Невідоме ім'я]"
    return user_info


def get_user_telegram_data(user: User) -> dict:
    """
    Повертає словник з даними користувача Telegram для запису в DataFrame.
    """
    return {
        'TG ID': user.id,
        'TG Name': user.username if user.username else '',
        'TG Full Name': user.full_name if user.full_name else ''
    }


def extract_main_id(id_string):
    """Витягує основний номер ID з рядка."""
    if isinstance(id_string, str):
        match = re.match(r'^\d+', id_string)
        if match:
            return int(match.group())
    return None


def load_status_state() -> dict:
    """Завантажує останній відомий стан статусів з JSON-файлу."""
    from vlk_bot.config import STATUS_FILE
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, "r", encoding='utf8') as f:
            return json.load(f)
    return {}


def save_status_state(state: dict):
    """Зберігає поточний стан статусів у JSON-файл."""
    from vlk_bot.config import STATUS_FILE
    with open(STATUS_FILE, "w", encoding='utf8') as f:
        json.dump(state, f, indent=4, ensure_ascii=False)


def get_ordinal_date(date_obj):
    """Конвертує дату в ordinal (порядковий номер робочого дня) для регресії."""
    # Якірна дата: 5 січня 1970 року (понеділок)
    anchor = datetime.date(1970, 1, 5)
    diff = (date_obj - anchor).days
    weeks = diff // 7
    days = diff % 7
    return weeks * 5 + min(days, 5)


def get_date_from_ordinal(ordinal):
    """Конвертує ordinal назад в дату."""
    anchor = datetime.date(1970, 1, 5)
    weeks = int(ordinal) // 5
    days = int(ordinal) % 5
    total_days = weeks * 7 + days
    return anchor + datetime.timedelta(days=total_days)


def get_next_working_days(count: int = 3) -> list:
    """
    Повертає список наступних робочих днів (без вихідних).
    """
    result = []
    current = datetime.date.today() + datetime.timedelta(days=1)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += datetime.timedelta(days=1)
    return result


def get_ua_weekday(date_obj):
    """Повертає скорочену назву дня тижня."""
    return date_obj.strftime('%a').title()


def id_to_numeric(id_val):
    """
    Конвертує ID рядок у числове значення для регресії.
    "1234" -> 1234.0
    "1234/1" -> 1234.01
    """
    s = str(id_val).strip()
    if not s:
        return None
    
    try:
        if '/' in s:
            parts = s.split('/')
            main = int(parts[0])
            sub = 0
            if len(parts) > 1 and parts[1].isdigit():
                sub = int(parts[1])
            return main + (sub / 100.0)
        return float(s)
    except ValueError:
        match = re.match(r'^(\d+)', s)
        if match:
            return float(match.group(1))
        return None


async def send_group_notification(context, text: str):
    """Надсилає повідомлення в групу."""
    from vlk_bot.config import GROUP_ID, is_bot_in_group
    
    if not is_bot_in_group:
        return
    try:
        await context.bot.send_message(chat_id=GROUP_ID, text=text, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Помилка надсилання в групу: {e}")

