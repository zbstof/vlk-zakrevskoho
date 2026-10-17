"""
Допоміжні функції.
"""

import datetime
import logging
import os
import re

import pandas as pd
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


def load_todo_cache() -> pd.DataFrame:
    """Завантажує кешований стан TODO_BOT з CSV-файлу."""
    from vlk_bot.config import TODO_BOT_CACHE_FILE
    if os.path.exists(TODO_BOT_CACHE_FILE):
        try:
            return pd.read_csv(TODO_BOT_CACHE_FILE, dtype=str).fillna('')
        except Exception as e:
            logger.warning(f"Помилка читання кешу TODO_BOT: {e}")
    return pd.DataFrame()


def save_todo_cache(df: pd.DataFrame):
    """Зберігає поточний стан TODO_BOT у CSV-файл."""
    from vlk_bot.config import TODO_BOT_CACHE_FILE
    try:
        df.to_csv(TODO_BOT_CACHE_FILE, index=False)
    except Exception as e:
        logger.error(f"Помилка збереження кешу TODO_BOT: {e}")


def compare_dataframes(current_df: pd.DataFrame, cached_df: pd.DataFrame) -> list[dict]:
    """
    Порівнює поточний стан з кешем, повертає список змін.
    Складність: O(n)
    """
    if cached_df.empty:
        return []
    
    if current_df.empty:
        return []
    
    current_df = current_df.copy()
    cached_df = cached_df.copy()
    
    current_df['Змінено_dt'] = pd.to_datetime(
        current_df['Змінено'], format="%d.%m.%Y %H:%M:%S", errors='coerce'
    )
    cached_df['Змінено_dt'] = pd.to_datetime(
        cached_df['Змінено'], format="%d.%m.%Y %H:%M:%S", errors='coerce'
    )
    
    current_df['Змінено_dt'] = current_df['Змінено_dt'].fillna(pd.Timestamp("2000-01-01"))
    cached_df['Змінено_dt'] = cached_df['Змінено_dt'].fillna(pd.Timestamp("2000-01-01"))
    
    current_latest = current_df.loc[current_df.groupby('ID')['Змінено_dt'].idxmax()]
    cached_latest = cached_df.loc[cached_df.groupby('ID')['Змінено_dt'].idxmax()]
    
    current_dict = current_latest.set_index('ID').to_dict('index')
    cached_dict = cached_latest.set_index('ID').to_dict('index')
    
    changes = []
    for user_id, current_row in current_dict.items():
        cached_row = cached_dict.get(user_id)
        
        if cached_row is None:
            changes.append({'id': user_id, 'type': 'new', 'row': current_row})
        elif (current_row.get('Статус') != cached_row.get('Статус') or
              current_row.get('Дата') != cached_row.get('Дата') or
              current_row.get('Змінено') != cached_row.get('Змінено')):
            changes.append({'id': user_id, 'type': 'changed', 'row': current_row})
    
    return changes


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


def check_deadline(date_str: str) -> bool:
    """
    Перевіряє, чи минув дедлайн для вказаної дати.
    Дедлайн — 7:00 ранку дня візиту (для вівторка-п'ятниці)
    або 7:00 ранку суботи (для понеділка).
    """
    if not date_str:
        return False
    try:
        visit_date = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
        now = datetime.datetime.now()
        today = now.date()
        
        # Для понеділка (weekday == 0) дедлайн у суботу (за 2 дні)
        if visit_date.weekday() == 0:
            deadline_date = visit_date - datetime.timedelta(days=2)
        else:
            deadline_date = visit_date
            
        if today > deadline_date:
            return True
        if today == deadline_date and now.hour >= 7:
            return True
        return False
    except ValueError:
        return False


async def send_group_notification(context, text: str):
    """Надсилає повідомлення в групу."""
    from vlk_bot.config import GROUP_ID, is_bot_in_group
    
    if not is_bot_in_group:
        return
    try:
        await context.bot.send_message(chat_id=GROUP_ID, text=text, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Помилка надсилання в групу: {e}")

