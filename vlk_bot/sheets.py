"""
Операції з Google Sheets API.
"""

import datetime
import logging
import os
import socket
import time

import pandas as pd
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

RETRY_DELAYS = [1, 5]
RETRY_EXCEPTIONS = (BrokenPipeError, ConnectionError, ConnectionResetError, OSError, socket.timeout, TimeoutError)


def _execute_with_retry(func_name: str, api_call_func):
    """
    Виконує API виклик з повторними спробами при мережевих помилках.
    
    Args:
        func_name: Назва функції для логування
        api_call_func: Функція без аргументів, яка виконує API виклик
    
    Returns:
        Результат API виклику
    
    Raises:
        Оригінальний виняток після всіх невдалих спроб
    """
    for attempt in range(len(RETRY_DELAYS) + 1):
        start_time = time.time()
        try:
            logger.info(f"{func_name}: API виклик розпочато...")
            result = api_call_func()
            elapsed = time.time() - start_time
            logger.info(f"{func_name}: API виклик завершено за {elapsed:.2f}с")
            return result
        except RETRY_EXCEPTIONS as e:
            elapsed = time.time() - start_time
            if attempt < len(RETRY_DELAYS):
                delay = RETRY_DELAYS[attempt]
                logger.warning(f"{func_name}: мережева помилка ({type(e).__name__}) після {elapsed:.2f}с, повтор через {delay}с...")
                time.sleep(delay)
            else:
                logger.error(f"{func_name}: мережева помилка після {len(RETRY_DELAYS) + 1} спроб ({elapsed:.2f}с): {e}")
                raise


def load_queue_data() -> pd.DataFrame | None:
    """Завантажує дані черги з Google Sheet."""
    from vlk_bot.config import SHEETS_SERVICE, SPREADSHEET_ID, SHEET_NAME, REQUIRED_COLUMNS
    
    if SHEETS_SERVICE is None:
        logger.error("Google Sheets API не ініціалізовано. Неможливо завантажити дані.")
        return None

    try:
        range_name = f"{SHEET_NAME}!A:{chr(ord('A') + len(REQUIRED_COLUMNS) - 1)}"
        result = _execute_with_retry(
            "load_queue_data",
            lambda: SHEETS_SERVICE.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID, range=range_name
            ).execute()
        )
        values = result.get('values', [])

        if not values:
            logger.warning("Google Sheet порожній. Ініціалізація заголовків.")
            return pd.DataFrame(columns=REQUIRED_COLUMNS)

        columns = values[0]
        data = values[1:]

        expected_num_columns = len(REQUIRED_COLUMNS)
        processed_data = []
        for row in data:
            if len(row) < expected_num_columns:
                processed_row = row + [''] * (expected_num_columns - len(row))
            elif len(row) > expected_num_columns:
                processed_row = row[:expected_num_columns]
            else:
                processed_row = row
            processed_data.append(processed_row)

        df = pd.DataFrame(processed_data, columns=REQUIRED_COLUMNS)

        logger.info(f"Дані успішно завантажено з Google Sheet. Завантажено {len(df)} записів.")
        return df

    except HttpError as err:
        logger.error(f"Google API HttpError при завантаженні даних: {err.resp.status} - {err.content}")
        return None
    except RETRY_EXCEPTIONS:
        return None
    except Exception as e:
        logger.error(f"Помилка завантаження даних з Google Sheet: {e}")
        return None


def save_queue_data(df_to_save) -> bool:
    """Зберігає дані черги у Google Sheet (додавання рядків)."""
    from vlk_bot.config import SHEETS_SERVICE, SPREADSHEET_ID, SHEET_NAME, REQUIRED_COLUMNS
    
    if SHEETS_SERVICE is None:
        logger.error("Google Sheets API не ініціалізовано. Неможливо зберегти дані.")
        return False
    if df_to_save.empty:
        logger.warning("Спроба зберегти порожній запис у Google Sheet. Пропущено.")
        return True

    try:
        data_to_append = df_to_save[REQUIRED_COLUMNS].values.tolist()

        _execute_with_retry(
            "save_queue_data",
            lambda: SHEETS_SERVICE.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID, range=SHEET_NAME,
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body={'values': data_to_append}
            ).execute()
        )
        
        logger.info(f"Новий запис успішно додано до Google Sheet '{SHEET_NAME}'. ID: {df_to_save.iloc[0]['ID']}")
        return True
    except HttpError as err:
        logger.error(f"Google API HttpError при збереженні даних: {err.resp.status} - {err.content}")
        return False
    except RETRY_EXCEPTIONS:
        return False
    except Exception as e:
        logger.error(f"Помилка збереження даних у Google Sheet: {e}")
        return False


def save_queue_data_full(df: pd.DataFrame) -> bool:
    """
    Повністю перезаписує Google Sheet даними з DataFrame.
    """
    from vlk_bot.config import SHEETS_SERVICE, SPREADSHEET_ID, SHEET_NAME, REQUIRED_COLUMNS
    
    if SHEETS_SERVICE is None:
        logger.error("Google Sheets API не ініціалізовано. Неможливо зберегти дані.")
        return False

    try:
        _execute_with_retry(
            "save_queue_data_full.clear",
            lambda: SHEETS_SERVICE.spreadsheets().values().clear(
                spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A:Z"
            ).execute()
        )
        logger.info(f"Google Sheet '{SHEET_NAME}' було очищено перед записом.")

        if df.empty:
            logger.info(f"DataFrame для запису порожній, записано лише заголовки.")
            body = {'values': [REQUIRED_COLUMNS]}
            _execute_with_retry(
                "save_queue_data_full.headers",
                lambda: SHEETS_SERVICE.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A1",
                    valueInputOption='RAW', body=body
                ).execute()
            )
            return True

        df_to_save = df.copy()
        for col in REQUIRED_COLUMNS:
            if col not in df_to_save.columns:
                df_to_save[col] = ''
        df_to_save = df_to_save[REQUIRED_COLUMNS]

        data_to_write = [df_to_save.columns.tolist()]
        data_to_write.extend(df_to_save.values.tolist())

        _execute_with_retry(
            "save_queue_data_full.update",
            lambda: SHEETS_SERVICE.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A1",
                valueInputOption='USER_ENTERED',
                body={'values': data_to_write}
            ).execute()
        )
        logger.info(f"Дані успішно записано до Google Sheet '{SHEET_NAME}'.")
        return True
    except HttpError as err:
        logger.error(f"Google API HttpError при повному збереженні даних: {err.resp.status} - {err.content}")
        return False
    except RETRY_EXCEPTIONS:
        return False
    except Exception as e:
        logger.error(f"Помилка при повному збереженні даних у Google Sheet: {e}")
        return False


async def get_stats_data(force_refresh: bool = False) -> pd.DataFrame | None:
    """
    Завантажує дані з аркуша 'Stats'.
    Використовує локальний кеш з TTL 30 хвилин.
    """
    from vlk_bot.config import (
        SHEETS_SERVICE, STATS_SHEET_ID, STATS_WORKSHEET_NAME, 
        DAILY_SHEETS_CACHE_DIR, STATS_CACHE_TTL_MINUTES
    )
    
    stats_cache_file = os.path.join(DAILY_SHEETS_CACHE_DIR, "_stats.csv")
    
    if not force_refresh and os.path.exists(stats_cache_file):
        mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(stats_cache_file))
        age_minutes = (datetime.datetime.now() - mod_time).total_seconds() / 60
        
        if age_minutes < STATS_CACHE_TTL_MINUTES:
            try:
                stats_df = pd.read_csv(stats_cache_file)
                if 'Останній номер що зайшов' in stats_df.columns:
                    stats_df['Останній номер що зайшов'] = pd.to_numeric(stats_df['Останній номер що зайшов'], errors='coerce')
                if 'Перший номер що зайшов' in stats_df.columns:
                    stats_df['Перший номер що зайшов'] = pd.to_numeric(stats_df['Перший номер що зайшов'], errors='coerce')
                stats_df['Дата прийому'] = pd.to_datetime(stats_df['Дата прийому'], format="%d.%m.%Y", dayfirst=True, errors='coerce')
                logger.debug(f"Stats з кешу (вік: {age_minutes:.1f} хв)")
                return stats_df
            except Exception as e:
                logger.warning(f"Помилка читання кешу stats: {e}, завантажуємо з API")
    
    try:
        range_name = f"{STATS_WORKSHEET_NAME}!A1:Z"
        result = _execute_with_retry(
            "get_stats_data",
            lambda: SHEETS_SERVICE.spreadsheets().values().get(
                spreadsheetId=STATS_SHEET_ID, range=range_name
            ).execute()
        )
        
        list_of_lists = result.get("values", [])

        if not list_of_lists:
            logger.warning("Аркуш 'Stats' порожній.")
            return pd.DataFrame()

        stats_df = pd.DataFrame(list_of_lists[1:], columns=list_of_lists[0])
        
        os.makedirs(DAILY_SHEETS_CACHE_DIR, exist_ok=True)
        stats_df.to_csv(stats_cache_file, index=False)
        logger.info(f"Stats завантажено з API та збережено в кеш ({len(stats_df)} рядків)")
        
        if 'Останній номер що зайшов' in stats_df.columns:
            stats_df['Останній номер що зайшов'] = pd.to_numeric(stats_df['Останній номер що зайшов'], errors='coerce')
        if 'Перший номер що зайшов' in stats_df.columns:
            stats_df['Перший номер що зайшов'] = pd.to_numeric(stats_df['Перший номер що зайшов'], errors='coerce')
        stats_df['Дата прийому'] = pd.to_datetime(stats_df['Дата прийому'], format="%d.%m.%Y", dayfirst=True, errors='coerce')
        
        return stats_df

    except HttpError as err:
        logger.error(f"Google API HttpError при завантаженні даних: {err.resp.status} - {err.content}. Перевірте адресу таблиці та права доступу.")
        return None
    except RETRY_EXCEPTIONS:
        return None
    except Exception as e:
        logger.error(f"Загальна помилка при завантаженні даних з 'Stats': {e}")
        return None


def update_active_sheet_status(user_id: str, new_status: str) -> bool:
    """
    Оновлює статус для ID в колонці C (Статус) аркуша Active.
    """
    from vlk_bot.config import SHEETS_SERVICE, ACTIVE_SHEET_ID, ACTIVE_WORKSHEET_NAME
    
    if SHEETS_SERVICE is None:
        logger.error("Google Sheets API не ініціалізовано. Неможливо оновити статус.")
        return False
    
    try:
        range_name = f"{ACTIVE_WORKSHEET_NAME}!A:D"
        result = _execute_with_retry(
            "update_active_sheet_status.get",
            lambda: SHEETS_SERVICE.spreadsheets().values().get(
                spreadsheetId=ACTIVE_SHEET_ID,
                range=range_name
            ).execute()
        )
        
        values = result.get('values', [])
        if not values:
            logger.warning(f"Active sheet порожній")
            return False
        
        row_index = None
        for i, row in enumerate(values):
            if len(row) >= 2 and str(row[1]).strip() == str(user_id).strip():
                row_index = i
                break
        
        if row_index is None:
            logger.warning(f"ID {user_id} не знайдено в Active sheet")
            return False
        
        cell_range = f"{ACTIVE_WORKSHEET_NAME}!C{row_index + 1}"
        _execute_with_retry(
            "update_active_sheet_status.update",
            lambda: SHEETS_SERVICE.spreadsheets().values().update(
                spreadsheetId=ACTIVE_SHEET_ID,
                range=cell_range,
                valueInputOption='USER_ENTERED',
                body={'values': [[new_status]]}
            ).execute()
        )
        
        logger.info(f"Статус ID {user_id} оновлено на '{new_status}' в Active sheet")
        return True
        
    except HttpError as err:
        logger.error(f"Google API HttpError при оновленні статусу: {err.resp.status} - {err.content}")
        return False
    except RETRY_EXCEPTIONS:
        return False
    except Exception as e:
        logger.error(f"Помилка оновлення статусу в Active sheet: {e}")
        return False


def get_sheets_list(spreadsheet_id: str) -> list:
    """
    Отримує список назв аркушів у таблиці.
    """
    from vlk_bot.config import SHEETS_SERVICE
    
    if SHEETS_SERVICE is None:
        logger.error("Google Sheets API не ініціалізовано.")
        return []
    
    try:
        spreadsheet = _execute_with_retry(
            "get_sheets_list",
            lambda: SHEETS_SERVICE.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        )
        sheets = spreadsheet.get('sheets', [])
        return [sheet['properties']['title'] for sheet in sheets]
    except HttpError as err:
        logger.error(f"Google API HttpError при отриманні списку аркушів: {err.resp.status}")
        return []
    except RETRY_EXCEPTIONS:
        return []
    except Exception as e:
        logger.error(f"Помилка отримання списку аркушів: {e}")
        return []


def get_users_for_date_from_active_sheet(target_date: str) -> list:
    """
    Отримує список користувачів з Active sheet, записаних на вказану дату.
    """
    from vlk_bot.config import SHEETS_SERVICE, ACTIVE_SHEET_ID, ACTIVE_WORKSHEET_NAME
    
    if SHEETS_SERVICE is None:
        logger.error("Google Sheets API не ініціалізовано.")
        return []
    
    try:
        range_name = f"{ACTIVE_WORKSHEET_NAME}!A:Z"
        result = _execute_with_retry(
            "get_users_for_date_from_active_sheet",
            lambda: SHEETS_SERVICE.spreadsheets().values().get(
                spreadsheetId=ACTIVE_SHEET_ID,
                range=range_name
            ).execute()
        )
        
        values = result.get('values', [])
        if len(values) < 3:
            return []
        
        data_start_idx = None
        for i, row in enumerate(values):
            if len(row) > 0 and row[0].strip() == '№':
                data_start_idx = i + 1
                break
        
        if data_start_idx is None:
            return []
        
        users = []
        for row in values[data_start_idx:]:
            if len(row) < 2:
                continue
            
            number = row[0].strip() if len(row) > 0 else ''
            user_id = row[1].strip() if len(row) > 1 else ''
            
            if not number or not user_id:
                continue
            
            tg_id = ''
            for col_idx in range(4, len(row)):
                cell_value = str(row[col_idx]).strip()
                if cell_value.isdigit() and len(cell_value) >= 6:
                    tg_id = cell_value
                    break
            
            users.append({
                'id': user_id,
                'tg_id': tg_id,
                'number': number
            })
        
        return users
        
    except HttpError as err:
        logger.error(f"Google API HttpError при читанні Active sheet: {err.resp.status}")
        return []
    except RETRY_EXCEPTIONS:
        return []
    except Exception as e:
        logger.error(f"Помилка читання Active sheet: {e}")
        return []
