"""
Модуль для синхронізації та аналізу щоденних аркушів.
"""

import csv
import datetime
import logging
import os
import time

import pandas as pd
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

DAILY_SHEETS_CACHE_DIR = "daily_sheets_cache"
SYNC_CACHE_TTL_MINUTES = 30


def ensure_cache_dir():
    """Створює директорію для кешу якщо не існує."""
    os.makedirs(DAILY_SHEETS_CACHE_DIR, exist_ok=True)


def download_stats(sheets_service, stats_sheet_id, stats_worksheet_name):
    """Завантажує stats аркуш."""
    try:
        range_name = f"{stats_worksheet_name}!A:Z"
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=stats_sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values:
            return None
        
        headers = values[0]
        data = values[1:]
        
        max_cols = len(headers)
        normalized_data = []
        for row in data:
            if len(row) < max_cols:
                normalized_data.append(row + [''] * (max_cols - len(row)))
            else:
                normalized_data.append(row[:max_cols])
        
        df = pd.DataFrame(normalized_data, columns=headers)
        
        stats_file = os.path.join(DAILY_SHEETS_CACHE_DIR, "_stats.csv")
        df.to_csv(stats_file, index=False)
        logger.info(f"Stats оновлено: {len(df)} рядків")
        
        return df
        
    except Exception as e:
        logger.error(f"Помилка завантаження stats: {e}")
        return None


def download_daily_sheet(sheets_service, stats_sheet_id, sheet_name, retry_delay=0.5):
    """
    Завантажує один щоденний аркуш за назвою.
    """
    try:
        date_obj = datetime.datetime.strptime(sheet_name, "%d.%m.%Y").date()
        cache_filename = date_obj.strftime("%Y-%m-%d.csv")
    except ValueError:
        logger.error(f"Невірний формат дати: {sheet_name}")
        return False
    
    cache_file = os.path.join(DAILY_SHEETS_CACHE_DIR, cache_filename)
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            range_name = f"{sheet_name}!A:Z"
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=stats_sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                logger.warning(f"Аркуш {sheet_name} порожній")
                return False
            
            with open(cache_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for row in values:
                    writer.writerow(row)
            
            logger.debug(f"Завантажено {sheet_name} -> {cache_filename}")
            return True
            
        except HttpError as err:
            if err.resp.status == 429:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limit для {sheet_name}, чекаю {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Rate limit для {sheet_name} після {max_retries} спроб")
                    return False
            elif err.resp.status != 400:
                logger.error(f"HTTP помилка для {sheet_name}: {err.resp.status}")
            return False
        except (ConnectionError, BrokenPipeError, OSError) as e:
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)
                logger.warning(f"Мережева помилка для {sheet_name} ({type(e).__name__}), чекаю {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Мережева помилка для {sheet_name} після {max_retries} спроб: {e}")
                return False
        except Exception as e:
            logger.error(f"Невідома помилка завантаження {sheet_name}: {e}")
            return False
    
    return False


def sync_daily_sheets(sheets_service, stats_sheet_id, stats_worksheet_name, 
                      force_refresh_stats=False, force_refresh_all_sheets=False):
    """
    Синхронізує щоденні аркуші на основі колонки "Аркуш" зі stats.
    """
    ensure_cache_dir()
    
    attendance_file = os.path.join(os.path.dirname(__file__), "..", "attendance_data.json")
    
    if not force_refresh_stats and not force_refresh_all_sheets and os.path.exists(attendance_file):
        mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(attendance_file))
        age_minutes = (datetime.datetime.now() - mod_time).total_seconds() / 60
        
        if age_minutes < SYNC_CACHE_TTL_MINUTES:
            logger.debug(f"Синхронізація пропущена (кеш свіжий, вік: {age_minutes:.1f} хв)")
            return True
    
    stats_file = os.path.join(DAILY_SHEETS_CACHE_DIR, "_stats.csv")
    should_refresh = force_refresh_stats
    
    if not should_refresh and os.path.exists(stats_file):
        mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(stats_file))
        if mod_time.date() < datetime.date.today():
            should_refresh = True
            logger.info("Stats застарів, оновлюємо...")
    else:
        should_refresh = True
    
    if should_refresh:
        stats_df = download_stats(sheets_service, stats_sheet_id, stats_worksheet_name)
        if stats_df is None:
            logger.error("Не вдалося завантажити stats")
            return False
    else:
        stats_df = pd.read_csv(stats_file)
        logger.debug(f"Використовуємо кешований stats")
    
    sheets_to_download = []
    for _, row in stats_df.iterrows():
        sheet_name = str(row['Аркуш']).strip()
        entered = str(row['Зайшов']).strip()
        
        if not entered or entered.lower() in ['nan', 'none', '']:
            continue
        
        if sheet_name and sheet_name != 'nan':
            try:
                datetime.datetime.strptime(sheet_name, "%d.%m.%Y")
                sheets_to_download.append(sheet_name)
            except ValueError:
                continue
    
    REFRESH_LAST_N_DAYS = 5
    
    if force_refresh_all_sheets:
        logger.info("Режим force_refresh_all_sheets: ігнорування кешу для всіх аркушів")
        sheets_to_update = sheets_to_download
    else:
        cutoff_date = datetime.date.today() - datetime.timedelta(days=REFRESH_LAST_N_DAYS)
        
        existing_sheets = set()
        for filename in os.listdir(DAILY_SHEETS_CACHE_DIR):
            if filename.endswith('.csv') and filename != '_stats.csv':
                try:
                    date_obj = datetime.datetime.strptime(filename[:-4], "%Y-%m-%d").date()
                    sheet_name = date_obj.strftime("%d.%m.%Y")
                    
                    if date_obj < cutoff_date:
                        existing_sheets.add(sheet_name)
                except:
                    continue
        
        sheets_to_update = [s for s in sheets_to_download if s not in existing_sheets]
    
    sheets_updated = False
    if sheets_to_update:
        logger.info(f"Завантаження {len(sheets_to_update)} аркушів (включно з оновленням останніх {REFRESH_LAST_N_DAYS} днів)...")
        for i, sheet_name in enumerate(sheets_to_update):
            if download_daily_sheet(sheets_service, stats_sheet_id, sheet_name):
                sheets_updated = True
            
            if i < len(sheets_to_update) - 1:
                time.sleep(0.3)
    
    if sheets_updated or should_refresh:
        logger.info("Оновлення attendance_data.json...")
        generate_attendance_json()
    elif os.path.exists(attendance_file):
        os.utime(attendance_file, None)
    
    return True


def parse_daily_sheet_attendance(csv_file):
    """
    Парсить щоденний аркуш і повертає дані про ФАКТИЧНУ відвідуваність.
    """
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
    
    if len(rows) < 4:
        return None
    
    data_start_idx = None
    for i, row in enumerate(rows):
        if len(row) > 0 and row[0].strip() == '№':
            data_start_idx = i + 1
            break
    
    if data_start_idx is None:
        return None
    
    attended = 0
    no_show = 0
    postponed = 0
    total = 0
    
    for row in rows[data_start_idx:]:
        if len(row) < 3:
            continue
        
        number = row[0].strip()
        person_id = row[1].strip()
        status = row[2].strip()
        
        if not number:
            continue
        
        if not number.isdigit():
            continue
        
        if not any(char.isdigit() for char in person_id):
            continue
            
        total += 1
        status_lower = status.lower()
        
        if 'зайшов' in status_lower and 'не зайшов' not in status_lower and "не з'явився" not in status_lower:
            attended += 1
        elif 'не зайшов' in status_lower or "не з'явився" in status_lower:
            no_show += 1
        elif 'відклав' in status_lower:
            postponed += 1
    
    if total == 0:
        return None
    
    return {
        'total': total,
        'attended': attended,
        'no_show': no_show,
        'postponed': postponed,
        'attendance_rate': attended / total
    }


def extract_attended_ids_from_sheet(csv_file):
    """
    Витягує список ID людей які ЗАЙШЛИ з щоденного аркуша.
    """
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
    
    if len(rows) < 4:
        return []
    
    data_start_idx = None
    for i, row in enumerate(rows):
        if len(row) > 0 and row[0].strip() == '№':
            data_start_idx = i + 1
            break
    
    if data_start_idx is None:
        return []
    
    attended_ids = []
    
    for row in rows[data_start_idx:]:
        if len(row) < 3:
            continue
        
        number = row[0].strip()
        person_id = row[1].strip()
        status = row[2].strip()
        
        if not number or not person_id or not number.isdigit():
            continue
        
        if not any(char.isdigit() for char in person_id):
            continue
            
        id_val = person_id.strip()
        
        status_lower = status.lower()
        if 'зайшов' in status_lower and 'не зайшов' not in status_lower and "не з'явився" not in status_lower:
            is_live = 'за живою чергою' in status_lower
            attended_ids.append({'id': id_val, 'is_live': is_live})
    
    return attended_ids


def get_historical_attendance_data():
    """
    Витягує історичні дані про фактичну відвідуваність з усіх щоденних аркушів.
    """
    from vlk_bot.utils import id_to_numeric
    
    ensure_cache_dir()
    
    files = sorted([f for f in os.listdir(DAILY_SHEETS_CACHE_DIR) 
                    if f.endswith('.csv') and f != '_stats.csv'])
    
    if not files:
        logger.warning("Немає щоденних аркушів")
        return None
    
    data = []
    for filename in files:
        try:
            date_obj = datetime.datetime.strptime(filename[:-4], "%Y-%m-%d").date()
        except:
            continue
        
        filepath = os.path.join(DAILY_SHEETS_CACHE_DIR, filename)
        attended_data = extract_attended_ids_from_sheet(filepath)
        
        if attended_data:
            attended_ids = []
            for item in attended_data:
                num_id = id_to_numeric(item['id'])
                if num_id is not None:
                    attended_ids.append(num_id)
            
            if not attended_ids:
                continue
                
            data.append({
                'date': date_obj,
                'attended_data': attended_data,
                'attended_ids': attended_ids,
                'count': len(attended_ids),
                'min_id': min(attended_ids),
                'max_id': max(attended_ids),
                'avg_id': sum(attended_ids) / len(attended_ids)
            })
    
    if not data:
        logger.warning("Не знайдено даних про відвідуваність")
        return None
    
    df = pd.DataFrame(data)
    df = df.sort_values('date')
    
    logger.info(f"Завантажено історичні дані про відвідуваність для {len(df)} днів")
    return df


def generate_attendance_json(output_file='attendance_data.json'):
    """
    Генерує attendance_data.json з усіма історичними точками відвідуваності.
    """
    import json
    
    ensure_cache_dir()
    
    stats_file = os.path.join(DAILY_SHEETS_CACHE_DIR, "_stats.csv")
    if not os.path.exists(stats_file):
        logger.error("Файл _stats.csv не знайдено. Запустіть синхронізацію спочатку")
        return False
    
    stats_df = pd.read_csv(stats_file)
    
    sheet_to_date = {}
    for _, row in stats_df.iterrows():
        sheet_name = str(row['Аркуш']).strip()
        date_str = str(row['Дата прийому']).strip()
        
        if sheet_name and sheet_name != 'nan' and date_str and date_str != 'nan':
            try:
                visit_date = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
                sheet_to_date[sheet_name] = visit_date
            except ValueError:
                continue
    
    files = sorted([f for f in os.listdir(DAILY_SHEETS_CACHE_DIR) 
                    if f.endswith('.csv') and f != '_stats.csv'])
    
    if not files:
        logger.warning("Немає щоденних аркушів для обробки")
        return False
    
    attendance_points = []
    
    for filename in files:
        try:
            file_date_obj = datetime.datetime.strptime(filename[:-4], "%Y-%m-%d").date()
            sheet_name = file_date_obj.strftime("%d.%m.%Y")
        except:
            continue
        
        if sheet_name not in sheet_to_date:
            continue
        
        visit_date = sheet_to_date[sheet_name]
        filepath = os.path.join(DAILY_SHEETS_CACHE_DIR, filename)
        attended_data = extract_attended_ids_from_sheet(filepath)
        
        if attended_data:
            for person_data in attended_data:
                attendance_points.append({
                    'date': visit_date.strftime('%Y-%m-%d'),
                    'id': person_data['id'],
                    'is_live': person_data['is_live']
                })
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'attendance_points': attendance_points,
                'total_points': len(attendance_points)
            }, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Згенеровано {len(attendance_points)} точок у файл {output_file}")
        return True
    except Exception as e:
        logger.error(f"Помилка збереження {output_file}: {e}")
        return False


def load_attendance_from_json(json_file='attendance_data.json'):
    """
    Завантажує дані відвідуваності з JSON файлу.
    """
    import json
    
    try:
        if not os.path.exists(json_file):
            logger.warning(f"Файл {json_file} не знайдено")
            return None
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Завантажено {data.get('total_points', 0)} точок з {json_file}")
        return data
    except Exception as e:
        logger.error(f"Помилка завантаження {json_file}: {e}")
        return None

