"""
Модуль для прогнозування дати візиту.
"""

import datetime
import glob
import logging
import os
import statistics
from dataclasses import dataclass
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

logger = logging.getLogger(__name__)

DAILY_SHEETS_CACHE_DIR = "daily_sheets_cache"


@dataclass
class DayStats:
    """Статистика за один день прийому."""
    date: str
    positions_processed: int
    entered_scheduled: int
    entered_live: int
    no_show_count: int
    postponed_count: int
    not_reached_count: int
    last_entered_id: Optional[str]


@dataclass
class QueueEntry:
    """Запис у черзі."""
    position: int
    queue_id: str
    status: str
    notes: str


def calculate_prediction(user_id, stats_df=None):
    """
    Розраховує прогноз дати візиту для user_id.
    """
    try:
        from vlk_bot.config import SHEETS_SERVICE, STATS_SHEET_ID, STATS_WORKSHEET_NAME
        from vlk_bot.sync import sync_daily_sheets
        
        sync_daily_sheets(SHEETS_SERVICE, STATS_SHEET_ID, STATS_WORKSHEET_NAME)
        prediction = calculate_prediction_with_daily_data(user_id, use_daily_sheets=True)
        if prediction:
            logger.info(f"Використано прогноз з {prediction.get('data_points', 0)} точок даних")
            return prediction
    except Exception as e:
        logger.error(f"Помилка прогнозування: {e}")
    
    return None


def calculate_prediction_from_attendance_json(user_id, attendance_data):
    """
    Розраховує прогноз на основі даних з attendance_data.json.
    """
    from vlk_bot.utils import get_ordinal_date, get_date_from_ordinal, id_to_numeric
    
    points = attendance_data.get('attendance_points', [])
    if len(points) < 5:
        return None
    
    processed_points = []
    for point in points:
        try:
            date_obj = datetime.datetime.strptime(point['date'], '%Y-%m-%d').date()
            ordinal = get_ordinal_date(date_obj)
            
            numeric_id = id_to_numeric(point['id'])
            if numeric_id is None:
                continue
                
            processed_points.append({
                'id': numeric_id,
                'ordinal': ordinal,
                'is_live': point.get('is_live', False)
            })
        except:
            continue
    
    if len(processed_points) < 5:
        return None
    
    points_df = pd.DataFrame(processed_points)
    
    id_groups = points_df.groupby('id').agg({
        'ordinal': 'mean',
        'is_live': 'max'
    }).reset_index()
    daily_stats = id_groups.sort_values('ordinal')
    
    X = daily_stats['id'].values
    Y = daily_stats['ordinal'].values
    is_live_mask = daily_stats['is_live'].values
    n = len(X)
    
    if n < 5:
        return None
    
    weightExpMin = -3
    weightExpMax = 1
    liveQueueWeight = 0
    
    weights = np.exp(weightExpMin + (np.arange(n) / (n - 1)) * (weightExpMax - weightExpMin))
    weights = np.where(is_live_mask, weights * liveQueueWeight, weights)
    
    sumW = np.sum(weights)
    sumWX = np.sum(weights * X)
    sumWY = np.sum(weights * Y)
    sumWXX = np.sum(weights * X**2)
    sumWXY = np.sum(weights * X * Y)
    
    denom = sumW * sumWXX - sumWX**2
    if denom == 0:
        return None
    
    slope = (sumW * sumWXY - sumWX * sumWY) / denom
    intercept = (sumWY - slope * sumWX) / sumW
    
    weightedMeanX = sumWX / sumW
    weightedVarX = np.sum(weights * (X - weightedMeanX)**2)
    
    yPred = slope * X + intercept
    residuals = Y - yPred
    weightedSumResSq = np.sum(weights * residuals**2)
    
    dof = sumW - 2
    if dof <= 0:
        return None
    
    mseWeighted = weightedSumResSq / dof
    
    tScore90 = scipy_stats.t.ppf(0.95, dof)
    tScore50 = scipy_stats.t.ppf(0.75, dof)
    
    predOrd = slope * user_id + intercept
    
    term3 = (user_id - weightedMeanX)**2 / weightedVarX
    sePred = np.sqrt(mseWeighted * (1 + 1/sumW + term3))
    
    margin90 = tScore90 * sePred
    margin50 = tScore50 * sePred
    
    l90_ord = predOrd - margin90
    h90_ord = predOrd + margin90
    l50_ord = predOrd - margin50
    h50_ord = predOrd + margin50
    
    max_hist_ord = points_df['ordinal'].max()
    min_feasible = max_hist_ord + 1
    
    if user_id > daily_stats['id'].max():
        l90_ord = max(l90_ord, min_feasible)
        l50_ord = max(l50_ord, min_feasible)
    
    return {
        'l90': get_date_from_ordinal(l90_ord),
        'l50': get_date_from_ordinal(l50_ord),
        'mean': get_date_from_ordinal(predOrd),
        'h50': get_date_from_ordinal(h50_ord),
        'h90': get_date_from_ordinal(h90_ord),
        'dist': {
            'loc': predOrd,
            'scale': sePred,
            'df': dof
        },
        'data_points': len(processed_points),
        'data_source': 'attendance_json'
    }


def calculate_prediction_with_daily_data(user_id, use_daily_sheets=True, use_json_cache=True):
    """
    Розраховує прогноз дати візиту використовуючи детальні дані зі щоденних аркушів.
    """
    from vlk_bot.utils import get_ordinal_date, get_date_from_ordinal, id_to_numeric
    from vlk_bot.sync import load_attendance_from_json, get_historical_attendance_data
    
    if not use_daily_sheets:
        return None
    
    if use_json_cache:
        attendance_data = load_attendance_from_json()
        if attendance_data:
            return calculate_prediction_from_attendance_json(user_id, attendance_data)
    
    hist_df = get_historical_attendance_data()
    
    if hist_df is None or len(hist_df) < 5:
        return None
    
    points = []
    
    for _, row in hist_df.iterrows():
        date_ordinal = get_ordinal_date(row['date'])
        for attended_item in row['attended_data']:
            numeric_id = id_to_numeric(attended_item['id'])
            if numeric_id is None:
                continue
                
            points.append({
                'id': numeric_id,
                'ordinal': date_ordinal,
                'is_live': attended_item['is_live']
            })
    
    if len(points) < 5:
        return None
    
    points_df = pd.DataFrame(points)
    
    id_groups = points_df.groupby('id').agg({
        'ordinal': 'mean',
        'is_live': 'max'
    }).reset_index()
    daily_stats = id_groups.sort_values('ordinal')
    
    X = daily_stats['id'].values
    Y = daily_stats['ordinal'].values
    is_live_mask = daily_stats['is_live'].values
    n = len(X)
    
    if n < 5:
        return None
    
    weightExpMin = -3
    weightExpMax = 1
    liveQueueWeight = 0
    
    weights = np.exp(weightExpMin + (np.arange(n) / (n - 1)) * (weightExpMax - weightExpMin))
    weights = np.where(is_live_mask, weights * liveQueueWeight, weights)
    
    sumW = np.sum(weights)
    sumWX = np.sum(weights * X)
    sumWY = np.sum(weights * Y)
    sumWXX = np.sum(weights * X**2)
    sumWXY = np.sum(weights * X * Y)
    
    denom = sumW * sumWXX - sumWX**2
    if denom == 0:
        return None
    
    slope = (sumW * sumWXY - sumWX * sumWY) / denom
    intercept = (sumWY - slope * sumWX) / sumW
    
    weightedMeanX = sumWX / sumW
    weightedVarX = np.sum(weights * (X - weightedMeanX)**2)
    
    yPred = slope * X + intercept
    residuals = Y - yPred
    weightedSumResSq = np.sum(weights * residuals**2)
    
    dof = sumW - 2
    if dof <= 0:
        return None
    
    mseWeighted = weightedSumResSq / dof
    
    tScore90 = scipy_stats.t.ppf(0.95, dof)
    tScore50 = scipy_stats.t.ppf(0.75, dof)
    
    predOrd = slope * user_id + intercept
    
    term3 = (user_id - weightedMeanX)**2 / weightedVarX
    sePred = np.sqrt(mseWeighted * (1 + 1/sumW + term3))
    
    margin90 = tScore90 * sePred
    margin50 = tScore50 * sePred
    
    l90_ord = predOrd - margin90
    h90_ord = predOrd + margin90
    l50_ord = predOrd - margin50
    h50_ord = predOrd + margin50
    
    max_hist_ord = points_df['ordinal'].max()
    min_feasible = max_hist_ord + 1
    
    if user_id > daily_stats['id'].max():
        l90_ord = max(l90_ord, min_feasible)
        l50_ord = max(l50_ord, min_feasible)
    
    return {
        'l90': get_date_from_ordinal(l90_ord),
        'l50': get_date_from_ordinal(l50_ord),
        'mean': get_date_from_ordinal(predOrd),
        'h50': get_date_from_ordinal(h50_ord),
        'h90': get_date_from_ordinal(h90_ord),
        'dist': {
            'loc': predOrd,
            'scale': sePred,
            'df': dof
        },
        'data_points': len(points),
        'data_source': 'daily_sheets',
        'using_daily_sheets': True
    }


def calculate_date_probability(date_obj, dist):
    """
    Обчислює кумулятивну ймовірність того, що черга настане до кінця вказаної дати.
    Повертає ймовірність у відсотках (0-100).
    """
    from vlk_bot.utils import get_ordinal_date
    
    try:
        ordinal = get_ordinal_date(date_obj)
        loc = dist['loc']
        scale = dist['scale']
        df = dist['df']
        # Використовуємо ordinal + 1, оскільки ordinal представляє початок дня,
        # і ми хочемо отримати ймовірність того, що черга настане ДО кінця цього дня.
        prob = scipy_stats.t.cdf(ordinal + 1, df, loc=loc, scale=scale)
        return prob * 100
    except Exception as e:
        logger.error(f"Помилка обчислення ймовірності для {date_obj}: {e}")
        return 0.0


def calculate_daily_entry_probability(tomorrow_ids: list, stats_df: pd.DataFrame, 
                                       target_date: datetime.date = None) -> dict:
    """
    Розраховує ймовірність проходження для списку ID.
    """
    from vlk_bot.utils import extract_main_id
    
    if stats_df is None or stats_df.empty:
        return {uid: 0.0 for uid in tomorrow_ids}
    
    if target_date is None:
        target_date = datetime.date.today() + datetime.timedelta(days=1)
    
    try:
        probabilities = {}
        
        for rank, uid in enumerate(tomorrow_ids, start=1):
            main_id = extract_main_id(uid)
            
            prediction = calculate_prediction(main_id, stats_df)
            
            if prediction and 'dist' in prediction:
                prob = calculate_date_probability(target_date, prediction['dist'])
                probabilities[uid] = round(prob, 1)
            else:
                target_col = 'Зайшов'
                counts = pd.to_numeric(stats_df[target_col], errors='coerce').dropna()
                counts = counts[counts > 0]
                counts = counts.tail(10)
                
                if counts.empty:
                    probabilities[uid] = 0.0
                else:
                    total_days = len(counts)
                    days_covered = (counts >= rank).sum()
                    prob = (days_covered / total_days) * 100
                    probabilities[uid] = round(prob, 1)
        
        return probabilities
        
    except Exception as e:
        logger.error(f"Помилка розрахунку ймовірності входу: {e}")
        return {uid: 0.0 for uid in tomorrow_ids}


def load_historical_stats(cache_dir: str) -> List[DayStats]:
    """
    Завантажує історичну статистику з усіх CSV файлів.
    """

    stats = []
    csv_files = sorted(glob.glob(os.path.join(cache_dir, '*.csv')))
    
    for csv_path in csv_files:
        filename = os.path.basename(csv_path)
        
        if filename.startswith('_'):
            continue
        
        try:
            date_str = filename.replace('.csv', '')
            datetime.datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            continue
        
        entries = parse_left_section(csv_path)
        day_stats = analyze_day(entries)
        
        if day_stats and day_stats.positions_processed > 0:
            day_stats.date = date_str
            stats.append(day_stats)
    
    return stats


def parse_left_section(csv_path: str) -> List[QueueEntry]:
    """
    Парсить ліву секцію CSV (результати попереднього дня).
    """
    import csv
    
    entries = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        lines = list(reader)
        
        for row in lines:
            if len(row) < 3:
                continue
            
            try:
                pos_str = row[0].strip() if row[0] else ''
                if not pos_str or not pos_str.isdigit():
                    continue
                
                position = int(pos_str)
                queue_id = row[1].strip() if len(row) > 1 else ''
                status = row[2].strip() if len(row) > 2 else ''
                notes = row[3].strip() if len(row) > 3 else ''
                
                if position > 0 and queue_id:
                    entries.append(QueueEntry(
                        position=position,
                        queue_id=queue_id,
                        status=status,
                        notes=notes
                    ))
            except (ValueError, IndexError):
                continue
    
    return entries


def parse_right_section(csv_path: str) -> List[QueueEntry]:
    """
    Парсить праву секцію CSV (поточна/майбутня черга).
    """
    import csv
    
    entries = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        lines = list(reader)
        
        for row in lines:
            if len(row) < 8:
                continue
            
            try:
                pos_str = row[5].strip() if row[5] else ''
                if not pos_str or not pos_str.isdigit():
                    continue
                
                position = int(pos_str)
                notes = row[6].strip() if len(row) > 6 else ''
                queue_id = row[7].strip() if len(row) > 7 else ''
                status = row[8].strip() if len(row) > 8 else ''
                
                if position > 0 and queue_id:
                    entries.append(QueueEntry(
                        position=position,
                        queue_id=queue_id,
                        status=status,
                        notes=notes
                    ))
            except (ValueError, IndexError):
                continue
    
    return entries


def analyze_day(entries: List[QueueEntry]) -> Optional[DayStats]:
    """
    Аналізує результати дня за коректним алгоритмом.
    """
    if not entries:
        return None
    
    first_30 = [e for e in entries if e.position <= 30]
    above_30 = [e for e in entries if e.position > 30]
    
    entered_in_30 = [e for e in first_30 if e.status == 'Зайшов']
    entered_live = [e for e in above_30 if 'за живою чергою' in e.status.lower()]
    
    if not entered_in_30:
        last_entered_pos = 0
        last_entered_id = None
    else:
        last_entered = max(entered_in_30, key=lambda x: x.position)
        last_entered_pos = last_entered.position
        last_entered_id = last_entered.queue_id
    
    no_show_count = 0
    for e in first_30:
        if e.position < last_entered_pos and e.status == "Не з'явився":
            no_show_count += 1
    
    postponed_count = len([e for e in first_30 if 'Відклав' in e.status])
    
    not_reached_count = len([e for e in first_30 
                             if e.position > last_entered_pos 
                             and 'Не зайшов' in e.status])
    
    return DayStats(
        date='',
        positions_processed=last_entered_pos,
        entered_scheduled=len(entered_in_30),
        entered_live=len(entered_live),
        no_show_count=no_show_count,
        postponed_count=postponed_count,
        not_reached_count=not_reached_count,
        last_entered_id=last_entered_id
    )


def calculate_metrics(stats: List[DayStats]) -> Dict:
    """
    Розраховує ключові метрики на основі історичних даних.
    """
    if not stats:
        return {}
    
    positions_processed = [s.positions_processed for s in stats]
    entered_counts = [s.entered_scheduled for s in stats]
    no_show_counts = [s.no_show_count for s in stats]
    live_counts = [s.entered_live for s in stats]
    
    total_should_enter = sum(s.positions_processed for s in stats)
    total_no_show = sum(no_show_counts)
    
    no_show_rate = total_no_show / total_should_enter if total_should_enter > 0 else 0
    
    return {
        'avg_positions_processed': statistics.mean(positions_processed),
        'std_positions_processed': statistics.stdev(positions_processed) if len(positions_processed) > 1 else 0,
        'avg_entered': statistics.mean(entered_counts),
        'std_entered': statistics.stdev(entered_counts) if len(entered_counts) > 1 else 0,
        'no_show_rate': no_show_rate,
        'avg_live_entries': statistics.mean(live_counts),
        'total_days': len(stats),
        'min_positions': min(positions_processed),
        'max_positions': max(positions_processed),
    }


def get_latest_csv(cache_dir: str) -> Optional[str]:
    """
    Знаходить найновіший CSV файл у кеші.
    """
    csv_files = []
    
    for f in glob.glob(os.path.join(cache_dir, '*.csv')):
        filename = os.path.basename(f)
        if filename.startswith('_'):
            continue
        try:
            date_str = filename.replace('.csv', '')
            date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            csv_files.append((date, f))
        except ValueError:
            continue
    
    if not csv_files:
        return None
    
    csv_files.sort(key=lambda x: x[0], reverse=True)
    return csv_files[0][1]

