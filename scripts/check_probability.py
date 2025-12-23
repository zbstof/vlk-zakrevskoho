#!/usr/bin/env python3
"""
Скрипт для перевірки ймовірності прийому для конкретного ID.
Використання: python3 scripts/check_probability.py <ID>
"""

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vlk_bot.prediction import (
    load_historical_stats,
    calculate_metrics,
    get_latest_csv,
    parse_right_section,
)


def get_working_days(start_date, num_days):
    """Повертає список робочих днів."""
    result = []
    current = start_date
    while len(result) < num_days:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result


def main():
    if len(sys.argv) < 2:
        print("Використання: python3 scripts/check_probability.py <ID>")
        print("Приклад: python3 scripts/check_probability.py 4355")
        sys.exit(1)
    
    target_id = sys.argv[1].strip()
    
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cache_dir = os.path.join(script_dir, 'daily_sheets_cache')
    
    historical_stats = load_historical_stats(cache_dir)
    metrics = calculate_metrics(historical_stats)
    
    latest_csv = get_latest_csv(cache_dir)
    if not latest_csv:
        print("Помилка: не знайдено CSV файлів")
        sys.exit(1)
    
    csv_date_str = os.path.basename(latest_csv).replace('.csv', '')
    base_date = datetime.strptime(csv_date_str, '%Y-%m-%d')
    
    queue = parse_right_section(latest_csv)
    
    target_entry = None
    for entry in queue:
        if entry.queue_id == target_id:
            target_entry = entry
            break
    
    if not target_entry:
        print(f"ID {target_id} не знайдено в поточній черзі")
        sys.exit(1)
    
    working_days = get_working_days(base_date + timedelta(days=1), 5)
    
    print()
    print("=" * 70)
    print(f"ЙМОВІРНІСТЬ ПРИЙОМУ ДЛЯ ID: {target_id}")
    print("=" * 70)
    print()
    print(f"Поточна позиція в черзі: {target_entry.position}")
    print()
    print("Статистика:")
    print(f"  - Середній прогрес за день: {metrics.get('avg_positions_processed', 0):.1f} позицій")
    print(f"  - Відсоток неявок: {metrics.get('no_show_rate', 0) * 100:.1f}%")
    print()
    print("Робочі дні:")
    for wd in working_days:
        day_name = wd.strftime('%d.%m.%Y (%A)')
        day_name = day_name.replace('Monday', 'Пн')
        day_name = day_name.replace('Tuesday', 'Вт')
        day_name = day_name.replace('Wednesday', 'Ср')
        day_name = day_name.replace('Thursday', 'Чт')
        day_name = day_name.replace('Friday', "Пт")
        print(f"  {day_name}")
    print()


if __name__ == '__main__':
    main()

