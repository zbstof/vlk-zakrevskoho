#!/usr/bin/env python3
"""
Скрипт для швидкого запуску розрахунку ймовірності прийому.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vlk_bot.prediction import load_historical_stats, calculate_metrics


def main():
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cache_dir = os.path.join(script_dir, 'daily_sheets_cache')
    
    print("Завантаження історичних даних...")
    historical_stats = load_historical_stats(cache_dir)
    
    if not historical_stats:
        print("Немає історичних даних")
        return
    
    metrics = calculate_metrics(historical_stats)
    
    print()
    print("=" * 50)
    print("СТАТИСТИКА ПРИЙОМУ")
    print("=" * 50)
    print()
    print(f"Всього днів: {metrics.get('total_days', 0)}")
    print(f"Середній прогрес за день: {metrics.get('avg_positions_processed', 0):.1f}")
    print(f"Стандартне відхилення: {metrics.get('std_positions_processed', 0):.1f}")
    print(f"Мінімум: {metrics.get('min_positions', 0)}")
    print(f"Максимум: {metrics.get('max_positions', 0)}")
    print(f"Відсоток неявок: {metrics.get('no_show_rate', 0) * 100:.1f}%")
    print(f"Середній вхід за живою чергою: {metrics.get('avg_live_entries', 0):.1f}")
    print()


if __name__ == '__main__':
    main()

