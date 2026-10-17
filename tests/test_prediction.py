"""
Тести для vlk_bot/prediction.py.
"""

import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import numpy as np

from vlk_bot import prediction


class TestDayStats:
    """Тести для DayStats dataclass."""

    def test_create_day_stats(self):
        """Створення DayStats."""
        stats = prediction.DayStats(
            date='2025-12-25',
            positions_processed=10,
            entered_scheduled=8,
            entered_live=2,
            no_show_count=1,
            postponed_count=0,
            not_reached_count=1,
            last_entered_id='1234'
        )
        assert stats.date == '2025-12-25'
        assert stats.positions_processed == 10


class TestQueueEntry:
    """Тести для QueueEntry dataclass."""

    def test_create_queue_entry(self):
        """Створення QueueEntry."""
        entry = prediction.QueueEntry(
            position=1,
            queue_id='1234',
            status='Ухвалено',
            notes='Примітка'
        )
        assert entry.position == 1
        assert entry.queue_id == '1234'


class TestCalculatePrediction:
    """Тести для calculate_prediction."""

    def test_with_error(self):
        """Помилка при розрахунку."""
        with patch('vlk_bot.sync.sync_daily_sheets', side_effect=Exception("Test")), \
             patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.STATS_SHEET_ID', 'test_id'), \
             patch('vlk_bot.config.STATS_WORKSHEET_NAME', 'Stats'):
            result = prediction.calculate_prediction(1234)
            assert result is None


class TestCalculatePredictionFromAttendanceJson:
    """Тести для calculate_prediction_from_attendance_json."""

    def test_insufficient_points(self):
        """Недостатньо точок."""
        attendance_data = {'attendance_points': []}
        result = prediction.calculate_prediction_from_attendance_json(1234, attendance_data)
        assert result is None

    def test_with_valid_data(self):
        """Валідні дані."""
        points = [
            {'date': f'2025-12-{10+i:02d}', 'id': str(1000 + i * 100), 'is_live': False}
            for i in range(10)
        ]
        attendance_data = {'attendance_points': points}
        
        with patch('vlk_bot.utils.id_to_numeric', side_effect=lambda x: int(x)):
            result = prediction.calculate_prediction_from_attendance_json(1500, attendance_data)


class TestCalculatePredictionWithDailyData:
    """Тести для calculate_prediction_with_daily_data."""

    def test_no_cache_files(self):
        """Немає файлів кешу."""
        with patch('vlk_bot.sync.load_attendance_from_json', return_value=None), \
             patch('vlk_bot.sync.get_historical_attendance_data', return_value=None):
            result = prediction.calculate_prediction_with_daily_data(1234, use_daily_sheets=True)
            assert result is None

    def test_with_attendance_json(self):
        """Є дані з attendance_data.json."""
        points = [
            {'date': f'2025-12-{10+i:02d}', 'id': str(1000 + i * 100), 'is_live': False}
            for i in range(10)
        ]
        attendance_data = {'attendance_points': points}

        with patch('vlk_bot.sync.load_attendance_from_json', return_value=attendance_data):
            result = prediction.calculate_prediction_with_daily_data(1500, use_daily_sheets=True)

    def test_with_historical_data(self):
        """Є історичні дані через get_historical_attendance_data."""
        hist_rows = []
        for i in range(10):
            hist_rows.append({
                'date': datetime.date(2025, 12, 10 + i),
                'attended_data': [
                    {'id': str(1000 + j * 10), 'is_live': False}
                    for j in range(5)
                ]
            })
        hist_df = pd.DataFrame(hist_rows)

        with patch('vlk_bot.sync.load_attendance_from_json', return_value=None), \
             patch('vlk_bot.sync.get_historical_attendance_data', return_value=hist_df):
            result = prediction.calculate_prediction_with_daily_data(1500, use_daily_sheets=True)


class TestCalculateDateProbability:
    """Тести для calculate_date_probability."""

    def test_basic_calculation(self):
        """Базовий розрахунок."""
        from scipy import stats as scipy_stats
        dist = scipy_stats.norm(loc=739000, scale=10)
        date = datetime.date(2025, 12, 25)
        
        result = prediction.calculate_date_probability(date, dist)
        assert 0 <= result <= 100

    def test_none_dist(self):
        """None distribution."""
        result = prediction.calculate_date_probability(datetime.date.today(), None)
        assert result == 0.0


class TestAnalyzeDay:
    """Тести для analyze_day."""

    def test_analyze_empty(self):
        """Порожній аналіз."""
        result = prediction.analyze_day([])
        assert result is None


class TestLoadHistoricalStats:
    """Тести для load_historical_stats."""

    def test_load_stats(self, tmp_path):
        """Завантаження статистики."""
        result = prediction.load_historical_stats(str(tmp_path))
        assert isinstance(result, list)


class TestGetLatestCsv:
    """Тести для get_latest_csv."""

    def test_no_files(self, tmp_path):
        """Немає файлів."""
        result = prediction.get_latest_csv(str(tmp_path))
        assert result is None

    def test_with_files(self, tmp_path):
        """Є файли."""
        csv_file = tmp_path / "2025-12-25.csv"
        df = pd.DataFrame({'col': [1]})
        df.to_csv(csv_file, index=False)
        
        result = prediction.get_latest_csv(str(tmp_path))
        assert result is not None


class TestCalculateDailyEntryProbability:
    """Тести для calculate_daily_entry_probability."""

    def test_basic(self):
        """Базовий розрахунок."""
        stats_df = pd.DataFrame({
            'Дата прийому': ['25.12.2025'],
            'Останній номер що зайшов': [100]
        })
        with patch.object(prediction, 'calculate_prediction', return_value=None):
            result = prediction.calculate_daily_entry_probability([1234], stats_df)


class TestCalculateMetrics:
    """Тести для calculate_metrics."""

    def test_basic(self):
        """Базовий розрахунок метрик."""
        stats = []
        for i in range(10):
            stats.append(prediction.DayStats(
                date=f'2025-12-{10+i:02d}',
                positions_processed=10,
                entered_scheduled=8,
                entered_live=2,
                no_show_count=1,
                postponed_count=0,
                not_reached_count=1,
                last_entered_id=str(1000 + i * 100)
            ))
        result = prediction.calculate_metrics(stats)
        assert isinstance(result, dict)

