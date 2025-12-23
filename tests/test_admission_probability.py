"""
Тести для admission_probability.py
"""

import pytest
import datetime
import os
import tempfile
import csv
from unittest.mock import patch, MagicMock
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import admission_probability as ap


class TestDataClasses:
    def test_day_stats_creation(self):
        stats = ap.DayStats(
            date='2024-01-01',
            positions_processed=15,
            entered_scheduled=12,
            entered_live=3,
            no_show_count=2,
            postponed_count=1,
            not_reached_count=0,
            last_entered_id='115'
        )
        assert stats.date == '2024-01-01'
        assert stats.positions_processed == 15
        assert stats.entered_scheduled == 12

    def test_queue_entry_creation(self):
        entry = ap.QueueEntry(
            position=5,
            queue_id='123',
            status='Зайшов',
            notes=''
        )
        assert entry.position == 5
        assert entry.queue_id == '123'
        assert entry.status == 'Зайшов'

    def test_todo_entry_creation(self):
        entry = ap.TodoEntry(
            seq_num=1,
            queue_id='456',
            scheduled_date=datetime.datetime(2025, 1, 15),
            notes='test'
        )
        assert entry.seq_num == 1
        assert entry.queue_id == '456'
        assert entry.scheduled_date == datetime.datetime(2025, 1, 15)


class TestParsingFunctions:
    def test_parse_left_section(self, tmp_path):
        csv_content = """Дата прийому: 01.01.2024,,,,
Попередній прийом:,,,,
№,ID,Статус,Примітки,
1,100,Зайшов,,
2,101,Зайшов,,
3,102,Не з'явився,,
4,103,Зайшов,,
5,104,Відклав,,
"""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content, encoding='utf-8')
        
        entries = ap.parse_left_section(str(csv_file))
        
        assert len(entries) == 5
        assert entries[0].position == 1
        assert entries[0].queue_id == '100'
        assert entries[0].status == 'Зайшов'

    def test_parse_right_section(self, tmp_path):
        csv_content = """Дата прийому: 01.01.2024,,,,Наступний прийом: 02.01.2024,,,
Попередній прийом:,,,,,Поточна черга:,,
№,ID,Статус,Примітки,,№,Примітки,ID,Статус
1,100,Зайшов,,,1,,150,
2,101,Зайшов,,,2,,151,
3,102,Зайшов,,,3,,152,
"""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content, encoding='utf-8')
        
        entries = ap.parse_right_section(str(csv_file))
        
        assert len(entries) == 3
        assert entries[0].position == 1
        assert entries[0].queue_id == '150'

    def test_parse_left_section_empty_file(self, tmp_path):
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("", encoding='utf-8')
        
        entries = ap.parse_left_section(str(csv_file))
        assert entries == []

    def test_parse_left_section_invalid_positions(self, tmp_path):
        csv_content = """№,ID,Статус,Примітки
invalid,100,Зайшов,
,101,Зайшов,
1,102,Зайшов,
"""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content, encoding='utf-8')
        
        entries = ap.parse_left_section(str(csv_file))
        assert len(entries) == 1
        assert entries[0].position == 1


class TestAnalyzeDay:
    def test_analyze_day_basic(self):
        entries = [
            ap.QueueEntry(1, '100', 'Зайшов', ''),
            ap.QueueEntry(2, '101', 'Зайшов', ''),
            ap.QueueEntry(3, '102', "Не з'явився", ''),
            ap.QueueEntry(4, '103', 'Зайшов', ''),
            ap.QueueEntry(5, '104', 'Відклав', ''),
        ]
        
        stats = ap.analyze_day(entries)
        
        assert stats is not None
        assert stats.positions_processed == 4
        assert stats.entered_scheduled == 3
        assert stats.no_show_count == 1
        assert stats.postponed_count == 1

    def test_analyze_day_empty(self):
        stats = ap.analyze_day([])
        assert stats is None

    def test_analyze_day_with_live_queue(self):
        entries = [
            ap.QueueEntry(1, '100', 'Зайшов', ''),
            ap.QueueEntry(2, '101', 'Зайшов', ''),
            ap.QueueEntry(31, '200', 'Зайшов (за живою чергою)', ''),
            ap.QueueEntry(32, '201', 'Зайшов (за живою чергою)', ''),
        ]
        
        stats = ap.analyze_day(entries)
        
        assert stats.entered_scheduled == 2
        assert stats.entered_live == 2

    def test_analyze_day_with_not_reached(self):
        entries = [
            ap.QueueEntry(1, '100', 'Зайшов', ''),
            ap.QueueEntry(2, '101', 'Зайшов', ''),
            ap.QueueEntry(3, '102', 'Зайшов', ''),
            ap.QueueEntry(4, '103', 'Не зайшов', ''),
            ap.QueueEntry(5, '104', 'Не зайшов', ''),
        ]
        
        stats = ap.analyze_day(entries)
        
        assert stats.positions_processed == 3
        assert stats.not_reached_count == 2


class TestMetricsCalculation:
    def test_calculate_metrics_basic(self):
        stats_list = [
            ap.DayStats('2024-01-01', 15, 12, 3, 2, 1, 0, '115'),
            ap.DayStats('2024-01-02', 14, 11, 3, 1, 2, 0, '129'),
            ap.DayStats('2024-01-03', 16, 13, 3, 2, 1, 0, '145'),
        ]
        
        metrics = ap.calculate_metrics(stats_list)
        
        assert 'avg_positions_processed' in metrics
        assert 'std_positions_processed' in metrics
        assert 'no_show_rate' in metrics
        assert 'total_days' in metrics
        assert metrics['total_days'] == 3
        assert 14 <= metrics['avg_positions_processed'] <= 16

    def test_calculate_metrics_empty(self):
        metrics = ap.calculate_metrics([])
        assert metrics == {}

    def test_calculate_metrics_single_day(self):
        stats_list = [
            ap.DayStats('2024-01-01', 15, 12, 3, 2, 1, 0, '115'),
        ]
        
        metrics = ap.calculate_metrics(stats_list)
        
        assert metrics['avg_positions_processed'] == 15
        assert metrics['std_positions_processed'] == 0
        assert metrics['total_days'] == 1


class TestWorkingDays:
    def test_get_working_days_basic(self):
        start = datetime.datetime(2025, 1, 6)
        days = ap.get_working_days(start, 5)
        
        assert len(days) == 5
        for d in days:
            assert d.weekday() < 5

    def test_get_working_days_starts_weekend(self):
        saturday = datetime.datetime(2025, 1, 4)
        days = ap.get_working_days(saturday, 3)
        
        assert len(days) == 3
        for d in days:
            assert d.weekday() < 5


class TestTodoFunctions:
    def test_count_todo_entries_for_date(self):
        todo_list = [
            ap.TodoEntry(1, '100', datetime.datetime(2025, 1, 15), ''),
            ap.TodoEntry(2, '101', datetime.datetime(2025, 1, 15), ''),
            ap.TodoEntry(3, '102', datetime.datetime(2025, 1, 16), ''),
        ]
        
        count = ap.count_todo_entries_for_date(todo_list, datetime.datetime(2025, 1, 15))
        assert count == 2

    def test_count_todo_entries_for_date_no_match(self):
        todo_list = [
            ap.TodoEntry(1, '100', datetime.datetime(2025, 1, 15), ''),
        ]
        
        count = ap.count_todo_entries_for_date(todo_list, datetime.datetime(2025, 1, 20))
        assert count == 0

    def test_count_todo_entries_before_date(self):
        todo_list = [
            ap.TodoEntry(1, '100', datetime.datetime(2025, 1, 10), ''),
            ap.TodoEntry(2, '101', datetime.datetime(2025, 1, 12), ''),
            ap.TodoEntry(3, '102', datetime.datetime(2025, 1, 15), ''),
            ap.TodoEntry(4, '103', datetime.datetime(2025, 1, 20), ''),
        ]
        
        count = ap.count_todo_entries_before_date(todo_list, datetime.datetime(2025, 1, 15))
        assert count == 2

    def test_count_todo_entries_before_date_with_after(self):
        todo_list = [
            ap.TodoEntry(1, '100', datetime.datetime(2025, 1, 10), ''),
            ap.TodoEntry(2, '101', datetime.datetime(2025, 1, 12), ''),
            ap.TodoEntry(3, '102', datetime.datetime(2025, 1, 14), ''),
        ]
        
        count = ap.count_todo_entries_before_date(
            todo_list,
            datetime.datetime(2025, 1, 15),
            after_date=datetime.datetime(2025, 1, 11)
        )
        assert count == 2


class TestNormalCDF:
    def test_normal_cdf_zero(self):
        result = ap._normal_cdf(0)
        assert abs(result - 0.5) < 0.01

    def test_normal_cdf_positive(self):
        result = ap._normal_cdf(2)
        assert result > 0.95

    def test_normal_cdf_negative(self):
        result = ap._normal_cdf(-2)
        assert result < 0.05


class TestAdmissionProbability:
    def test_calculate_admission_probability_basic(self):
        queue = [
            ap.QueueEntry(1, '150', '', ''),
            ap.QueueEntry(2, '151', '', ''),
            ap.QueueEntry(3, '152', '', ''),
        ]
        
        metrics = {
            'avg_positions_processed': 15,
            'std_positions_processed': 3,
            'no_show_rate': 0.2,
        }
        
        base_date = datetime.datetime(2025, 1, 6)
        
        results = ap.calculate_admission_probability(queue, metrics, [], base_date, 3)
        
        assert len(results) == 3
        assert results[0]['position'] == 1
        assert results[0]['queue_id'] == '150'
        assert 'day_probabilities' in results[0]
        assert len(results[0]['day_probabilities']) == 3

    def test_calculate_admission_probability_high_position(self):
        queue = [
            ap.QueueEntry(50, '200', '', ''),
        ]
        
        metrics = {
            'avg_positions_processed': 15,
            'std_positions_processed': 3,
            'no_show_rate': 0.2,
        }
        
        base_date = datetime.datetime(2025, 1, 6)
        
        results = ap.calculate_admission_probability(queue, metrics, [], base_date, 5)
        
        assert len(results) == 1
        probs = results[0]['day_probabilities']
        for i in range(len(probs) - 1):
            assert probs[i] <= probs[i + 1]


class TestGetLatestCSV:
    def test_get_latest_csv(self, tmp_path):
        (tmp_path / "2024-01-01.csv").write_text("test", encoding='utf-8')
        (tmp_path / "2024-01-02.csv").write_text("test", encoding='utf-8')
        (tmp_path / "2024-01-03.csv").write_text("test", encoding='utf-8')
        (tmp_path / "_stats.csv").write_text("test", encoding='utf-8')
        
        result = ap.get_latest_csv(str(tmp_path))
        
        assert result is not None
        assert "2024-01-03.csv" in result

    def test_get_latest_csv_empty_dir(self, tmp_path):
        result = ap.get_latest_csv(str(tmp_path))
        assert result is None

    def test_get_latest_csv_only_stats(self, tmp_path):
        (tmp_path / "_stats.csv").write_text("test", encoding='utf-8')
        
        result = ap.get_latest_csv(str(tmp_path))
        assert result is None


class TestLoadHistoricalStats:
    def test_load_historical_stats(self, tmp_path, sample_daily_csv_content):
        csv_file = tmp_path / "2024-01-01.csv"
        csv_file.write_text(sample_daily_csv_content, encoding='utf-8')
        
        stats = ap.load_historical_stats(str(tmp_path))
        
        assert len(stats) == 1
        assert stats[0].date == '2024-01-01'

    def test_load_historical_stats_skips_stats_file(self, tmp_path, sample_daily_csv_content):
        (tmp_path / "_stats.csv").write_text("test", encoding='utf-8')
        csv_file = tmp_path / "2024-01-01.csv"
        csv_file.write_text(sample_daily_csv_content, encoding='utf-8')
        
        stats = ap.load_historical_stats(str(tmp_path))
        
        assert len(stats) == 1

    def test_load_historical_stats_skips_invalid_dates(self, tmp_path, sample_daily_csv_content):
        (tmp_path / "invalid.csv").write_text("test", encoding='utf-8')
        csv_file = tmp_path / "2024-01-01.csv"
        csv_file.write_text(sample_daily_csv_content, encoding='utf-8')
        
        stats = ap.load_historical_stats(str(tmp_path))
        
        assert len(stats) == 1


class TestFormatResults:
    def test_format_results_basic(self):
        results = [
            {
                'position': 1,
                'queue_id': '150',
                'effective_position': 0.8,
                'estimated_days': 0.1,
                'day_positions': [1, 0, 0, 0, 0],
                'day_effective_positions': [0.8, 0, 0, 0, 0],
                'day_probabilities': [95.0, 100.0, 100.0, 100.0, 100.0],
                'notes': ''
            }
        ]
        
        metrics = {
            'total_days': 10,
            'avg_positions_processed': 15.0,
            'std_positions_processed': 3.0,
            'min_positions': 10,
            'max_positions': 20,
            'no_show_rate': 0.2
        }
        
        working_days = [
            datetime.datetime(2025, 1, 7),
            datetime.datetime(2025, 1, 8),
            datetime.datetime(2025, 1, 9),
            datetime.datetime(2025, 1, 10),
            datetime.datetime(2025, 1, 13),
        ]
        
        todo_counts = {}
        
        output = ap.format_results(results, metrics, working_days, todo_counts)
        
        assert "РОЗРАХУНОК ЙМОВІРНОСТІ ПРИЙОМУ" in output
        assert "150" in output


class TestFetchTodoList:
    def test_fetch_todo_list_success(self):
        mock_response = MagicMock()
        mock_response.read.return_value = b"1,100,15.01.2025,note\n2,101,16.01.2025,\n"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        
        with patch('urllib.request.urlopen', return_value=mock_response):
            entries = ap.fetch_todo_list()
            
            assert len(entries) == 2
            assert entries[0].queue_id == '100'
            assert entries[1].queue_id == '101'

    def test_fetch_todo_list_error(self):
        with patch('urllib.request.urlopen', side_effect=Exception("Network error")):
            entries = ap.fetch_todo_list()
            assert entries == []

