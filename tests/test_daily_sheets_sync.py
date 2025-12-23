"""
Тести для vlk_bot.sync та пов'язаних модулів.
"""

import pytest
import datetime
import os
import json
from unittest.mock import patch, MagicMock
import pandas as pd
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from vlk_bot import sync
from vlk_bot.utils import id_to_numeric
from vlk_bot.prediction import calculate_prediction_from_attendance_json, calculate_prediction_with_daily_data


class TestIdToNumeric:
    def test_simple_integer(self):
        assert id_to_numeric("1234") == 1234.0

    def test_with_suffix(self):
        assert id_to_numeric("1234/1") == 1234.01
        assert id_to_numeric("1234/2") == 1234.02
        assert id_to_numeric("1234/10") == 1234.10

    def test_empty_string(self):
        assert id_to_numeric("") is None

    def test_non_numeric(self):
        assert id_to_numeric("abc") is None

    def test_mixed_with_digits_first(self):
        result = id_to_numeric("123abc")
        assert result == 123.0

    def test_whitespace(self):
        assert id_to_numeric("  1234  ") == 1234.0


class TestEnsureCacheDir:
    def test_creates_directory(self, tmp_path):
        cache_dir = tmp_path / "daily_sheets_cache"
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(cache_dir)):
            sync.ensure_cache_dir()
            assert cache_dir.exists()

    def test_already_exists(self, tmp_path):
        cache_dir = tmp_path / "daily_sheets_cache"
        cache_dir.mkdir()
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(cache_dir)):
            sync.ensure_cache_dir()
            assert cache_dir.exists()


class TestParseDailySheetAttendance:
    def test_basic_attendance(self, tmp_path, sample_daily_csv_content):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(sample_daily_csv_content, encoding='utf-8')
        
        result = sync.parse_daily_sheet_attendance(str(csv_file))
        
        assert result is not None
        assert 'total' in result
        assert 'attended' in result
        assert 'no_show' in result
        assert 'postponed' in result
        assert 'attendance_rate' in result

    def test_empty_file(self, tmp_path):
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("", encoding='utf-8')
        
        result = sync.parse_daily_sheet_attendance(str(csv_file))
        assert result is None

    def test_short_file(self, tmp_path):
        csv_file = tmp_path / "short.csv"
        csv_file.write_text("row1\nrow2\nrow3", encoding='utf-8')
        
        result = sync.parse_daily_sheet_attendance(str(csv_file))
        assert result is None


class TestExtractAttendedIds:
    def test_extract_basic(self, tmp_path, sample_daily_csv_content):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(sample_daily_csv_content, encoding='utf-8')
        
        result = sync.extract_attended_ids_from_sheet(str(csv_file))
        
        assert len(result) > 0
        for item in result:
            assert 'id' in item
            assert 'is_live' in item

    def test_empty_file(self, tmp_path):
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("", encoding='utf-8')
        
        result = sync.extract_attended_ids_from_sheet(str(csv_file))
        assert result == []

    def test_live_queue_detection(self, tmp_path):
        csv_content = """Дата прийому: 01.01.2024,,,,
Попередній прийом:,,,,
№,ID,Статус,Примітки,
1,100,Зайшов,,
2,101,Зайшов (за живою чергою),,
"""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content, encoding='utf-8')
        
        result = sync.extract_attended_ids_from_sheet(str(csv_file))
        
        live_entries = [r for r in result if r['is_live']]
        normal_entries = [r for r in result if not r['is_live']]
        
        assert len(live_entries) == 1
        assert len(normal_entries) == 1


class TestGenerateAttendanceJson:
    def test_generate_json(self, tmp_path, sample_daily_csv_content):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        
        csv_file = cache_dir / "2024-01-01.csv"
        csv_file.write_text(sample_daily_csv_content, encoding='utf-8')
        
        stats_content = """Аркуш,Дата прийому,Зайшов
01.01.2024,01.01.2024,14
"""
        stats_file = cache_dir / "_stats.csv"
        stats_file.write_text(stats_content, encoding='utf-8')
        
        output_file = tmp_path / "attendance.json"
        
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(cache_dir)):
            result = sync.generate_attendance_json(str(output_file))
        
        assert result is True
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        assert 'attendance_points' in data
        assert 'total_points' in data


class TestLoadAttendanceFromJson:
    def test_load_valid_json(self, tmp_path, sample_attendance_json):
        json_file = tmp_path / "attendance.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(sample_attendance_json, f)
        
        result = sync.load_attendance_from_json(str(json_file))
        
        assert result is not None
        assert 'attendance_points' in result
        assert result['total_points'] == 8

    def test_load_missing_file(self, tmp_path):
        result = sync.load_attendance_from_json(str(tmp_path / "nonexistent.json"))
        assert result is None

    def test_load_invalid_json(self, tmp_path):
        json_file = tmp_path / "invalid.json"
        json_file.write_text("not valid json", encoding='utf-8')
        
        result = sync.load_attendance_from_json(str(json_file))
        assert result is None


class TestCalculatePredictionFromAttendanceJson:
    def test_prediction_basic(self, sample_attendance_json):
        sample_attendance_json['attendance_points'] = [
            {'date': '2024-01-01', 'id': '100', 'is_live': False},
            {'date': '2024-01-02', 'id': '110', 'is_live': False},
            {'date': '2024-01-03', 'id': '120', 'is_live': False},
            {'date': '2024-01-04', 'id': '130', 'is_live': False},
            {'date': '2024-01-05', 'id': '140', 'is_live': False},
            {'date': '2024-01-08', 'id': '150', 'is_live': False},
        ]
        sample_attendance_json['total_points'] = 6
        
        result = calculate_prediction_from_attendance_json(200, sample_attendance_json)
        
        assert result is not None
        assert 'mean' in result
        assert 'l90' in result
        assert 'h90' in result
        assert 'dist' in result

    def test_prediction_insufficient_data(self):
        data = {
            'attendance_points': [
                {'date': '2024-01-01', 'id': '100', 'is_live': False},
                {'date': '2024-01-02', 'id': '110', 'is_live': False},
            ],
            'total_points': 2
        }
        
        result = calculate_prediction_from_attendance_json(200, data)
        assert result is None


class TestGetHistoricalAttendanceData:
    def test_get_data(self, tmp_path, sample_daily_csv_content):
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(tmp_path)):
            csv_file = tmp_path / "2024-01-01.csv"
            csv_file.write_text(sample_daily_csv_content, encoding='utf-8')
            
            result = sync.get_historical_attendance_data()
            
            assert result is not None
            assert 'date' in result.columns
            assert 'attended_ids' in result.columns

    def test_empty_cache(self, tmp_path):
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(tmp_path)):
            result = sync.get_historical_attendance_data()
            assert result is None


class TestDownloadStats:
    def test_download_success(self, tmp_path, mock_sheets_service):
        mock_sheets_service.spreadsheets().values().get().execute.return_value = {
            'values': [
                ['Аркуш', 'Дата прийому', 'Зайшов'],
                ['01.01.2024', '01.01.2024', '14'],
                ['02.01.2024', '02.01.2024', '15'],
            ]
        }
        
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(tmp_path)):
            result = sync.download_stats(mock_sheets_service, 'sheet_id', 'Stats')
        
        assert result is not None
        assert len(result) == 2

    def test_download_empty(self, tmp_path, mock_sheets_service):
        mock_sheets_service.spreadsheets().values().get().execute.return_value = {
            'values': []
        }
        
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(tmp_path)):
            result = sync.download_stats(mock_sheets_service, 'sheet_id', 'Stats')
        
        assert result is None


class TestDownloadDailySheet:
    def test_download_success(self, tmp_path, mock_sheets_service):
        mock_sheets_service.spreadsheets().values().get().execute.return_value = {
            'values': [
                ['row1', 'data1'],
                ['row2', 'data2'],
            ]
        }
        
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(tmp_path)):
            result = sync.download_daily_sheet(mock_sheets_service, 'sheet_id', '01.01.2024')
        
        assert result is True
        assert (tmp_path / "2024-01-01.csv").exists()

    def test_download_invalid_date(self, tmp_path, mock_sheets_service):
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(tmp_path)):
            result = sync.download_daily_sheet(mock_sheets_service, 'sheet_id', 'invalid_date')
        
        assert result is False

    def test_download_empty_sheet(self, tmp_path, mock_sheets_service):
        mock_sheets_service.spreadsheets().values().get().execute.return_value = {
            'values': []
        }
        
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(tmp_path)):
            result = sync.download_daily_sheet(mock_sheets_service, 'sheet_id', '01.01.2024')
        
        assert result is False


class TestSyncDailySheets:
    def test_sync_success(self, tmp_path, mock_sheets_service):
        mock_sheets_service.spreadsheets().values().get().execute.return_value = {
            'values': [
                ['Аркуш', 'Дата прийому', 'Зайшов'],
                ['01.01.2024', '01.01.2024', '14'],
            ]
        }
        
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(tmp_path)):
            with patch('vlk_bot.sync.generate_attendance_json', return_value=True):
                result = sync.sync_daily_sheets(
                    mock_sheets_service, 
                    'sheet_id', 
                    'Stats',
                    force_refresh_stats=True
                )
        
        assert result is True

    def test_sync_with_cache(self, tmp_path, mock_sheets_service):
        with patch.object(sync, 'DAILY_SHEETS_CACHE_DIR', str(tmp_path)):
            with patch('os.path.exists', return_value=True):
                with patch('os.path.getmtime', return_value=datetime.datetime.now().timestamp()):
                    result = sync.sync_daily_sheets(
                        mock_sheets_service, 
                        'sheet_id', 
                        'Stats',
                        force_refresh_stats=False
                    )
            
            assert result is True


class TestCalculatePredictionWithDailyData:
    def test_prediction_disabled(self):
        result = calculate_prediction_with_daily_data(100, use_daily_sheets=False)
        assert result is None

    def test_prediction_with_json_cache(self, sample_attendance_json):
        sample_attendance_json['attendance_points'] = [
            {'date': '2024-01-01', 'id': '100', 'is_live': False},
            {'date': '2024-01-02', 'id': '110', 'is_live': False},
            {'date': '2024-01-03', 'id': '120', 'is_live': False},
            {'date': '2024-01-04', 'id': '130', 'is_live': False},
            {'date': '2024-01-05', 'id': '140', 'is_live': False},
            {'date': '2024-01-08', 'id': '150', 'is_live': False},
        ]
        sample_attendance_json['total_points'] = 6
        
        with patch('vlk_bot.sync.load_attendance_from_json', return_value=sample_attendance_json):
            result = calculate_prediction_with_daily_data(200, use_daily_sheets=True, use_json_cache=True)
        
        assert result is not None
        assert result.get('data_source') == 'attendance_json'

    def test_prediction_no_cache(self):
        with patch('vlk_bot.sync.load_attendance_from_json', return_value=None):
            with patch('vlk_bot.sync.get_historical_attendance_data', return_value=None):
                result = calculate_prediction_with_daily_data(100, use_daily_sheets=True, use_json_cache=True)
        
        assert result is None
