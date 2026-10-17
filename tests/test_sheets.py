"""
Тести для vlk_bot/sheets.py.
"""

import datetime
import os
import time
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest
from googleapiclient.errors import HttpError

from vlk_bot import sheets


class TestExecuteWithRetry:
    """Тести для _execute_with_retry."""

    def test_success_first_attempt(self):
        """Успішний виклик з першої спроби."""
        mock_func = MagicMock(return_value="success")
        result = sheets._execute_with_retry("test_func", mock_func)
        assert result == "success"
        assert mock_func.call_count == 1

    def test_retry_on_connection_error(self):
        """Повторна спроба при помилці з'єднання."""
        mock_func = MagicMock(side_effect=[ConnectionError("test"), "success"])
        with patch.object(sheets, 'time') as mock_time:
            mock_time.time.return_value = 0.0
            result = sheets._execute_with_retry("test_func", mock_func)
        assert result == "success"
        assert mock_func.call_count == 2

    def test_retry_on_broken_pipe(self):
        """Повторна спроба при BrokenPipeError."""
        mock_func = MagicMock(side_effect=[BrokenPipeError("test"), "success"])
        with patch.object(sheets, 'time') as mock_time:
            mock_time.time.return_value = 0.0
            result = sheets._execute_with_retry("test_func", mock_func)
        assert result == "success"
        assert mock_func.call_count == 2

    def test_fail_after_max_retries(self):
        """Помилка після всіх спроб."""
        mock_func = MagicMock(side_effect=ConnectionError("persistent error"))
        with patch.object(sheets, 'time') as mock_time:
            mock_time.time.return_value = 0.0
            with pytest.raises(ConnectionError):
                sheets._execute_with_retry("test_func", mock_func)
        assert mock_func.call_count == 3


class TestLoadQueueData:
    """Тести для load_queue_data."""

    @patch('vlk_bot.sheets.RETRY_DELAYS', [0])
    def test_no_service(self):
        """Повертає None якщо сервіс не ініціалізовано."""
        with patch('vlk_bot.config.SHEETS_SERVICE', None):
            result = sheets.load_queue_data()
            assert result is None

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_empty_sheet(self, mock_retry):
        """Повертає порожній DataFrame для порожнього аркуша."""
        mock_retry.return_value = {'values': []}
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.SPREADSHEET_ID', 'test_id'), \
             patch('vlk_bot.config.SHEET_NAME', 'test_sheet'), \
             patch('vlk_bot.config.REQUIRED_COLUMNS', ['ID', 'Дата', 'Статус']):
            result = sheets.load_queue_data()
            assert isinstance(result, pd.DataFrame)
            assert result.empty
            assert list(result.columns) == ['ID', 'Дата', 'Статус']

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_load_data_success(self, mock_retry):
        """Успішне завантаження даних."""
        mock_retry.return_value = {
            'values': [
                ['ID', 'Дата', 'Статус'],
                ['1234', '25.12.2025', 'Ухвалено'],
                ['5678', '26.12.2025', 'На розгляді']
            ]
        }
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.SPREADSHEET_ID', 'test_id'), \
             patch('vlk_bot.config.SHEET_NAME', 'test_sheet'), \
             patch('vlk_bot.config.REQUIRED_COLUMNS', ['ID', 'Дата', 'Статус']):
            result = sheets.load_queue_data()
            assert len(result) == 2
            assert result.iloc[0]['ID'] == '1234'
            assert result.iloc[1]['Статус'] == 'На розгляді'

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_load_data_pads_short_rows(self, mock_retry):
        """Доповнення коротких рядків."""
        mock_retry.return_value = {
            'values': [
                ['ID', 'Дата', 'Статус'],
                ['1234']
            ]
        }
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.SPREADSHEET_ID', 'test_id'), \
             patch('vlk_bot.config.SHEET_NAME', 'test_sheet'), \
             patch('vlk_bot.config.REQUIRED_COLUMNS', ['ID', 'Дата', 'Статус']):
            result = sheets.load_queue_data()
            assert result.iloc[0]['ID'] == '1234'
            assert result.iloc[0]['Дата'] == ''
            assert result.iloc[0]['Статус'] == ''

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_load_data_truncates_long_rows(self, mock_retry):
        """Обрізання довгих рядків."""
        mock_retry.return_value = {
            'values': [
                ['ID', 'Дата', 'Статус'],
                ['1234', '25.12.2025', 'Ухвалено', 'Extra', 'More']
            ]
        }
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.SPREADSHEET_ID', 'test_id'), \
             patch('vlk_bot.config.SHEET_NAME', 'test_sheet'), \
             patch('vlk_bot.config.REQUIRED_COLUMNS', ['ID', 'Дата', 'Статус']):
            result = sheets.load_queue_data()
            assert len(result.columns) == 3


class TestSaveQueueData:
    """Тести для save_queue_data."""

    def test_no_service(self):
        """Повертає False якщо сервіс не ініціалізовано."""
        with patch('vlk_bot.config.SHEETS_SERVICE', None):
            df = pd.DataFrame({'ID': ['1234']})
            result = sheets.save_queue_data(df)
            assert result is False

    def test_empty_dataframe(self):
        """Повертає True для порожнього DataFrame."""
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()):
            df = pd.DataFrame()
            result = sheets.save_queue_data(df)
            assert result is True

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_save_success(self, mock_retry):
        """Успішне збереження."""
        mock_retry.return_value = {}
        df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': ['25.12.2025'],
            'Статус': ['Ухвалено']
        })
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.SPREADSHEET_ID', 'test_id'), \
             patch('vlk_bot.config.SHEET_NAME', 'test_sheet'), \
             patch('vlk_bot.config.REQUIRED_COLUMNS', ['ID', 'Дата', 'Статус']):
            result = sheets.save_queue_data(df)
            assert result is True


class TestSaveQueueDataFull:
    """Тести для save_queue_data_full."""

    def test_no_service(self):
        """Повертає False якщо сервіс не ініціалізовано."""
        with patch('vlk_bot.config.SHEETS_SERVICE', None):
            df = pd.DataFrame({'ID': ['1234']})
            result = sheets.save_queue_data_full(df)
            assert result is False

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_save_empty_dataframe(self, mock_retry):
        """Збереження порожнього DataFrame."""
        mock_retry.return_value = {}
        df = pd.DataFrame()
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.SPREADSHEET_ID', 'test_id'), \
             patch('vlk_bot.config.SHEET_NAME', 'test_sheet'), \
             patch('vlk_bot.config.REQUIRED_COLUMNS', ['ID', 'Дата', 'Статус']):
            result = sheets.save_queue_data_full(df)
            assert result is True
            assert mock_retry.call_count == 2

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_save_with_data(self, mock_retry):
        """Збереження з даними."""
        mock_retry.return_value = {}
        df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': ['25.12.2025'],
            'Статус': ['Ухвалено']
        })
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.SPREADSHEET_ID', 'test_id'), \
             patch('vlk_bot.config.SHEET_NAME', 'test_sheet'), \
             patch('vlk_bot.config.REQUIRED_COLUMNS', ['ID', 'Дата', 'Статус']):
            result = sheets.save_queue_data_full(df)
            assert result is True


class TestGetStatsData:
    """Тести для get_stats_data."""

    @pytest.mark.asyncio
    async def test_cached_data(self, tmp_path):
        """Використання кешованих даних."""
        cache_file = tmp_path / "_stats.csv"
        test_df = pd.DataFrame({
            'Дата прийому': ['25.12.2025'],
            'Останній номер що зайшов': ['100'],
            'Перший номер що зайшов': ['50']
        })
        test_df.to_csv(cache_file, index=False)
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.STATS_SHEET_ID', 'test_id'), \
             patch('vlk_bot.config.STATS_WORKSHEET_NAME', 'Stats'), \
             patch('vlk_bot.config.DAILY_SHEETS_CACHE_DIR', str(tmp_path)), \
             patch('vlk_bot.config.STATS_CACHE_TTL_MINUTES', 30):
            result = await sheets.get_stats_data()
            assert result is not None
            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_force_refresh(self, tmp_path):
        """Примусове оновлення з API."""
        cache_file = tmp_path / "_stats.csv"
        test_df = pd.DataFrame({
            'Дата прийому': ['25.12.2025'],
            'Останній номер що зайшов': ['100']
        })
        test_df.to_csv(cache_file, index=False)
        
        mock_service = MagicMock()
        mock_service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
            'values': [
                ['Дата прийому', 'Останній номер що зайшов', 'Перший номер що зайшов'],
                ['26.12.2025', '200', '150']
            ]
        }
        
        with patch('vlk_bot.config.SHEETS_SERVICE', mock_service), \
             patch('vlk_bot.config.STATS_SHEET_ID', 'test_id'), \
             patch('vlk_bot.config.STATS_WORKSHEET_NAME', 'Stats'), \
             patch('vlk_bot.config.DAILY_SHEETS_CACHE_DIR', str(tmp_path)), \
             patch('vlk_bot.config.STATS_CACHE_TTL_MINUTES', 30), \
             patch('vlk_bot.sheets._execute_with_retry') as mock_retry:
            mock_retry.return_value = {
                'values': [
                    ['Дата прийому', 'Останній номер що зайшов', 'Перший номер що зайшов'],
                    ['26.12.2025', '200', '150']
                ]
            }
            result = await sheets.get_stats_data(force_refresh=True)
            assert result is not None


class TestUpdateActiveSheetStatus:
    """Тести для update_active_sheet_status."""

    def test_no_service(self):
        """Повертає False якщо сервіс не ініціалізовано."""
        with patch('vlk_bot.config.SHEETS_SERVICE', None):
            result = sheets.update_active_sheet_status('1234', 'Ухвалено')
            assert result is False

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_user_not_found(self, mock_retry):
        """Користувач не знайдений."""
        mock_retry.return_value = {
            'values': [
                ['№', 'ID', 'Статус'],
                ['1', '5678', 'Активний']
            ]
        }
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.ACTIVE_SHEET_ID', 'test_id'), \
             patch('vlk_bot.config.ACTIVE_WORKSHEET_NAME', 'Active'):
            result = sheets.update_active_sheet_status('1234', 'Ухвалено')
            assert result is False

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_update_success(self, mock_retry):
        """Успішне оновлення статусу."""
        mock_retry.return_value = {
            'values': [
                ['№', 'ID', 'Статус'],
                ['1', '1234', 'Активний']
            ]
        }
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.ACTIVE_SHEET_ID', 'test_id'), \
             patch('vlk_bot.config.ACTIVE_WORKSHEET_NAME', 'Active'):
            result = sheets.update_active_sheet_status('1234', 'Ухвалено')
            assert result is True


class TestGetSheetsList:
    """Тести для get_sheets_list."""

    def test_no_service(self):
        """Повертає порожній список якщо сервіс не ініціалізовано."""
        with patch('vlk_bot.config.SHEETS_SERVICE', None):
            result = sheets.get_sheets_list('test_id')
            assert result == []

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_get_sheets_success(self, mock_retry):
        """Успішне отримання списку аркушів."""
        mock_retry.return_value = {
            'sheets': [
                {'properties': {'title': 'Sheet1'}},
                {'properties': {'title': 'Sheet2'}}
            ]
        }
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()):
            result = sheets.get_sheets_list('test_id')
            assert result == ['Sheet1', 'Sheet2']


class TestDeleteRowFromActiveSheet:
    """Тести для delete_row_from_active_sheet."""

    def test_no_service(self):
        """Повертає False якщо сервіс не ініціалізовано."""
        with patch('vlk_bot.config.SHEETS_SERVICE', None):
            result = sheets.delete_row_from_active_sheet('1234')
            assert result is False

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_user_not_found(self, mock_retry):
        """Користувач не знайдений."""
        mock_retry.return_value = {
            'values': [
                ['№', 'ID', 'Статус'],
                ['1', '5678', 'Активний']
            ]
        }
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.ACTIVE_SHEET_ID', 'test_id'), \
             patch('vlk_bot.config.ACTIVE_WORKSHEET_NAME', 'Active'):
            result = sheets.delete_row_from_active_sheet('1234')
            assert result is False

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_delete_success(self, mock_retry):
        """Успішне видалення рядка."""
        mock_retry.side_effect = [
            {'values': [['№', 'ID', 'Статус'], ['1', '1234', 'Активний']]},
            {'sheets': [{'properties': {'title': 'Active', 'sheetId': 0}}]},
            {}
        ]
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.ACTIVE_SHEET_ID', 'test_id'), \
             patch('vlk_bot.config.ACTIVE_WORKSHEET_NAME', 'Active'):
            result = sheets.delete_row_from_active_sheet('1234')
            assert result is True


class TestGetUsersForDateFromActiveSheet:
    """Тести для get_users_for_date_from_active_sheet."""

    def test_no_service(self):
        """Повертає порожній список якщо сервіс не ініціалізовано."""
        with patch('vlk_bot.config.SHEETS_SERVICE', None):
            result = sheets.get_users_for_date_from_active_sheet('25.12.2025')
            assert result == []

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_empty_sheet(self, mock_retry):
        """Порожній аркуш."""
        mock_retry.return_value = {'values': []}
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.ACTIVE_SHEET_ID', 'test_id'), \
             patch('vlk_bot.config.ACTIVE_WORKSHEET_NAME', 'Active'):
            result = sheets.get_users_for_date_from_active_sheet('25.12.2025')
            assert result == []

    @patch('vlk_bot.sheets._execute_with_retry')
    def test_get_users_success(self, mock_retry):
        """Успішне отримання користувачів."""
        mock_retry.return_value = {
            'values': [
                ['Дата', '25.12.2025'],
                ['Header'],
                ['№', 'ID', 'Статус', 'Примітки', 'TG ID'],
                ['1', '1234', 'Активний', '', '123456789'],
                ['2', '5678', 'Активний', '', '987654321']
            ]
        }
        
        with patch('vlk_bot.config.SHEETS_SERVICE', MagicMock()), \
             patch('vlk_bot.config.ACTIVE_SHEET_ID', 'test_id'), \
             patch('vlk_bot.config.ACTIVE_WORKSHEET_NAME', 'Active'):
            result = sheets.get_users_for_date_from_active_sheet('25.12.2025')
            assert len(result) == 2
            assert result[0]['id'] == '1234'
            assert result[0]['tg_id'] == '123456789'


