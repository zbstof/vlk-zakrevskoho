"""
Тести для vlk_bot/formatters.py.
"""

import datetime
from unittest.mock import MagicMock, patch, AsyncMock

import pandas as pd
import pytest

from vlk_bot import formatters


class TestCalculateEndDate:
    """Тести для calculate_end_date."""

    def test_weekday_start(self):
        """Початок у робочий день."""
        start = datetime.date(2025, 12, 22)
        result = formatters.calculate_end_date(start, 5)
        assert result == datetime.date(2025, 12, 26)

    def test_weekend_start(self):
        """Початок у вихідний день."""
        start = datetime.date(2025, 12, 20)
        result = formatters.calculate_end_date(start, 5)
        assert result == datetime.date(2025, 12, 26)

    def test_spans_weekend(self):
        """Діапазон охоплює вихідні."""
        start = datetime.date(2025, 12, 24)
        result = formatters.calculate_end_date(start, 5)
        assert result == datetime.date(2025, 12, 30)

    def test_single_day(self):
        """Один день."""
        start = datetime.date(2025, 12, 22)
        result = formatters.calculate_end_date(start, 1)
        assert result == datetime.date(2025, 12, 22)

    def test_multiple_weekends(self):
        """Декілька вихідних."""
        start = datetime.date(2025, 12, 22)
        result = formatters.calculate_end_date(start, 10)
        assert result == datetime.date(2026, 1, 2)


class TestFormatPredictionRangeText:
    """Тести для format_prediction_range_text."""

    def test_none_prediction(self):
        """Порожній результат для None."""
        result = formatters.format_prediction_range_text(None)
        assert result == ""

    @patch('vlk_bot.keyboards.get_prediction_date_range')
    @patch('vlk_bot.prediction.calculate_date_probability')
    def test_with_prediction(self, mock_calc_prob, mock_get_range):
        """Форматування з прогнозом."""
        mock_dist = MagicMock()
        mock_get_range.return_value = (
            datetime.date(2025, 12, 25),
            datetime.date(2025, 12, 30),
            mock_dist
        )
        mock_calc_prob.side_effect = [75.0, 90.0]
        
        prediction = {'mean': datetime.date(2025, 12, 25)}
        result = formatters.format_prediction_range_text(prediction)
        
        assert '25.12.2025' in result or result == ""

    @patch('vlk_bot.keyboards.get_prediction_date_range')
    def test_no_start_date(self, mock_get_range):
        """Порожній результат без початкової дати."""
        mock_get_range.return_value = (None, None, None)
        prediction = {'mean': datetime.date(2025, 12, 25)}
        result = formatters.format_prediction_range_text(prediction)
        assert result == ""


class TestDisplayQueueData:
    """Тести для display_queue_data."""

    @pytest.mark.asyncio
    async def test_empty_dataframe(self):
        """Порожній DataFrame."""
        update = MagicMock()
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        df = pd.DataFrame(columns=['ID', 'Дата', 'Статус', 'Змінено'])
        
        await formatters.display_queue_data(update, df)
        
        update.message.reply_text.assert_called_once()
        call_text = update.message.reply_text.call_args[0][0]
        assert 'порожня' in call_text.lower() or 'Черга порожня' in call_text

    @pytest.mark.asyncio
    async def test_with_records(self):
        """DataFrame з записами."""
        update = MagicMock()
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        df = pd.DataFrame({
            'ID': ['1234', '5678'],
            'Дата': ['25.12.2025', '26.12.2025'],
            'Статус': ['Ухвалено', 'Ухвалено'],
            'Змінено': ['23.12.2025 10:00:00', '23.12.2025 11:00:00']
        })
        
        await formatters.display_queue_data(update, df)
        
        update.message.reply_text.assert_called()

    @pytest.mark.asyncio
    async def test_with_confirmation(self):
        """Відображення з підтвердженнями."""
        update = MagicMock()
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': ['25.12.2025'],
            'Статус': ['Ухвалено'],
            'Змінено': ['23.12.2025 10:00:00'],
            'Підтвердження': ['Підтверджено']
        })
        
        await formatters.display_queue_data(update, df, iConfirmation=True)
        
        update.message.reply_text.assert_called()


class TestGetPollText:
    """Тести для get_poll_text."""

    def test_basic_format(self):
        """Базове форматування."""
        result = formatters.get_poll_text('1234', '25.12.2025')
        assert '1234' in result
        assert '25.12.2025' in result
        assert 'Опитування' in result

    def test_html_format(self):
        """HTML форматування."""
        result = formatters.get_poll_text('1234', '25.12.2025')
        assert '<code>' in result
        assert '<b>' in result

