"""
Тести для vlk_bot/scheduler.py.
"""

import datetime
from unittest.mock import MagicMock, patch, AsyncMock

import pandas as pd
import pytest

from vlk_bot import scheduler


class TestSendUserNotification:
    """Тести для send_user_notification."""

    @pytest.mark.asyncio
    async def test_empty_tg_id(self):
        """Порожній TG ID."""
        context = MagicMock()
        await scheduler.send_user_notification(context, '', 'test')
        context.bot.send_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_whitespace_tg_id(self):
        """TG ID з пробілами."""
        context = MagicMock()
        await scheduler.send_user_notification(context, '   ', 'test')
        context.bot.send_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_valid_tg_id(self):
        """Валідний TG ID."""
        context = MagicMock()
        context.bot = AsyncMock()
        context.bot.send_message = AsyncMock()
        
        await scheduler.send_user_notification(context, '123456789', 'test message')
        
        context.bot.send_message.assert_called_once_with(
            chat_id=123456789,
            text='test message',
            parse_mode='HTML'
        )

    @pytest.mark.asyncio
    async def test_send_error_handling(self):
        """Обробка помилки відправки."""
        context = MagicMock()
        context.bot = AsyncMock()
        context.bot.send_message = AsyncMock(side_effect=Exception("Network error"))
        
        await scheduler.send_user_notification(context, '123456789', 'test message')


class TestNotifyStatus:
    """Тести для notify_status."""

    @pytest.mark.asyncio
    async def test_empty_queue(self):
        """Порожня черга."""
        context = MagicMock()
        
        with patch('vlk_bot.scheduler.load_queue_data', return_value=None):
            with patch('vlk_bot.config.queue_df', None):
                await scheduler.notify_status(context)

    @pytest.mark.asyncio
    async def test_status_change_notification(self):
        """Сповіщення при зміні статусу."""
        context = MagicMock()
        context.bot = AsyncMock()
        context.bot.send_message = AsyncMock()
        
        queue_df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': ['25.12.2025'],
            'Примітки': [''],
            'Статус': ['Ухвалено'],
            'Змінено': ['23.12.2025 10:00:00'],
            'Попередня дата': [''],
            'TG ID': ['123456789']
        })
        
        cached_df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': ['25.12.2025'],
            'Примітки': [''],
            'Статус': ['На розгляді'],
            'Змінено': ['22.12.2025 10:00:00'],
            'Попередня дата': [''],
            'TG ID': ['123456789']
        })
        
        with patch('vlk_bot.scheduler.load_queue_data', return_value=queue_df), \
             patch('vlk_bot.scheduler.load_todo_cache', return_value=cached_df), \
             patch('vlk_bot.scheduler.save_todo_cache'):
            await scheduler.notify_status(context)
            
            context.bot.send_message.assert_called()


class TestCheckNewDailySheet:
    """Тести для check_new_daily_sheet."""

    @pytest.mark.asyncio
    async def test_already_sent_today(self):
        """Опитування вже надіслано сьогодні."""
        context = MagicMock()
        context.bot_data = {'poll_sent_for_date': datetime.date.today()}
        
        await scheduler.check_new_daily_sheet(context)

    @pytest.mark.asyncio
    async def test_no_sheets_found(self):
        """Аркуші не знайдено."""
        context = MagicMock()
        context.bot_data = {}
        
        with patch('vlk_bot.scheduler.get_sheets_list', return_value=[]), \
             patch('vlk_bot.config.STATS_SHEET_ID', 'test_id'):
            await scheduler.check_new_daily_sheet(context)


class TestDateReminder:
    """Тести для date_reminder."""

    @pytest.mark.asyncio
    async def test_reminder_empty_queue(self):
        """Порожня черга."""
        context = MagicMock()
        
        with patch('vlk_bot.scheduler.load_queue_data', return_value=None):
            await scheduler.date_reminder(context)

    @pytest.mark.asyncio
    async def test_reminder_with_data(self):
        """Нагадування з даними."""
        context = MagicMock()
        context.bot = AsyncMock()
        context.bot.send_message = AsyncMock()
        
        tomorrow = datetime.date.today() + datetime.timedelta(days=1)
        queue_df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': [tomorrow.strftime('%d.%m.%Y')],
            'Примітки': [''],
            'Статус': ['Ухвалено'],
            'Змінено': ['23.12.2025 10:00:00'],
            'TG ID': ['123456789']
        })
        
        with patch('vlk_bot.scheduler.load_queue_data', return_value=queue_df):
            await scheduler.date_reminder(context)


class TestExpireUnansweredPolls:
    """Тести для expire_unanswered_polls."""

    @pytest.mark.asyncio
    async def test_expire_polls(self):
        """Закінчення терміну опитувань."""
        context = MagicMock()
        context.bot_data = {}
        
        await scheduler.expire_unanswered_polls(context)

