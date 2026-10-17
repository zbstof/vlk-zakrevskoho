"""
Тести для vlk_bot/utils.py.
"""

import datetime
import json
from unittest.mock import patch, MagicMock

import pytest

from vlk_bot import utils


class TestIdToNumeric:
    """Тести для id_to_numeric."""

    def test_simple_id(self):
        """Простий ID."""
        result = utils.id_to_numeric('1234')
        assert result == 1234

    def test_compound_id(self):
        """Складений ID."""
        result = utils.id_to_numeric('1234/5')
        assert result is not None

    def test_invalid_id(self):
        """Невалідний ID."""
        result = utils.id_to_numeric('abc')
        assert result is None


class TestExtractMainId:
    """Тести для extract_main_id."""

    def test_simple_id(self):
        """Простий ID."""
        result = utils.extract_main_id('1234')
        assert result == 1234

    def test_compound_id(self):
        """Складений ID."""
        result = utils.extract_main_id('1234/5')
        assert result == 1234


class TestGetNextWorkingDays:
    """Тести для get_next_working_days."""

    def test_basic(self):
        """Базовий тест."""
        result = utils.get_next_working_days(5)
        assert len(result) == 5
        for day in result:
            assert day.weekday() < 5


class TestGetOrdinalDate:
    """Тести для get_ordinal_date."""

    def test_basic(self):
        """Базовий тест."""
        date = datetime.date(2025, 12, 25)
        result = utils.get_ordinal_date(date)
        assert isinstance(result, int)
        assert result > 0


class TestGetDateFromOrdinal:
    """Тести для get_date_from_ordinal."""

    def test_basic(self):
        """Базовий тест."""
        date = datetime.date(2025, 12, 25)
        ordinal = utils.get_ordinal_date(date)
        result = utils.get_date_from_ordinal(ordinal)
        assert result == date


class TestIsAdmin:
    """Тести для is_admin."""

    def test_is_admin(self):
        """Адміністратор."""
        with patch('vlk_bot.config.ADMIN_IDS', [123, 456]):
            result = utils.is_admin(123)
            assert result is True

    def test_not_admin(self):
        """Не адміністратор."""
        with patch('vlk_bot.config.ADMIN_IDS', [123, 456]):
            result = utils.is_admin(789)
            assert result is False


class TestIsBanned:
    """Тести для is_banned."""

    def test_is_banned_when_in_banlist(self):
        """Користувач у банлісті."""
        with patch('vlk_bot.config.BANLIST', [123, 456]):
            result = utils.is_banned(123)
            assert result is True

    def test_is_not_banned(self):
        """Користувач не в банлісті."""
        with patch('vlk_bot.config.BANLIST', [456]):
            result = utils.is_banned(123)
            assert result is False


class TestGetUserLogInfo:
    """Тести для get_user_log_info."""

    def test_with_username(self):
        """З username."""
        user = MagicMock()
        user.id = 123
        user.username = 'testuser'
        user.first_name = 'Test'
        user.last_name = 'User'
        
        result = utils.get_user_log_info(user)
        assert '123' in result
        assert 'testuser' in result

    def test_without_username(self):
        """Без username."""
        user = MagicMock()
        user.id = 123
        user.username = None
        user.first_name = 'Test'
        user.last_name = None
        
        result = utils.get_user_log_info(user)
        assert '123' in result


class TestGetUserTelegramData:
    """Тести для get_user_telegram_data."""

    def test_basic(self):
        """Базовий тест."""
        user = MagicMock()
        user.id = 123
        user.username = 'testuser'
        user.first_name = 'Test'
        user.last_name = 'User'
        
        result = utils.get_user_telegram_data(user)
        assert 'TG ID' in result


class TestLoadSaveTodoCache:
    """Тести для load_todo_cache та save_todo_cache."""

    def test_load_nonexistent_file(self):
        """Завантаження неіснуючого файлу."""
        import pandas as pd
        result = utils.load_todo_cache()
        assert isinstance(result, pd.DataFrame)


class TestSendGroupNotification:
    """Тести для send_group_notification."""

    @pytest.mark.asyncio
    async def test_send_notification_call(self):
        """Виклик send_group_notification."""
        from unittest.mock import AsyncMock
        
        context = MagicMock()
        context.bot = AsyncMock()
        context.bot.send_message = AsyncMock()
        
        with patch('vlk_bot.config.is_bot_in_group', True), \
             patch('vlk_bot.config.GROUP_ID', '999'):
            await utils.send_group_notification(context, 'Test')
            context.bot.send_message.assert_called_once_with(
                chat_id='999', text='Test', parse_mode='HTML'
            )

    @pytest.mark.asyncio
    async def test_send_notification_skipped_when_not_in_group(self):
        """Не надсилає, якщо бот не в групі."""
        from unittest.mock import AsyncMock
        
        context = MagicMock()
        context.bot = AsyncMock()
        context.bot.send_message = AsyncMock()
        
        with patch('vlk_bot.config.is_bot_in_group', False):
            await utils.send_group_notification(context, 'Test')
            context.bot.send_message.assert_not_called()

