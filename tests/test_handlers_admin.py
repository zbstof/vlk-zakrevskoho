"""
Тести для vlk_bot/handlers_admin.py.
"""

from unittest.mock import MagicMock, patch, AsyncMock

import pandas as pd
import pytest

from vlk_bot import handlers_admin


class TestPerformQueueCleanup:
    """Тести для perform_queue_cleanup."""

    @pytest.mark.asyncio
    async def test_cleanup_no_data(self):
        """Очищення без даних."""
        with patch('vlk_bot.handlers_admin.load_queue_data', return_value=None):
            result = await handlers_admin.perform_queue_cleanup("test")

    @pytest.mark.asyncio
    async def test_cleanup_with_empty_data(self):
        """Очищення з порожніми даними."""
        df = pd.DataFrame()
        
        with patch('vlk_bot.handlers_admin.load_queue_data', return_value=df):
            result = await handlers_admin.perform_queue_cleanup("test")


class TestOpenSheetCommand:
    """Тести для open_sheet_command."""

    @pytest.mark.asyncio
    async def test_non_admin(self):
        """Не адміністратор."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        
        with patch('vlk_bot.handlers_admin.is_admin', return_value=False):
            await handlers_admin.open_sheet_command(update, context)
            update.message.reply_text.assert_called()

    @pytest.mark.asyncio
    async def test_admin(self):
        """Адміністратор."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        
        with patch('vlk_bot.handlers_admin.is_admin', return_value=True), \
             patch('vlk_bot.config.SPREADSHEET_ID', 'test_id'):
            await handlers_admin.open_sheet_command(update, context)
            update.message.reply_text.assert_called()


class TestGrantAdmin:
    """Тести для grant_admin."""

    @pytest.mark.asyncio
    async def test_non_admin(self):
        """Не адміністратор."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.args = ['456']
        
        with patch('vlk_bot.handlers_admin.is_admin', return_value=False):
            await handlers_admin.grant_admin(update, context)

    @pytest.mark.asyncio
    async def test_admin_no_args(self):
        """Адміністратор без аргументів."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.args = []
        
        with patch('vlk_bot.handlers_admin.is_admin', return_value=True):
            await handlers_admin.grant_admin(update, context)

    @pytest.mark.asyncio
    async def test_admin_with_invalid_args(self):
        """Адміністратор з невалідними аргументами."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.args = ['not_a_number']
        
        with patch('vlk_bot.handlers_admin.is_admin', return_value=True):
            await handlers_admin.grant_admin(update, context)
            update.message.reply_text.assert_called()


class TestDropAdmin:
    """Тести для drop_admin."""

    @pytest.mark.asyncio
    async def test_non_admin(self):
        """Не адміністратор."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.args = ['456']
        
        with patch('vlk_bot.handlers_admin.is_admin', return_value=False):
            await handlers_admin.drop_admin(update, context)


class TestBan:
    """Тести для ban."""

    @pytest.mark.asyncio
    async def test_non_admin(self):
        """Не адміністратор."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.args = ['456']
        
        with patch('vlk_bot.handlers_admin.is_admin', return_value=False):
            await handlers_admin.ban(update, context)


class TestUnban:
    """Тести для unban."""

    @pytest.mark.asyncio
    async def test_non_admin(self):
        """Не адміністратор."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.args = ['456']
        
        with patch('vlk_bot.handlers_admin.is_admin', return_value=False):
            await handlers_admin.unban(update, context)


class TestTestPoll:
    """Тести для test_poll."""

    @pytest.mark.asyncio
    async def test_non_admin(self):
        """Не адміністратор."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.args = []
        
        with patch('vlk_bot.handlers_admin.is_admin', return_value=False):
            await handlers_admin.test_poll(update, context)


class TestRunCleanupCommand:
    """Тести для run_cleanup_command."""

    @pytest.mark.asyncio
    async def test_non_admin(self):
        """Не адміністратор."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        
        with patch('vlk_bot.handlers_admin.is_admin', return_value=False):
            await handlers_admin.run_cleanup_command(update, context)


class TestRunNotifyCommand:
    """Тести для run_notify_command."""

    @pytest.mark.asyncio
    async def test_non_admin(self):
        """Не адміністратор."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        
        with patch('vlk_bot.handlers_admin.is_admin', return_value=False):
            await handlers_admin.run_notify_command(update, context)

