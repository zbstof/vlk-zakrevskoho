"""
Тести для обробників (handlers).
"""

import datetime
from unittest.mock import MagicMock, patch, AsyncMock

import pandas as pd
import pytest
from telegram.ext import ConversationHandler

from vlk_bot import handlers_show, handlers_status, handlers_cancel, handlers_poll, handlers_common


class TestHandlersShow:
    """Тести для handlers_show."""

    @pytest.mark.asyncio
    async def test_show_start_no_data(self):
        """Початок без даних."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.handlers_show.load_queue_data', return_value=None):
            result = await handlers_show.show_start(update, context)
            assert result == ConversationHandler.END

    @pytest.mark.asyncio
    async def test_show_start_success(self):
        """Успішний початок."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        queue_df = pd.DataFrame({'ID': ['1234']})
        
        with patch('vlk_bot.handlers_show.load_queue_data', return_value=queue_df), \
             patch('vlk_bot.config.queue_df', queue_df):
            from vlk_bot.config import SHOW_GETTING_OPTION
            result = await handlers_show.show_start(update, context)
            assert result == SHOW_GETTING_OPTION

    @pytest.mark.asyncio
    async def test_show_get_option_all(self):
        """Показ всіх записів."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = 'Показати всі записи'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        queue_df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': ['25.12.2025'],
            'Статус': ['Ухвалено'],
            'Змінено': ['23.12.2025 10:00:00']
        })
        
        with patch('vlk_bot.config.queue_df', queue_df), \
             patch('vlk_bot.handlers_show.display_queue_data', new_callable=AsyncMock):
            result = await handlers_show.show_get_option(update, context)
            assert result == ConversationHandler.END

    @pytest.mark.asyncio
    async def test_show_get_date_invalid(self):
        """Некоректний формат дати."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = 'invalid'
        update.message.reply_html = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.config.queue_df', pd.DataFrame()):
            from vlk_bot.config import SHOW_GETTING_DATE
            result = await handlers_show.show_get_date(update, context)
            assert result == SHOW_GETTING_DATE


class TestHandlersStatus:
    """Тести для handlers_status."""

    @pytest.mark.asyncio
    async def test_status_start_no_data(self):
        """Початок без даних."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.handlers_status.load_queue_data', return_value=None):
            result = await handlers_status.status_start(update, context)
            assert result == ConversationHandler.END

    @pytest.mark.asyncio
    async def test_status_start_success(self):
        """Успішний початок."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        queue_df = pd.DataFrame({'ID': ['1234']})
        
        with patch('vlk_bot.handlers_status.load_queue_data', return_value=queue_df):
            result = await handlers_status.status_start(update, context)
            from vlk_bot.config import STATUS_GETTING_ID
            assert result == STATUS_GETTING_ID[0]

    @pytest.mark.asyncio
    async def test_status_get_id_invalid(self):
        """Некоректний ID."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = 'abc'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.config.queue_df', pd.DataFrame()):
            from vlk_bot.config import STATUS_GETTING_ID
            result = await handlers_status.status_get_id(update, context)
            assert result == STATUS_GETTING_ID[0]

    @pytest.mark.asyncio
    async def test_status_get_id_not_found(self):
        """ID не знайдено."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = '9999'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        queue_df = pd.DataFrame({'ID': ['1234'], 'Дата': ['25.12.2025']})
        
        with patch('vlk_bot.config.queue_df', queue_df):
            result = await handlers_status.status_get_id(update, context)
            assert result == ConversationHandler.END

    @pytest.mark.asyncio
    async def test_status_get_id_found(self):
        """ID знайдено."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = '1234'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        queue_df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': ['25.12.2025'],
            'Статус': ['Ухвалено'],
            'Змінено': ['23.12.2025 10:00:00'],
            'Попередня дата': ['']
        })
        
        with patch('vlk_bot.config.queue_df', queue_df), \
             patch('vlk_bot.handlers_status.get_stats_data', new_callable=AsyncMock, return_value=None):
            result = await handlers_status.status_get_id(update, context)
            assert result == ConversationHandler.END


class TestHandlersCancel:
    """Тести для handlers_cancel."""

    @pytest.mark.asyncio
    async def test_cancel_record_start_banned(self):
        """Заблокований користувач."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.handlers_cancel.is_banned', return_value=True):
            result = await handlers_cancel.cancel_record_start(update, context)
            assert result == ConversationHandler.END

    @pytest.mark.asyncio
    async def test_cancel_record_start_no_data(self):
        """Початок без даних."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.handlers_cancel.is_banned', return_value=False), \
             patch('vlk_bot.handlers_cancel.load_queue_data', return_value=None):
            result = await handlers_cancel.cancel_record_start(update, context)
            assert result == ConversationHandler.END

    @pytest.mark.asyncio
    async def test_cancel_record_start_success(self):
        """Успішний початок."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        queue_df = pd.DataFrame({'ID': ['1234']})
        
        with patch('vlk_bot.handlers_cancel.is_banned', return_value=False), \
             patch('vlk_bot.handlers_cancel.load_queue_data', return_value=queue_df), \
             patch('vlk_bot.handlers_cancel.get_user_telegram_data', return_value={}):
            result = await handlers_cancel.cancel_record_start(update, context)
            from vlk_bot.config import CANCEL_GETTING_ID
            assert result == CANCEL_GETTING_ID[0]


class TestHandlersPoll:
    """Тести для handlers_poll."""

    @pytest.mark.asyncio
    async def test_delete_confirmation_message(self):
        """Видалення повідомлення після затримки."""
        context = MagicMock()
        context.bot = AsyncMock()
        context.bot.delete_message = AsyncMock()
        
        with patch('vlk_bot.handlers_poll.asyncio.sleep', new_callable=AsyncMock):
            await handlers_poll.delete_confirmation_message(context, 123, 456, delay=0)

    @pytest.mark.asyncio
    async def test_handle_poll_response_invalid_format(self):
        """Некоректний формат callback."""
        update = MagicMock()
        update.callback_query = AsyncMock()
        update.callback_query.data = 'invalid'
        update.callback_query.answer = AsyncMock()
        update.callback_query.from_user = MagicMock()
        
        context = MagicMock()
        
        await handlers_poll.handle_poll_response(update, context)

    @pytest.mark.asyncio
    async def test_handle_poll_response_confirm(self):
        """Підтвердження візиту."""
        update = MagicMock()
        update.callback_query = AsyncMock()
        update.callback_query.data = 'poll_confirm_1234'
        update.callback_query.answer = AsyncMock()
        update.callback_query.edit_message_text = AsyncMock()
        update.callback_query.from_user = MagicMock(id=123)
        
        context = MagicMock()
        context.bot_data = {'next_reception_sheet': '25.12.2025'}
        
        with patch('vlk_bot.handlers_poll.update_active_sheet_status', return_value=True), \
             patch('vlk_bot.handlers_poll.get_confirmation_status', return_value=''), \
             patch('vlk_bot.handlers_poll.update_confirmation_status', return_value=True), \
             patch('vlk_bot.handlers_poll.load_queue_data', return_value=pd.DataFrame()):
            await handlers_poll.handle_poll_response(update, context)
            
            update.callback_query.edit_message_text.assert_called_once()


class TestHandlersCommon:
    """Тести для handlers_common."""

    @pytest.mark.asyncio
    async def test_start_command(self):
        """Команда /start."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123, first_name='Test')
        update.effective_user.mention_html.return_value = '<a href="tg://user?id=123">Test</a>'
        update.message = AsyncMock()
        update.message.reply_photo = AsyncMock()
        update.message.reply_html = AsyncMock()
        
        context = MagicMock()
        
        with patch('builtins.open', side_effect=FileNotFoundError()):
            await handlers_common.start(update, context)
        update.message.reply_html.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancel_action(self):
        """Скасування дії."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {'some_key': 'some_value'}
        
        result = await handlers_common.cancel_conversation(update, context)
        assert result == ConversationHandler.END
        assert context.user_data == {}

    @pytest.mark.asyncio
    async def test_help_command(self):
        """Команда /help."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        
        with patch('vlk_bot.handlers_common.is_admin', return_value=False):
            await handlers_common.help_command(update, context)
        update.message.reply_text.assert_called_once()

