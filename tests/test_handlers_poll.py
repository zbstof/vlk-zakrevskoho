"""
Тести для vlk_bot/handlers_poll.py.
"""

import asyncio
import datetime
from unittest.mock import MagicMock, patch, AsyncMock

import pandas as pd
import pytest

from vlk_bot import handlers_poll
from vlk_bot.config import (
    POLL_CONFIRM, POLL_RESCHEDULE, POLL_CANCEL,
    POLL_CANCEL_CONFIRM, POLL_CANCEL_ABORT, POLL_CANCEL_RESCHEDULE,
    POLL_DATE, POLL_DATE_OTHER
)


class TestDeleteConfirmationMessage:
    """Тести для delete_confirmation_message."""

    @pytest.mark.asyncio
    async def test_success(self):
        """Успішне видалення."""
        context = MagicMock()
        context.bot = AsyncMock()
        context.bot.delete_message = AsyncMock()
        
        with patch('vlk_bot.handlers_poll.asyncio.sleep', new_callable=AsyncMock):
            await handlers_poll.delete_confirmation_message(context, 123, 456, delay=0)
            context.bot.delete_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Обробка помилки."""
        context = MagicMock()
        context.bot = AsyncMock()
        context.bot.delete_message = AsyncMock(side_effect=Exception("Test"))
        
        with patch('vlk_bot.handlers_poll.asyncio.sleep', new_callable=AsyncMock):
            await handlers_poll.delete_confirmation_message(context, 123, 456, delay=0)


class TestHandlePollResponse:
    """Тести для handle_poll_response."""

    @pytest.mark.asyncio
    async def test_invalid_format(self):
        """Некоректний формат."""
        update = MagicMock()
        update.callback_query = AsyncMock()
        update.callback_query.data = 'invalid'
        update.callback_query.answer = AsyncMock()
        update.callback_query.from_user = MagicMock(id=123)
        
        context = MagicMock()
        
        await handlers_poll.handle_poll_response(update, context)

    @pytest.mark.asyncio
    async def test_confirm_action(self):
        """Підтвердження візиту."""
        update = MagicMock()
        update.callback_query = AsyncMock()
        update.callback_query.data = f'{POLL_CONFIRM}_1234'
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
            update.callback_query.edit_message_text.assert_called()

    @pytest.mark.asyncio
    async def test_reschedule_action(self):
        """Перенесення візиту."""
        update = MagicMock()
        update.callback_query = AsyncMock()
        update.callback_query.data = f'{POLL_RESCHEDULE}_1234'
        update.callback_query.answer = AsyncMock()
        update.callback_query.edit_message_text = AsyncMock()
        update.callback_query.from_user = MagicMock(id=123)
        
        context = MagicMock()
        context.bot_data = {}
        
        with patch('vlk_bot.handlers_poll.get_stats_data', new_callable=AsyncMock, return_value=None), \
             patch('vlk_bot.handlers_poll.calculate_prediction', return_value=None), \
             patch('vlk_bot.handlers_poll.get_confirmation_status', return_value=''), \
             patch('vlk_bot.handlers_poll.load_queue_data', return_value=pd.DataFrame()):
            await handlers_poll.handle_poll_response(update, context)

    @pytest.mark.asyncio
    async def test_cancel_action(self):
        """Скасування візиту."""
        update = MagicMock()
        update.callback_query = AsyncMock()
        update.callback_query.data = f'{POLL_CANCEL}_1234'
        update.callback_query.answer = AsyncMock()
        update.callback_query.edit_message_text = AsyncMock()
        update.callback_query.from_user = MagicMock(id=123)
        
        context = MagicMock()
        
        with patch('vlk_bot.handlers_poll.get_confirmation_status', return_value=''), \
             patch('vlk_bot.handlers_poll.load_queue_data', return_value=pd.DataFrame()), \
             patch('vlk_bot.handlers_poll.check_deadline', return_value=False):
            await handlers_poll.handle_poll_response(update, context)


class TestHandlePollCancelActions:
    """Тести для handle_poll_cancel_actions."""

    @pytest.mark.asyncio
    async def test_cancel_confirm(self):
        """Підтвердження скасування."""
        update = MagicMock()
        update.callback_query = AsyncMock()
        update.callback_query.data = f'{POLL_CANCEL_CONFIRM}_1234'
        update.callback_query.answer = AsyncMock()
        update.callback_query.edit_message_text = AsyncMock()
        update.callback_query.from_user = MagicMock(id=123)
        
        context = MagicMock()
        context.bot_data = {'next_reception_sheet': '25.12.2025'}
        
        with patch('vlk_bot.handlers_poll.delete_row_from_active_sheet', return_value=True), \
             patch('vlk_bot.handlers_poll.get_confirmation_status', return_value=''), \
             patch('vlk_bot.handlers_poll.load_queue_data', return_value=pd.DataFrame()), \
             patch('vlk_bot.handlers_poll.send_group_notification', new_callable=AsyncMock):
            await handlers_poll.handle_poll_cancel_actions(update, context)

    @pytest.mark.asyncio
    async def test_cancel_abort(self):
        """Відміна скасування."""
        update = MagicMock()
        update.callback_query = AsyncMock()
        update.callback_query.data = f'{POLL_CANCEL_ABORT}_1234'
        update.callback_query.answer = AsyncMock()
        update.callback_query.edit_message_text = AsyncMock()
        update.callback_query.from_user = MagicMock(id=123)
        
        context = MagicMock()
        context.bot_data = {'next_reception_sheet': '25.12.2025'}
        
        with patch('vlk_bot.handlers_poll.get_confirmation_status', return_value=''), \
             patch('vlk_bot.handlers_poll.load_queue_data', return_value=pd.DataFrame()):
            await handlers_poll.handle_poll_cancel_actions(update, context)

    @pytest.mark.asyncio
    async def test_cancel_reschedule(self):
        """Перенесення при скасуванні."""
        update = MagicMock()
        update.callback_query = AsyncMock()
        update.callback_query.data = f'{POLL_CANCEL_RESCHEDULE}_1234'
        update.callback_query.answer = AsyncMock()
        update.callback_query.edit_message_text = AsyncMock()
        update.callback_query.from_user = MagicMock(id=123)
        
        context = MagicMock()
        context.bot_data = {}
        
        with patch('vlk_bot.handlers_poll.get_stats_data', new_callable=AsyncMock, return_value=None), \
             patch('vlk_bot.handlers_poll.calculate_prediction', return_value=None), \
             patch('vlk_bot.handlers_poll.get_confirmation_status', return_value=''), \
             patch('vlk_bot.handlers_poll.load_queue_data', return_value=pd.DataFrame()):
            await handlers_poll.handle_poll_cancel_actions(update, context)


class TestHandlePollDateSelection:
    """Тести для handle_poll_date_selection."""

    @pytest.mark.asyncio
    async def test_date_selection(self):
        """Вибір дати."""
        update = MagicMock()
        update.callback_query = AsyncMock()
        update.callback_query.data = f'{POLL_DATE}_1234_25.12.2025'
        update.callback_query.answer = AsyncMock()
        update.callback_query.edit_message_text = AsyncMock()
        update.callback_query.from_user = MagicMock(id=123)
        
        context = MagicMock()
        context.bot_data = {'next_reception_sheet': '24.12.2025'}
        
        with patch('vlk_bot.handlers_poll.update_active_sheet_status', return_value=True), \
             patch('vlk_bot.handlers_poll.get_confirmation_status', return_value=''), \
             patch('vlk_bot.handlers_poll.load_queue_data', return_value=pd.DataFrame()), \
             patch('vlk_bot.handlers_poll.send_group_notification', new_callable=AsyncMock):
            await handlers_poll.handle_poll_date_selection(update, context)

    @pytest.mark.asyncio
    async def test_date_other(self):
        """Інша дата."""
        update = MagicMock()
        update.callback_query = AsyncMock()
        update.callback_query.data = f'{POLL_DATE_OTHER}_1234'
        update.callback_query.answer = AsyncMock()
        update.callback_query.edit_message_text = AsyncMock()
        update.callback_query.from_user = MagicMock(id=123)
        update.callback_query.message = MagicMock(chat=MagicMock(id=123))
        
        context = MagicMock()
        context.bot_data = {}
        context.user_data = {}
        
        with patch('vlk_bot.handlers_poll.get_confirmation_status', return_value=''), \
             patch('vlk_bot.handlers_poll.load_queue_data', return_value=pd.DataFrame()):
            await handlers_poll.handle_poll_date_selection(update, context)


class TestHandlePollCustomDate:
    """Тести для handle_poll_custom_date."""

    @pytest.mark.asyncio
    async def test_no_pending_custom_date(self):
        """Немає очікуваної дати."""
        from telegram.ext import ApplicationHandlerStop
        
        update = MagicMock()
        update.effective_chat = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = '25.12.2025'
        
        context = MagicMock()
        context.bot_data = {}
        
        try:
            await handlers_poll.handle_poll_custom_date(update, context)
        except ApplicationHandlerStop:
            pass

    @pytest.mark.asyncio
    async def test_with_pending_date_valid(self):
        """Валідна дата."""
        from telegram.ext import ApplicationHandlerStop
        
        update = MagicMock()
        update.effective_chat = MagicMock(id=123)
        update.message = AsyncMock()
        
        tomorrow = datetime.date.today() + datetime.timedelta(days=1)
        while tomorrow.weekday() >= 5:
            tomorrow += datetime.timedelta(days=1)
        update.message.text = tomorrow.strftime('%d.%m.%Y')
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.bot_data = {
            'pending_custom_date': {123: {'user_id': '1234', 'message_id': 456}},
            'next_reception_sheet': '24.12.2025'
        }
        context.bot = AsyncMock()
        context.bot.delete_message = AsyncMock()
        
        with patch('vlk_bot.handlers_poll.update_active_sheet_status', return_value=True), \
             patch('vlk_bot.handlers_poll.get_confirmation_status', return_value=''), \
             patch('vlk_bot.handlers_poll.load_queue_data', return_value=pd.DataFrame()), \
             patch('vlk_bot.handlers_poll.send_group_notification', new_callable=AsyncMock):
            try:
                await handlers_poll.handle_poll_custom_date(update, context)
            except ApplicationHandlerStop:
                pass

    @pytest.mark.asyncio
    async def test_with_pending_date_invalid(self):
        """Невалідна дата."""
        from telegram.ext import ApplicationHandlerStop
        
        update = MagicMock()
        update.effective_chat = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = 'invalid'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.bot_data = {'pending_custom_date': {123: {'user_id': '1234', 'message_id': 456}}}
        
        try:
            await handlers_poll.handle_poll_custom_date(update, context)
        except ApplicationHandlerStop:
            pass

