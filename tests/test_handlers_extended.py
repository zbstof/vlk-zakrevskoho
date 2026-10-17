"""
Розширені тести для handlers.
"""

import datetime
from unittest.mock import MagicMock, patch, AsyncMock

import pandas as pd
import pytest
from telegram.ext import ConversationHandler

from vlk_bot import handlers_show, handlers_cancel, handlers_join


class TestHandlersShowGetOption:
    """Тести для handlers_show.show_get_option."""

    @pytest.mark.asyncio
    async def test_show_by_date(self):
        """Показ за датою."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = 'Показати записи на конкретну дату'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        queue_df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': ['25.12.2025'],
            'Статус': ['Ухвалено'],
            'Змінено': ['23.12.2025 10:00:00']
        })
        
        with patch('vlk_bot.config.queue_df', queue_df):
            from vlk_bot.config import SHOW_GETTING_DATE
            result = await handlers_show.show_get_option(update, context)
            assert result == SHOW_GETTING_DATE

    @pytest.mark.asyncio
    async def test_show_unknown_option(self):
        """Невідома опція."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = 'Невідома опція'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.config.queue_df', pd.DataFrame()):
            from vlk_bot.config import SHOW_GETTING_OPTION
            result = await handlers_show.show_get_option(update, context)
            assert result == SHOW_GETTING_OPTION


class TestHandlersShowGetDate:
    """Тести для handlers_show.show_get_date."""

    @pytest.mark.asyncio
    async def test_valid_date(self):
        """Валідна дата."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        
        tomorrow = datetime.date.today() + datetime.timedelta(days=1)
        while tomorrow.weekday() >= 5:
            tomorrow += datetime.timedelta(days=1)
        update.message.text = tomorrow.strftime('%d.%m.%Y')
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        queue_df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': [tomorrow.strftime('%d.%m.%Y')],
            'Статус': ['Ухвалено'],
            'Змінено': ['23.12.2025 10:00:00']
        })
        
        with patch('vlk_bot.config.queue_df', queue_df):
            result = await handlers_show.show_get_date(update, context)
            assert result == ConversationHandler.END

    @pytest.mark.asyncio
    async def test_past_date(self):
        """Дата в минулому."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = '01.01.2020'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.config.queue_df', pd.DataFrame()):
            from vlk_bot.config import SHOW_GETTING_DATE
            result = await handlers_show.show_get_date(update, context)
            assert result == SHOW_GETTING_DATE

    @pytest.mark.asyncio
    async def test_weekend_date(self):
        """Дата на вихідний."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        
        next_saturday = datetime.date.today()
        while next_saturday.weekday() != 5:
            next_saturday += datetime.timedelta(days=1)
        update.message.text = next_saturday.strftime('%d.%m.%Y')
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.config.queue_df', pd.DataFrame()):
            from vlk_bot.config import SHOW_GETTING_DATE
            result = await handlers_show.show_get_date(update, context)
            assert result == SHOW_GETTING_DATE


class TestHandlersCancelGetId:
    """Тести для handlers_cancel.cancel_record_get_id."""

    @pytest.mark.asyncio
    async def test_invalid_id_format(self):
        """Невалідний формат ID."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = 'abc'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {'telegram_user_data': {}}
        
        with patch('vlk_bot.config.queue_df', pd.DataFrame()):
            from vlk_bot.config import CANCEL_GETTING_ID
            result = await handlers_cancel.cancel_record_get_id(update, context)
            assert result == CANCEL_GETTING_ID[0]

    @pytest.mark.asyncio
    async def test_id_not_found(self):
        """ID не знайдено."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = '9999'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {'telegram_user_data': {}}
        
        queue_df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': ['25.12.2025'],
            'Статус': ['Ухвалено'],
            'Змінено': ['23.12.2025 10:00:00']
        })
        
        with patch('vlk_bot.config.queue_df', queue_df):
            result = await handlers_cancel.cancel_record_get_id(update, context)
            assert result == ConversationHandler.END

    @pytest.mark.asyncio
    async def test_already_cancelled(self):
        """Вже скасовано."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = '1234'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {'telegram_user_data': {}}
        
        queue_df = pd.DataFrame({
            'ID': ['1234'],
            'Дата': [''],
            'Статус': ['Ухвалено'],
            'Змінено': ['23.12.2025 10:00:00']
        })
        
        with patch('vlk_bot.config.queue_df', queue_df):
            result = await handlers_cancel.cancel_record_get_id(update, context)
            assert result == ConversationHandler.END


class TestHandlersJoin:
    """Тести для handlers_join."""

    @pytest.mark.asyncio
    async def test_join_start_no_data(self):
        """Початок без даних."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.handlers_join.load_queue_data', return_value=None), \
             patch('vlk_bot.handlers_join.is_banned', return_value=False):
            result = await handlers_join.join_start(update, context)
            assert result == ConversationHandler.END

    @pytest.mark.asyncio
    async def test_join_start_banned(self):
        """Заблокований користувач."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.handlers_join.is_banned', return_value=True):
            result = await handlers_join.join_start(update, context)
            assert result == ConversationHandler.END

    @pytest.mark.asyncio
    async def test_join_start_success(self):
        """Успішний початок."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        queue_df = pd.DataFrame({'ID': ['1234']})
        
        with patch('vlk_bot.handlers_join.load_queue_data', return_value=queue_df), \
             patch('vlk_bot.handlers_join.is_banned', return_value=False), \
             patch('vlk_bot.handlers_join.get_user_telegram_data', return_value={}):
            from vlk_bot.config import JOIN_GETTING_ID
            result = await handlers_join.join_start(update, context)
            assert result == JOIN_GETTING_ID

    @pytest.mark.asyncio
    async def test_join_get_id_invalid(self):
        """Невалідний ID."""
        update = MagicMock()
        update.effective_user = MagicMock(id=123)
        update.message = AsyncMock()
        update.message.text = 'abc'
        update.message.reply_text = AsyncMock()
        
        context = MagicMock()
        context.user_data = {}
        
        with patch('vlk_bot.config.queue_df', pd.DataFrame()):
            from vlk_bot.config import JOIN_GETTING_ID
            result = await handlers_join.join_get_id(update, context)
            assert result == JOIN_GETTING_ID


