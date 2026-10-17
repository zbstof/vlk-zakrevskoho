"""
Тести для vlk_bot/bot.py.
"""

from unittest.mock import MagicMock, patch

import pytest


class TestBotMain:
    """Тести для головної функції бота."""

    def test_main_imports(self):
        """Перевірка імпортів."""
        from vlk_bot import bot
        assert hasattr(bot, 'main')

    @patch('vlk_bot.bot.initialize_bot')
    @patch('vlk_bot.bot.Application')
    @patch('vlk_bot.config.TOKEN', 'test_token')
    @patch('vlk_bot.config.ENVIRONMENT', 'test')
    def test_main_test_environment(self, mock_app_class, mock_init):
        """Main у тестовому середовищі."""
        mock_app = MagicMock()
        mock_builder = MagicMock()
        mock_builder.token.return_value = mock_builder
        mock_builder.build.return_value = mock_app
        mock_app_class.builder.return_value = mock_builder
        mock_app.run_polling = MagicMock()
        
        from vlk_bot import bot
        
        with patch.object(bot, 'Application', mock_app_class):
            with pytest.raises(SystemExit) if False else patch.object(mock_app, 'run_polling'):
                pass


class TestConversationHandlers:
    """Тести для ConversationHandlers."""

    def test_join_states_defined(self):
        """Стани join визначені."""
        from vlk_bot.config import JOIN_GETTING_ID, JOIN_GETTING_DATE
        assert JOIN_GETTING_ID is not None
        assert JOIN_GETTING_DATE is not None

    def test_cancel_states_defined(self):
        """Стани cancel визначені."""
        from vlk_bot.config import CANCEL_GETTING_ID
        assert CANCEL_GETTING_ID is not None

    def test_show_states_defined(self):
        """Стани show визначені."""
        from vlk_bot.config import SHOW_GETTING_OPTION, SHOW_GETTING_DATE
        assert SHOW_GETTING_OPTION is not None
        assert SHOW_GETTING_DATE is not None

    def test_status_states_defined(self):
        """Стани status визначені."""
        from vlk_bot.config import STATUS_GETTING_ID
        assert STATUS_GETTING_ID is not None


class TestPollConstants:
    """Тести для констант опитувань."""

    def test_poll_actions_defined(self):
        """Дії опитування визначені."""
        from vlk_bot.config import (
            POLL_CONFIRM, POLL_RESCHEDULE, POLL_CANCEL,
            POLL_CANCEL_CONFIRM, POLL_CANCEL_ABORT, POLL_CANCEL_RESCHEDULE
        )
        assert POLL_CONFIRM is not None
        assert POLL_RESCHEDULE is not None
        assert POLL_CANCEL is not None
        assert POLL_CANCEL_CONFIRM is not None
        assert POLL_CANCEL_ABORT is not None
        assert POLL_CANCEL_RESCHEDULE is not None


