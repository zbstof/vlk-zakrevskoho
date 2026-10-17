"""Спільні фікстури для тестів."""

import json
import os

import pandas as pd
import pytest
from unittest.mock import MagicMock, AsyncMock

from telegram import Update, User, Message, Chat
from telegram.ext import ContextTypes


@pytest.fixture(autouse=True)
def _isolate_filesystem(tmp_path, monkeypatch):
    """Ізолює тести від реальної файлової системи."""
    cache_dir = str(tmp_path / '_isolated_cache')
    os.makedirs(cache_dir, exist_ok=True)

    monkeypatch.setattr('vlk_bot.config.TODO_BOT_CACHE_FILE', str(tmp_path / 'todo_bot_cache.csv'))
    monkeypatch.setattr('vlk_bot.config.DAILY_SHEETS_CACHE_DIR', cache_dir)
    monkeypatch.setattr('vlk_bot.sync.DAILY_SHEETS_CACHE_DIR', cache_dir)
    monkeypatch.setattr('vlk_bot.prediction.DAILY_SHEETS_CACHE_DIR', cache_dir)


@pytest.fixture(autouse=True)
def _isolate_external_services(monkeypatch):
    """Ізолює тести від зовнішніх сервісів (Google Sheets, Telegram)."""
    monkeypatch.setattr('vlk_bot.config.SHEETS_SERVICE', None)
    monkeypatch.setattr('vlk_bot.config.is_bot_in_group', False)


@pytest.fixture
def mock_update():
    """Створює мок об'єкта Update для тестування Telegram обробників."""
    update = MagicMock(spec=Update)
    update.effective_user = MagicMock(spec=User)
    update.effective_user.id = 12345
    update.effective_user.username = "testuser"
    update.effective_user.full_name = "Test User"
    update.effective_user.mention_html.return_value = "<a href='tg://user?id=12345'>Test User</a>"

    update.message = MagicMock(spec=Message)
    update.message.text = "some text"
    update.message.chat = MagicMock(spec=Chat)
    update.message.chat.type = 'private'
    update.message.reply_text = AsyncMock()
    update.message.reply_html = AsyncMock()
    update.message.reply_photo = AsyncMock()
    return update


@pytest.fixture
def mock_context():
    """Створює мок об'єкта ContextTypes для тестування Telegram обробників."""
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    context.user_data = {}
    context.bot_data = {}
    context.bot = MagicMock()
    context.bot.send_message = AsyncMock()
    context.args = []
    return context


@pytest.fixture
def sample_queue_df():
    """Створює тестовий DataFrame черги."""
    return pd.DataFrame({
        'ID': ['100', '101', '102'],
        'Дата': ['01.01.2025', '02.01.2025', '03.01.2025'],
        'Примітки': ['', 'note', ''],
        'Статус': ['Ухвалено', 'На розгляді', 'Ухвалено'],
        'Змінено': ['01.01.2025 10:00:00', '01.01.2025 11:00:00', '01.01.2025 12:00:00'],
        'Попередня дата': ['', '', '31.12.2024'],
        'TG ID': ['111', '222', '333'],
        'TG Name': ['user1', 'user2', 'user3'],
        'TG Full Name': ['User One', 'User Two', 'User Three']
    })


@pytest.fixture
def sample_stats_df():
    """Створює тестовий DataFrame статистики."""
    dates = [
        '01.01.2024', '02.01.2024', '03.01.2024', '04.01.2024', '05.01.2024',
        '08.01.2024', '09.01.2024', '10.01.2024'
    ]
    return pd.DataFrame({
        'Дата прийому': dates,
        'Останній номер що зайшов': [10, 20, 30, 40, 50, 60, 70, 80],
        'Перший номер що зайшов': [1, 11, 21, 31, 41, 51, 61, 71],
        'Зайшов': [10, 10, 10, 10, 10, 10, 10, 10],
        'Аркуш': dates
    })


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Створює тимчасову директорію для кешу."""
    cache_dir = tmp_path / "daily_sheets_cache"
    cache_dir.mkdir()
    return cache_dir


@pytest.fixture
def sample_daily_csv_content():
    """Повертає зразок вмісту щоденного CSV файлу."""
    return """Дата прийому: 01.01.2024,,,,Наступний прийом: 02.01.2024,,,
Попередній прийом:,,,,,Поточна черга:,,
№,ID,Статус,Примітки,,№,Примітки,ID,Статус
1,100,Зайшов,,,1,,150,
2,101,Зайшов,,,2,,151,
3,102,Зайшов,,,3,,152,
4,103,Не з'явився,,,4,,153,
5,104,Зайшов,,,5,,154,
6,105,Зайшов,,,6,,155,
7,106,Зайшов,,,7,,156,
8,107,Зайшов,,,8,,157,
9,108,Зайшов,,,9,,158,
10,109,Зайшов,,,10,,159,
11,110,Зайшов,,,11,,160,
12,111,Зайшов,,,12,,161,
13,112,Зайшов,,,13,,162,
14,113,Зайшов,,,14,,163,
15,114,Зайшов,,,15,,164,
"""


@pytest.fixture
def sample_attendance_json():
    """Повертає зразок даних attendance_data.json."""
    return {
        'attendance_points': [
            {'date': '2024-01-01', 'id': '100', 'is_live': False},
            {'date': '2024-01-01', 'id': '101', 'is_live': False},
            {'date': '2024-01-01', 'id': '102', 'is_live': False},
            {'date': '2024-01-02', 'id': '110', 'is_live': False},
            {'date': '2024-01-02', 'id': '111', 'is_live': False},
            {'date': '2024-01-02', 'id': '112', 'is_live': True},
            {'date': '2024-01-03', 'id': '120', 'is_live': False},
            {'date': '2024-01-03', 'id': '121', 'is_live': False},
        ],
        'total_points': 8
    }


@pytest.fixture
def mock_sheets_service():
    """Створює мок Google Sheets API сервісу."""
    service = MagicMock()
    spreadsheets = MagicMock()
    values = MagicMock()

    service.spreadsheets.return_value = spreadsheets
    spreadsheets.values.return_value = values

    values.get.return_value.execute.return_value = {'values': []}
    values.append.return_value.execute.return_value = {}
    values.update.return_value.execute.return_value = {}
    values.clear.return_value.execute.return_value = {}

    return service


@pytest.fixture
def status_state_file(tmp_path):
    """Створює тимчасовий файл стану статусів."""
    status_file = tmp_path / "status.json"
    initial_state = {
        '100': {'date': '01.01.2025', 'status': 'Ухвалено', 'modified': '01.01.2025 10:00:00', 'confirmation': ''},
        '101': {'date': '02.01.2025', 'status': 'На розгляді', 'modified': '01.01.2025 11:00:00', 'confirmation': ''}
    }
    status_file.write_text(json.dumps(initial_state, ensure_ascii=False))
    return status_file


@pytest.fixture
def mock_config():
    """Створює мок конфігурації."""
    return {
        'BOT_SETTINGS': {
            'TOKEN': 'test_token',
            'ADMIN_IDS': '12345',
            'GROUP_ID': '-100123456789',
            'STATUS_FILE': 'status.json',
            'BANLIST': '',
            'ENVIRONMENT': 'test'
        },
        'GOOGLE_SHEETS': {
            'SERVICE_ACCOUNT_KEY_PATH': 'service_account_key.json',
            'SPREADSHEET_ID': 'test_spreadsheet_id',
            'SHEET_NAME': 'TODO',
            'STATS_SHEET_ID': 'test_stats_id',
            'STATS_WORKSHEET_NAME': 'Stats',
            'ACTIVE_SHEET_ID': 'test_active_id',
            'ACTIVE_WORKSHEET_NAME': 'Active'
        }
    }
