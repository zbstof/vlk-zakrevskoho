import datetime
import re
from unittest.mock import MagicMock, AsyncMock

import pandas as pd
import pytest
from telegram import Update, User, Message, Chat
from telegram.ext import ContextTypes

import vlk_bot.config as config
from vlk_bot.config import (
    REQUIRED_COLUMNS,
    JOIN_GETTING_ID,
    JOIN_GETTING_DATE,
)
from vlk_bot.formatters import calculate_end_date
from vlk_bot.handlers_admin import grant_admin, perform_queue_cleanup
from vlk_bot.handlers_common import start
from vlk_bot.handlers_join import join_start, join_get_id, join_get_date
from vlk_bot.keyboards import MAIN_KEYBOARD, date_keyboard
from vlk_bot.prediction import calculate_prediction
from vlk_bot.utils import (
    get_ordinal_date,
    get_date_from_ordinal,
    extract_main_id,
    is_admin,
    is_banned,
)


@pytest.fixture
def mock_update():
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
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    context.user_data = {}
    context.bot = MagicMock()
    context.bot.send_message = AsyncMock()
    context.args = []
    return context


@pytest.fixture
def sample_queue_df():
    return pd.DataFrame({
        'ID': ['100', '101'],
        'Дата': ['01.01.2025', '02.01.2025'],
        'Примітки': ['', 'note'],
        'Статус': ['Ухвалено', 'На розгляді'],
        'Змінено': ['01.01.2025 10:00:00', '01.01.2025 11:00:00'],
        'Попередня дата': ['', ''],
        'TG ID': [111, 222],
        'TG Name': ['user1', 'user2'],
        'TG Full Name': ['User One', 'User Two']
    }, columns=REQUIRED_COLUMNS)


@pytest.fixture
def empty_queue_df():
    return pd.DataFrame(columns=REQUIRED_COLUMNS)


@pytest.fixture
def mock_prediction_disabled(monkeypatch):
    """Вимикає синхронізацію та завантаження даних для прогнозування."""
    monkeypatch.setattr('vlk_bot.sync.sync_daily_sheets', lambda *a, **kw: True)
    monkeypatch.setattr('vlk_bot.sync.load_attendance_from_json', lambda *a, **kw: None)
    monkeypatch.setattr('vlk_bot.sync.get_historical_attendance_data', lambda *a, **kw: None)


@pytest.fixture
def mock_join_flow(monkeypatch, empty_queue_df):
    """Налаштовує моки для flow запису в чергу."""
    monkeypatch.setattr('vlk_bot.handlers_join.is_banned', lambda _: False)
    monkeypatch.setattr('vlk_bot.handlers_join.load_queue_data', lambda: empty_queue_df)
    monkeypatch.setattr('vlk_bot.handlers_join.save_queue_data', lambda _: True)

    async def mock_check(*args):
        return (True, "")
    monkeypatch.setattr('vlk_bot.handlers_join.check_id_for_queue', mock_check)

    async def mock_stats(*args, **kwargs):
        return None
    monkeypatch.setattr('vlk_bot.handlers_join.get_stats_data', mock_stats)


@pytest.fixture
def mock_admin_config(monkeypatch):
    """Налаштовує моки для адмін-команд."""
    admin_ids = [12345]
    mock_config_obj = MagicMock()
    mock_config_obj.__getitem__ = MagicMock(return_value={'ADMIN_IDS': '12345'})

    monkeypatch.setattr(config, 'ADMIN_IDS', admin_ids)
    monkeypatch.setattr(config, 'config', mock_config_obj)
    monkeypatch.setattr(config, 'save_config', lambda: None)

    return admin_ids


def test_get_ordinal_date():
    assert get_ordinal_date(datetime.date(1970, 1, 5)) == 0
    assert get_ordinal_date(datetime.date(1970, 1, 9)) == 4
    assert get_ordinal_date(datetime.date(1970, 1, 10)) == 5
    assert get_ordinal_date(datetime.date(1970, 1, 12)) == 5


def test_get_date_from_ordinal():
    anchor = datetime.date(1970, 1, 5)
    assert get_date_from_ordinal(0) == anchor
    assert get_date_from_ordinal(5) == datetime.date(1970, 1, 12)


def test_extract_main_id():
    assert extract_main_id("123") == 123
    assert extract_main_id("123/1") == 123
    assert extract_main_id("abc") is None
    assert extract_main_id(123) is None


def test_is_admin(monkeypatch):
    monkeypatch.setattr(config, 'ADMIN_IDS', [123, 456])
    assert is_admin(123) is True
    assert is_admin(999) is False


def test_is_banned(monkeypatch):
    monkeypatch.setattr(config, 'BANLIST', [111])
    assert is_banned(111) is True
    assert is_banned(222) is False


def test_calculate_end_date():
    start = datetime.date(2023, 1, 2)
    assert calculate_end_date(start, 2) == datetime.date(2023, 1, 3)


def test_date_keyboard_format():
    today = datetime.date(2025, 12, 1)
    keyboard = date_keyboard(today=today, days_to_check=1, days_ahead=1)

    buttons = keyboard.keyboard
    first_button_text = buttons[0][0].text

    assert "02.12.25" in first_button_text
    assert re.search(r'\d{2}\.\d{2}\.\d{2}', first_button_text)


def test_calculate_prediction_insufficient_data(mock_prediction_disabled):
    assert calculate_prediction(100, None) is None
    assert calculate_prediction(100, pd.DataFrame()) is None

    df = pd.DataFrame({
        'Останній номер що зайшов': ['1', '2'],
        'Дата прийому': ['01.01.2025', '02.01.2025']
    })
    assert calculate_prediction(100, df) is None


def test_calculate_prediction_valid():
    dates = [
        '01.01.2024',
        '02.01.2024',
        '03.01.2024',
        '04.01.2024',
        '05.01.2024',
        '08.01.2024'
    ]
    ids_max = [10, 20, 30, 40, 50, 60]
    ids_min = [5, 15, 25, 35, 45, 55]

    df = pd.DataFrame({
        'Останній номер що зайшов': ids_max,
        'Перший номер що зайшов': ids_min,
        'Дата прийому': dates
    })

    res = calculate_prediction(70, df)

    assert res is not None
    assert isinstance(res['mean'], datetime.date)
    assert isinstance(res['l90'], datetime.date)
    assert isinstance(res['h90'], datetime.date)


@pytest.mark.asyncio
async def test_start_private_chat(mock_update, mock_context):
    mock_update.message.chat.type = 'private'
    await start(mock_update, mock_context)

    assert mock_update.message.reply_photo.called or mock_update.message.reply_html.called


@pytest.mark.asyncio
async def test_join_start_banned(mock_update, mock_context, monkeypatch):
    monkeypatch.setattr('vlk_bot.handlers_join.is_banned', lambda _: True)

    res = await join_start(mock_update, mock_context)

    assert res == -1
    mock_update.message.reply_text.assert_called_with(
        "Ваш обліковий запис заблоковано. Зверніться до адміністраторів щоб розблокувати.",
        reply_markup=MAIN_KEYBOARD
    )


@pytest.mark.asyncio
async def test_join_start_success(mock_update, mock_context, mock_join_flow):
    res = await join_start(mock_update, mock_context)

    assert res == JOIN_GETTING_ID
    assert mock_context.user_data['telegram_user_data']['TG ID'] == 12345


@pytest.mark.asyncio
async def test_join_get_id_invalid(mock_update, mock_context):
    mock_update.message.text = "invalid_id"
    res = await join_get_id(mock_update, mock_context)
    assert res == JOIN_GETTING_ID
    mock_update.message.reply_text.assert_called()
    assert "Невірний формат" in mock_update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_join_get_id_valid_no_stats(mock_update, mock_context, mock_join_flow):
    mock_update.message.text = "999"
    config.queue_df = pd.DataFrame(columns=REQUIRED_COLUMNS)

    res = await join_get_id(mock_update, mock_context)

    assert res == JOIN_GETTING_DATE
    assert mock_context.user_data['temp_id'] == "999"
    assert "Виберіть бажану дату запису" in mock_update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_join_get_date_invalid(mock_update, mock_context):
    mock_context.user_data = {'temp_id': '999'}
    mock_update.message.text = "invalid date"

    res = await join_get_date(mock_update, mock_context)
    assert res == JOIN_GETTING_DATE
    assert "Невірний формат дати" in mock_update.message.reply_html.call_args[0][0]


@pytest.mark.asyncio
async def test_join_get_date_past(mock_update, mock_context):
    mock_context.user_data = {'temp_id': '999'}
    past_date = datetime.date.today() - datetime.timedelta(days=1)
    mock_update.message.text = past_date.strftime("%d.%m.%Y")

    res = await join_get_date(mock_update, mock_context)
    assert res == JOIN_GETTING_DATE
    assert "Дата повинна бути пізнішою за поточну" in mock_update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_join_get_date_success(mock_update, mock_context, mock_join_flow):
    mock_context.user_data = {
        'temp_id': '999',
        'telegram_user_data': {'TG ID': 12345, 'TG Name': 'test', 'TG Full Name': 'Test'},
        'previous_state': '',
        'user_notes': ''
    }

    future_date = datetime.date.today() + datetime.timedelta(days=10)
    while future_date.weekday() >= 5:
        future_date += datetime.timedelta(days=1)

    mock_update.message.text = future_date.strftime("%d.%m.%Y")
    config.queue_df = pd.DataFrame(columns=REQUIRED_COLUMNS)

    res = await join_get_date(mock_update, mock_context)

    assert res == -1
    assert "успішно створили заявку" in mock_update.message.reply_text.call_args[0][0]
    assert not config.queue_df.empty
    assert config.queue_df.iloc[-1]['ID'] == '999'


@pytest.mark.asyncio
async def test_grant_admin_unauthorized(mock_update, mock_context, monkeypatch):
    monkeypatch.setattr(config, 'ADMIN_IDS', [999])

    await grant_admin(mock_update, mock_context)

    mock_update.message.reply_text.assert_called_with(
        "У вас недостатньо прав для виконання цієї команди.",
        reply_markup=MAIN_KEYBOARD
    )


@pytest.mark.asyncio
async def test_grant_admin_success(mock_update, mock_context, mock_admin_config):
    mock_context.args = ["67890"]

    await grant_admin(mock_update, mock_context)

    assert 67890 in mock_admin_config
    assert "успішно доданий" in mock_update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_perform_queue_cleanup(sample_queue_df, monkeypatch):
    today = datetime.date.today()
    past = today - datetime.timedelta(days=10)
    future = today + datetime.timedelta(days=10)

    df = pd.DataFrame({
        'ID': ['1', '2', '3'],
        'Дата': [past.strftime("%d.%m.%Y"), future.strftime("%d.%m.%Y"), ''],
        'Статус': ['Ухвалено', 'Ухвалено', 'Відхилено'],
        'Змінено': [
            '01.01.2023 10:00:00',
            '01.01.2025 10:00:00',
            '01.01.2023 10:00:00'
        ],
        'TG ID': ['1', '2', '3']
    }, columns=REQUIRED_COLUMNS)

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = ''

    monkeypatch.setattr('vlk_bot.handlers_admin.load_queue_data', lambda: df)
    monkeypatch.setattr('vlk_bot.handlers_admin.save_queue_data_full', lambda _: True)

    removed = await perform_queue_cleanup()

    assert removed >= 1
    assert len(config.queue_df) < 3
    assert '2' in config.queue_df['ID'].values
