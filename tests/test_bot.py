"""
Тести для VLK_Zakrevskoho_81_BOT.py
"""

import pytest
import pandas as pd
import datetime
import numpy as np
import re
import sys
import os
from unittest.mock import MagicMock, patch, AsyncMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import VLK_Zakrevskoho_81_BOT as bot

bot.ADMIN_IDS = [12345]
bot.BANLIST = []


def test_get_ordinal_date():
    assert bot.get_ordinal_date(datetime.date(1970, 1, 5)) == 0
    assert bot.get_ordinal_date(datetime.date(1970, 1, 9)) == 4
    assert bot.get_ordinal_date(datetime.date(1970, 1, 10)) == 5
    assert bot.get_ordinal_date(datetime.date(1970, 1, 12)) == 5


def test_get_date_from_ordinal():
    anchor = datetime.date(1970, 1, 5)
    assert bot.get_date_from_ordinal(0) == anchor
    assert bot.get_date_from_ordinal(5) == datetime.date(1970, 1, 12)


def test_extract_main_id():
    assert bot.extract_main_id("123") == 123
    assert bot.extract_main_id("123/1") == 123
    assert bot.extract_main_id("abc") is None
    assert bot.extract_main_id(123) is None


def test_is_admin():
    bot.ADMIN_IDS = [123, 456]
    assert bot.is_admin(123) is True
    assert bot.is_admin(999) is False


def test_is_banned():
    bot.BANLIST = [111]
    assert bot.is_banned(111) is True
    assert bot.is_banned(222) is False


def test_calculate_end_date():
    start = datetime.date(2023, 1, 2)
    assert bot.calculate_end_date(start, 2) == datetime.date(2023, 1, 3)
    
    start_friday = datetime.date(2023, 1, 6)
    assert bot.calculate_end_date(start_friday, 3) == datetime.date(2023, 1, 10)


def test_date_keyboard_format():
    today = datetime.date(2025, 12, 1)
    keyboard = bot.date_keyboard(today=today, days_to_check=1, days_ahead=1)
    
    buttons = keyboard.keyboard
    first_button_text = buttons[0][0].text
    
    assert "02.12.25" in first_button_text
    assert re.search(r'\d{2}\.\d{2}\.\d{2}', first_button_text)


def test_get_next_working_days():
    days = bot.get_next_working_days(3)
    assert len(days) == 3
    for d in days:
        assert d.weekday() < 5


def test_generate_date_options():
    today = datetime.date(2025, 1, 6)
    options = bot.generate_date_options(today=today, days_to_check=1, days_ahead=5)
    
    assert len(options) == 5
    for opt in options:
        assert 'date' in opt
        assert 'text' in opt
        assert 'date_str' in opt
        assert opt['date'].weekday() < 5


def test_get_poll_keyboard():
    keyboard = bot.get_poll_keyboard("123")
    assert keyboard is not None
    assert len(keyboard.inline_keyboard) == 3


def test_get_poll_text():
    text = bot.get_poll_text("123", "01.01.2025")
    assert "123" in text
    assert "01.01.2025" in text


def test_calculate_date_probability():
    dist = {'loc': 100, 'scale': 10, 'df': 50}
    prob = bot.calculate_date_probability(datetime.date(2025, 1, 6), dist)
    assert isinstance(prob, float)
    assert 0 <= prob <= 100


def test_calculate_prediction_insufficient_data():
    with patch('daily_sheets_sync.calculate_prediction_with_daily_data', return_value=None):
        assert bot.calculate_prediction(100, None) is None
        assert bot.calculate_prediction(100, pd.DataFrame()) is None


def test_calculate_prediction_valid():
    dates = [
        '01.01.2024', '02.01.2024', '03.01.2024', '04.01.2024', '05.01.2024', '08.01.2024'
    ]
    ids_max = [10, 20, 30, 40, 50, 60]
    ids_min = [5, 15, 25, 35, 45, 55]
    
    df = pd.DataFrame({
        'Останній номер що зайшов': ids_max,
        'Перший номер що зайшов': ids_min,
        'Дата прийому': dates
    })
    
    res = bot.calculate_prediction(70, df)
    
    assert res is not None
    assert isinstance(res['mean'], datetime.date)
    assert isinstance(res['l90'], datetime.date)
    assert isinstance(res['h90'], datetime.date)


def test_get_user_log_info():
    user = MagicMock()
    user.id = 12345
    user.username = "testuser"
    user.full_name = "Test User"
    
    info = bot.get_user_log_info(user)
    assert "12345" in info
    assert "testuser" in info


def test_get_user_telegram_data():
    user = MagicMock()
    user.id = 12345
    user.username = "testuser"
    user.full_name = "Test User"
    
    data = bot.get_user_telegram_data(user)
    assert data['TG ID'] == 12345
    assert data['TG Name'] == "testuser"
    assert data['TG Full Name'] == "Test User"


def test_format_prediction_range_text():
    prediction = {
        'mean': datetime.date(2025, 6, 15),
        'h90': datetime.date(2025, 7, 15),
        'dist': {'loc': 100, 'scale': 10, 'df': 50}
    }
    text = bot.format_prediction_range_text(prediction, datetime.date(2025, 1, 1))
    assert "15.06.2025" in text or "15.07.2025" in text


@pytest.mark.asyncio
async def test_start_private_chat(mock_update, mock_context):
    mock_update.message.chat.type = 'private'
    await bot.start(mock_update, mock_context)
    assert mock_update.message.reply_photo.called or mock_update.message.reply_html.called


@pytest.mark.asyncio
async def test_join_start_banned(mock_update, mock_context):
    with patch('VLK_Zakrevskoho_81_BOT.is_banned', return_value=True):
        res = await bot.join_start(mock_update, mock_context)
        assert res == -1
        mock_update.message.reply_text.assert_called_with(
            "Ваш обліковвй запис заблоковано. Зверніться до адміністраторів щоб розблокувати.",
            reply_markup=bot.MAIN_KEYBOARD
        )


@pytest.mark.asyncio
async def test_join_start_success(mock_update, mock_context):
    with patch('VLK_Zakrevskoho_81_BOT.is_banned', return_value=False):
        with patch('VLK_Zakrevskoho_81_BOT.load_queue_data', return_value=pd.DataFrame(columns=bot.REQUIRED_COLUMNS)):
            res = await bot.join_start(mock_update, mock_context)
            assert res == bot.JOIN_GETTING_ID
            assert mock_context.user_data['telegram_user_data']['TG ID'] == 12345


@pytest.mark.asyncio
async def test_join_get_id_invalid(mock_update, mock_context):
    mock_update.message.text = "invalid_id"
    res = await bot.join_get_id(mock_update, mock_context)
    assert res == bot.JOIN_GETTING_ID
    mock_update.message.reply_text.assert_called()
    assert "Невірний формат" in mock_update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_join_get_id_valid_no_stats(mock_update, mock_context):
    mock_update.message.text = "999"
    bot.queue_df = pd.DataFrame(columns=bot.REQUIRED_COLUMNS)
    
    with patch('VLK_Zakrevskoho_81_BOT.check_id_for_queue', new_callable=AsyncMock) as mock_check:
        mock_check.return_value = (True, "")
        with patch('VLK_Zakrevskoho_81_BOT.get_stats_data', new_callable=AsyncMock) as mock_stats:
            mock_stats.return_value = None
            
            res = await bot.join_get_id(mock_update, mock_context)
            
            assert res == bot.JOIN_GETTING_DATE
            assert mock_context.user_data['temp_id'] == "999"
            assert "Виберіть бажану дату запису" in mock_update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_join_get_date_invalid(mock_update, mock_context):
    mock_context.user_data = {'temp_id': '999'}
    mock_update.message.text = "invalid date"
    
    res = await bot.join_get_date(mock_update, mock_context)
    assert res == bot.JOIN_GETTING_DATE
    assert "Невірний формат дати" in mock_update.message.reply_html.call_args[0][0]


@pytest.mark.asyncio
async def test_join_get_date_past(mock_update, mock_context):
    mock_context.user_data = {'temp_id': '999'}
    past_date = datetime.date.today() - datetime.timedelta(days=1)
    mock_update.message.text = past_date.strftime("%d.%m.%Y")
    
    res = await bot.join_get_date(mock_update, mock_context)
    assert res == bot.JOIN_GETTING_DATE
    assert "Дата повинна бути пізнішою за поточну" in mock_update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_join_get_date_weekend(mock_update, mock_context):
    mock_context.user_data = {'temp_id': '999'}
    saturday = datetime.date.today()
    while saturday.weekday() != 5:
        saturday += datetime.timedelta(days=1)
    mock_update.message.text = saturday.strftime("%d.%m.%Y")
    
    res = await bot.join_get_date(mock_update, mock_context)
    assert res == bot.JOIN_GETTING_DATE
    mock_update.message.reply_html.assert_called()


@pytest.mark.asyncio
async def test_join_get_date_success(mock_update, mock_context):
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
    bot.queue_df = pd.DataFrame(columns=bot.REQUIRED_COLUMNS)
    
    with patch('VLK_Zakrevskoho_81_BOT.save_queue_data', return_value=True):
        with patch('VLK_Zakrevskoho_81_BOT.send_group_notification', new_callable=AsyncMock):
            res = await bot.join_get_date(mock_update, mock_context)
            assert res == -1
            assert "успішно створили заявку" in mock_update.message.reply_text.call_args[0][0]
            assert not bot.queue_df.empty
            assert bot.queue_df.iloc[-1]['ID'] == '999'


@pytest.mark.asyncio
async def test_cancel_record_start(mock_update, mock_context):
    with patch('VLK_Zakrevskoho_81_BOT.is_banned', return_value=False):
        with patch('VLK_Zakrevskoho_81_BOT.load_queue_data', return_value=pd.DataFrame(columns=bot.REQUIRED_COLUMNS)):
            res = await bot.cancel_record_start(mock_update, mock_context)
            assert res == bot.CANCEL_GETTING_ID


@pytest.mark.asyncio
async def test_cancel_record_get_id_invalid(mock_update, mock_context):
    mock_update.message.text = "invalid_id"
    mock_context.user_data = {'telegram_user_data': {'TG ID': 12345}}
    
    res = await bot.cancel_record_get_id(mock_update, mock_context)
    assert res == bot.CANCEL_GETTING_ID
    assert "Невірний формат" in mock_update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_status_start(mock_update, mock_context):
    with patch('VLK_Zakrevskoho_81_BOT.load_queue_data', return_value=pd.DataFrame(columns=bot.REQUIRED_COLUMNS)):
        res = await bot.status_start(mock_update, mock_context)
        assert res == bot.STATUS_GETTING_ID


@pytest.mark.asyncio
async def test_status_get_id_not_found(mock_update, mock_context):
    mock_update.message.text = "999"
    bot.queue_df = pd.DataFrame(columns=bot.REQUIRED_COLUMNS)
    
    res = await bot.status_get_id(mock_update, mock_context)
    assert res == -1
    assert "не знайдено" in mock_update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_show_start(mock_update, mock_context):
    with patch('VLK_Zakrevskoho_81_BOT.load_queue_data', return_value=pd.DataFrame(columns=bot.REQUIRED_COLUMNS)):
        res = await bot.show_start(mock_update, mock_context)
        assert res == bot.SHOW_GETTING_OPTION


@pytest.mark.asyncio
async def test_grant_admin_unauthorized(mock_update, mock_context):
    bot.ADMIN_IDS = [999]
    await bot.grant_admin(mock_update, mock_context)
    mock_update.message.reply_text.assert_called_with(
        "У вас недостатньо прав для виконання цієї команди.",
        reply_markup=bot.MAIN_KEYBOARD
    )


@pytest.mark.asyncio
async def test_grant_admin_success(mock_update, mock_context):
    bot.ADMIN_IDS = [12345]
    bot.config['BOT_SETTINGS'] = {'ADMIN_IDS': '12345'}
    
    mock_context.args = ["67890"]
    
    with patch('VLK_Zakrevskoho_81_BOT.save_config'):
        await bot.grant_admin(mock_update, mock_context)
        
    assert 67890 in bot.ADMIN_IDS
    assert "успішно доданий" in mock_update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_drop_admin_unauthorized(mock_update, mock_context):
    bot.ADMIN_IDS = [999]
    await bot.drop_admin(mock_update, mock_context)
    mock_update.message.reply_text.assert_called_with(
        "У вас недостатньо прав для виконання цієї команди.",
        reply_markup=bot.MAIN_KEYBOARD
    )


@pytest.mark.asyncio
async def test_ban_unauthorized(mock_update, mock_context):
    bot.ADMIN_IDS = [999]
    await bot.ban(mock_update, mock_context)
    mock_update.message.reply_text.assert_called_with(
        "У вас недостатньо прав для виконання цієї команди.",
        reply_markup=bot.MAIN_KEYBOARD
    )


@pytest.mark.asyncio
async def test_ban_success(mock_update, mock_context):
    bot.ADMIN_IDS = [12345]
    bot.BANLIST = []
    bot.config['BOT_SETTINGS'] = {'BANLIST': ''}
    
    mock_context.args = ["99999"]
    
    with patch('VLK_Zakrevskoho_81_BOT.save_config'):
        await bot.ban(mock_update, mock_context)
        
    assert 99999 in bot.BANLIST


@pytest.mark.asyncio
async def test_unban_unauthorized(mock_update, mock_context):
    bot.ADMIN_IDS = [999]
    await bot.unban(mock_update, mock_context)
    mock_update.message.reply_text.assert_called_with(
        "У вас недостатньо прав для виконання цієї команди.",
        reply_markup=bot.MAIN_KEYBOARD
    )


@pytest.mark.asyncio
async def test_perform_queue_cleanup(sample_queue_df):
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
        'TG ID': ['1', '2', '3'],
        'Примітки': ['', '', ''],
        'Попередня дата': ['', '', ''],
        'TG Name': ['', '', ''],
        'TG Full Name': ['', '', '']
    })
            
    with patch('VLK_Zakrevskoho_81_BOT.load_queue_data', return_value=df):
        with patch('VLK_Zakrevskoho_81_BOT.save_queue_data_full', return_value=True):
            removed = await bot.perform_queue_cleanup()
            
            assert removed >= 1
            assert len(bot.queue_df) < 3
            assert '2' in bot.queue_df['ID'].values


@pytest.mark.asyncio
async def test_cancel_conversation(mock_update, mock_context):
    res = await bot.cancel_conversation(mock_update, mock_context)
    assert res == -1
    mock_update.message.reply_text.assert_called()


@pytest.mark.asyncio
async def test_prediction_command(mock_update, mock_context):
    await bot.prediction_command(mock_update, mock_context)
    mock_update.message.reply_text.assert_called()
    assert "zbstof.github.io" in mock_update.message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_help_command(mock_update, mock_context):
    await bot.help_command(mock_update, mock_context)
    mock_update.message.reply_text.assert_called()

