"""
UI компоненти: клавіатури та кнопки.
"""

import datetime
import logging

from telegram import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)

BUTTON_TEXT_JOIN = "Записатися / Перенести"
BUTTON_TEXT_SHOW = "Переглянути чергу"
BUTTON_TEXT_CANCEL_RECORD = "Скасувати запис"
BUTTON_TEXT_PREDICTION = "Прогноз черги"
BUTTON_TEXT_CANCEL_OP = "Скасувати ввід"
BUTTON_TEXT_STATUS = "Переглянути статус"
BUTTON_TEXT_SHOW_ALL = "Показати всі записи"
BUTTON_TEXT_SHOW_DATE = "Показати записи на конкретну дату"

button_join = KeyboardButton(BUTTON_TEXT_JOIN)
button_show = KeyboardButton(BUTTON_TEXT_SHOW)
button_cancel_record = KeyboardButton(BUTTON_TEXT_CANCEL_RECORD)
button_prediction = KeyboardButton(BUTTON_TEXT_PREDICTION)
button_cancel_op = KeyboardButton(BUTTON_TEXT_CANCEL_OP)
button_status = KeyboardButton(BUTTON_TEXT_STATUS)

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [[button_join, button_cancel_record], [button_status, button_show], [button_prediction]],
    one_time_keyboard=False,
    resize_keyboard=True
)

CANCEL_KEYBOARD = ReplyKeyboardMarkup(
    [[KeyboardButton(BUTTON_TEXT_CANCEL_OP)]], 
    one_time_keyboard=True, 
    resize_keyboard=True
)

SHOW_OPTION_KEYBOARD = ReplyKeyboardMarkup([
    [KeyboardButton(BUTTON_TEXT_SHOW_ALL)],
    [KeyboardButton(BUTTON_TEXT_SHOW_DATE)],
    [KeyboardButton(BUTTON_TEXT_CANCEL_OP)]
], one_time_keyboard=True, resize_keyboard=True)


def generate_date_options(today=None, days_to_check=0, days_ahead=15, 
                          start_date=None, end_date=None, prediction_dist=None) -> list:
    """
    Генерує список дат для вибору з текстом кнопки та ймовірністю.
    """
    from vlk_bot.utils import get_ua_weekday
    from vlk_bot.prediction import calculate_date_probability
    
    if today is None:
        today = datetime.date.today()
    
    date_options = []
    current_check_date = today + datetime.timedelta(days=days_to_check)
    
    logger.debug(f"generate_date_options: start_date={start_date}, end_date={end_date}")
    
    if start_date and end_date:
        iter_date = max(current_check_date, start_date)
        limit_date = end_date
        
        while iter_date <= limit_date:
            if iter_date.weekday() < 5:
                date_str_short = iter_date.strftime("%d.%m.%y")
                date_str_full = iter_date.strftime("%d.%m.%Y")
                weekday_str = get_ua_weekday(iter_date)
                button_text = f"{weekday_str}: {date_str_short}"
                
                if prediction_dist:
                    percent = calculate_date_probability(iter_date, prediction_dist)
                    if percent >= 0.1:
                        button_text = f"{button_text} ({percent:.0f}%)"
                
                date_options.append({
                    'date': iter_date,
                    'text': button_text,
                    'date_str': date_str_full
                })
            iter_date += datetime.timedelta(days=1)
            if len(date_options) >= 30:
                break
    else:
        buttons_added = 0
        iter_date = current_check_date
        while buttons_added < days_ahead:
            if iter_date.weekday() < 5:
                date_str_short = iter_date.strftime("%d.%m.%y")
                date_str_full = iter_date.strftime("%d.%m.%Y")
                weekday_str = get_ua_weekday(iter_date)
                button_text = f"{weekday_str}: {date_str_short}"
                
                if prediction_dist:
                    percent = calculate_date_probability(iter_date, prediction_dist)
                    if percent >= 0.1:
                        button_text = f"{button_text} ({percent:.0f}%)"
                
                date_options.append({
                    'date': iter_date,
                    'text': button_text,
                    'date_str': date_str_full
                })
                buttons_added += 1
            iter_date += datetime.timedelta(days=1)
    
    return date_options


def date_keyboard(today=None, days_to_check=0, days_ahead=15, 
                  start_date=None, end_date=None, prediction_dist=None) -> ReplyKeyboardMarkup:
    """
    Створює ReplyKeyboardMarkup з датами.
    """
    if today is None:
        today = datetime.date.today()
    
    date_options = generate_date_options(today, days_to_check, days_ahead, 
                                         start_date, end_date, prediction_dist)
    
    flat_keyboard_buttons = [KeyboardButton(opt['text']) for opt in date_options]
    
    chunk_size = 3
    keyboard_buttons = [flat_keyboard_buttons[i:i + chunk_size] 
                        for i in range(0, len(flat_keyboard_buttons), chunk_size)]
    keyboard_buttons.append([button_cancel_op])
    
    return ReplyKeyboardMarkup(keyboard_buttons, one_time_keyboard=True, resize_keyboard=True)


def get_prediction_date_range(prediction: dict, today: datetime.date = None) -> tuple:
    """
    Обчислює діапазон дат з прогнозу.
    """
    if prediction is None:
        return None, None, None
    
    if today is None:
        today = datetime.date.today()
    
    min_date = today + datetime.timedelta(days=1)
    start_date = prediction.get('mean')
    end_date = prediction.get('h90')
    prediction_dist = prediction.get('dist')
    
    if start_date:
        start_date = max(start_date, min_date)
        while start_date.weekday() >= 5:
            start_date += datetime.timedelta(days=1)
        
        if end_date and start_date > end_date:
            start_date = min_date
            while start_date.weekday() >= 5:
                start_date += datetime.timedelta(days=1)
            end_date = None
    
    return start_date, end_date, prediction_dist


def date_keyboard_from_prediction(prediction: dict, today: datetime.date = None, 
                                   days_ahead: int = 15) -> ReplyKeyboardMarkup:
    """
    Створює клавіатуру з датами на основі прогнозу.
    """
    if today is None:
        today = datetime.date.today()
    
    start_date, end_date, prediction_dist = get_prediction_date_range(prediction, today)
    return date_keyboard(today, 1, days_ahead, start_date=start_date, 
                        end_date=end_date, prediction_dist=prediction_dist)


def date_inline_keyboard(user_id: str, today=None, days_to_check=0, days_ahead=15, 
                         start_date=None, end_date=None, prediction_dist=None, 
                         columns=2) -> InlineKeyboardMarkup:
    """
    Створює InlineKeyboardMarkup з датами для опитування.
    """
    from vlk_bot.config import POLL_DATE, POLL_DATE_OTHER, POLL_CANCEL_ABORT
    
    if today is None:
        today = datetime.date.today()
    
    date_options = generate_date_options(today, days_to_check, days_ahead, 
                                         start_date, end_date, prediction_dist)
    
    flat_buttons = []
    for opt in date_options:
        callback_data = f"{POLL_DATE}_{user_id}_{opt['date_str']}"
        flat_buttons.append(InlineKeyboardButton(opt['text'], callback_data=callback_data))
    
    keyboard_buttons = [flat_buttons[i:i + columns] for i in range(0, len(flat_buttons), columns)]
    keyboard_buttons.append([
        InlineKeyboardButton("Інша дата", callback_data=f"{POLL_DATE_OTHER}_{user_id}"),
        InlineKeyboardButton("Скасувати", callback_data=f"{POLL_CANCEL_ABORT}_{user_id}")
    ])
    
    return InlineKeyboardMarkup(keyboard_buttons)


def date_inline_keyboard_from_prediction(user_id: str, prediction: dict, 
                                          today: datetime.date = None, 
                                          days_ahead: int = 15, 
                                          columns: int = 2) -> InlineKeyboardMarkup:
    """
    Створює InlineKeyboardMarkup з датами на основі прогнозу.
    """
    if today is None:
        today = datetime.date.today()
    
    start_date, end_date, prediction_dist = get_prediction_date_range(prediction, today)
    return date_inline_keyboard(user_id, today, 1, days_ahead, 
                               start_date, end_date, prediction_dist, columns)


def get_poll_keyboard(user_id: str) -> InlineKeyboardMarkup:
    """Повертає клавіатуру для опитування."""
    from vlk_bot.config import POLL_CONFIRM, POLL_RESCHEDULE, POLL_CANCEL
    
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Підтвердити візит", callback_data=f"{POLL_CONFIRM}_{user_id}")],
        [InlineKeyboardButton("Перенести запис", callback_data=f"{POLL_RESCHEDULE}_{user_id}")],
        [InlineKeyboardButton("Скасувати запис", callback_data=f"{POLL_CANCEL}_{user_id}")]
    ])

