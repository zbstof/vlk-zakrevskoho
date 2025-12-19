"""
Обробники опитувань (poll) та callback handlers.
"""

import asyncio
import datetime
import logging
import re

import pandas as pd
from scipy import stats
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ApplicationHandlerStop

from vlk_bot.config import (
    POLL_CONFIRM, POLL_RESCHEDULE, POLL_CANCEL, POLL_DATE_OTHER, POLL_CANCEL_CONFIRM, POLL_CANCEL_ABORT,
    POLL_CANCEL_RESCHEDULE, days_ahead
)
from vlk_bot.formatters import calculate_end_date
from vlk_bot.formatters import get_poll_text
from vlk_bot.keyboards import get_poll_keyboard, date_inline_keyboard_from_prediction, MAIN_KEYBOARD
from vlk_bot.prediction import calculate_prediction, calculate_date_probability, calculate_prediction_with_daily_data
from vlk_bot.sheets import load_queue_data, save_queue_data, update_active_sheet_status, get_stats_data
from vlk_bot.utils import get_ordinal_date
from vlk_bot.utils import get_user_log_info, get_user_telegram_data, extract_main_id, save_status_state, \
    load_status_state, send_group_notification
from vlk_bot.utils import id_to_numeric

logger = logging.getLogger(__name__)


async def delete_confirmation_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = 10):
    """Видаляє повідомлення після затримки."""
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.debug(f"Не вдалося видалити повідомлення: {e}")


async def handle_poll_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробляє відповіді на опитування."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user = query.from_user
    
    parts = data.split('_', 2)
    if len(parts) < 3:
        logger.warning(f"Невірний формат callback: {data}")
        return
    
    action = f"{parts[0]}_{parts[1]}"
    user_id = parts[2]
    
    logger.info(f"Poll response: action={action}, user_id={user_id}, from={get_user_log_info(user)}")
    
    last_known_state = load_status_state()
    
    if action == POLL_CONFIRM:
        update_active_sheet_status(user_id, "Підтвердив візит")
        
        visit_date = context.bot_data.get('next_reception_sheet', '')
        if not visit_date and user_id in last_known_state:
            visit_date = last_known_state.get(user_id, {}).get('date', '')
        
        if user_id in last_known_state:
            last_known_state[user_id]['confirmation'] = 'Підтверджено'
            save_status_state(last_known_state)
        
        date_text = f" на <code>{visit_date}</code>" if visit_date else ""
        await query.edit_message_text(
            f"Ви підтвердили візит для ID <code>{user_id}</code>{date_text}.\n"
            f"Дякуємо! Чекаємо вас.",
            parse_mode="HTML"
        )
        
    elif action == POLL_RESCHEDULE:
        today = datetime.date.today()
        
        stats_df = await get_stats_data()
        prediction = calculate_prediction(extract_main_id(user_id), stats_df)
        
        if prediction:
            keyboard = date_inline_keyboard_from_prediction(user_id, prediction, today, days_ahead)
        else:
            from vlk_bot.keyboards import date_inline_keyboard
            keyboard = date_inline_keyboard(user_id, today, 1, days_ahead)
        
        await query.edit_message_text(
            f"Оберіть нову дату для ID <code>{user_id}</code>:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        
    elif action == POLL_CANCEL:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Так, скасувати", callback_data=f"{POLL_CANCEL_CONFIRM}_{user_id}")],
            [InlineKeyboardButton("Ні, передумав", callback_data=f"{POLL_CANCEL_ABORT}_{user_id}")]
        ])
        
        await query.edit_message_text(
            f"<b>УВАГА:</b> Скасування повністю видалить ваше місце в черзі!\n\n"
            f"Ви впевнені, що хочете скасувати запис для ID <code>{user_id}</code>?",
            reply_markup=keyboard,
            parse_mode="HTML"
        )


async def handle_poll_cancel_actions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробляє дії скасування опитування."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    parts = data.split('_', 3)
    if len(parts) < 4:
        logger.warning(f"Невірний формат cancel callback: {data}")
        return
    
    action = f"{parts[0]}_{parts[1]}_{parts[2]}"
    user_id = parts[3]
    user = query.from_user
    
    import vlk_bot.config as config_module
    
    if action == POLL_CANCEL_CONFIRM:
        config_module.queue_df = load_queue_data()
        
        telegram_user_data = get_user_telegram_data(user)
        new_entry = {
            'ID': user_id,
            'Дата': '',
            'Примітки': 'Через опитування',
            'Статус': 'Ухвалено',
            'Змінено': datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
            'Попередня дата': context.bot_data.get('next_reception_sheet', ''),
            **telegram_user_data
        }
        
        new_entry_df = pd.DataFrame([new_entry])
        if save_queue_data(new_entry_df):
            config_module.queue_df = pd.concat([config_module.queue_df, new_entry_df], ignore_index=True)
            update_active_sheet_status(user_id, "Скасував")
            
            last_known_state = load_status_state()
            if user_id in last_known_state:
                last_known_state[user_id]['confirmation'] = 'Скасовано'
                save_status_state(last_known_state)
            
            await query.edit_message_text(
                f"Запис для ID <code>{user_id}</code> скасовано.\n"
                f"Дякуємо за повідомлення!",
                parse_mode="HTML"
            )
        else:
            await query.edit_message_text(
                f"Помилка при скасуванні запису. Спробуйте пізніше.",
                parse_mode="HTML"
            )
            
    elif action == POLL_CANCEL_ABORT:
        next_sheet = context.bot_data.get('next_reception_sheet', 'невідома дата')
        await query.edit_message_text(
            get_poll_text(user_id, next_sheet),
            reply_markup=get_poll_keyboard(user_id),
            parse_mode="HTML"
        )
        
    elif action == POLL_CANCEL_RESCHEDULE:
        today = datetime.date.today()
        
        stats_df = await get_stats_data()
        prediction = calculate_prediction(extract_main_id(user_id), stats_df)
        
        if prediction:
            keyboard = date_inline_keyboard_from_prediction(user_id, prediction, today, days_ahead)
        else:
            from vlk_bot.keyboards import date_inline_keyboard
            keyboard = date_inline_keyboard(user_id, today, 1, days_ahead)
        
        await query.edit_message_text(
            f"Оберіть нову дату для ID <code>{user_id}</code>:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )


async def handle_poll_date_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробляє вибір дати з опитування."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user = query.from_user
    
    if data.startswith(f"{POLL_DATE_OTHER}_"):
        user_id = data.replace(f"{POLL_DATE_OTHER}_", "")
        
        context.user_data['poll_awaiting_custom_date'] = True
        context.user_data['poll_reschedule_user_id'] = user_id
        
        await query.edit_message_text(
            f"Введіть бажану дату у форматі <code>ДД.ММ.РРРР</code>\n"
            f"(наприклад, 25.12.2025)\n\n"
            f"Номер: <code>{user_id}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Скасувати", callback_data=f"{POLL_CANCEL_ABORT}_{user_id}")]
            ])
        )
        return
    
    parts = data.split('_')
    if len(parts) < 4:
        logger.warning(f"Невірний формат date callback: {data}")
        return
    
    user_id = parts[2]
    date_str = parts[3]
    
    import vlk_bot.config as config_module
    config_module.queue_df = load_queue_data()
    
    telegram_user_data = get_user_telegram_data(user)
    prev_date = context.bot_data.get('next_reception_sheet', '')
    
    new_entry = {
        'ID': user_id,
        'Дата': date_str,
        'Примітки': 'Через опитування',
        'Статус': 'Ухвалено',
        'Змінено': datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        'Попередня дата': prev_date,
        **telegram_user_data
    }
    
    new_entry_df = pd.DataFrame([new_entry])
    if save_queue_data(new_entry_df):
        config_module.queue_df = pd.concat([config_module.queue_df, new_entry_df], ignore_index=True)
        update_active_sheet_status(user_id, "Відклав візит")
        
        last_known_state = load_status_state()
        if user_id in last_known_state:
            last_known_state[user_id]['confirmation'] = f'Перенесено на {date_str}'
            save_status_state(last_known_state)
        
        await query.edit_message_text(
            f"Запис для ID <code>{user_id}</code> перенесено на <code>{date_str}</code>.",
            parse_mode="HTML"
        )
    else:
        await query.edit_message_text(
            f"Помилка при перенесенні запису. Спробуйте пізніше.",
            parse_mode="HTML"
        )


async def handle_poll_custom_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обробляє введення користувацької дати для перенесення запису через опитування.
    Використовує ApplicationHandlerStop щоб зупинити fallback.
    """
    if not context.user_data.get('poll_awaiting_custom_date'):
        return
    
    import vlk_bot.config as config_module
    user_tg_id = str(update.effective_user.id)
    date_input = update.message.text.strip()
    user_id = context.user_data.get('poll_reschedule_user_id', '')
    
    date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{2,4})', date_input)
    if not date_match:
        await update.message.reply_text(
            f"Невірний формат дати. Будь ласка, введіть дату у форматі <code>ДД.ММ.РРРР</code>\n"
            f"Номер: <code>{user_id}</code>",
            parse_mode="HTML"
        )
        raise ApplicationHandlerStop
    
    day, month, year = date_match.groups()
    if len(year) == 2:
        year = '20' + year
    date_str = f"{day.zfill(2)}.{month.zfill(2)}.{year}"
    
    try:
        chosen_date = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
    except ValueError:
        await update.message.reply_text(
            f"Некоректна дата. Будь ласка, перевірте та спробуйте ще раз.\n"
            f"Номер: <code>{user_id}</code>",
            parse_mode="HTML"
        )
        raise ApplicationHandlerStop
    
    today = datetime.date.today()
    if chosen_date <= today:
        await update.message.reply_text(
            f"Дата має бути в майбутньому. Будь ласка, оберіть іншу дату.\n"
            f"Номер: <code>{user_id}</code>",
            parse_mode="HTML"
        )
        raise ApplicationHandlerStop
    
    if chosen_date.weekday() >= 5:
        await update.message.reply_text(
            f"Обрана дата ({date_str}) припадає на вихідний. Будь ласка, оберіть робочий день.\n"
            f"Номер: <code>{user_id}</code>",
            parse_mode="HTML"
        )
        raise ApplicationHandlerStop
    
    # Очищаємо прапорці тільки після успішної валідації
    context.user_data.pop('poll_awaiting_custom_date', None)
    context.user_data.pop('poll_reschedule_user_id', None)
    
    warning_shown = context.user_data.get('poll_warning_shown', False)
    warned_date_str = context.user_data.get('poll_warned_date')
    
    context.user_data.pop('poll_warning_shown', None)
    context.user_data.pop('poll_warned_date', None)
    
    if not (warning_shown and warned_date_str == date_str):
        try:
            numeric_id = id_to_numeric(user_id)
            if numeric_id:
                prediction = calculate_prediction_with_daily_data(int(numeric_id))
                if prediction and prediction.get('dist'):
                    dist = prediction['dist']
                    warn_msg = None
                    
                    chosen_ord = get_ordinal_date(chosen_date)
                    chosen_prob = stats.t.cdf(chosen_ord + 1, dist['df'], loc=dist['loc'], scale=dist['scale']) * 100
                    
                    if chosen_date < prediction['mean'] and chosen_prob < 50:
                        try:
                            prob_mean = calculate_date_probability(prediction['mean'], dist)
                            prob_h90 = calculate_date_probability(prediction['h90'], dist)
                            range_info = f"<code>{prediction['mean'].strftime('%d.%m.%Y')}</code> ({prob_mean:.0f}%) - <code>{prediction['h90'].strftime('%d.%m.%Y')}</code> ({prob_h90:.0f}%)"
                        except:
                            range_info = f"<code>{prediction['mean'].strftime('%d.%m.%Y')}</code> - <code>{prediction['h90'].strftime('%d.%m.%Y')}</code>"
                        
                        warn_msg = (
                            f"⚠️ <b>Попередження:</b> Для обраної дати <code>{date_str}</code> ви маєте "
                            f"<b>низьку ймовірність</b> почати ВЛК ({chosen_prob:.0f}%).\n"
                            f"Рекомендовано обирати дату з інтервалу {range_info}."
                        )
                    elif chosen_date > prediction['h90']:
                        current_start = today + datetime.timedelta(days=1)
                        while current_start.weekday() >= 5:
                            current_start += datetime.timedelta(days=1)
                        
                        standard_window_end = calculate_end_date(current_start, 15)
                        threshold_date = max(prediction['h90'], standard_window_end)
                        
                        if chosen_date > threshold_date:
                            try:
                                prob_mean = calculate_date_probability(prediction['mean'], dist)
                                prob_h90 = calculate_date_probability(prediction['h90'], dist)
                                range_info = f"<code>{prediction['mean'].strftime('%d.%m.%Y')}</code> ({prob_mean:.0f}%) - <code>{prediction['h90'].strftime('%d.%m.%Y')}</code> ({prob_h90:.0f}%)"
                            except:
                                range_info = f"<code>{prediction['mean'].strftime('%d.%m.%Y')}</code> - <code>{prediction['h90'].strftime('%d.%m.%Y')}</code>"
                            
                            warn_msg = (
                                f"⚠️ <b>Попередження:</b> Обрана дата <code>{date_str}</code> <b>занадто далеко в майбутньому</b>. "
                                f"Вам не треба так довго чекати, рекомендований інтервал: {range_info}."
                            )
                    
                    if warn_msg:
                        context.user_data['poll_warning_shown'] = True
                        context.user_data['poll_warned_date'] = date_str
                        context.user_data['poll_awaiting_custom_date'] = True
                        context.user_data['poll_reschedule_user_id'] = user_id
                        
                        await update.message.reply_text(
                            f"{warn_msg}\n\nЯкщо ви бажаєте залишити цю дату, введіть її ще раз.",
                            parse_mode="HTML"
                        )
                        raise ApplicationHandlerStop
        except ApplicationHandlerStop:
            raise
        except Exception as e:
            logger.warning(f"Помилка перевірки дати для попередження в poll: {e}")
    
    update_active_sheet_status(user_id, "Відклав візит")
    
    telegram_user_data = {
        'TG ID': user_tg_id,
        'TG Name': update.effective_user.username if update.effective_user.username else '',
        'TG Full Name': update.effective_user.full_name if update.effective_user.full_name else ''
    }
    
    previous_date = context.bot_data.get('next_reception_sheet', '')
    
    new_entry = {
        'ID': user_id,
        'Дата': chosen_date.strftime("%d.%m.%Y"),
        'Примітки': 'Через опитування',
        'Статус': 'Ухвалено',
        'Змінено': datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        'Попередня дата': previous_date,
        **telegram_user_data
    }
    
    new_entry_df = pd.DataFrame([new_entry])
    config_module.queue_df = load_queue_data()
    
    if save_queue_data(new_entry_df):
        config_module.queue_df = pd.concat([config_module.queue_df, new_entry_df], ignore_index=True)
        
        await update.message.reply_text(
            f"Запис перенесено.\n"
            f"Номер: <code>{user_id}</code>\n"
            f"Нова дата: <code>{date_str}</code>",
            parse_mode="HTML",
            reply_markup=MAIN_KEYBOARD
        )
        
        notification_text = f"✅ Користувач {update.effective_user.mention_html()} подав заявку на перенесення запису для ID <code>{user_id}</code> на <code>{date_str}</code>"
        await send_group_notification(context, notification_text)
        
        logger.info(f"Користувач {user_id} подав заявку на перенесення запису на {date_str} (ручне введення)")
    else:
        await update.message.reply_text(
            "Виникла помилка при перенесенні запису. Спробуйте пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
    
    raise ApplicationHandlerStop

