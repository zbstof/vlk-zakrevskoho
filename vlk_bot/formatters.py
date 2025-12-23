"""
Функції форматування тексту та дат.
"""

import datetime
import logging

import pandas as pd
from telegram import Update

logger = logging.getLogger(__name__)


def calculate_end_date(start_date, days_count):
    """
    Обчислює кінцеву дату, додаючи вказану кількість робочих днів (Пн-Пт) до початкової дати.
    """
    temp_date = start_date
    added = 0
    # Якщо початкова дата є робочим днем, вона враховується як перший день
    if temp_date.weekday() < 5:
        added = 1
    
    while added < days_count:
        temp_date += datetime.timedelta(days=1)
        if temp_date.weekday() < 5:
            added += 1
    return temp_date


def format_prediction_range_text(prediction: dict, today: datetime.date = None, 
                                  days_ahead: int = 15) -> str:
    """
    Форматує текст діапазону прогнозу з ймовірностями.
    """
    from vlk_bot.keyboards import get_prediction_date_range
    from vlk_bot.prediction import calculate_date_probability
    
    if prediction is None:
        return ""
    
    if today is None:
        today = datetime.date.today()
    
    start_date, end_date, prediction_dist = get_prediction_date_range(prediction, today)
    
    if not start_date or not prediction_dist:
        return ""
    
    try:
        prob_start = calculate_date_probability(start_date, prediction_dist)
        
        if end_date:
            prob_end = calculate_date_probability(end_date, prediction_dist)
            end_str = f"`{end_date.strftime('%d.%m.%Y')}` ({prob_end:.0f}%)"
        else:
            est_end = calculate_end_date(start_date, days_ahead)
            prob_end = calculate_date_probability(est_end, prediction_dist)
            end_str = f"`{est_end.strftime('%d.%m.%Y')}` ({prob_end:.0f}%)"
        
        return f"`{start_date.strftime('%d.%m.%Y')}` ({prob_start:.0f}%) - {end_str}"
    except Exception as e:
        logger.error(f"Помилка форматування діапазону: {e}")
        return f"`{prediction.get('mean', today).strftime('%d.%m.%Y')}` - `{prediction.get('h90', today).strftime('%d.%m.%Y')}`"


async def display_queue_data(update: Update, data_frame: pd.DataFrame, 
                             title: str = "Поточна черга:", 
                             reply_markup=None, iConfirmation=False) -> None:
    """
    Відображає чергу з пагінацією.
    """
    from vlk_bot.utils import load_status_state
    
    temp_df = data_frame.copy()
    temp_df['Змінено_dt'] = pd.to_datetime(temp_df['Змінено'].astype(str), 
                                            format="%d.%m.%Y %H:%M:%S", 
                                            dayfirst=True, errors='coerce')
    temp_df['Змінено_dt'] = temp_df['Змінено_dt'].fillna("01.01.2025 00:00:00")

    temp_df_sorted = temp_df.sort_values(by=['ID', 'Змінено_dt'], ascending=[True, True])
    actual_records = temp_df_sorted.drop_duplicates(subset='ID', keep='last')

    actual_queue = actual_records[
        (actual_records['Дата'].astype(str).str.strip() != '') &
        (actual_records['Статус'].astype(str).str.strip().str.lower() == 'ухвалено')
    ].copy()

    if actual_queue.empty:
        await update.message.reply_text(
            f"{title}\nЧерга порожня або жоден запис ще не ухвалено. Гарна нагода записатися!", 
            reply_markup=reply_markup
        )
        return

    try:
        current_date_obj = datetime.date.today()
        actual_queue['Дата_dt'] = pd.to_datetime(actual_queue['Дата'].astype(str), 
                                                  format="%d.%m.%Y", 
                                                  dayfirst=True, errors='coerce')
        actual_queue = actual_queue.dropna(subset=['Дата_dt'])

        sorted_df_for_display = actual_queue.sort_values(
            by=['Дата_dt', 'ID'], ascending=[True, True]
        ).loc[actual_queue['Дата_dt'].dt.date >= current_date_obj].drop(
            columns=['Дата_dt', 'Змінено_dt']
        )
    except Exception as e:
        logger.error(f"Помилка сортування черги для відображення: {e}.")
        sorted_df_for_display = actual_queue.sort_values(
            by=['Дата', 'ID'], ascending=[True, True]
        ).drop(columns=['Змінено_dt'])

    queue_lines = []
    if iConfirmation:
        last_known_state = load_status_state()       
        for index, row in sorted_df_for_display.iterrows():
            last_status_info = last_known_state.get(row['ID'])
            queue_lines.append(
                f"**{len(queue_lines) + 1}.** ID: `{row['ID']}`, "
                f"Дата: `{row['Дата']}`, `{last_status_info['confirmation']}`"
            )
    else:    
        for index, row in sorted_df_for_display.iterrows():
            queue_lines.append(f"**{len(queue_lines) + 1}.** ID: `{row['ID']}`, Дата: `{row['Дата']}`")
    
    base_queue_text = f"**{title} {sorted_df_for_display.shape[0]} записів**\n"
    current_message_parts = [base_queue_text]
    current_part_length = len(base_queue_text)
    MAX_MESSAGE_LENGTH = 1500

    for line in queue_lines:
        if current_part_length + len(line) + 1 > MAX_MESSAGE_LENGTH:
            await update.message.reply_text(
                current_message_parts[-1], parse_mode='Markdown', reply_markup=reply_markup
            )
            current_message_parts.append(line)
            current_part_length = len(line)
        else:
            if len(current_message_parts) == 1:
                current_message_parts[0] += f"\n{line}"
            else:
                current_message_parts[-1] += f"\n{line}"
            current_part_length += len(line) + 1

    if current_message_parts[-1]:
        await update.message.reply_text(
            current_message_parts[-1], parse_mode='Markdown', reply_markup=reply_markup
        )


def get_poll_text(user_id: str, date: str) -> str:
    """Повертає текст опитування."""
    return (
        f"<b>Опитування щодо візиту</b>\n\n"
        f"Ваш номер: <code>{user_id}</code>\n"
        f"Дата візиту: <code>{date}</code>\n\n"
        f"Будь ласка, підтвердіть свій візит або оберіть іншу дію:"
    )

