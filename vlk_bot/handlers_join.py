"""
ConversationHandler для запису в чергу.
"""

import datetime
import logging
import re

import pandas as pd
from scipy import stats
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from vlk_bot.config import JOIN_GETTING_ID, JOIN_GETTING_DATE, days_ahead
from vlk_bot.formatters import format_prediction_range_text, calculate_end_date
from vlk_bot.keyboards import (
    MAIN_KEYBOARD, CANCEL_KEYBOARD, date_keyboard, date_keyboard_from_prediction
)
from vlk_bot.prediction import calculate_prediction, calculate_date_probability
from vlk_bot.sheets import load_queue_data, save_queue_data, get_stats_data
from vlk_bot.utils import (
    get_user_log_info, get_user_telegram_data, is_admin, is_banned,
    extract_main_id, get_ordinal_date, send_group_notification
)

logger = logging.getLogger(__name__)


async def check_id_for_queue(main_id: int, previous_state: str, last_status: str):
    """Перевіряє чи ID може бути записаний в чергу."""
    stats_df = await get_stats_data()
    
    if stats_df is None or stats_df.empty:
        return True, ''
    
    try:
        last_entered = stats_df['Останній номер що зайшов'].dropna().max()
        
        if main_id and last_entered and main_id <= last_entered:
            if previous_state and last_status == 'Ухвалено':
                return True, f'Ваш номер `{main_id}` вже проходив ВЛК. Якщо потрібно повторно пройти ВЛК - оберіть нову дату.\n'
            else:
                return False, f'Ваш номер `{main_id}` вже проходив ВЛК. Запис неможливий.'
    except Exception as e:
        logger.error(f"Помилка перевірки ID: {e}")
    
    return True, ''


async def join_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запускає процес запису в чергу."""
    if is_banned(update.effective_user.id):
        logger.warning(f"Заблокований користувач {get_user_log_info(update.effective_user)} намагався створити новий запис.")
        await update.message.reply_text(
            "Ваш обліковий запис заблоковано. Зверніться до адміністраторів щоб розблокувати.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear()
        return ConversationHandler.END
    
    import vlk_bot.config as config_module
    config_module.queue_df = load_queue_data()
    
    if config_module.queue_df is None:
        logger.error(f"Помилка завантаження даних для запису користувача {get_user_log_info(update.effective_user)}.")
        await update.message.reply_text(
            "Сталася помилка при завантаженні даних. Будь ласка, спробуйте повторити спробу пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear()
        return ConversationHandler.END

    logger.info(f"Користувач {get_user_log_info(update.effective_user)} розпочав запис/перенесення.")
    context.user_data['telegram_user_data'] = get_user_telegram_data(update.effective_user)
    await update.message.reply_text(
        "Будь ласка, введіть свій номер в списку первинної черги. Це може бути ціле число (наприклад, `9999`) "
        "або два цілих числа, розділені слешем (наприклад, `9999/1`). "
        "Цей номер надалі буде вашим ID в черзі.",
        parse_mode='Markdown',
        reply_markup=CANCEL_KEYBOARD
    )
    return JOIN_GETTING_ID


async def join_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує ID від користувача."""
    import vlk_bot.config as config_module
    queue_df = config_module.queue_df
    
    user_id_input = update.message.text.strip()
    id_pattern = r"^(\d+|\d+\/\d+)$"
    
    if not re.match(id_pattern, user_id_input):
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний ID: '{user_id_input}'")
        await update.message.reply_text(
            "Невірний формат номеру. Будь ласка, введіть ціле число або два цілих числа, розділені слешем (наприклад, `9999` або `9999/1`).",
            parse_mode='Markdown',
            reply_markup=CANCEL_KEYBOARD
        )
        return JOIN_GETTING_ID

    context.user_data['temp_id'] = user_id_input
    context.user_data.pop('warning_shown', None)
    context.user_data.pop('prediction_bounds', None)
    
    temp_df_for_prev = queue_df.copy()
    temp_df_for_prev['Змінено_dt'] = pd.to_datetime(temp_df_for_prev['Змінено'].astype(str), format="%d.%m.%Y %H:%M:%S", dayfirst=True, errors='coerce')
    temp_df_for_prev['Змінено_dt'] = temp_df_for_prev['Змінено_dt'].fillna("01.01.2025 00:00:00")

    last_record_for_id = temp_df_for_prev[(temp_df_for_prev['ID'] == user_id_input) & (temp_df_for_prev['Статус'] == 'Ухвалено')].sort_values(by='Змінено_dt', ascending=False)
    
    previous_date = ''
    if not last_record_for_id.empty:
        last_date = last_record_for_id.iloc[0]['Дата']
        last_note = last_record_for_id.iloc[0]['Примітки']
        last_status = last_record_for_id.iloc[0]['Статус']
        if pd.isna(last_date) or last_date == '':
            previous_date = ''
        else:
            previous_date = last_date
            
        context.user_data['previous_state'] = previous_date
        context.user_data['user_notes'] = last_note
        await update.message.reply_text(
            f"Номер `{user_id_input}` вже записаний в черзі.\nВаш попередній запис {'на дату' if previous_date else ''} `{previous_date if previous_date else 'Скасовано'}` буде оновлено.",
            parse_mode='Markdown'
        )
    else:
        last_status = ''
        context.user_data['previous_state'] = ''
        await update.message.reply_text(
            f"Ваш номер `{user_id_input}` прийнято. ",
            parse_mode='Markdown'
        )
    
    can_register, user_warning = await check_id_for_queue(extract_main_id(user_id_input), context.user_data['previous_state'], last_status)
    
    if is_admin(update.effective_user.id):
        can_register = True  
        user_warning = ''  
    
    if can_register:
        today = datetime.date.today()
        
        stats_df = await get_stats_data()
        prediction = calculate_prediction(extract_main_id(user_id_input), stats_df)
        
        prediction_text = ""
        if prediction:
            context.user_data['prediction_bounds'] = prediction
            range_info = format_prediction_range_text(prediction, today, days_ahead)
            DATE_KEYBOARD = date_keyboard_from_prediction(prediction, today, days_ahead)
            prediction_text = f"{range_info}. *Відсоток означає ймовірність того, що ви зможете почати ВЛК в цей день.*"
        else:
            context.user_data.pop('prediction_bounds', None)
            DATE_KEYBOARD = date_keyboard(today, 1, days_ahead)

        if user_warning != '':
            context.user_data['user_notes'] = 'Остання спроба'
        
        await update.message.reply_text(
            f"{'УВАГА: '+user_warning if user_warning != '' else ''}"
            f"Виберіть бажану дату запису. Ви можете обрати одну з рекомендованих дат: {prediction_text}\n\n"
            f"Або введіть дату з клавіатури. Дата повинна бути в форматі `ДД.ММ.РРРР`, пізнішою за поточну (`{today.strftime('%d.%m.%Y')}`) та бути робочим днем (Понеділок - П'ятниця).",
            parse_mode='Markdown',
            reply_markup=DATE_KEYBOARD
        )
        return JOIN_GETTING_DATE
    else:
        await update.message.reply_text(
            f"{user_warning}",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear()
        return ConversationHandler.END


async def join_get_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує дату від користувача."""
    import vlk_bot.config as config_module
    
    date_input = update.message.text.strip()
    
    user_id = context.user_data.get('temp_id')
    previous_state = context.user_data.get('previous_state', '')
    user_notes = context.user_data.get('user_notes', '')
    telegram_user_data = context.user_data.get('telegram_user_data')

    match_full = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4}|\d{2})', date_input)
    
    try:
        if match_full:
            date_text = match_full.group(0)
            if len(match_full.group(3)) == 2:
                 chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%y").date()
            else:
                 chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%Y").date()
        else:
            raise ValueError()

    except ValueError:
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний формат дати: '{date_input}'")
        today = datetime.date.today()
        DATE_KEYBOARD=date_keyboard(today, 1, days_ahead)
        await update.message.reply_html(
            "Невірний формат дати. Будь ласка, введіть дату у форматі <code>ДД.ММ.РРРР</code> (наприклад, 25.12.2025) або скасуйте дію.",
            reply_markup=DATE_KEYBOARD
        )
        return JOIN_GETTING_DATE

    current_date_obj = datetime.date.today()
    prediction = context.user_data.get('prediction_bounds')

    if chosen_date <= current_date_obj:
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів дату раніше ніж наступний робочий день: '{date_input}'")
        await update.message.reply_text(
            f"Дата повинна бути пізнішою за поточну (`{current_date_obj.strftime('%d.%m.%Y')}`). Будь ласка, спробуйте ще раз або скасуйте дію.",
            parse_mode='Markdown',
            reply_markup=date_keyboard_from_prediction(prediction, current_date_obj, days_ahead)
        )
        return JOIN_GETTING_DATE
    
    if chosen_date.weekday() >= 5:
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів вихідний день: '{date_input}'")
        await update.message.reply_html(
            "Ви обрали вихідний день (Субота або Неділя). Будь ласка, оберіть <code>робочий день</code> (Понеділок - П'ятниця) або скасуйте дію.",
            reply_markup=date_keyboard_from_prediction(prediction, current_date_obj, days_ahead)
        )
        return JOIN_GETTING_DATE

    if previous_state:
        try:
            previous_date_obj = datetime.datetime.strptime(previous_state, "%d.%m.%Y").date()
            if chosen_date == previous_date_obj:
                logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів дату, що співпадає з попереднім записом: '{chosen_date.strftime('%d.%m.%Y')}'")
                await update.message.reply_text(
                    f"Дата не повинна співпадати з поточною датою запису (`{chosen_date.strftime('%d.%m.%Y')}`). Будь ласка, оберіть іншу дату або скасуйте дію.",
                    parse_mode='Markdown',
                    reply_markup=date_keyboard_from_prediction(prediction, current_date_obj, days_ahead)
                )
                return JOIN_GETTING_DATE
        except ValueError:
            logger.warning(f"Не вдалося розпарсити попередню дату: '{previous_state}'")        

    warning_shown = context.user_data.get('warning_shown', False)
    warned_date_str = context.user_data.get('warned_date')

    if prediction:
        if warning_shown and warned_date_str and warned_date_str == chosen_date.strftime("%d.%m.%Y"):
            pass 
        else:
            warn_msg = None

            dist = prediction['dist']
            try:
                chosen_ord = get_ordinal_date(chosen_date)
                chosen_prob = stats.t.cdf(chosen_ord + 1, dist['df'], loc=dist['loc'], scale=dist['scale']) * 100
            except Exception as e:
                logger.error(f"Error calculating chosen date probability: {e}")
                chosen_prob = 0
                
            if chosen_date < prediction['mean']:
                if chosen_prob < 50:
                    try:
                        prob_mean = calculate_date_probability(prediction['mean'], dist)
                        prob_h90 = calculate_date_probability(prediction['h90'], dist)
                        
                        range_info = f"`{prediction['mean'].strftime('%d.%m.%Y')}` ({prob_mean:.0f}%) - `{prediction['h90'].strftime('%d.%m.%Y')}` ({prob_h90:.0f}%)"
                    except Exception as e:
                        logger.error(f"Помилка обчислення ймовірностей діапазону для попередження: {e}")
                        range_info = f"`{prediction['mean'].strftime('%d.%m.%Y')}` - `{prediction['h90'].strftime('%d.%m.%Y')}`"

                    warn_msg = (
                        f"⚠️ *Попередження:* Для обраної дати `{chosen_date.strftime('%d.%m.%Y')}` ви маєте *низьку ймовірність* почати ВЛК ({chosen_prob:.0f}%).\n"
                        f"Рекомендовано обирати дату з інтервалу {range_info}."
                    )
            elif chosen_date > prediction['h90']:
                current_start = datetime.date.today() + datetime.timedelta(days=1)
                while current_start.weekday() >= 5:
                    current_start += datetime.timedelta(days=1)
                
                standard_window_end = calculate_end_date(current_start, days_ahead)
                threshold_date = max(prediction['h90'], standard_window_end)

                if chosen_date > threshold_date:
                    example_date = prediction['h90']
                    if example_date < current_start:
                        example_date = current_start

                    try:
                        example_prob = calculate_date_probability(example_date, dist)
                        example_prob_str = f"{example_prob:.0f}%"
                    except Exception as e:
                            example_prob_str = ""

                    warn_msg = (
                        f"⚠️ *Попередження:* Обрана дата `{chosen_date.strftime('%d.%m.%Y')}` *занадто далеко в майбутньому*. "
                        f"Вам не треба так довго чекати, шанс успішно почати ВЛК майже гарантований для ближчих дат (наприклад {example_prob_str} для `{example_date.strftime('%d.%m.%Y')}`)."
                    )
                
            if warn_msg:
                context.user_data['warning_shown'] = True
                context.user_data['warned_date'] = chosen_date.strftime("%d.%m.%Y")
                
                await update.message.reply_text(
                    f"{warn_msg}\n\nЯкщо ви бажаєте залишити цю дату, введіть її ще раз або натисніть кнопку щоб обрати одну з рекомендованих.",
                    parse_mode='Markdown',
                    reply_markup=date_keyboard_from_prediction(prediction)
                )
                return JOIN_GETTING_DATE
            else:
                context.user_data.pop('warning_shown', None)
                context.user_data.pop('warned_date', None)

    new_entry = {
        'ID': user_id,
        'Дата': chosen_date.strftime("%d.%m.%Y"),
        'Примітки': user_notes,
        'Статус': 'На розгляді',
        'Змінено': datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        'Попередня дата': previous_state,
        **telegram_user_data
    }
    
    new_entry_df = pd.DataFrame([new_entry])
    
    if save_queue_data(new_entry_df):
        config_module.queue_df = pd.concat([config_module.queue_df, new_entry_df], ignore_index=True)
        if previous_state:
            notification_text = f"✅ Користувач {update.effective_user.mention_html()}\nпереніс запис для\nID <code>{user_id}</code> на <code>{chosen_date.strftime('%d.%m.%Y')}</code>" 
        else:
            notification_text = f"✅ Користувач {update.effective_user.mention_html()}\nстворив запис для\nID <code>{user_id}</code> на <code>{chosen_date.strftime('%d.%m.%Y')}</code>" 
        await send_group_notification(context, notification_text)
        message_text = f"Ви успішно створили заявку на запис/перенос дати в черзі!\nВаш ID: `{user_id}`, Обрана дата: `{chosen_date.strftime('%d.%m.%Y')}`\nСтатус заявки: `На розгляді`\nВаша заявка на розгляді у адміністраторів.\nЯкщо вона буде \"Ухвалена\", то через деякий час з'явиться в жовтій таблиці 🟡TODO."
        await update.message.reply_text(message_text, parse_mode='Markdown', reply_markup=MAIN_KEYBOARD)
        logger.info(f"Запис користувача {get_user_log_info(update.effective_user)} (ID: {user_id}) оновлено/додано на дату: {chosen_date.strftime('%d.%m.%Y')}. Попередня дата: {previous_state if previous_state else 'новий запис'}")
        context.user_data.clear()
        return ConversationHandler.END
    else:
        logger.error(f"Не вдалося зберегти запис користувача {get_user_log_info(update.effective_user)} (ID: {user_id}) на дату: {chosen_date.strftime('%d.%m.%Y')}.")
        await update.message.reply_text(
            "Сталася технічна помилка при збереженні вашого запису. Будь ласка, спробуйте повторити спробу пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear()
        return ConversationHandler.END

