"""
Заплановані завдання.
"""

import datetime
import logging

import pandas as pd
from pytz import timezone

from vlk_bot.formatters import (
    get_poll_text, get_status_notification_text, get_reminder_notification_text,
    get_poll_expire_text
)
from vlk_bot.keyboards import get_poll_keyboard
from vlk_bot.sheets import (
    load_queue_data, get_sheets_list, get_users_for_date_from_active_sheet,
    update_confirmation_status
)
from vlk_bot.utils import (
    get_next_working_days, load_todo_cache, save_todo_cache, compare_dataframes
)

logger = logging.getLogger(__name__)


async def send_user_notification(context, tg_id: str, text: str):
    """Надсилає повідомлення користувачу."""
    if not tg_id or not tg_id.strip():
        return
    try:
        await context.bot.send_message(chat_id=int(tg_id), text=text, parse_mode='HTML')
    except Exception as e:
        logger.warning(f"Не вдалося надіслати повідомлення користувачу {tg_id}: {e}")


async def notify_status(context) -> None:
    """Функція для відстеження зміни статусу запису та надсилання сповіщень."""
    logger.info("Початок перевірки зміни статусів записів.")
    
    import vlk_bot.config as config_module
    config_module.queue_df = load_queue_data()
    queue_df = config_module.queue_df
    
    if queue_df is None or queue_df.empty:
        logger.warning("Черга порожня або не завантажена")
        return
    
    queue_df['TG ID'] = queue_df['TG ID'].astype(str)
    
    cached_df = load_todo_cache()
    
    changes = compare_dataframes(queue_df, cached_df)
    
    if not changes:
        logger.debug("Змін не виявлено")
        save_todo_cache(queue_df)
        return
    
    logger.info(f"Виявлено {len(changes)} змін")
    
    for change in changes:
        user_id = change['id']
        row = change['row']
        
        target_date = row.get('Дата', '')
        note = row.get('Примітки', '')
        current_status = row.get('Статус', '')
        prev_date = row.get('Попередня дата', '')
        tg_id = str(row.get('TG ID', ''))
        
        notification = get_status_notification_text(
            user_id, current_status, target_date, prev_date, note
        )
        if notification:
            await send_user_notification(context, tg_id, notification)
    
    save_todo_cache(queue_df)
    logger.info("Завершення перевірки зміни статусів записів.")


async def date_reminder(context) -> None:
    """Функція для нагадування запланованого візиту."""
    logger.info("Початок процедури нагадування і підтвердження дати візиту.")
    
    import vlk_bot.config as config_module
    config_module.queue_df = load_queue_data()
    queue_df = config_module.queue_df
    
    if queue_df is None or queue_df.empty:
        logger.warning("Черга порожня або не завантажена")
        return
    
    queue_df['Змінено_dt'] = pd.to_datetime(queue_df['Змінено'], format="%d.%m.%Y %H:%M:%S", errors='coerce')
    queue_df['Змінено_dt'] = queue_df['Змінено_dt'].fillna(pd.Timestamp("2000-01-01 00:00:00"))
    queue_df['Дата_dt'] = pd.to_datetime(queue_df['Дата'], format="%d.%m.%Y", errors='coerce').dt.date
    queue_df.dropna(inplace=True)
    queue_df['TG ID'] = queue_df['TG ID'].astype(str)    

    latest_entries = queue_df.loc[queue_df.groupby('ID')['Змінено_dt'].idxmax()]
    
    current_date_obj = datetime.date.today()
    one_day_later = current_date_obj + datetime.timedelta(days=1)
    three_days_later = current_date_obj + datetime.timedelta(days=3)
    
    for index, row in latest_entries.iterrows():
        user_id = row['ID']
        target_date = row['Дата']
        target_date_dt = row['Дата_dt']
        note = row['Примітки']
        current_status = row['Статус']
        tg_id = row['TG ID']
        remind = False
        nr_days = ''
     
        if target_date_dt == current_date_obj:
            remind = True
            nr_days = 'на сьогодні'        
        if target_date_dt == one_day_later:
            remind = True
            nr_days = 'на завтра'
        if target_date_dt == three_days_later:
            remind = True
            nr_days = 'за 3 дні'
        
        if remind and current_status == 'Ухвалено':
            notification = get_reminder_notification_text(
                user_id, target_date, nr_days, note
            )
            await send_user_notification(context, tg_id, notification)

    logger.info("Завершення процедури нагадування і підтвердження дати візиту.")


async def check_new_daily_sheet(context) -> None:
    """Перевіряє чи з'явився аркуш з датою наступного прийомного дня."""
    from vlk_bot.config import STATS_SHEET_ID
    
    logger.info("Перевірка появи нового щоденного аркуша...")
    
    if context.bot_data.get('poll_sent_for_date'):
        today = datetime.date.today()
        if context.bot_data['poll_sent_for_date'] == today:
            logger.debug("Опитування вже надіслано сьогодні, пропускаємо перевірку")
            return
    
    existing_sheets = get_sheets_list(STATS_SHEET_ID)
    if not existing_sheets:
        logger.warning("Не вдалося отримати список аркушів")
        return
    
    next_working_days = get_next_working_days(3)
    
    found_sheet = None
    found_date = None
    for work_day in next_working_days:
        sheet_name = work_day.strftime("%d.%m.%Y")
        if sheet_name in existing_sheets:
            found_sheet = sheet_name
            found_date = work_day
            break
    
    if not found_sheet:
        logger.debug("Аркуш наступного прийомного дня не знайдено")
        context.bot_data.pop('sheet_detected_at', None)
        context.bot_data.pop('next_reception_sheet', None)
        context.bot_data.pop('next_reception_date', None)
        return
    
    kyiv_tz = timezone('Europe/Kyiv')
    now = datetime.datetime.now(kyiv_tz)
    
    if context.bot_data.get('next_reception_sheet') != found_sheet:
        context.bot_data['sheet_detected_at'] = now
        context.bot_data['next_reception_sheet'] = found_sheet
        context.bot_data['next_reception_date'] = found_date
        logger.info(f"Виявлено аркуш {found_sheet} о {now.strftime('%H:%M:%S')}")
    else:
        detected_at = context.bot_data.get('sheet_detected_at')
        if detected_at:
            minutes_elapsed = (now - detected_at).total_seconds() / 60
            
            if minutes_elapsed >= 30:
                logger.info(f"Минуло 30 хвилин після виявлення аркуша {found_sheet}, запускаємо опитування")
                await send_visit_poll(context)
                context.bot_data['poll_sent_for_date'] = datetime.date.today()


async def send_visit_poll(context) -> None:
    """Надсилає опитування про візит користувачам з Active sheet."""
    next_sheet = context.bot_data.get('next_reception_sheet')
    if not next_sheet:
        logger.warning("Аркуш наступного прийомного дня не виявлено")
        return
    
    logger.info(f"Надсилання опитування для дати {next_sheet}")
    
    users = get_users_for_date_from_active_sheet(next_sheet)
    
    if not users:
        logger.info(f"Не знайдено користувачів для дати {next_sheet}")
        return
    
    sent_count = 0
    error_count = 0
    
    for user_data in users:
        user_id = user_data.get('id')
        tg_id = user_data.get('tg_id')
        
        if not tg_id or not tg_id.strip():
            logger.debug(f"Пропущено ID {user_id} - немає TG ID")
            continue
        
        try:
            await context.bot.send_message(
                chat_id=int(tg_id),
                text=get_poll_text(user_id, next_sheet),
                reply_markup=get_poll_keyboard(user_id),
                parse_mode="HTML"
            )
            sent_count += 1
            logger.info(f"Опитування надіслано: ID {user_id}, TG {tg_id}")
        except Exception as e:
            error_count += 1
            logger.warning(f"Помилка надсилання опитування ID {user_id}: {e}")
    
    logger.info(f"Опитування завершено: надіслано {sent_count}, помилок {error_count}")


async def expire_unanswered_polls(context) -> None:
    """
    Завершує опитування для користувачів, які не відповіли.
    Запускається о 7:00 наступного дня після надсилання опитування.
    """
    logger.info("Перевірка невідповіданих опитувань...")
    
    poll_sent_date = context.bot_data.get('poll_sent_for_date')
    if not poll_sent_date:
        logger.debug("Опитування не було надіслано, пропускаємо")
        return
    
    today = datetime.date.today()
    if poll_sent_date >= today:
        logger.debug("Опитування надіслано сьогодні, ще рано закривати")
        return
    
    next_sheet = context.bot_data.get('next_reception_sheet')
    if not next_sheet:
        logger.debug("Немає даних про аркуш опитування")
        return
    
    from vlk_bot.sheets import get_confirmation_status
    
    users = get_users_for_date_from_active_sheet(next_sheet)
    
    if not users:
        logger.debug("Немає користувачів для перевірки")
        return
    
    import vlk_bot.config as config_module
    queue_df = config_module.queue_df
    if queue_df is None:
        queue_df = load_queue_data()
    
    expired_count = 0
    for user_data in users:
        user_id = user_data.get('id')
        tg_id = user_data.get('tg_id')
        
        if not user_id:
            continue
        
        confirmation = get_confirmation_status(user_id, queue_df)
        
        if not confirmation:
            update_confirmation_status(user_id, 'Не відповів')
            expired_count += 1
            
            if tg_id and tg_id.strip():
                try:
                    await context.bot.send_message(
                        chat_id=int(tg_id),
                        text=get_poll_expire_text(user_id, next_sheet),
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.warning(f"Не вдалося надіслати повідомлення про закриття опитування {user_id}: {e}")
    
    if expired_count > 0:
        logger.info(f"Закрито {expired_count} невідповіданих опитувань")
    
    context.bot_data.pop('poll_sent_for_date', None)
    context.bot_data.pop('next_reception_sheet', None)
    context.bot_data.pop('next_reception_date', None)
    context.bot_data.pop('sheet_detected_at', None)
    
    logger.info("Завершення перевірки невідповіданих опитувань")

