"""
Заплановані завдання.
"""

import datetime
import logging

import pandas as pd
from pytz import timezone

from vlk_bot.formatters import get_poll_text
from vlk_bot.keyboards import get_poll_keyboard
from vlk_bot.sheets import load_queue_data, get_sheets_list, get_users_for_date_from_active_sheet
from vlk_bot.utils import get_next_working_days, load_status_state, save_status_state

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
    
    # 1. Завантажуємо дані з Google Sheets
    import vlk_bot.config as config_module
    config_module.queue_df = load_queue_data()
    queue_df = config_module.queue_df
    
    if queue_df is None or queue_df.empty:
        logger.warning("Черга порожня або не завантажена")
        return
    
    # 2. Очищаємо та готуємо дані
    queue_df['Змінено_dt'] = pd.to_datetime(queue_df['Змінено'], format="%d.%m.%Y %H:%M:%S", errors='coerce')
    # Використовуємо стару дату (2000 рік), щоб записи без дати зміни не перекривали актуальні записи при сортуванні
    queue_df['Змінено_dt'] = queue_df['Змінено_dt'].fillna(pd.Timestamp("2000-01-01 00:00:00"))
    queue_df.dropna(inplace=True)
    queue_df['TG ID'] = queue_df['TG ID'].astype(str)    

    # 3. Знаходимо найактуальніший запис для кожного користувача
    latest_entries = queue_df.loc[queue_df.groupby('ID')['Змінено_dt'].idxmax()]

    # 4. Завантажуємо останній відомий стан
    last_known_state = load_status_state()
    
    # 5. Перевіряємо зміни та відправляємо сповіщення
    new_state = {}
    for index, row in latest_entries.iterrows():
        user_id = row['ID']
        target_date = row['Дата']
        note = row['Примітки']
        current_status = row['Статус']
        modified = row['Змінено']
        prev_date = row['Попередня дата']
        tg_id = row['TG ID']
              
        last_status_info = last_known_state.get(user_id)

        if not last_status_info:
            confirmation = ''
        elif 'confirmation' not in last_status_info:
            confirmation = ''
        else:
            confirmation = last_status_info['confirmation']
      
        # Якщо стан змінився або це новий запис
        if ((not last_status_info) 
            or (last_status_info['status'] != current_status and last_status_info['date'] == target_date and last_status_info['modified'] == modified)
            or (last_status_info['date'] != target_date or last_status_info['modified'] != modified)
        ):
            # Формуємо текст повідомлення
            if current_status != 'На розгляді':
                if target_date != '':
                    to_date = f" на <code>{target_date}</code>"
                    if prev_date != '':
                        rmc = 'перенесення'
                    else:
                        rmc = 'створення'
                else:
                    rmc = 'скасування'
                    to_date = ""
                emo = '🟢' if current_status == 'Ухвалено' else '🔴'
                notification_text = f"{emo} Заявку на {rmc} запису ID <code>{user_id}</code> {to_date}\n<code>{current_status}</code>"
                notification_warning = f'\nПримітка: <code>{note}</code>' if note !='' else ''
                notification = notification_text+notification_warning
                await send_user_notification(context, tg_id, notification)
        
        # Оновлюємо стан для збереження
        new_state[user_id] = {
            'date': target_date,
            'status': current_status,
            'modified': modified,
            'confirmation': confirmation
        }

    # 6. Зберігаємо оновлений стан
    save_status_state(new_state)
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
            emo = '❗️'
            notification_text = f"{emo}<code>Нагадування!</code>\n  Для вашого номеру <code>{user_id}</code> призначено візит {nr_days}: <code>{target_date}</code>"
            notification_warning = f'\nПримітка: <code>{note}</code>' if note !='' else ''
            notification = notification_text+notification_warning
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
    
    last_known_state = load_status_state()
    users = get_users_for_date_from_active_sheet(next_sheet)
    
    if not users:
        logger.debug("Немає користувачів для перевірки")
        return
    
    expired_count = 0
    for user_data in users:
        user_id = user_data.get('id')
        tg_id = user_data.get('tg_id')
        
        if not user_id:
            continue
        
        user_state = last_known_state.get(user_id, {})
        confirmation = user_state.get('confirmation', '')
        
        if not confirmation:
            last_known_state[user_id] = {
                **user_state,
                'confirmation': 'Не відповів'
            }
            expired_count += 1
            
            if tg_id and tg_id.strip():
                try:
                    await context.bot.send_message(
                        chat_id=int(tg_id),
                        text=(
                            f"Час на відповідь на опитування щодо візиту на <code>{next_sheet}</code> "
                            f"для номеру <code>{user_id}</code> минув.\n\n"
                            f"Якщо ви плануєте прийти - просто приходьте за розкладом."
                        ),
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.warning(f"Не вдалося надіслати повідомлення про закриття опитування {user_id}: {e}")
    
    if expired_count > 0:
        save_status_state(last_known_state)
        logger.info(f"Закрито {expired_count} невідповіданих опитувань")
    
    context.bot_data.pop('poll_sent_for_date', None)
    context.bot_data.pop('next_reception_sheet', None)
    context.bot_data.pop('next_reception_date', None)
    context.bot_data.pop('sheet_detected_at', None)
    
    logger.info("Завершення перевірки невідповіданих опитувань")

