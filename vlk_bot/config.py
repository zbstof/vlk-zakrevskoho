"""
Конфігурація бота та глобальні змінні.
"""

import configparser
import locale
import logging
import os

import httplib2
from google.oauth2 import service_account
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import build

API_TIMEOUT = 10

DEBUG = False
is_bot_in_group = True

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('apscheduler').setLevel(logging.DEBUG)
logging.getLogger('asyncio').setLevel(logging.DEBUG)

config = configparser.ConfigParser()

TOKEN = ""
ADMIN_IDS = []
GROUP_ID = ""
STATUS_FILE = ""
BANLIST = []
ENVIRONMENT = "production"
SERVICE_ACCOUNT_KEY_PATH = ""
SPREADSHEET_ID = ""
SHEET_NAME = ""
STATS_SHEET_ID = ""
STATS_WORKSHEET_NAME = ""
ACTIVE_SHEET_ID = ""
ACTIVE_WORKSHEET_NAME = ""

POLL_CONFIRM = "poll_confirm"
POLL_RESCHEDULE = "poll_reschedule"
POLL_CANCEL = "poll_cancel"
POLL_DATE = "poll_date"
POLL_DATE_OTHER = "poll_date_other"
POLL_CANCEL_CONFIRM = "poll_cancel_confirm"
POLL_CANCEL_ABORT = "poll_cancel_abort"
POLL_CANCEL_RESCHEDULE = "poll_cancel_reschedule"

SERVICE_ACCOUNT_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SHEETS_SERVICE = None
CREDS = None

queue_df = None

DAILY_SHEETS_CACHE_DIR = "daily_sheets_cache"

JOIN_GETTING_ID, JOIN_GETTING_DATE = range(2)
CANCEL_GETTING_ID = range(2, 3)
SHOW_GETTING_OPTION, SHOW_GETTING_DATE = range(3, 5)
STATUS_GETTING_ID = range(5, 6)

REQUIRED_COLUMNS = ['ID', 'Дата', 'Примітки', 'Статус', 'Змінено', 'Попередня дата', 'TG ID', 'TG Name', 'TG Full Name']
days_ahead = 15

STATS_CACHE_TTL_MINUTES = 30


def save_config():
    """Зберігає config.ini."""
    with open('config.ini', 'w') as configfile:
        config.write(configfile)


def initialize_bot():
    """Ініціалізує бота: завантажує конфігурацію та підключається до Google Sheets."""
    global TOKEN, ADMIN_IDS, GROUP_ID, STATUS_FILE, BANLIST, ENVIRONMENT
    global SERVICE_ACCOUNT_KEY_PATH, SPREADSHEET_ID, SHEET_NAME
    global STATS_SHEET_ID, STATS_WORKSHEET_NAME
    global ACTIVE_SHEET_ID, ACTIVE_WORKSHEET_NAME
    global SHEETS_SERVICE, CREDS, queue_df

    try:
        try:
            locale.setlocale(locale.LC_TIME, 'uk_UA.UTF-8')
        except locale.Error:
            logger.warning("Не вдалося встановити локаль uk_UA.UTF-8, дати можуть відображатися англійською.")

        config.read('config.ini')
        
        TOKEN = config['BOT_SETTINGS']['TOKEN']
        admin_ids_str = config['BOT_SETTINGS']['ADMIN_IDS']
        ADMIN_IDS = [int(id_str.strip()) for id_str in admin_ids_str.split(',') if id_str.strip()]
        GROUP_ID = config['BOT_SETTINGS']['GROUP_ID']
        STATUS_FILE = config['BOT_SETTINGS']['STATUS_FILE']
        ban_ids_str = config['BOT_SETTINGS']['BANLIST']
        BANLIST = [int(id_str.strip()) for id_str in ban_ids_str.split(',') if id_str.strip()]    

        ENVIRONMENT = config['BOT_SETTINGS'].get('ENVIRONMENT', 'production').strip().lower()

        SERVICE_ACCOUNT_KEY_PATH = config['GOOGLE_SHEETS']['SERVICE_ACCOUNT_KEY_PATH']
        SPREADSHEET_ID = config['GOOGLE_SHEETS']['SPREADSHEET_ID']
        SHEET_NAME = config['GOOGLE_SHEETS']['SHEET_NAME']
        STATS_SHEET_ID = config['GOOGLE_SHEETS']['STATS_SHEET_ID']
        STATS_WORKSHEET_NAME = config['GOOGLE_SHEETS']['STATS_WORKSHEET_NAME']
        ACTIVE_SHEET_ID = config['GOOGLE_SHEETS']['ACTIVE_SHEET_ID']
        ACTIVE_WORKSHEET_NAME = config['GOOGLE_SHEETS']['ACTIVE_WORKSHEET_NAME']
        
        logger.info("Константи успішно завантажено з config.ini")

    except KeyError as e:
        logger.error(f"Помилка: Не знайдено ключ '{e}' у файлі config.ini. Перевірте, чи всі налаштування присутні.")
        if __name__ == "__main__":
            exit(1)
    except FileNotFoundError:
        logger.error("Помилка: Файл config.ini не знайдено. Будь ласка, створіть його.")
        if __name__ == "__main__":
            exit(1)
    except Exception as e:
        logger.error(f"Невідома помилка при читанні config.ini: {e}")
        if __name__ == "__main__":
            exit(1)

    try:
        CREDS = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY_PATH, scopes=SERVICE_ACCOUNT_SCOPES
        )
        http = httplib2.Http(timeout=API_TIMEOUT)
        authorized_http = AuthorizedHttp(CREDS, http=http)
        SHEETS_SERVICE = build('sheets', 'v4', http=authorized_http)
        logger.info(f"Успішно підключено до Google Sheets API (timeout={API_TIMEOUT}с).")
    except FileNotFoundError:
        logger.error(f"Помилка: Файл ключа сервісного облікового запису не знайдено за шляхом: {SERVICE_ACCOUNT_KEY_PATH}")
        if __name__ == "__main__":
            exit(1)
    except Exception as e:
        logger.error(f"Помилка ініціалізації Google Sheets API: {e}")
        if __name__ == "__main__":
            exit(1)
            
    from vlk_bot.sheets import load_queue_data
    queue_df = load_queue_data()
    
    os.makedirs(DAILY_SHEETS_CACHE_DIR, exist_ok=True)

