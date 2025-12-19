#!/usr/bin/env python3
"""
Скрипт для синхронізації даних в GitHub Actions.

Використання:
    python scripts/sync_for_github_actions.py              # Стандартний режим
    python scripts/sync_for_github_actions.py --force-all  # Перезавантажити всі аркуші
"""

import argparse
import logging
import os
import sys

from google.oauth2 import service_account
from googleapiclient.discovery import build

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vlk_bot.sync import sync_daily_sheets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# немає config.ini у GitHub Actions, тому вказуємо properties тут
STATS_SHEET_ID = '1d9OG-0b7wxxqrOujC9v6ikhjMKL2ei3wfrfaG61zSjA'
STATS_WORKSHEET_NAME = 'Stats'
# цей ключ створюється на льоту в GitHub Actions з секрету SERVICE_ACCOUNT_KEY
SERVICE_ACCOUNT_KEY_PATH = 'service_account_key.json'
SERVICE_ACCOUNT_SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


def main():
    parser = argparse.ArgumentParser(description='Синхронізація даних з Google Sheets')
    parser.add_argument(
        '--force-all',
        action='store_true',
        help='Перезавантажити всі аркуші (ігнорувати кеш)'
    )
    args = parser.parse_args()
    
    if args.force_all:
        logger.info('Режим: повне перезавантаження всіх аркушів')
    
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_KEY_PATH, 
        scopes=SERVICE_ACCOUNT_SCOPES
    )
    sheets_service = build('sheets', 'v4', credentials=creds)
    
    success = sync_daily_sheets(
        sheets_service, 
        STATS_SHEET_ID, 
        STATS_WORKSHEET_NAME, 
        force_refresh_stats=True,
        force_refresh_all_sheets=args.force_all
    )
    
    if success:
        logger.info('Синхронізація успішна')
        return 0
    else:
        logger.error('Помилка синхронізації')
        return 1


if __name__ == '__main__':
    exit(main())

