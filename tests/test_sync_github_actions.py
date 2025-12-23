"""
Тести для sync_for_github_actions.py
"""

import pytest
import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sync_for_github_actions


class TestMain:
    def test_main_success(self):
        mock_creds = MagicMock()
        mock_service = MagicMock()
        
        with patch('sync_for_github_actions.service_account.Credentials.from_service_account_file', return_value=mock_creds):
            with patch('sync_for_github_actions.build', return_value=mock_service):
                with patch('sync_for_github_actions.sync_daily_sheets', return_value=True):
                    with patch('sys.argv', ['sync_for_github_actions.py']):
                        result = sync_for_github_actions.main()
        
        assert result == 0

    def test_main_failure(self):
        mock_creds = MagicMock()
        mock_service = MagicMock()
        
        with patch('sync_for_github_actions.service_account.Credentials.from_service_account_file', return_value=mock_creds):
            with patch('sync_for_github_actions.build', return_value=mock_service):
                with patch('sync_for_github_actions.sync_daily_sheets', return_value=False):
                    with patch('sys.argv', ['sync_for_github_actions.py']):
                        result = sync_for_github_actions.main()
        
        assert result == 1

    def test_main_force_all_flag(self):
        mock_creds = MagicMock()
        mock_service = MagicMock()
        
        with patch('sync_for_github_actions.service_account.Credentials.from_service_account_file', return_value=mock_creds):
            with patch('sync_for_github_actions.build', return_value=mock_service):
                with patch('sync_for_github_actions.sync_daily_sheets', return_value=True) as mock_sync:
                    with patch('sys.argv', ['sync_for_github_actions.py', '--force-all']):
                        result = sync_for_github_actions.main()
        
        assert result == 0
        mock_sync.assert_called_once()
        call_kwargs = mock_sync.call_args[1]
        assert call_kwargs.get('force_refresh_all_sheets') is True

    def test_main_service_account_file_used(self):
        mock_creds = MagicMock()
        mock_service = MagicMock()
        
        with patch('sync_for_github_actions.service_account.Credentials.from_service_account_file', return_value=mock_creds) as mock_from_file:
            with patch('sync_for_github_actions.build', return_value=mock_service):
                with patch('sync_for_github_actions.sync_daily_sheets', return_value=True):
                    with patch('sys.argv', ['sync_for_github_actions.py']):
                        sync_for_github_actions.main()
        
        mock_from_file.assert_called_once_with(
            sync_for_github_actions.SERVICE_ACCOUNT_KEY_PATH,
            scopes=sync_for_github_actions.SERVICE_ACCOUNT_SCOPES
        )

    def test_main_sheets_service_built_correctly(self):
        mock_creds = MagicMock()
        mock_service = MagicMock()
        
        with patch('sync_for_github_actions.service_account.Credentials.from_service_account_file', return_value=mock_creds):
            with patch('sync_for_github_actions.build', return_value=mock_service) as mock_build:
                with patch('sync_for_github_actions.sync_daily_sheets', return_value=True):
                    with patch('sys.argv', ['sync_for_github_actions.py']):
                        sync_for_github_actions.main()
        
        mock_build.assert_called_once_with('sheets', 'v4', credentials=mock_creds)

    def test_main_sync_called_with_correct_params(self):
        mock_creds = MagicMock()
        mock_service = MagicMock()
        
        with patch('sync_for_github_actions.service_account.Credentials.from_service_account_file', return_value=mock_creds):
            with patch('sync_for_github_actions.build', return_value=mock_service):
                with patch('sync_for_github_actions.sync_daily_sheets', return_value=True) as mock_sync:
                    with patch('sys.argv', ['sync_for_github_actions.py']):
                        sync_for_github_actions.main()
        
        mock_sync.assert_called_once_with(
            mock_service,
            sync_for_github_actions.STATS_SHEET_ID,
            sync_for_github_actions.STATS_WORKSHEET_NAME,
            force_refresh_stats=True,
            force_refresh_all_sheets=False
        )


class TestModuleConstants:
    def test_stats_sheet_id_defined(self):
        assert hasattr(sync_for_github_actions, 'STATS_SHEET_ID')
        assert sync_for_github_actions.STATS_SHEET_ID is not None

    def test_stats_worksheet_name_defined(self):
        assert hasattr(sync_for_github_actions, 'STATS_WORKSHEET_NAME')
        assert sync_for_github_actions.STATS_WORKSHEET_NAME == 'Stats'

    def test_service_account_key_path_defined(self):
        assert hasattr(sync_for_github_actions, 'SERVICE_ACCOUNT_KEY_PATH')
        assert sync_for_github_actions.SERVICE_ACCOUNT_KEY_PATH == 'service_account_key.json'

    def test_service_account_scopes_defined(self):
        assert hasattr(sync_for_github_actions, 'SERVICE_ACCOUNT_SCOPES')
        assert 'https://www.googleapis.com/auth/spreadsheets.readonly' in sync_for_github_actions.SERVICE_ACCOUNT_SCOPES

