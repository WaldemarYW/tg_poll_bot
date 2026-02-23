import asyncio
import unittest
from unittest.mock import MagicMock

from google_sheets_logger import (
    NO_NOTE_KEY,
    SheetsReferralEvent,
    SheetsReferralLogger,
    sanitize_sheet_name,
    sanitize_stats_sheet_name,
)


class TestGoogleSheetsLogger(unittest.TestCase):
    def test_sanitize_sheet_name_removes_invalid_chars_and_keeps_group_id(self):
        name = sanitize_sheet_name(12345, "Sales [Q1]/Lead:*?\\")
        self.assertTrue(name.endswith(" [12345]"))
        self.assertNotIn("[Q1]", name)
        self.assertNotIn("/", name)
        self.assertNotIn(":", name)
        self.assertLessEqual(len(name), 95)

    def test_sanitize_sheet_name_trims_stably(self):
        long_title = "A" * 200
        name = sanitize_sheet_name(77, long_title)
        self.assertTrue(name.endswith(" [77]"))
        self.assertLessEqual(len(name), 95)

    def test_sanitize_stats_sheet_name_has_suffix(self):
        name = sanitize_stats_sheet_name(77, "HR VOLODYMYR")
        self.assertTrue(name.endswith(" [Статистика]"))
        self.assertLessEqual(len(name), 95)

    def test_build_event_row_uses_no_note_key_when_note_missing(self):
        event = SheetsReferralEvent(
            group_id=1,
            group_title="Group",
            referrer_id=10,
            referrer_username=None,
            referred_user_id=20,
            referred_username=None,
            note_id=None,
            note_title=None,
            note_url=None,
        )
        row = SheetsReferralLogger.build_event_row(
            event,
            sheet_name="Group [1]",
            event_ts_utc="2026-02-23T15:04:05Z",
        )
        self.assertEqual(row[0], NO_NOTE_KEY)
        self.assertEqual(row[4], "")
        self.assertEqual(row[2], "")

    def test_build_event_row_adds_usernames_when_present(self):
        event = SheetsReferralEvent(
            group_id=1,
            group_title="Group",
            referrer_id=10,
            referrer_username="ref_user",
            referred_user_id=20,
            referred_username="lead_user",
            note_id=30,
            note_title="Note",
            note_url="",
        )
        row = SheetsReferralLogger.build_event_row(
            event,
            sheet_name="Group [1]",
            event_ts_utc="2026-02-23T15:04:05Z",
        )
        self.assertEqual(row[8], "@ref_user")
        self.assertEqual(row[2], "@lead_user")


class TestGoogleSheetsLoggerAsync(unittest.IsolatedAsyncioTestCase):
    async def test_log_referral_click_event_calls_append(self):
        logger = SheetsReferralLogger(
            enabled=True,
            spreadsheet_id="sheet-id",
            service_account_json="/tmp/fake.json",
            timeout_sec=1,
        )
        logger._append_row_sync = MagicMock()
        logger._upsert_stats_sheet_sync = MagicMock()

        event = SheetsReferralEvent(
            group_id=1,
            group_title="Team A",
            referrer_id=10,
            referrer_username="ref_user",
            referred_user_id=20,
            referred_username="new_user",
            note_id=30,
            note_title="Campaign",
            note_url="https://example.com",
        )

        await logger.log_referral_click_event(event)
        logger._append_row_sync.assert_called_once()
        logger._upsert_stats_sheet_sync.assert_called_once()


if __name__ == "__main__":
    unittest.main()
