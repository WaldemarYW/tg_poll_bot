import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

NO_NOTE_KEY = "__NO_NOTE__"
INVALID_SHEET_CHARS_RE = re.compile(r"[\[\]:*?/\\]")
MAX_SHEET_NAME_LEN = 95
STATS_SUFFIX = "[Статистика]"

HEADERS = [
    "назва_примітки",
    "посилання_примітки",
    "юзернейм_запрошеного",
    "час_події_utc",
    "id_примітки",
    "id_групи",
    "назва_групи",
    "id_реферера",
    "юзернейм_реферера",
    "id_запрошеного",
    "джерело",
    "час_запису_ботом",
]
STATS_HEADERS = [
    "Назва реклами",
    "id",
    "Посилання",
    "Кількість переходів",
]


def sanitize_sheet_name(group_id: Optional[int], group_title: Optional[str]) -> str:
    if group_id is None:
        base = "group_unknown"
        suffix = ""
    else:
        base = (group_title or f"group_{group_id}").strip() or f"group_{group_id}"
        suffix = f" [{group_id}]"

    safe_base = INVALID_SHEET_CHARS_RE.sub(" ", base)
    safe_base = " ".join(safe_base.split())
    if not safe_base:
        safe_base = "group_unknown"

    if not suffix:
        return safe_base[:MAX_SHEET_NAME_LEN]

    base_limit = MAX_SHEET_NAME_LEN - len(suffix)
    trimmed_base = safe_base[:max(base_limit, 1)].rstrip()
    if not trimmed_base:
        trimmed_base = "group"
    return f"{trimmed_base}{suffix}"


def sanitize_stats_sheet_name(group_id: Optional[int], group_title: Optional[str]) -> str:
    base = (group_title or "").strip() if group_title else ""
    if not base:
        base = f"group_{group_id}" if group_id is not None else "group_unknown"
    safe_base = INVALID_SHEET_CHARS_RE.sub(" ", base)
    safe_base = " ".join(safe_base.split()) or "group"
    suffix = f" {STATS_SUFFIX}"
    max_base_len = max(1, MAX_SHEET_NAME_LEN - len(suffix))
    return f"{safe_base[:max_base_len].rstrip() or 'group'}{suffix}"


@dataclass
class SheetsReferralEvent:
    group_id: Optional[int]
    group_title: Optional[str]
    referrer_id: int
    referrer_username: Optional[str]
    referred_user_id: int
    referred_username: Optional[str]
    note_id: Optional[int]
    note_title: Optional[str]
    note_url: Optional[str]
    source: str = "ref_link"


class SheetsReferralLogger:
    def __init__(
        self,
        *,
        enabled: bool,
        spreadsheet_id: Optional[str],
        service_account_json: Optional[str],
        timeout_sec: float = 5.0,
        logger: Optional[logging.Logger] = None,
    ):
        self.enabled = enabled
        self.spreadsheet_id = spreadsheet_id or ""
        self.service_account_json = service_account_json or ""
        self.timeout_sec = timeout_sec
        self.logger = logger or logging.getLogger(__name__)

        self._client = None
        self._spreadsheet = None
        self._lock = asyncio.Lock()
        self._config_error_logged = False

    @staticmethod
    def build_event_row(event: SheetsReferralEvent, *, sheet_name: str, event_ts_utc: str) -> List[str]:
        note_title = (event.note_title or "").strip() or NO_NOTE_KEY
        note_url = (event.note_url or "").strip()
        referrer_username = (event.referrer_username or "").strip()
        referred_username = (event.referred_username or "").strip()
        return [
            note_title,
            note_url,
            f"@{referred_username}" if referred_username else "",
            event_ts_utc,
            str(event.note_id) if event.note_id is not None else "",
            str(event.group_id) if event.group_id is not None else "",
            event.group_title or "",
            str(event.referrer_id),
            f"@{referrer_username}" if referrer_username else "",
            str(event.referred_user_id),
            event.source,
            event_ts_utc,
        ]

    async def log_referral_click_event(self, event: SheetsReferralEvent) -> None:
        if not self.enabled:
            return

        if not self.spreadsheet_id or not self.service_account_json:
            if not self._config_error_logged:
                self.logger.error(
                    "Google Sheets logger is enabled but missing config: "
                    "GOOGLE_SHEETS_SPREADSHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON"
                )
                self._config_error_logged = True
            return

        event_ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        sheet_name = sanitize_sheet_name(event.group_id, event.group_title)
        row = self.build_event_row(event, sheet_name=sheet_name, event_ts_utc=event_ts_utc)

        try:
            async with self._lock:
                await asyncio.wait_for(
                    asyncio.to_thread(self._append_row_sync, sheet_name, row),
                    timeout=self.timeout_sec,
                )
                await asyncio.wait_for(
                    asyncio.to_thread(self._upsert_stats_sheet_sync, event),
                    timeout=self.timeout_sec,
                )
        except Exception as exc:
            self.logger.error(
                "Failed to log referral click to Google Sheets "
                "(group_id=%s referrer_id=%s referred_user_id=%s note_id=%s): %s",
                event.group_id,
                event.referrer_id,
                event.referred_user_id,
                event.note_id,
                exc,
            )

    def _append_row_sync(self, sheet_name: str, row: List[str]) -> None:
        spreadsheet = self._get_spreadsheet_sync()
        worksheet = self._get_or_create_worksheet_sync(spreadsheet, sheet_name)
        self._ensure_headers_sync(worksheet)
        worksheet.append_row(
            row,
            value_input_option="RAW",
            insert_data_option="INSERT_ROWS",
            table_range="A1",
        )

    def _get_spreadsheet_sync(self):
        if self._spreadsheet is not None:
            return self._spreadsheet

        if self._client is None:
            try:
                import gspread
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError("gspread is not installed") from exc

            self._client = gspread.service_account(filename=self.service_account_json)

        self._spreadsheet = self._client.open_by_key(self.spreadsheet_id)
        return self._spreadsheet

    def _get_or_create_worksheet_sync(self, spreadsheet, sheet_name: str):
        import gspread

        try:
            return spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            return spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=len(HEADERS) + 4)

    def _get_or_create_stats_worksheet_sync(self, spreadsheet, sheet_name: str):
        import gspread

        try:
            return spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            return spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=len(STATS_HEADERS) + 2)

    def _ensure_headers_sync(self, worksheet) -> None:
        first_row = worksheet.row_values(1)
        if first_row[: len(HEADERS)] != HEADERS:
            end_col = self._column_letter(len(HEADERS))
            worksheet.update(f"A1:{end_col}1", [HEADERS], value_input_option="RAW")

    def _ensure_stats_headers_sync(self, worksheet) -> None:
        first_row = worksheet.row_values(1)
        if first_row[: len(STATS_HEADERS)] != STATS_HEADERS:
            end_col = self._column_letter(len(STATS_HEADERS))
            worksheet.update(f"A1:{end_col}1", [STATS_HEADERS], value_input_option="RAW")

    def _upsert_stats_sheet_sync(self, event: SheetsReferralEvent) -> None:
        note_title = (event.note_title or "").strip() or NO_NOTE_KEY
        if note_title == NO_NOTE_KEY:
            return

        spreadsheet = self._get_spreadsheet_sync()
        stats_sheet_name = sanitize_stats_sheet_name(event.group_id, event.group_title)
        worksheet = self._get_or_create_stats_worksheet_sync(spreadsheet, stats_sheet_name)
        self._ensure_stats_headers_sync(worksheet)

        note_url = (event.note_url or "").strip()
        note_id_str = str(event.note_id) if event.note_id is not None else ""
        rows = worksheet.get_all_values()
        for idx, row in enumerate(rows[1:], start=2):
            existing_title = row[0].strip() if len(row) > 0 else ""
            existing_id = row[1].strip() if len(row) > 1 else ""
            existing_url = row[2].strip() if len(row) > 2 else ""
            # Primary key by note id when available to avoid merging
            # different notes with the same title and empty URL.
            is_same_note = (
                (note_id_str and existing_id == note_id_str)
                or (not note_id_str and existing_title == note_title and existing_url == note_url)
            )
            if is_same_note:
                current_count_raw = row[3].strip() if len(row) > 3 else "0"
                try:
                    current_count = int(current_count_raw)
                except ValueError:
                    current_count = 0
                worksheet.update(f"D{idx}", [[str(current_count + 1)]], value_input_option="RAW")
                return

        target_row = self._find_first_free_stats_row(rows)
        worksheet.update(
            f"A{target_row}:D{target_row}",
            [[note_title, note_id_str, note_url, "1"]],
            value_input_option="RAW",
        )

    @staticmethod
    def _find_first_free_stats_row(rows: List[List[str]]) -> int:
        """
        Find first row (starting from 2) where A/B/C/D are all empty, ignoring
        any data in columns E+.
        """
        if len(rows) <= 1:
            return 2

        for idx, row in enumerate(rows[1:], start=2):
            col_a = row[0].strip() if len(row) > 0 else ""
            col_b = row[1].strip() if len(row) > 1 else ""
            col_c = row[2].strip() if len(row) > 2 else ""
            col_d = row[3].strip() if len(row) > 3 else ""
            if not col_a and not col_b and not col_c and not col_d:
                return idx

        return len(rows) + 1

    @staticmethod
    def _column_letter(index: int) -> str:
        letters = ""
        n = index
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            letters = chr(65 + remainder) + letters
        return letters
