import tempfile
import unittest
from pathlib import Path

try:
    import bot_poll
except ModuleNotFoundError as exc:  # pragma: no cover
    bot_poll = None
    IMPORT_ERROR = str(exc)
else:
    IMPORT_ERROR = ""


@unittest.skipIf(bot_poll is None, f"bot_poll dependencies are missing: {IMPORT_ERROR}")
class TestReferralClicks(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_db_path = bot_poll.DB_PATH
        temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        temp_file.close()
        self.temp_db_path = Path(temp_file.name)
        bot_poll.DB_PATH = self.temp_db_path
        await bot_poll.init_db()

    async def asyncTearDown(self):
        bot_poll.DB_PATH = self.original_db_path
        if self.temp_db_path.exists():
            self.temp_db_path.unlink()

    async def test_record_referral_click_returns_true_then_false_for_duplicate(self):
        inserted_first = await bot_poll.record_referral_click(
            referrer_id=100,
            referred_user_id=200,
            note_id=1,
            group_id=99,
        )
        inserted_second = await bot_poll.record_referral_click(
            referrer_id=100,
            referred_user_id=200,
            note_id=1,
            group_id=99,
        )

        self.assertTrue(inserted_first)
        self.assertFalse(inserted_second)

    async def test_record_referral_click_allows_same_user_with_different_note(self):
        inserted_first = await bot_poll.record_referral_click(
            referrer_id=100,
            referred_user_id=200,
            note_id=1,
            group_id=99,
        )
        inserted_second = await bot_poll.record_referral_click(
            referrer_id=100,
            referred_user_id=200,
            note_id=2,
            group_id=99,
        )

        self.assertTrue(inserted_first)
        self.assertTrue(inserted_second)

    async def test_try_claim_notification_is_atomic(self):
        await bot_poll.ensure_poll_row(user_id=321, referrer_id=100, note_id=1, group_id=99)
        await bot_poll.update_poll_response(user_id=321, device="Так, є")

        first_claim = await bot_poll.try_claim_notification(321)
        second_claim = await bot_poll.try_claim_notification(321)

        self.assertTrue(first_claim)
        self.assertFalse(second_claim)

    async def test_update_poll_response_does_not_reset_notified_on_device_update(self):
        await bot_poll.ensure_poll_row(user_id=654, referrer_id=100, note_id=1, group_id=99)
        await bot_poll.update_poll_response(user_id=654, age="18-24")
        await bot_poll.mark_notified(654)

        await bot_poll.update_poll_response(user_id=654, device="Так, є")

        self.assertTrue(await bot_poll.was_notified(654))


if __name__ == "__main__":
    unittest.main()
