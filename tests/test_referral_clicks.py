import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

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
        bot_poll.PHONE_CONTACT_WAITERS.clear()
        bot_poll.PHONE_CONTACT_TIMEOUT_TASKS.clear()
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
        await bot_poll.update_poll_response(user_id=654, age="16-24")
        await bot_poll.mark_notified(654)

        await bot_poll.update_poll_response(user_id=654, device="Так, є")

        self.assertTrue(await bot_poll.was_notified(654))

    async def test_init_db_adds_phone_number_column(self):
        async with bot_poll.aiosqlite.connect(bot_poll.DB_PATH) as db:
            async with db.execute("PRAGMA table_info(poll_responses)") as cursor:
                columns = [row[1] async for row in cursor]

        self.assertIn("phone_number", columns)

    async def test_finalize_phone_contact_wait_clears_waiter_and_timeout(self):
        bot_poll.set_phone_contact_waiter(777)
        timeout_task = MagicMock()
        timeout_task.done.return_value = False
        bot_poll.PHONE_CONTACT_TIMEOUT_TASKS[777] = timeout_task

        with patch.object(bot_poll, "remove_contact_keyboard_to_chat", new=AsyncMock()) as remove_mock, \
            patch.object(bot_poll, "notify_group_about_poll", new=AsyncMock()) as notify_mock, \
            patch.object(bot_poll, "send_manager_contact_to_chat", new=AsyncMock()) as manager_mock:
            await bot_poll.finalize_phone_contact_wait(
                bot=AsyncMock(),
                user_id=777,
                chat_id=888,
                send_manager=True,
                remove_keyboard_text="Добре.",
            )

        self.assertNotIn(777, bot_poll.PHONE_CONTACT_WAITERS)
        self.assertNotIn(777, bot_poll.PHONE_CONTACT_TIMEOUT_TASKS)
        timeout_task.cancel.assert_called_once()
        remove_mock.assert_awaited_once()
        notify_mock.assert_awaited_once()
        manager_mock.assert_awaited_once()

    async def test_build_group_lead_message_formats_note_without_url(self):
        poll_row = {
            "age": "16-24",
            "income": "30-50 тис",
            "device": "Так, є",
            "phone_number": "+380991112233",
        }
        user_row = {
            "username": "dominika_103",
            "first_name": None,
            "last_name": None,
        }
        referrer_row = {
            "username": "hr_volodymyr",
            "first_name": None,
            "last_name": None,
        }
        note_row = {
            "title": "Вакансії на дому",
            "url": "https://example.com",
        }

        message = bot_poll.build_group_lead_message(
            poll_row=poll_row,
            user_row=user_row,
            user_id=111,
            referrer_row=referrer_row,
            referrer_id=222,
            note_row=note_row,
            note_id=123,
        )

        self.assertIn("✴️ НОВА АНКЕТА", message)
        self.assertIn("ℹ️ Користувач: @dominika_103", message)
        self.assertIn("☎️ Номер телефону: +380991112233", message)
        self.assertIn("⏳ Вік: 16-24", message)
        self.assertIn("💰 Бажаний дохід: 30-50 тис", message)
        self.assertIn("💻 Ноутбук: Так, є", message)
        self.assertIn("🪧 Примітка: Вакансії на дому [123]", message)
        self.assertIn("📥 Реферал від: @hr_volodymyr", message)
        self.assertNotIn("https://example.com", message)
        self.assertNotIn("Профіль користувача", message)

    async def test_build_group_lead_message_hides_note_when_missing(self):
        poll_row = {
            "age": "25-30",
            "income": "20-30 тис",
            "device": "Ні, немає",
            "phone_number": None,
        }
        user_row = {
            "username": None,
            "first_name": "Test",
            "last_name": "User",
        }

        message = bot_poll.build_group_lead_message(
            poll_row=poll_row,
            user_row=user_row,
            user_id=333,
            referrer_row=None,
            referrer_id=None,
            note_row=None,
            note_id=None,
        )

        self.assertIn("ℹ️ Користувач: Test User (ID: 333)", message)
        self.assertIn("📥 Реферал від: чистий запуск", message)
        self.assertNotIn("🪧 Примітка:", message)
        self.assertNotIn("Профіль користувача", message)
        self.assertNotIn("☎️ Номер телефону:", message)

    async def test_normalize_phone_number_keeps_valid_plus_380(self):
        self.assertEqual(
            bot_poll.normalize_phone_number("+380991112233"),
            "+380991112233",
        )

    async def test_normalize_phone_number_adds_plus_3_to_380(self):
        self.assertEqual(
            bot_poll.normalize_phone_number("380991112233"),
            "+380991112233",
        )

    async def test_normalize_phone_number_adds_plus_3_to_80(self):
        self.assertEqual(
            bot_poll.normalize_phone_number("80991112233"),
            "+380991112233",
        )

    async def test_normalize_phone_number_cleans_spaces_and_keeps_foreign_number(self):
        self.assertEqual(
            bot_poll.normalize_phone_number("38 099 111 22 33"),
            "+380991112233",
        )
        self.assertEqual(
            bot_poll.normalize_phone_number("+49 (151) 123-45-67"),
            "+491511234567",
        )

    async def test_normalize_manager_username_variants(self):
        self.assertEqual(
            bot_poll.normalize_manager_username("@hr_volodymyr"),
            "@hr_volodymyr",
        )
        self.assertEqual(
            bot_poll.normalize_manager_username("hr_volodymyr"),
            "@hr_volodymyr",
        )
        self.assertEqual(
            bot_poll.normalize_manager_username("https://t.me/hr_volodymyr"),
            "@hr_volodymyr",
        )
        self.assertIsNone(bot_poll.normalize_manager_username("https://example.com"))

    async def test_normalize_note_contact_input_uses_default_for_dash(self):
        self.assertEqual(
            bot_poll.normalize_note_contact_input("-"),
            bot_poll.DEFAULT_MANAGER_USERNAME,
        )
        self.assertEqual(
            bot_poll.normalize_note_contact_input(""),
            bot_poll.DEFAULT_MANAGER_USERNAME,
        )

    async def test_normalize_note_contact_input_accepts_username(self):
        self.assertEqual(
            bot_poll.normalize_note_contact_input("custom_manager"),
            "@custom_manager",
        )
        self.assertEqual(
            bot_poll.normalize_note_contact_input("@custom_manager"),
            "@custom_manager",
        )
        self.assertIsNone(
            bot_poll.normalize_note_contact_input("https://example.com"),
        )

    async def test_resolve_manager_username_for_user_uses_note_username(self):
        await bot_poll.ensure_poll_row(user_id=501, referrer_id=100, note_id=1, group_id=99)

        async with bot_poll.aiosqlite.connect(bot_poll.DB_PATH) as db:
            await db.execute(
                "INSERT INTO notes (id, owner_id, group_id, title, url) VALUES (?, ?, ?, ?, ?)",
                (1, 100, 99, "Test note", "@custom_manager"),
            )
            await db.commit()

        manager_username = await bot_poll.resolve_manager_username_for_user(501)
        self.assertEqual(manager_username, "@custom_manager")

    async def test_resolve_manager_username_for_user_falls_back_to_default(self):
        await bot_poll.ensure_poll_row(user_id=502, referrer_id=100, note_id=1, group_id=99)

        async with bot_poll.aiosqlite.connect(bot_poll.DB_PATH) as db:
            await db.execute(
                "INSERT INTO notes (id, owner_id, group_id, title, url) VALUES (?, ?, ?, ?, ?)",
                (1, 100, 99, "Old note", "https://example.com"),
            )
            await db.commit()

        manager_username = await bot_poll.resolve_manager_username_for_user(502)
        self.assertEqual(manager_username, bot_poll.DEFAULT_MANAGER_USERNAME)

    async def test_send_manager_contact_to_chat_uses_note_username(self):
        await bot_poll.ensure_poll_row(user_id=503, referrer_id=100, note_id=1, group_id=99)

        async with bot_poll.aiosqlite.connect(bot_poll.DB_PATH) as db:
            await db.execute(
                "INSERT INTO notes (id, owner_id, group_id, title, url) VALUES (?, ?, ?, ?, ?)",
                (1, 100, 99, "Manager note", "@custom_manager"),
            )
            await db.commit()

        send_message = AsyncMock()
        fake_bot = MagicMock()
        fake_bot.send_message = send_message

        await bot_poll.send_manager_contact_to_chat(fake_bot, chat_id=999, user_id=503, skip_delay=True)

        send_message.assert_awaited_once()
        kwargs = send_message.await_args.kwargs
        self.assertEqual(kwargs["chat_id"], 999)
        self.assertIn("@custom_manager", kwargs["text"])
        button = kwargs["reply_markup"].inline_keyboard[0][0]
        self.assertEqual(button.text, "Написати менеджеру✅")
        self.assertEqual(button.url, "https://t.me/custom_manager?text=%2B")


if __name__ == "__main__":
    unittest.main()
