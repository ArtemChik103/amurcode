import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

import app

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover - exercised only when dependency is absent
    sync_playwright = None
    PlaywrightTimeoutError = TimeoutError


class PlaywrightUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sync_playwright is None:
            raise unittest.SkipTest("playwright is not installed")
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.playwright = sync_playwright().start()
        cls.browser = cls.playwright.chromium.launch(headless=True)

    @classmethod
    def tearDownClass(cls):
        if sync_playwright is None:
            return
        cls.browser.close()
        cls.playwright.stop()
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def setUp(self):
        self.context = self.browser.new_context(accept_downloads=True, locale="ru-RU")
        self.page = self.context.new_page()
        self.page.on("pageerror", lambda error: self.fail(f"browser page error: {error}"))
        self.page.on("console", self._fail_on_console_error)
        self.page.goto(f"http://127.0.0.1:{self.port}/", wait_until="networkidle")
        self.page.get_by_text("Быстрый старт").wait_for(timeout=15000)

    def tearDown(self):
        self.context.close()

    def _fail_on_console_error(self, message):
        if message.type == "error":
            self.fail(f"browser console error: {message.text}")

    def click_button(self, name):
        self.page.get_by_role("button", name=name).first.click()

    def test_quick_actions_cover_all_scenarios(self):
        expectations = [
            ("Показать СКК", "СКК"),
            ("Показать КИК", "КИК"),
            ("Показать 2/3", "2/3"),
            ("Показать ОКВ", "ОКВ"),
            ("Сравнить СКК", "Что изменилось:"),
            ("Где проблемы с исполнением", "Короткий вывод"),
        ]
        for button, expected_text in expectations:
            with self.subTest(button=button):
                self.click_button(button)
                if expected_text in {"СКК", "КИК", "2/3", "ОКВ"}:
                    self.page.locator(".eyebrow", has_text=expected_text).wait_for(timeout=10000)
                else:
                    self.page.get_by_text(expected_text).first.wait_for(timeout=10000)
                self.page.get_by_text("Короткий вывод").wait_for(timeout=10000)

    def test_smart_search_code_text_and_compare_enter_flow(self):
        search = self.page.get_by_placeholder("Например: покажи СКК по Благовещенску").first

        search.fill("6105")
        self.page.get_by_text("Похоже, вы ищете СКК").wait_for(timeout=10000)
        search.press("Enter")
        self.page.locator(".eyebrow", has_text="СКК").wait_for(timeout=10000)
        self.page.get_by_text("Главное").wait_for(timeout=10000)

        search.fill("Благовещенск")
        self.page.get_by_text("Искать по введённому тексту").wait_for(timeout=10000)
        search.press("Enter")
        self.page.get_by_text("Короткий вывод").wait_for(timeout=10000)

        search.fill("сравни СКК")
        self.page.get_by_text("Сравнить периоды").wait_for(timeout=10000)
        search.press("Enter")
        self.page.get_by_text("Что изменилось:").wait_for(timeout=10000)

    def test_assistant_rule_based_flow_and_alternative_action(self):
        self.page.get_by_placeholder("Например: покажи СКК по Благовещенску").nth(1).fill("Покажи СКК")
        self.click_button("Понять запрос")
        self.page.get_by_text("Я понял запрос").wait_for(timeout=10000)
        self.page.get_by_text("Правила").wait_for(timeout=10000)
        self.click_button("Показать результат")
        self.page.locator(".eyebrow", has_text="СКК").wait_for(timeout=10000)
        self.page.get_by_text("Короткий вывод").wait_for(timeout=10000)

    def test_tabs_trace_readiness_empty_state_and_export(self):
        self.click_button("Показать СКК")
        self.click_button("Проверить перед показом")
        self.page.get_by_text("Готовность демонстрации").wait_for(timeout=10000)
        self.page.get_by_text("Данные загружены").wait_for(timeout=10000)

        self.click_button("Понятная таблица")
        self.page.get_by_text("Статус").wait_for(timeout=10000)
        self.page.locator(".status-pill").first.wait_for(timeout=10000)

        self.click_button("Все суммы")
        self.page.get_by_text("Все суммы по объектам").wait_for(timeout=10000)

        self.click_button("Исходные строки")
        self.page.get_by_role("heading", name="Исходные строки").wait_for(timeout=10000)
        self.page.locator("button[title='Откуда цифра']").first.click()
        self.page.get_by_text("Откуда взялась цифра").wait_for(timeout=10000)
        self.page.get_by_text("Технические данные").wait_for(timeout=10000)
        self.page.get_by_role("button", name="×").click()

        self.click_button("Расширенные настройки")
        self.page.get_by_placeholder("объект, бюджет, получатель").fill("zzzz-no-data")
        self.page.wait_for_timeout(500)
        self.page.get_by_text("Ничего не найдено").first.wait_for(timeout=10000)
        self.click_button("Очистить поиск")
        self.page.get_by_text("Короткий вывод").wait_for(timeout=10000)

        with self.page.expect_download() as download_info:
            self.click_button("Скачать таблицу")
        download = download_info.value
        self.assertRegex(download.suggested_filename, r"analytics_.*\.csv")
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / download.suggested_filename
            download.save_as(target)
            content = target.read_text(encoding="utf-8-sig")
        self.assertIn("Отчёт", content)
        self.assertIn("Показатели", content)

    def test_no_technical_words_on_primary_screen(self):
        body_text = self.page.locator("body").inner_text()
        self.assertNotIn("snapshot", body_text)
        self.assertNotIn("trace", body_text)
        self.assertNotIn("CSV", body_text)

    def test_vue_runtime_loaded(self):
        try:
            self.page.wait_for_function("() => window.Vue && document.querySelector('[v-cloak]') === null", timeout=3000)
        except PlaywrightTimeoutError:
            # Vue removes v-cloak by rendering the app; this keeps the failure message concrete.
            self.fail("Vue runtime did not render the app")
