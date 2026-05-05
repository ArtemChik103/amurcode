import os
import time
import unittest

import app

LLM_TEST_DELAY = float(os.environ.get("LLM_TEST_DELAY", "6"))


@unittest.skipIf(os.environ.get("RUN_LLM_TESTS") != "1" or not os.environ.get("GROQ_API_KEY"), "LLM tests are opt-in")
class GroqIntegrationTests(unittest.TestCase):
    def setUp(self):
        time.sleep(LLM_TEST_DELAY)

    def assertAssistantAction(self, message, expected, context=None):
        payload = app.assistant_response(
            message,
            context or {"mode": "slice", "template": "all", "date": "2026-04-01"},
        )
        self.assertEqual(payload["mode"], "llm", payload)
        self.assertIn("action", payload)
        action = payload["action"]
        for key, value in expected.items():
            self.assertEqual(action.get(key), value, f"{key} mismatch in {action}")
        return payload

    def sleep_between_llm_cases(self):
        time.sleep(LLM_TEST_DELAY)

    def test_groq_assistant_returns_valid_action(self):
        payload = self.assertAssistantAction(
            "покажи проблемные СКК без кассы",
            {"template": "skk", "post_filter": "no_cash", "open_view": "problems", "q": ""},
            {"mode": "slice", "template": "all"},
        )
        self.assertIn(payload["action"]["template"], app.TEMPLATES)

    def test_groq_assistant_handles_dates_problem_filters_and_downloads(self):
        cases = [
            (
                "покажи СКК за март 2026",
                {"mode": "slice", "template": "skk", "date": "2026-03-01", "q": ""},
            ),
            (
                "сравни СКК февраль 2025 и апрель 2026",
                {"mode": "compare", "template": "skk", "base": "2025-02-01", "target": "2026-04-01", "open_view": "changes", "q": ""},
            ),
            (
                "где есть документы но нет оплат по СКК",
                {"mode": "slice", "template": "skk", "post_filter": "no_payments", "open_view": "problems", "q": ""},
            ),
            (
                "покажи объекты без договоров по ОКВ",
                {"mode": "slice", "template": "okv", "post_filter": "no_documents", "open_view": "problems", "q": ""},
            ),
            (
                "показать непроверенные проблемы СКК",
                {"mode": "slice", "template": "skk", "post_filter": "unreviewed", "open_view": "problems", "q": ""},
            ),
            (
                "покажи контроль загрузки",
                {"mode": "slice", "open": "control", "q": ""},
            ),
            (
                "скачать excel по СКК",
                {"mode": "slice", "template": "skk", "download": "excel", "q": ""},
            ),
        ]
        for message, expected in cases:
            with self.subTest(message=message):
                self.assertAssistantAction(message, expected)
            self.sleep_between_llm_cases()

    def test_groq_assistant_extended_query_matrix(self):
        cases = [
            ("покажи СКК на 01.03.2026", {"mode": "slice", "template": "skk", "date": "2026-03-01", "q": ""}),
            ("покажи СКК на 1 апреля 2026", {"mode": "slice", "template": "skk", "date": "2026-04-01", "q": ""}),
            ("покажи КИК за февраль 2026", {"mode": "slice", "template": "kik", "date": "2026-02-01", "q": ""}),
            ("сравнить СКК с 01.02.2025 по 01.04.2026", {"mode": "compare", "template": "skk", "base": "2025-02-01", "target": "2026-04-01", "open_view": "changes", "q": ""}),
            ("сравни КИК март 2025 и март 2026", {"mode": "compare", "template": "kik", "base": "2025-03-01", "target": "2026-03-01", "open_view": "changes", "q": ""}),
            ("покажи проблемные СКК", {"mode": "slice", "template": "skk", "post_filter": "execution_problems", "open_view": "problems", "q": ""}),
            ("покажи СКК без кассы", {"mode": "slice", "template": "skk", "post_filter": "no_cash", "open_view": "problems", "q": ""}),
            ("найди СКК с низким кассовым исполнением", {"mode": "slice", "template": "skk", "post_filter": "low_cash", "open_view": "problems", "q": ""}),
            ("покажи разрывы данных по СКК", {"mode": "slice", "template": "skk", "post_filter": "data_gap", "open_view": "problems", "q": ""}),
            ("покажи объекты без соглашений по 2/3", {"mode": "slice", "template": "two_thirds", "post_filter": "no_documents", "open_view": "problems", "q": ""}),
            ("покажи ОКВ без платежей", {"mode": "slice", "template": "okv", "post_filter": "no_payments", "open_view": "problems", "q": ""}),
            ("покажи непроверенные ОКВ", {"mode": "slice", "template": "okv", "post_filter": "unreviewed", "open_view": "problems", "q": ""}),
            ("собери отчет 2/3", {"mode": "slice", "template": "two_thirds", "q": ""}),
            ("покажи капитальные вложения", {"mode": "slice", "template": "okv", "q": ""}),
            ("покажи только лимиты и БО по 970", {"mode": "slice", "template": "two_thirds", "metrics": ["limit", "obligation"], "q": ""}),
            ("покажи платежи БУАУ по ОКВ", {"mode": "slice", "template": "okv", "metrics": ["cash", "payment", "buau"], "q": ""}),
            ("скк благовещенск март 2025", {"mode": "slice", "template": "skk", "date": "2025-03-01", "q": "благовещенск"}),
            ("найди Благовещенск 6105 на 01.04.2026", {"mode": "slice", "template": "skk", "date": "2026-04-01", "q": "Благовещенск"}),
            ("найди Тында", {"mode": "slice", "q": "Тында"}),
            ("скачать pdf по КИК за март 2026", {"mode": "slice", "template": "kik", "date": "2026-03-01", "download": "pdf", "q": ""}),
            ("скачать таблицу по СКК", {"mode": "slice", "template": "skk", "download": "csv", "q": ""}),
            ("проверить качество данных по СКК", {"mode": "slice", "template": "skk", "open": "control", "q": ""}),
        ]
        for message, expected in cases:
            with self.subTest(message=message):
                self.assertAssistantAction(message, expected)
            self.sleep_between_llm_cases()


if __name__ == "__main__":
    unittest.main()
