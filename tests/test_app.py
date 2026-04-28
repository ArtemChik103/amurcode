import json
import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer

import app


class ParserTests(unittest.TestCase):
    def test_parse_amount_supports_russian_and_machine_formats(self):
        self.assertEqual(app.parse_amount("1 234 567,89"), 1234567.89)
        self.assertEqual(app.parse_amount("10000000.00"), 10000000.0)
        self.assertEqual(app.parse_amount(""), 0.0)
        self.assertEqual(app.parse_amount(None), 0.0)

    def test_parse_date_normalizes_known_input_formats(self):
        self.assertEqual(app.parse_date("20.08.2025"), "2025-08-20")
        self.assertEqual(app.parse_date("2025-03-07 00:00:00.000"), "2025-03-07")
        self.assertEqual(app.parse_date("2026-04-01"), "2026-04-01")

    def test_normalize_code_removes_separators_and_keeps_cyrillic_letters(self):
        self.assertEqual(app.normalize_code("13.2.01.97003"), "1320197003")
        self.assertEqual(app.normalize_code("101016105Б"), "101016105Б")


class DataLoadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = app.STORE

    def test_loads_expected_dataset_volume_and_sources(self):
        self.assertEqual(self.store.meta["records"], 4037)
        self.assertEqual(
            set(self.store.meta["sources"]),
            {"РЧБ", "Соглашения", "ГЗ: контракты", "ГЗ: платежи", "БУАУ"},
        )

    def test_detects_expected_budgets_and_snapshots(self):
        self.assertIn("Областной бюджет Амурской области", self.store.meta["budgets"])
        self.assertIn("Бюджет г. Тынды", self.store.meta["budgets"])
        self.assertIn("2025-02-01", self.store.meta["snapshots"])
        self.assertIn("2026-04-01", self.store.meta["snapshots"])

    def test_code_filter_finds_rcb_and_agreement_records(self):
        result = app.aggregate(
            app.apply_filters(
                self.store.records,
                {"code": ["970"], "start": ["2025-01-01"], "end": ["2026-04-01"]},
            )
        )
        self.assertEqual(result["count"], 997)
        self.assertGreaterEqual(len(result["rows"]), 10)
        top_sources = result["rows"][0]["sources"]
        self.assertIn("РЧБ", top_sources)
        self.assertIn("Соглашения", top_sources)
        self.assertGreater(result["totals"]["limit"], 0)
        self.assertGreater(result["totals"]["agreement"], 0)

    def test_text_budget_and_source_filters_are_combined(self):
        filtered = app.apply_filters(
            self.store.records,
            {
                "q": ["Тынды"],
                "budget": ["Бюджет г. Тынды"],
                "source": ["БУАУ"],
                "start": ["2025-08-01"],
                "end": ["2025-12-31"],
            },
        )
        self.assertGreater(len(filtered), 0)
        self.assertTrue(all(record["source"] == "БУАУ" for record in filtered))
        self.assertTrue(all("Тынды".lower() in " ".join(str(v) for v in record.values()).lower() for record in filtered))

    def test_aggregate_has_stable_shape_for_frontend(self):
        result = app.aggregate(self.store.records)
        self.assertEqual(
            set(result.keys()),
            {"totals", "rows", "details", "timeline", "count"},
        )
        self.assertLessEqual(len(result["rows"]), 300)
        self.assertLessEqual(len(result["details"]), 500)
        self.assertTrue(result["timeline"])
        for metric in ["limit", "obligation", "cash", "agreement", "contract", "payment", "buau"]:
            self.assertIn(metric, result["totals"])


class HttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def request(self, path):
        conn = HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            conn.request("GET", path)
            response = conn.getresponse()
            body = response.read()
            return response.status, response.getheader("Content-Type"), body
        finally:
            conn.close()

    def test_meta_endpoint_returns_json(self):
        status, content_type, body = self.request("/api/meta")
        self.assertEqual(status, 200)
        self.assertIn("application/json", content_type)
        payload = json.loads(body.decode("utf-8"))
        self.assertEqual(payload["records"], 4037)
        self.assertIn("РЧБ", payload["sources"])

    def test_query_endpoint_returns_filtered_result(self):
        status, content_type, body = self.request("/api/query?code=6105&source=%D0%A0%D0%A7%D0%91")
        self.assertEqual(status, 200)
        self.assertIn("application/json", content_type)
        payload = json.loads(body.decode("utf-8"))
        self.assertGreater(payload["count"], 0)
        self.assertTrue(payload["rows"])
        self.assertTrue(all("РЧБ" in row["sources"] for row in payload["rows"]))

    def test_static_index_is_served(self):
        status, content_type, body = self.request("/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", content_type)
        self.assertIn("Конструктор аналитических выборок", body.decode("utf-8"))


class StaticFilesTests(unittest.TestCase):
    def test_frontend_assets_exist_and_reference_api(self):
        index = (app.STATIC_DIR / "index.html").read_text(encoding="utf-8")
        script = (app.STATIC_DIR / "app.js").read_text(encoding="utf-8")
        styles = (app.STATIC_DIR / "styles.css").read_text(encoding="utf-8")

        self.assertIn("/static/app.js", index)
        self.assertIn("/api/meta", script)
        self.assertIn("/api/query", script)
        self.assertIn("grid-template-columns", styles)


if __name__ == "__main__":
    unittest.main()
