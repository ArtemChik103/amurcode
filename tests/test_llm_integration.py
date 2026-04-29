import os
import unittest

import app


@unittest.skipIf(os.environ.get("RUN_LLM_TESTS") != "1" or not os.environ.get("GROQ_API_KEY"), "LLM tests are opt-in")
class GroqIntegrationTests(unittest.TestCase):
    def test_groq_assistant_returns_valid_action(self):
        payload = app.assistant_response(
            "покажи проблемные СКК без кассы",
            {"mode": "slice", "template": "all"},
        )
        self.assertIn(payload["mode"], {"llm", "rule_based"})
        self.assertIn("action", payload)
        self.assertIn(payload["action"]["template"], app.TEMPLATES)


if __name__ == "__main__":
    unittest.main()
