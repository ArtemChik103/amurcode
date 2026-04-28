import shutil
import subprocess
import unittest
from pathlib import Path


class FrontendScenarioTests(unittest.TestCase):
    def test_vue_user_scenarios_and_click_handlers(self):
        if not shutil.which("node"):
            self.skipTest("node is required for frontend scenario tests")
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            ["node", str(root / "tests" / "frontend_scenarios_runner.js")],
            cwd=root,
            text=True,
            capture_output=True,
            timeout=30,
        )
        if result.returncode != 0:
            self.fail(f"frontend scenario runner failed\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")
        self.assertIn("frontend scenarios ok", result.stdout)


if __name__ == "__main__":
    unittest.main()
