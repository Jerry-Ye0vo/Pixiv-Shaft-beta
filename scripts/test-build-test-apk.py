#!/usr/bin/env python3
"""Check build-script behavior with a fake Gradle wrapper; no SDK, network or keys needed."""
from pathlib import Path
import hashlib
import os
import re
import shutil
import subprocess
import tempfile
import unittest


class BuildTestApkTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="shaft build test ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "scripts").mkdir()
        self.script = self.root / "scripts/build-test-apk.sh"
        shutil.copy(Path(__file__).with_name("build-test-apk.sh"), self.script)
        (self.root / "gradle.properties").write_text("SHAFT_BETA_REVISION=2\n")
        (self.root / "gradlew").write_text('''#!/usr/bin/env bash
set -eu
printf '%s\\n' "$*" >> calls.txt
if [[ -f fail-build ]]; then exit 9; fi
if [[ "$1" == :app:assembleGithubDebug ]]; then
    mkdir -p app/build/outputs/apk/github/debug
    printf '%s' "$*" > app/build/outputs/apk/github/debug/app-github-debug.apk
fi
''')
        sdk = self.root / "sdk"
        (sdk / "platform-tools").mkdir(parents=True)
        adb = sdk / "platform-tools/adb"
        adb.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$ADB_LOG"\n')
        adb.chmod(0o755)
        self.env = dict(os.environ, ANDROID_HOME=str(sdk), ADB_LOG=str(self.root / "adb-calls.txt"))
        for key in ("GITHUB_RUN_NUMBER", "GITHUB_ACTIONS", "SHAFT_BETA_REVISION"):
            self.env.pop(key, None)

    def run_build(self, *arguments, success=True):
        result = subprocess.run(["bash", str(self.script), *arguments], cwd=self.root,
                                env=self.env, capture_output=True, text=True)
        if success:
            self.assertEqual(0, result.returncode, result.stderr)
        return result

    def revisions(self):
        return [int(value) for value in re.findall(r"-PSHAFT_BETA_REVISION=(\d+)",
                (self.root / "calls.txt").read_text())]

    def test_local_builds_increment_and_name_artifacts(self):
        self.run_build()
        self.run_build()
        self.assertEqual([2, 3], self.revisions())
        names = [p.name for p in (self.root / "dist/test-apk").glob("*.apk")]
        self.assertTrue(any("4.9.9-beta.2-" in name for name in names))
        self.assertTrue(any("4.9.9-beta.3-" in name for name in names))

    def test_ci_rerun_keeps_its_iteration(self):
        self.env["GITHUB_RUN_NUMBER"] = "18"
        self.run_build()
        self.run_build()
        self.assertEqual([18, 18], self.revisions())
        self.assertFalse((self.root / ".beta-build-revision").exists())

    def test_override_advances_local_counter_without_rolling_it_back(self):
        self.run_build("--revision", "20")
        self.run_build("--revision", "5")
        self.run_build()
        self.assertEqual([20, 5, 21], self.revisions())

    def test_tests_and_packaging_share_version_and_options(self):
        self.run_build("--test", "--offline", "--abi", "x86_64", "--install", "--output", "test output")
        self.assertEqual([2, 2], self.revisions())
        calls = (self.root / "calls.txt").read_text().splitlines()
        self.assertIn(":app:testGithubDebugUnitTest", calls[0])
        for call in calls:
            for flag in ("--offline", "-PSHAFT_TEST_ABI=x86_64", "-PSHAFT_REQUIRE_BETA_SIGNING=true"):
                self.assertIn(flag, call)
        apk = next((self.root / "test output").glob("*.apk"))
        checksum = Path(str(apk) + ".sha256").read_text()
        self.assertIn(hashlib.sha256(apk.read_bytes()).hexdigest(), checksum)
        self.assertIn(apk.name, checksum)
        self.assertNotIn(str(self.root), checksum)
        self.assertIn("install -r ", (self.root / "adb-calls.txt").read_text())

    def test_bad_revisions_do_not_start_gradle(self):
        for value in ("0", "-1", "1x", "10000", "01"):
            with self.subTest(value=value):
                self.assertEqual(2, self.run_build("--revision", value, success=False).returncode)
        self.assertFalse((self.root / "calls.txt").exists())

    def test_existing_lock_does_not_allocate_another_number(self):
        (self.root / ".beta-build-revision.lock").mkdir()
        self.assertNotEqual(0, self.run_build(success=False).returncode)
        self.assertFalse((self.root / ".beta-build-revision").exists())

    def test_failed_build_does_not_publish_a_stale_apk_or_reuse_iteration(self):
        stale = self.root / "app/build/outputs/apk/github/debug/old.apk"
        stale.parent.mkdir(parents=True)
        stale.write_bytes(b"stale")
        (self.root / "fail-build").touch()
        self.assertEqual(9, self.run_build("--output", "failed", success=False).returncode)
        self.assertFalse((self.root / "failed").exists())
        self.assertEqual("2", (self.root / ".beta-build-revision").read_text().strip())
        self.assertFalse((self.root / ".beta-build-revision.lock").exists())

    def test_revision_limit_does_not_corrupt_the_counter(self):
        self.run_build("--revision", "9999")
        self.assertEqual(2, self.run_build(success=False).returncode)
        self.assertEqual("9999", (self.root / ".beta-build-revision").read_text().strip())
        self.assertFalse((self.root / ".beta-build-revision.lock").exists())


if __name__ == "__main__":
    unittest.main()
