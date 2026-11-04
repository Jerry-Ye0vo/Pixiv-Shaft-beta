#!/usr/bin/env python3
"""Regression fixtures for legacy and current Android build-tools output."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


EXPECTED = "98ff2935b059672b8f9ea236120cd1f2a55b08bbc5de85cd6ddc296ac1b7be8a"
OTHER = "a" * 64


class CertificateVerificationTest(unittest.TestCase):
    def check_report(self, report, expected=EXPECTED, success=True):
        with tempfile.TemporaryDirectory(prefix="beta cert test ") as directory:
            pin = Path(directory) / "certificate.sha256"
            pin.write_text(expected + "\n")
            result = subprocess.run(
                [sys.executable, str(Path(__file__).with_name("verify-apk-certificate.py")), str(pin)],
                input=report, text=True, capture_output=True,
            )
        if success:
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual(EXPECTED + "\n", result.stdout)
        else:
            self.assertNotEqual(0, result.returncode)
            self.assertIn("::error::", result.stderr)
            self.assertEqual("", result.stdout)
        return result

    def test_legacy_signer_format(self):
        self.check_report("Verifies\nSigner #1 certificate SHA-256 digest: " + EXPECTED + "\n")

    def test_actual_v2_format_from_failed_beta_build(self):
        self.check_report("Verifies\nVerified using v2 scheme (APK Signature Scheme v2): true\n"
                          "Number of signers: 1\nV2 Signer: certificate SHA-256 digest: " + EXPECTED + "\n")

    def test_repeated_certificate_across_signature_schemes(self):
        report = "\n".join(prefix + " certificate SHA-256 digest: " + EXPECTED.upper()
                           for prefix in ("Signer #1", "V2 Signer:", "V3 Signer:", "V3.1 Signer:"))
        self.check_report(report)

    def test_public_key_digest_does_not_pass_as_certificate(self):
        self.check_report("V2 Signer: public key SHA-256 digest: " + EXPECTED + "\n"
                          "V2 Signer: certificate SHA-256 digest: " + OTHER, success=False)

    def test_source_stamp_does_not_override_apk_signer(self):
        self.check_report("Source Stamp Signer certificate SHA-256 digest: " + OTHER + "\n"
                          "V2 Signer: certificate SHA-256 digest: " + EXPECTED)

    def test_additional_unexpected_apk_signer_is_rejected(self):
        self.check_report("Signer #1 certificate SHA-256 digest: " + EXPECTED + "\n"
                          "Signer #2 certificate SHA-256 digest: " + OTHER, success=False)

    def test_missing_certificate_is_rejected(self):
        self.check_report("Verifies\nNumber of signers: 1\n", success=False)

    def test_invalid_pin_is_rejected(self):
        self.check_report("Signer #1 certificate SHA-256 digest: " + EXPECTED,
                          expected="not-a-sha256", success=False)


if __name__ == "__main__":
    unittest.main()
