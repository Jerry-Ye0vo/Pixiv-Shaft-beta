#!/usr/bin/env python3
"""Check certificate pins after a successful `apksigner verify --print-certs`."""
from pathlib import Path
import re
import sys


def verify(report, expected):
    expected = re.sub(r"\s+", "", expected).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise ValueError("Invalid configured Beta certificate SHA-256")
    # Older build-tools use `Signer #1 certificate ...`; newer releases use
    # `V2 Signer: certificate ...`. Match APK signers, excluding SourceStamp
    # certificates and public-key digests. The same cert can occur in several schemes.
    digests = {value.lower() for value in re.findall(
        r"^(?:Signer #\d+|V\d+(?:\.\d+)? Signer)(?::|\s).*?"
        r"certificate SHA-256 digest:\s*([0-9a-fA-F]{64})\s*$",
        report, re.MULTILINE,
    )}
    if not digests:
        raise ValueError("No APK signer certificate SHA-256 found in apksigner output")
    if digests != {expected}:
        raise ValueError("APK certificate mismatch: expected " + expected +
                         "; got " + ", ".join(sorted(digests)))
    return expected


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: verify-apk-certificate.py EXPECTED_SHA256_FILE < apksigner-report")
    try:
        print(verify(sys.stdin.read(), Path(sys.argv[1]).read_text()))
    except (OSError, ValueError) as error:
        print("::error::" + str(error), file=sys.stderr)
        sys.exit(1)
