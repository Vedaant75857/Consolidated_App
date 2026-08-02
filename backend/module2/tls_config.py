"""Portable TLS CA-bundle configuration for outbound Python clients."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

CA_ENV = "PROCIP_CA_BUNDLE"


def _candidate_paths() -> Iterable[Path]:
    configured = os.environ.get(CA_ENV) or os.environ.get("REQUESTS_CA_BUNDLE")
    if configured:
        yield Path(configured).expanduser()
        return
    yield Path(r"C:\Bain\Setup\Zscaler\zscaler.pem")
    yield Path(r"C:\Bain\Setup\Zscaler\zscaler 2026_05.pem")
    yield Path.home() / "zscaler.pem"
    yield Path("/usr/local/share/ca-certificates/zscaler.pem")
    yield Path("/etc/ssl/certs/zscaler.pem")


def find_ca_bundle() -> Path | None:
    """Return the first configured/existing PEM bundle, if available."""
    for candidate in _candidate_paths():
        try:
            if candidate.is_file() and candidate.stat().st_size > 0:
                return candidate.resolve()
        except OSError:
            continue
    return None


def configure_tls() -> Path | None:
    """Configure common Python TLS clients without weakening verification."""
    bundle = find_ca_bundle()
    if bundle is None:
        return None
    bundle_text = str(bundle)
    os.environ.setdefault("REQUESTS_CA_BUNDLE", bundle_text)
    os.environ.setdefault("SSL_CERT_FILE", bundle_text)
    return bundle
