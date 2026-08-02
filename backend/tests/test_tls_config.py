import os
from pathlib import Path

from backend import tls_config


def test_explicit_bundle_sets_standard_client_variables(monkeypatch, tmp_path: Path):
    bundle = tmp_path / "zscaler.pem"
    bundle.write_text("certificate", encoding="ascii")
    monkeypatch.setenv("PROCIP_CA_BUNDLE", str(bundle))
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)

    assert tls_config.configure_tls() == bundle.resolve()
    assert Path(os.environ["REQUESTS_CA_BUNDLE"]) == bundle.resolve()
    assert Path(os.environ["SSL_CERT_FILE"]) == bundle.resolve()


def test_existing_standard_variables_are_not_overwritten(monkeypatch, tmp_path: Path):
    configured = tmp_path / "configured.pem"
    requests_bundle = tmp_path / "requests.pem"
    ssl_bundle = tmp_path / "ssl.pem"
    for path in (configured, requests_bundle, ssl_bundle):
        path.write_text("certificate", encoding="ascii")
    monkeypatch.setenv("PROCIP_CA_BUNDLE", str(configured))
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(requests_bundle))
    monkeypatch.setenv("SSL_CERT_FILE", str(ssl_bundle))

    assert tls_config.configure_tls() == configured.resolve()
    assert Path(os.environ["REQUESTS_CA_BUNDLE"]) == requests_bundle.resolve()
    assert Path(os.environ["SSL_CERT_FILE"]) == ssl_bundle.resolve()


def test_missing_bundle_does_not_disable_verification(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("PROCIP_CA_BUNDLE", str(tmp_path / "missing.pem"))
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)

    assert tls_config.configure_tls() is None
    assert "REQUESTS_CA_BUNDLE" not in os.environ
    assert "SSL_CERT_FILE" not in os.environ
