import tls_config


def test_tls_config_does_not_raise():
    """find_ca_bundle and configure_tls must run without errors."""
    tls_config.find_ca_bundle()
    tls_config.configure_tls()


def test_configure_tls_returns_none_when_no_bundle(monkeypatch):
    """When no CA bundle is found, configure_tls returns None."""
    monkeypatch.delenv("PROCIP_CA_BUNDLE", raising=False)
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    assert tls_config.configure_tls() is None
