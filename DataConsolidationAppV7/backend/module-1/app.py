"""Module 1 — Data Stitching API (Flask, port 3001)."""

import atexit
import json
import logging
import math
import os
import threading
import time
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("module1")

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env.local"))

from flask import Flask, g, request
from flask_cors import CORS
from flask.json.provider import DefaultJSONProvider

from shared.db import cleanup_stale_sessions, cleanup_all_sessions
from shared.utils import json_default


def _nan_to_none(obj):
    """Recursively replace float NaN/Infinity with None for JSON safety."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _nan_to_none(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_nan_to_none(v) for v in obj]
    return obj


class AppJSONProvider(DefaultJSONProvider):
    def default(self, o):  # type: ignore[override]
        return json_default(o)

    def dumps(self, obj, **kwargs):
        kwargs.setdefault("default", self.default)
        return json.dumps(_nan_to_none(obj), allow_nan=False, **kwargs)


def create_app() -> Flask:
    app = Flask(__name__)
    app.json = AppJSONProvider(app)
    CORS(app)
    app.config["MAX_CONTENT_LENGTH"] = 300 * 1024 * 1024  # 300 MB

    @app.before_request
    def _start_request_timer():
        g.request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex[:12]
        g._start_ts = time.perf_counter()

    @app.after_request
    def _log_request(resp):
        try:
            elapsed_ms = (time.perf_counter() - float(getattr(g, "_start_ts", time.perf_counter()))) * 1000
            path = request.path
            method = request.method
            status = resp.status_code
            rid = getattr(g, "request_id", "")
            logger.info("[Module-1] %s %s -> %s in %.1fms (rid=%s)", method, path, status, elapsed_ms, rid)
            resp.headers["X-Request-Id"] = rid
            resp.headers["X-Response-Time-Ms"] = f"{elapsed_ms:.1f}"
        except Exception:
            pass
        return resp

    _blueprints: list[tuple[str, str]] = [
        ("routes.data_loading_routes", "data_loading_bp"),
        ("routes.inventory_routes", "inventory_bp"),
        ("routes.appending_routes", "appending_bp"),
        ("routes.header_normalisation_routes", "header_normalisation_bp"),
        ("routes.merging_routes", "merging_bp"),
        ("routes.insights_routes", "insights_bp"),
    ]
    for mod_path, bp_attr in _blueprints:
        try:
            mod = __import__(mod_path, fromlist=[bp_attr])
            app.register_blueprint(getattr(mod, bp_attr), url_prefix="/api")
        except Exception as exc:
            logger.warning("[Module-1] failed to load %s.%s: %s", mod_path, bp_attr, exc)

    return app


def _session_cleanup_loop() -> None:
    import time
    while True:
        time.sleep(60 * 60)
        cleaned = cleanup_stale_sessions()
        if cleaned:
            logger.info("Cleaned up %d stale session(s).", cleaned)


app = create_app()

_startup_cleaned = cleanup_all_sessions()
if _startup_cleaned:
    logger.info("[Module-1] Startup: cleared %d leftover session(s).", _startup_cleaned)


def _on_exit():
    cleaned = cleanup_all_sessions()
    logger.info("[Module-1] Shutdown: deleted %d session(s).", cleaned)


atexit.register(_on_exit)

if __name__ == "__main__":
    t = threading.Thread(target=_session_cleanup_loop, daemon=True)
    t.start()

    port = int(os.environ.get("NODE_PORT", "3001"))
    logger.info("[Module-1] Data Stitching API running on http://localhost:%d", port)
    app.run(host="0.0.0.0", port=port, debug=False)
