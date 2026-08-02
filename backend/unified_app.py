"""Thin one-process host for the three independently-owned Flask backends."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Callable

from flask import Flask, abort, jsonify, send_from_directory
from werkzeug.middleware.dispatcher import DispatcherMiddleware

try:
    from .tls_config import configure_tls
except ImportError:  # pragma: no cover - launcher path
    from tls_config import configure_tls

configure_tls()

try:  # Supports both ``python backend/unified_app.py`` and package imports.
    from .unified_loader import load_module_app
except ImportError:  # pragma: no cover - exercised by the launcher command
    from unified_loader import load_module_app


MODULE_MOUNTS = {
    "module1": "/api/module1",
    "module2": "/api/module2",
    "module3": "/api/module3",
}

# The suite build is intentionally optional: development still runs the Vite
# server, while a production/local build can be served by this same process.
# Keep the default relative to the repository rather than the caller's cwd so
# ``python backend/unified_app.py`` and package imports behave identically.
DEFAULT_SUITE_DIST = Path(__file__).resolve().parents[1] / "frontend" / "suite" / "dist"


def _suite_dist_dir() -> Path:
    """Resolve the built suite directory, allowing a deployment override."""

    configured = os.environ.get("UNIFIED_FRONTEND_DIST")
    if configured:
        path = Path(configured).expanduser()
        if not path.is_absolute():
            path = (Path(__file__).resolve().parents[1] / path).resolve()
        return path
    return DEFAULT_SUITE_DIST


class _ExistingApiPath:
    """Expose a legacy ``/api/*`` Flask app beneath one module namespace."""

    def __init__(self, app: Callable):
        self.app = app

    def __call__(self, environ, start_response):
        # DispatcherMiddleware strips ``/api/moduleN``.  The legacy apps still
        # own their existing /api routes, so restore that internal path here.
        forwarded = environ.copy()
        path = forwarded.get("PATH_INFO", "") or "/"
        forwarded["PATH_INFO"] = "/api" if path == "/" else f"/api{path}"
        return self.app(forwarded, start_response)


class _UnavailableModule:
    def __init__(self, detail: str):
        self.detail = detail

    def __call__(self, _environ, start_response):
        body = json.dumps({"status": "unavailable", "detail": self.detail}).encode("utf-8")
        start_response("503 Service Unavailable", [("Content-Type", "application/json"), ("Content-Length", str(len(body)))])
        return [body]


def create_app() -> Flask:
    """Compose module apps; no module routes or services live in this host."""
    host = Flask(__name__)
    suite_dist = _suite_dist_dir()
    loaded: dict[str, Flask] = {}
    errors: dict[str, str] = {}
    mounts = {}

    for module_name, mount_path in MODULE_MOUNTS.items():
        try:
            module_app = load_module_app(module_name)
            loaded[module_name] = module_app
            mounts[mount_path] = _ExistingApiPath(module_app.wsgi_app)
        except Exception as exc:  # Keep startup diagnostics available per module.
            errors[module_name] = f"{type(exc).__name__}: {exc}"
            mounts[mount_path] = _UnavailableModule(errors[module_name])

    @host.get("/api/health")
    def health():
        modules = {}
        for name, app in loaded.items():
            try:
                response = app.test_client().get("/api/health")
                modules[name] = {"status": "ok" if response.status_code < 400 else "unhealthy"}
            except Exception as exc:  # pragma: no cover - defensive readiness reporting
                modules[name] = {"status": "unhealthy", "detail": str(exc)}
        for name, detail in errors.items():
            modules[name] = {"status": "unavailable", "detail": detail}
        ready = len(modules) == len(MODULE_MOUNTS) and all(item["status"] == "ok" for item in modules.values())
        return jsonify({"status": "ok" if ready else "degraded", "modules": modules}), 200 if ready else 503

    @host.route("/", defaults={"path": ""}, methods=["GET", "HEAD"])
    @host.route("/<path:path>", methods=["GET", "HEAD"])
    def suite_static(path: str):
        """Serve built suite files and deep-link requests without touching APIs.

        DispatcherMiddleware handles the three module API mounts before this
        Flask application sees a request.  Explicitly reject any other
        ``/api`` path so a missing API endpoint can never receive ``index.html``
        as an SPA fallback response.
        """

        normalized = path.strip("/")
        if normalized == "api" or normalized.startswith("api/"):
            abort(404)
        if not suite_dist.is_dir():
            abort(404)

        if normalized:
            candidate = suite_dist / normalized
            # Requests with a file extension are assets, not client-side
            # routes; preserve a normal 404 for a missing asset.
            if candidate.suffix:
                return send_from_directory(suite_dist, normalized)
            # Existing extensionless files (for example a manifest) should be
            # served directly; send_from_directory also enforces safe joins.
            if candidate.is_file():
                return send_from_directory(suite_dist, normalized)

        # React Router owns paths such as /stitcher/, /normalizer/, and
        # /summarizer/ after the static index has loaded.
        return send_from_directory(suite_dist, "index.html")

    host.wsgi_app = DispatcherMiddleware(host.wsgi_app, mounts)
    host.config["UNIFIED_MODULE_APPS"] = loaded
    host.config["UNIFIED_MODULE_ERRORS"] = errors
    host.config["UNIFIED_FRONTEND_DIST"] = str(suite_dist)
    return host


app = create_app()


if __name__ == "__main__":
    port = int(os.environ.get("UNIFIED_BACKEND_PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
