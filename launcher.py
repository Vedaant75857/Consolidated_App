"""
DataScopingTool — single-process launcher.

Starts all three Flask module backends and a landing-page static server,
then opens the user's default browser.  Designed to run both in development
(``python launcher.py``) and when frozen by PyInstaller into a single EXE.
"""

import logging
import os
import socket
import sys
import threading
import time
import webbrowser

from waitress import create_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DataScopingTool] %(levelname)s: %(message)s",
)
log = logging.getLogger("launcher")

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _base_path() -> str:
    """Root directory — _MEIPASS when frozen, repo root when running raw."""
    if getattr(sys, "frozen", False):
        return sys._MEIPASS  # type: ignore[attr-defined]
    return os.path.dirname(os.path.abspath(__file__))


BASE = _base_path()


def _resolve(*parts: str) -> str:
    """Join *parts* onto BASE and return an absolute path."""
    return os.path.normpath(os.path.join(BASE, *parts))

# ---------------------------------------------------------------------------
# Static-file serving helper
# ---------------------------------------------------------------------------

def _add_static_serving(app, dist_folder: str, name_prefix: str):
    """Register a catch-all route on *app* that serves the Vite build in
    *dist_folder*.  Requests starting with ``/api`` are left for the
    module's own blueprint routes.
    """
    from flask import send_from_directory

    endpoint_index = f"{name_prefix}_serve_index"
    endpoint_file = f"{name_prefix}_serve_static"

    @app.route("/", endpoint=endpoint_index)
    def _serve_index():
        return send_from_directory(dist_folder, "index.html")

    @app.route("/<path:path>", endpoint=endpoint_file)
    def _serve_static(path):
        if path.startswith("api/"):
            from flask import abort
            abort(404)
        full = os.path.join(dist_folder, path)
        if os.path.isfile(full):
            return send_from_directory(dist_folder, path)
        return send_from_directory(dist_folder, "index.html")


def _port_available(port: int) -> bool:
    """Return True if *port* is free on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False

# ---------------------------------------------------------------------------
# Module 1 — Data Stitcher  (port 3001)
# ---------------------------------------------------------------------------

LOCAL_PACKAGE_NAMES = frozenset([
    "routes", "shared", "db", "agents", "services",
    "data_loading", "data_quality_assessment", "header-normalisation",
    "merging", "appending", "inventory", "summary", "ai-core", "ai_core",
])


def _import_module_app(backend_dir: str, module_alias: str):
    """Import a module's ``app.py`` in isolation.

    Each module has its own ``app.py`` plus local packages (``db``, ``agents``,
    ``shared``, ``routes``, ``services``, etc.) that share the same top-level
    names.  To prevent cross-contamination we:
      1. Remove any previously-loaded local packages from sys.modules.
      2. Prepend the backend dir to sys.path.
      3. Load the module's app.py under a unique alias.
      4. Rename all local packages in sys.modules to ``<alias>.<pkg>`` so they
         don't collide with the next module.
    """
    import importlib, importlib.util

    # Step 1 — evict stale local packages from previous modules
    to_delete = [
        name for name in sys.modules
        if name.split(".")[0] in LOCAL_PACKAGE_NAMES
    ]
    for name in to_delete:
        del sys.modules[name]

    # Step 2 — put this backend first on sys.path
    if backend_dir in sys.path:
        sys.path.remove(backend_dir)
    sys.path.insert(0, backend_dir)

    # Step 3 — load app.py under a unique alias
    app_file = os.path.join(backend_dir, "app.py")
    spec = importlib.util.spec_from_file_location(module_alias, app_file)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_alias] = mod
    spec.loader.exec_module(mod)

    # Step 4 — rename local packages so the next module doesn't collide
    renames: dict[str, object] = {}
    for name in list(sys.modules):
        top = name.split(".")[0]
        if top in LOCAL_PACKAGE_NAMES:
            renames[f"{module_alias}.{name}"] = sys.modules.pop(name)
    sys.modules.update(renames)

    return mod


def _create_module1_app():
    """Import Module 1's Flask app and bolt on static-file serving."""
    m1_backend = _resolve("DataConsolidationAppV7", "backend", "module-1")
    os.environ.setdefault("NODE_PORT", "3001")

    mod = _import_module_app(m1_backend, "_m1_app")
    flask_app = mod.app  # module-level `app = create_app()` already ran

    dist = _resolve("DataConsolidationAppV7", "frontend", "dist")
    _add_static_serving(flask_app, dist, "m1")
    return flask_app

# ---------------------------------------------------------------------------
# Module 2 — Data Normalizer  (port 5000)
# ---------------------------------------------------------------------------

def _create_module2_app():
    """Import Module 2's Flask app and bolt on static-file serving."""
    m2_backend = _resolve("ProcIP_Module2-main", "backend")
    os.environ.setdefault("FLASK_PORT", "5000")

    mod = _import_module_app(m2_backend, "_m2_app")
    flask_app = mod.app

    dist = _resolve("ProcIP_Module2-main", "frontend", "dist")
    _add_static_serving(flask_app, dist, "m2")
    return flask_app

# ---------------------------------------------------------------------------
# Module 3 — Spend Summarizer  (port 3005)
# ---------------------------------------------------------------------------

def _create_module3_app():
    """Import Module 3's Flask app and bolt on static-file serving."""
    m3_backend = _resolve("SummarizationModule", "backend")
    os.environ.setdefault("PORT", "3005")

    mod = _import_module_app(m3_backend, "_m3_app")
    flask_app = mod.app

    dist = _resolve("SummarizationModule", "frontend", "dist")
    _add_static_serving(flask_app, dist, "m3")
    return flask_app

# ---------------------------------------------------------------------------
# Landing page  (port 3000)
# ---------------------------------------------------------------------------

def _create_landing_app():
    """Lightweight Flask app that serves the landing-page build."""
    from flask import Flask, send_from_directory

    landing_app = Flask(__name__, static_folder=None)
    dist = _resolve("landing-page", "dist")

    @landing_app.route("/", defaults={"path": ""})
    @landing_app.route("/<path:path>")
    def serve(path):
        full = os.path.join(dist, path)
        if path and os.path.isfile(full):
            return send_from_directory(dist, path)
        return send_from_directory(dist, "index.html")

    return landing_app

# ---------------------------------------------------------------------------
# Server runner
# ---------------------------------------------------------------------------

_servers: list = []  # keep references so we can .close() on shutdown


def _run_server(app, port: int, label: str):
    """Start a waitress server for *app* on *port* (blocking call)."""
    srv = create_server(app, host="0.0.0.0", port=port)
    _servers.append(srv)
    log.info("%s ready on http://localhost:%d", label, port)
    srv.run()


SERVICES = [
    # (factory_fn, port, label)
    (_create_landing_app, 3000, "Landing Page"),
    (_create_module1_app, 3001, "Module 1 — Data Stitcher"),
    (_create_module2_app, 5000, "Module 2 — Data Normalizer"),
    (_create_module3_app, 3005, "Module 3 — Spend Summarizer"),
]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(r"""
    ╔══════════════════════════════════════════╗
    ║        DataScopingTool  v1.0             ║
    ║  Starting all services — please wait…    ║
    ╚══════════════════════════════════════════╝
    """)

    # Pre-check all ports
    for _, port, label in SERVICES:
        if not _port_available(port):
            log.error("Port %d is already in use (%s). "
                      "Please close the other program and try again.", port, label)
            input("Press Enter to exit...")
            sys.exit(1)

    # Create & start each service in a daemon thread
    for factory, port, label in SERVICES:
        try:
            app = factory()
        except Exception:
            log.exception("Failed to initialise %s", label)
            input("Press Enter to exit...")
            sys.exit(1)

        t = threading.Thread(target=_run_server, args=(app, port, label), daemon=True)
        t.start()

    # Give servers a moment to bind
    time.sleep(2)

    url = "http://localhost:3000"
    log.info("Opening %s in your browser…", url)
    webbrowser.open(url)

    print("\n  All services are running.")
    print("  Close this window (or press Ctrl+C) to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down…")
        for srv in _servers:
            try:
                srv.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
