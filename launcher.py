"""
DataScopingTool — single-process launcher.

Starts all three Flask module backends and a landing-page static server,
then opens the user's default browser.  Designed to run both in development
(``python launcher.py``) and when frozen by PyInstaller into a single EXE.
"""

import logging
import os
import shutil
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

# When frozen, point SSL at the bundled certifi CA bundle so httpx/portkey
# can verify HTTPS certificates without relying on the system store.
if getattr(sys, "frozen", False):
    _cert = os.path.join(BASE, "certifi", "cacert.pem")
    if os.path.isfile(_cert):
        os.environ.setdefault("SSL_CERT_FILE", _cert)


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

# Maps normalised backend directory paths to their module alias so the
# import-redirect hook can resolve bare local package names at runtime.
_BACKEND_DIR_TO_ALIAS: dict[str, str] = {}


class _ModuleRedirectFinder:
    """``sys.meta_path`` finder that intercepts bare local-package imports
    (e.g. ``shared.db``) and redirects them to the correct module-specific
    alias (e.g. ``_m1_app.shared.db``) by inspecting the caller's file path.

    Without this, any lazy ``from shared.db import X`` executed after all
    modules are loaded would resolve to whichever module's backend dir
    happens to be first on ``sys.path`` — usually Module 3, which has a
    completely different ``shared.db``.
    """

    def find_module(self, fullname, path=None):
        top = fullname.split(".")[0]
        if top not in LOCAL_PACKAGE_NAMES:
            return None
        alias = self._caller_alias()
        if alias is None:
            return None
        aliased = f"{alias}.{fullname}"
        if aliased in sys.modules:
            return self
        return None

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
        alias = self._caller_alias()
        if alias is None:
            raise ImportError(fullname)
        aliased = f"{alias}.{fullname}"
        mod = sys.modules.get(aliased)
        if mod is None:
            raise ImportError(fullname)
        sys.modules[fullname] = mod
        return mod

    @staticmethod
    def _caller_alias() -> str | None:
        """Walk the call stack to find which module backend the caller lives
        in, then return the corresponding alias (e.g. ``_m1_app``)."""
        import inspect
        frame = inspect.currentframe()
        try:
            f = frame
            for _ in range(15):
                f = getattr(f, "f_back", None)
                if f is None:
                    break
                fname = (f.f_globals.get("__file__") or "")
                if not fname or "importlib" in fname:
                    continue
                norm = os.path.normcase(os.path.normpath(fname))
                for dir_prefix, alias in _BACKEND_DIR_TO_ALIAS.items():
                    if norm.startswith(dir_prefix):
                        return alias
        finally:
            del frame
        return None


_redirect_finder = _ModuleRedirectFinder()


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
      5. Register the backend dir for the runtime import-redirect hook.
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

    # Step 5 — register for runtime redirect hook
    norm_dir = os.path.normcase(os.path.normpath(backend_dir)) + os.sep
    _BACKEND_DIR_TO_ALIAS[norm_dir] = module_alias

    return mod


def _post_load_cleanup():
    """Final cleanup after all modules are loaded.

    Evicts any remaining bare local package names from ``sys.modules`` and
    removes module backend dirs from ``sys.path`` so that stale lazy imports
    cannot silently resolve to the wrong module.  The ``_ModuleRedirectFinder``
    on ``sys.meta_path`` handles any future lazy imports by routing them to
    the correct aliased version.
    """
    to_delete = [
        name for name in sys.modules
        if name.split(".")[0] in LOCAL_PACKAGE_NAMES
    ]
    for name in to_delete:
        del sys.modules[name]

    dirs_to_remove = {
        os.path.normcase(os.path.normpath(d.rstrip(os.sep)))
        for d in _BACKEND_DIR_TO_ALIAS
    }
    sys.path[:] = [
        p for p in sys.path
        if os.path.normcase(os.path.normpath(p)) not in dirs_to_remove
    ]

    if _redirect_finder not in sys.meta_path:
        sys.meta_path.insert(0, _redirect_finder)


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

_servers: list = []          # keep references so we can .close() on shutdown
_server_errors: list = []    # (label, port) pairs for threads that failed to start


def _run_server(app, port: int, label: str):
    """Start a waitress server for *app* on *port* (blocking call).

    Binds to 127.0.0.1 (localhost only) so Windows Firewall never prompts
    the user — this is a local desktop tool, not a network service.
    """
    try:
        srv = create_server(app, host="127.0.0.1", port=port)
        _servers.append(srv)
        log.info("%s ready on http://localhost:%d", label, port)
        srv.run()
    except Exception:
        log.exception("Server thread crashed for %s (port %d)", label, port)
        _server_errors.append((label, port))


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

def _wait_for_servers(ports: list[int], timeout: int = 15) -> set[int]:
    """Block until every port in *ports* accepts a TCP connection.

    Returns the set of ports that did NOT come up within *timeout* seconds.
    """
    deadline = time.time() + timeout
    pending = set(ports)
    while pending and time.time() < deadline:
        for port in list(pending):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    pending.discard(port)
        if pending:
            time.sleep(0.3)
    return pending


def main():
    print(r"""
    ╔══════════════════════════════════════════╗
    ║        DataScopingTool  v1.0             ║
    ║  Starting all services — please wait…    ║
    ╚══════════════════════════════════════════╝
    """)

    total = len(SERVICES)

    # Pre-check all ports
    for _, port, label in SERVICES:
        if not _port_available(port):
            log.error("Port %d is already in use (%s). "
                      "Please close the other program and try again.", port, label)
            input("Press Enter to exit...")
            sys.exit(1)

    # Create all apps first (sequentially, so module isolation works)
    apps: list[tuple] = []
    for idx, (factory, port, label) in enumerate(SERVICES, 1):
        print(f"  [{idx}/{total}] Loading {label} ...")
        try:
            app = factory()
        except Exception:
            log.exception("Failed to initialise %s", label)
            input("Press Enter to exit...")
            sys.exit(1)
        apps.append((app, port, label))

    # All modules loaded — seal the import environment so stale bare
    # package names can't accidentally resolve to the wrong module.
    _post_load_cleanup()

    # Start each service in a daemon thread
    print(f"\n  Starting {total} servers ...")
    for app, port, label in apps:
        t = threading.Thread(target=_run_server, args=(app, port, label), daemon=True)
        t.start()

    # Wait until every server is actually accepting connections
    all_ports = [port for _, port, _ in SERVICES]
    stalled = _wait_for_servers(all_ports, timeout=15)

    # Also check if any server thread crashed during startup
    if _server_errors:
        for label, port in _server_errors:
            log.error("  ✗ %s (port %d) failed to start.", label, port)
        print("\n  Some services failed to start. Check the log above.")
        input("Press Enter to exit...")
        sys.exit(1)

    if stalled:
        port_to_label = {port: label for _, port, label in SERVICES}
        for port in stalled:
            log.warning("  ✗ %s (port %d) did not respond in time.",
                        port_to_label.get(port, "Unknown"), port)
        print("\n  Some services are slow to start — the app may not work correctly.")
        print("  Opening the browser anyway. If it shows an error, wait a moment and refresh.\n")
    else:
        print("  All services ready.")

    url = "http://localhost:3000"
    log.info("Opening %s in your browser…", url)
    webbrowser.open(url)

    print("\n  Close this window (or press Ctrl+C) to stop.\n")

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

        for alias, label in [
            ("_m1_app", "Module 1"),
            ("_m2_app", "Module 2"),
            ("_m3_app", "Module 3"),
        ]:
            mod = sys.modules.get(alias)
            if mod is None:
                continue
            fn = getattr(mod, "cleanup_all_sessions", None)
            if fn:
                try:
                    fn()
                    log.info("  %s sessions cleaned.", label)
                except Exception:
                    log.warning("  %s session cleanup failed.", label)

        for backend_dir in [
            _resolve("DataConsolidationAppV7", "backend"),
            _resolve("ProcIP_Module2-main", "backend"),
            _resolve("SummarizationModule", "backend"),
        ]:
            for root, dirs, _ in os.walk(backend_dir):
                for d in dirs:
                    if d == "__pycache__":
                        try:
                            shutil.rmtree(os.path.join(root, d))
                        except OSError:
                            pass
        log.info("Cache and session cleanup complete.")


if __name__ == "__main__":
    main()
