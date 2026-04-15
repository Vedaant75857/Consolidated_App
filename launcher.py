"""
DataScopingTool — single-process launcher.

Starts all three Flask module backends and a landing-page static server,
then opens the user's default browser.  Designed to run both in development
(``python launcher.py``) and when frozen by PyInstaller into a portable
one-folder app.
"""

import argparse
import atexit
import logging
import logging.handlers
import os
import platform
import shutil
import socket
import sys
import threading
import time
import urllib.request
import webbrowser

from waitress import create_server


def _safe_pause():
    """Prompt user to press Enter, but skip gracefully in non-interactive shells
    (e.g., when the EXE is launched by the build smoke test via ``start``)."""
    if sys.stdin and sys.stdin.readable():
        try:
            _safe_pause()
        except (EOFError, OSError):
            pass


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _base_path() -> str:
    """Root directory of the application.

    In one-folder frozen mode ``sys._MEIPASS`` points to the folder that
    contains the EXE and all unpacked data — no temp extraction involved.
    In development it is simply the directory containing this file.
    """
    if getattr(sys, "frozen", False):
        return sys._MEIPASS  # type: ignore[attr-defined]
    return os.path.dirname(os.path.abspath(__file__))


BASE = _base_path()


def _resolve(*parts: str) -> str:
    """Join *parts* onto BASE and return an absolute path."""
    return os.path.normpath(os.path.join(BASE, *parts))


# ---------------------------------------------------------------------------
# Logging — console + rotating file  (Step 2)
# ---------------------------------------------------------------------------

# Place logs next to the EXE (user-visible) rather than inside _internal.
if getattr(sys, "frozen", False):
    _APP_DIR = os.path.dirname(sys.executable)
else:
    _APP_DIR = BASE
_LOG_DIR = os.path.join(_APP_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_FILE = os.path.join(_LOG_DIR, "datascopingtool.log")

_log_formatter = logging.Formatter(
    "%(asctime)s [DataScopingTool] %(levelname)s: %(message)s"
)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_log_formatter)

_file_handler = logging.handlers.RotatingFileHandler(
    _LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
_file_handler.setFormatter(_log_formatter)

logging.basicConfig(level=logging.INFO, handlers=[_console_handler, _file_handler])
log = logging.getLogger("launcher")

# ---------------------------------------------------------------------------
# SSL / TLS certificate setup  (Step 11)
# ---------------------------------------------------------------------------

def _setup_ssl():
    """Configure the SSL certificate bundle for HTTPS requests.

    Priority order:
    1. User-provided ``ca-bundle.pem`` next to the EXE
    2. Pre-existing ``SSL_CERT_FILE`` env var (corporate IT may set this)
    3. Bundled certifi CA bundle
    """
    user_bundle = os.path.join(BASE, "ca-bundle.pem")
    bundled_cert = os.path.join(BASE, "certifi", "cacert.pem")

    if os.path.isfile(user_bundle):
        os.environ["SSL_CERT_FILE"] = user_bundle
        log.info("SSL: using user-provided ca-bundle.pem at %s", user_bundle)
    elif os.environ.get("SSL_CERT_FILE"):
        log.info("SSL: using pre-existing SSL_CERT_FILE=%s",
                 os.environ["SSL_CERT_FILE"])
    elif os.path.isfile(bundled_cert):
        os.environ["SSL_CERT_FILE"] = bundled_cert
        log.info("SSL: using bundled certifi at %s", bundled_cert)
    else:
        log.warning("SSL: no CA bundle found — HTTPS calls may fail")


if getattr(sys, "frozen", False):
    _setup_ssl()

# ---------------------------------------------------------------------------
# Resource validation  (Step 12)
# ---------------------------------------------------------------------------

_REQUIRED_RESOURCES = [
    ("landing-page", "dist", "index.html"),
    ("DataConsolidationAppV7", "frontend", "dist", "index.html"),
    ("ProcIP_Module2-main", "frontend", "dist", "index.html"),
    ("SummarizationModule", "frontend", "dist", "index.html"),
    ("DataConsolidationAppV7", "backend", "module-1", "app.py"),
    ("ProcIP_Module2-main", "backend", "app.py"),
    ("SummarizationModule", "backend", "app.py"),
]

_OPTIONAL_RESOURCES = [
    ("certifi", "cacert.pem"),
]


def _validate_resources() -> bool:
    """Check that all critical bundled resources exist. Returns True if OK."""
    all_ok = True
    for parts in _REQUIRED_RESOURCES:
        path = _resolve(*parts)
        if not os.path.isfile(path):
            log.error("Missing required resource: %s", path)
            all_ok = False
    for parts in _OPTIONAL_RESOURCES:
        path = _resolve(*parts)
        if not os.path.isfile(path):
            log.warning("Optional resource missing: %s", path)
    return all_ok

# ---------------------------------------------------------------------------
# Static-file serving helper  (Step 7 — 404 for missing assets)
# ---------------------------------------------------------------------------

_ASSET_EXTENSIONS = frozenset([
    ".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico",
    ".woff", ".woff2", ".ttf", ".eot", ".map", ".webp", ".avif",
])


def _add_static_serving(app, dist_folder: str, name_prefix: str):
    """Register routes on *app* that serve the Vite build in *dist_folder*.

    API routes (``/api/...``) pass through to the module's blueprints.
    Requests for files with known asset extensions return 404 if missing
    on disk (instead of falling back to ``index.html``).
    Non-file SPA routes fall back to ``index.html`` for client-side routing.
    """
    from flask import send_from_directory, abort

    endpoint_index = f"{name_prefix}_serve_index"
    endpoint_file = f"{name_prefix}_serve_static"

    @app.route("/", endpoint=endpoint_index)
    def _serve_index():
        return send_from_directory(dist_folder, "index.html")

    @app.route("/<path:path>", endpoint=endpoint_file)
    def _serve_static(path):
        if path.startswith("api/"):
            abort(404)
        full = os.path.join(dist_folder, path)
        if os.path.isfile(full):
            return send_from_directory(dist_folder, path)
        ext = os.path.splitext(path)[1].lower()
        if ext in _ASSET_EXTENSIONS:
            abort(404)
        return send_from_directory(dist_folder, "index.html")

# ---------------------------------------------------------------------------
# Port helpers  (Step 6 — dynamic port fallback)
# ---------------------------------------------------------------------------

def _port_available(port: int) -> bool:
    """Return True if *port* is free on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _pick_port(preferred: int, label: str, scan_range: int = 10) -> int:
    """Return *preferred* if free, otherwise try the next *scan_range* ports.

    Returns -1 if no port could be found.
    """
    for offset in range(scan_range + 1):
        candidate = preferred + offset
        if _port_available(candidate):
            if offset > 0:
                log.warning("%s: preferred port %d busy, using %d instead",
                            label, preferred, candidate)
            return candidate
    log.error("No free port found for %s (tried %d–%d)",
              label, preferred, preferred + scan_range)
    return -1


# Actual runtime ports — filled during startup, read by /config.json
_RUNTIME_PORTS: dict[str, int] = {}

# ---------------------------------------------------------------------------
# Runtime config endpoint  (Step 4)
# ---------------------------------------------------------------------------

def _add_config_endpoint(app):
    """Register ``GET /config.json`` returning the actual runtime URLs."""
    from flask import jsonify

    @app.route("/config.json")
    def _serve_config():
        return jsonify({
            "home": f"http://localhost:{_RUNTIME_PORTS.get('home', 3000)}",
            "stitcher": f"http://localhost:{_RUNTIME_PORTS.get('stitcher', 3001)}",
            "normalizer": f"http://localhost:{_RUNTIME_PORTS.get('normalizer', 5000)}",
            "summarizer": f"http://localhost:{_RUNTIME_PORTS.get('summarizer', 3005)}",
        })

# ---------------------------------------------------------------------------
# Import isolation  (Step 8 — hardened)
#
# All three module backends have local packages with identical top-level
# names (shared, routes, db, services, etc.).  The aliasing system below
# prevents them from colliding inside a single Python interpreter.
#
# MAINTENANCE: if you add a new top-level package directory inside any
# module backend, add its name to LOCAL_PACKAGE_NAMES below.
# ---------------------------------------------------------------------------

LOCAL_PACKAGE_NAMES = frozenset([
    "routes", "shared", "db", "agents", "services",
    "data_loading", "data_quality_assessment", "header-normalisation",
    "merging", "appending", "inventory", "summary", "ai-core", "ai_core",
])

_BACKEND_DIR_TO_ALIAS: dict[str, str] = {}


class _ModuleRedirectFinder:
    """``sys.meta_path`` finder that intercepts bare local-package imports
    and redirects them to the correct module-specific alias by inspecting
    the caller's file path.
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
            log.debug("Import redirect: %s → %s", fullname, aliased)
            return self
        return None

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
        alias = self._caller_alias()
        if alias is None:
            log.debug("Import redirect failed — no caller alias for %s", fullname)
            raise ImportError(fullname)
        aliased = f"{alias}.{fullname}"
        mod = sys.modules.get(aliased)
        if mod is None:
            raise ImportError(fullname)
        log.debug("Import redirect loaded: %s → %s", fullname, aliased)
        sys.modules[fullname] = mod
        return mod

    @staticmethod
    def _caller_alias() -> str | None:
        """Walk the call stack to determine which module backend the caller
        belongs to, then return its alias (e.g. ``_m1_app``)."""
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
        except Exception:
            log.debug("Stack walk failed during import redirect", exc_info=True)
        finally:
            del frame
        return None


_redirect_finder = _ModuleRedirectFinder()


def _import_module_app(backend_dir: str, module_alias: str):
    """Import a module's ``app.py`` in isolation.

    Each module has its own ``app.py`` plus local packages that share the
    same top-level names.  To prevent cross-contamination we:
      1. Remove any previously-loaded local packages from sys.modules.
      2. Prepend the backend dir to sys.path.
      3. Load the module's app.py under a unique alias.
      4. Rename all local packages in sys.modules to ``<alias>.<pkg>``.
      5. Register the backend dir for the runtime import-redirect hook.
    """
    import importlib, importlib.util

    to_delete = [
        name for name in sys.modules
        if name.split(".")[0] in LOCAL_PACKAGE_NAMES
    ]
    for name in to_delete:
        del sys.modules[name]

    if backend_dir in sys.path:
        sys.path.remove(backend_dir)
    sys.path.insert(0, backend_dir)

    app_file = os.path.join(backend_dir, "app.py")
    spec = importlib.util.spec_from_file_location(module_alias, app_file)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_alias] = mod
    spec.loader.exec_module(mod)

    renames: dict[str, object] = {}
    for name in list(sys.modules):
        top = name.split(".")[0]
        if top in LOCAL_PACKAGE_NAMES:
            renames[f"{module_alias}.{name}"] = sys.modules.pop(name)
    sys.modules.update(renames)

    norm_dir = os.path.normcase(os.path.normpath(backend_dir)) + os.sep
    _BACKEND_DIR_TO_ALIAS[norm_dir] = module_alias

    return mod


def _post_load_cleanup():
    """Seal the import environment after all modules are loaded.

    Evicts remaining bare local package names, removes backend dirs from
    sys.path, installs the redirect finder, and validates that no stale
    bare names leaked through.
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

    leaked = [
        name for name in sys.modules
        if name.split(".")[0] in LOCAL_PACKAGE_NAMES
    ]
    if leaked:
        log.warning("Import isolation: bare package names still in sys.modules "
                     "after cleanup: %s", leaked)

# ---------------------------------------------------------------------------
# Module factories
# ---------------------------------------------------------------------------

def _create_module1_app():
    """Import Module 1's Flask app and bolt on static-file serving."""
    m1_backend = _resolve("DataConsolidationAppV7", "backend", "module-1")
    os.environ["NODE_PORT"] = str(_RUNTIME_PORTS.get("stitcher", 3001))

    mod = _import_module_app(m1_backend, "_m1_app")
    flask_app = mod.app

    dist = _resolve("DataConsolidationAppV7", "frontend", "dist")
    _add_static_serving(flask_app, dist, "m1")
    return flask_app


def _create_module2_app():
    """Import Module 2's Flask app and bolt on static-file serving."""
    m2_backend = _resolve("ProcIP_Module2-main", "backend")
    os.environ["FLASK_PORT"] = str(_RUNTIME_PORTS.get("normalizer", 5000))

    mod = _import_module_app(m2_backend, "_m2_app")
    flask_app = mod.app

    dist = _resolve("ProcIP_Module2-main", "frontend", "dist")
    _add_static_serving(flask_app, dist, "m2")
    return flask_app


def _create_module3_app():
    """Import Module 3's Flask app and bolt on static-file serving."""
    m3_backend = _resolve("SummarizationModule", "backend")
    os.environ["PORT"] = str(_RUNTIME_PORTS.get("summarizer", 3005))

    mod = _import_module_app(m3_backend, "_m3_app")
    flask_app = mod.app

    dist = _resolve("SummarizationModule", "frontend", "dist")
    _add_static_serving(flask_app, dist, "m3")
    return flask_app


def _create_landing_app():
    """Lightweight Flask app that serves the landing-page build."""
    from flask import Flask, send_from_directory, abort

    landing_app = Flask(__name__, static_folder=None)
    dist = _resolve("landing-page", "dist")

    @landing_app.route("/", defaults={"path": ""})
    @landing_app.route("/<path:path>")
    def serve(path):
        if path.startswith("api/"):
            abort(404)
        full = os.path.join(dist, path)
        if path and os.path.isfile(full):
            return send_from_directory(dist, path)
        ext = os.path.splitext(path)[1].lower()
        if ext in _ASSET_EXTENSIONS:
            abort(404)
        return send_from_directory(dist, "index.html")

    return landing_app

# ---------------------------------------------------------------------------
# Server runner
# ---------------------------------------------------------------------------

_servers: list = []
_server_errors: list = []
_server_threads: list[threading.Thread] = []


def _run_server(app, port: int, label: str):
    """Start a waitress server for *app* on *port* (blocking call).

    Binds to 127.0.0.1 only so Windows Firewall never prompts the user.
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
    (_create_landing_app,  3000, "Landing Page",              "home"),
    (_create_module1_app,  3001, "Module 1 — Data Stitcher",  "stitcher"),
    (_create_module2_app,  5000, "Module 2 — Data Normalizer", "normalizer"),
    (_create_module3_app,  3005, "Module 3 — Spend Summarizer", "summarizer"),
]

_HEALTH_PATHS: dict[int, str] = {}

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def _wait_for_health(ports: list[int], timeout: int = 60) -> set[int]:
    """Block until every service returns HTTP 200 on its health endpoint.

    Returns the set of ports that did NOT pass within *timeout* seconds.
    """
    deadline = time.time() + timeout
    pending = set(ports)
    while pending and time.time() < deadline:
        for port in list(pending):
            path = _HEALTH_PATHS.get(port, "/")
            url = f"http://127.0.0.1:{port}{path}"
            try:
                with urllib.request.urlopen(url, timeout=2) as resp:
                    if resp.status == 200:
                        log.info("  Health OK: port %d (%s)", port, path)
                        pending.discard(port)
            except Exception:
                pass
        if pending:
            time.sleep(0.5)
    return pending

# ---------------------------------------------------------------------------
# Shutdown / cleanup  (Step 10)
# ---------------------------------------------------------------------------

_shutdown_done = False
_shutdown_lock = threading.Lock()


def _cleanup():
    """Shared cleanup logic — safe to call multiple times."""
    global _shutdown_done
    with _shutdown_lock:
        if _shutdown_done:
            return
        _shutdown_done = True

    log.info("Running cleanup...")

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
                log.warning("  %s session cleanup failed.", label, exc_info=True)

    for backend_dir in [
        _resolve("DataConsolidationAppV7", "backend"),
        _resolve("ProcIP_Module2-main", "backend"),
        _resolve("SummarizationModule", "backend"),
    ]:
        for root, dirs, _ in os.walk(backend_dir):
            for d in dirs:
                if d == "__pycache__":
                    target = os.path.join(root, d)
                    for attempt in range(3):
                        try:
                            shutil.rmtree(target)
                            break
                        except OSError:
                            if attempt < 2:
                                time.sleep(0.5)

    log.info("Cleanup complete.")


atexit.register(_cleanup)


def _cleanup_stale_sessions():
    """Remove session directories from previous runs older than 24 hours."""
    cutoff = time.time() - 86400
    for backend_dir in [
        _resolve("DataConsolidationAppV7", "backend"),
        _resolve("ProcIP_Module2-main", "backend"),
        _resolve("SummarizationModule", "backend"),
    ]:
        for session_dir_name in (".sessions", "sessions"):
            session_root = os.path.join(backend_dir, session_dir_name)
            if not os.path.isdir(session_root):
                continue
            for entry in os.listdir(session_root):
                entry_path = os.path.join(session_root, entry)
                if not os.path.isdir(entry_path):
                    continue
                try:
                    mtime = os.path.getmtime(entry_path)
                    if mtime < cutoff:
                        shutil.rmtree(entry_path, ignore_errors=True)
                        log.info("Cleaned stale session dir: %s", entry_path)
                except OSError:
                    pass

# ---------------------------------------------------------------------------
# Diagnostics mode  (Step 3)
# ---------------------------------------------------------------------------

def _run_diagnostics():
    """Run non-destructive health checks and print results."""
    print("\n  DataScopingTool Diagnostics\n  " + "=" * 35)
    all_ok = True

    print(f"\n  Python:       {sys.version}")
    print(f"  Frozen:       {getattr(sys, 'frozen', False)}")
    print(f"  Base path:    {BASE}")
    print(f"  Platform:     {platform.platform()}")
    print(f"  Architecture: {platform.machine()}")
    print(f"  Log file:     {_LOG_FILE}")

    writable = os.access(BASE, os.W_OK)
    print(f"\n  Base path writable: {'YES' if writable else 'NO'}")
    if not writable:
        all_ok = False

    print("\n  Port availability:")
    for _, preferred, label, _ in SERVICES:
        avail = _port_available(preferred)
        status = "FREE" if avail else "IN USE"
        print(f"    {preferred:>5d}  {status:>6s}  ({label})")
        if not avail:
            all_ok = False

    print("\n  SSL certificate:")
    cert = os.environ.get("SSL_CERT_FILE", "")
    if cert and os.path.isfile(cert):
        print(f"    OK: {cert}")
    elif cert:
        print(f"    MISSING: {cert}")
        all_ok = False
    else:
        print("    NOT SET")

    print("\n  Required resources:")
    for parts in _REQUIRED_RESOURCES:
        path = _resolve(*parts)
        exists = os.path.isfile(path)
        status = "OK" if exists else "MISSING"
        print(f"    {status:>7s}  {os.path.join(*parts)}")
        if not exists:
            all_ok = False

    print("\n  DuckDB:")
    try:
        import duckdb
        conn = duckdb.connect(":memory:")
        result = conn.execute("SELECT 1 AS test").fetchone()
        conn.close()
        print(f"    OK: version {duckdb.__version__}, query returned {result}")
    except Exception as exc:
        print(f"    FAIL: {exc}")
        all_ok = False

    print("\n  Disk space:")
    try:
        usage = shutil.disk_usage(BASE)
        free_gb = usage.free / (1024 ** 3)
        print(f"    {free_gb:.1f} GB free on {os.path.splitdrive(BASE)[0] or BASE}")
        if free_gb < 1.0:
            print("    WARNING: less than 1 GB free")
    except Exception as exc:
        print(f"    Could not determine: {exc}")

    print(f"\n  {'ALL CHECKS PASSED' if all_ok else 'SOME CHECKS FAILED'}")
    log.info("Diagnostics completed. Result: %s",
             "PASS" if all_ok else "FAIL")
    return 0 if all_ok else 1

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="DataScopingTool — launch all services")
    parser.add_argument(
        "--diagnostics", action="store_true",
        help="Run health checks and exit without launching the app")
    args = parser.parse_args()

    log.info("Starting DataScopingTool — Python %s, frozen=%s, base=%s",
             sys.version, getattr(sys, "frozen", False), BASE)
    log.info("Log file: %s", _LOG_FILE)

    if args.diagnostics:
        sys.exit(_run_diagnostics())

    print(r"""
    ╔══════════════════════════════════════════╗
    ║        DataScopingTool  v1.0             ║
    ║  Starting all services — please wait…    ║
    ╚══════════════════════════════════════════╝
    """)
    print(f"  Log file: {_LOG_FILE}\n")

    # --- Validate bundled resources ---
    t0 = time.perf_counter()
    if not _validate_resources():
        print("\n  Critical resources are missing. Cannot start.")
        print(f"  Check the log at {_LOG_FILE} for details.")
        _safe_pause()
        sys.exit(1)
    log.info("Resource validation passed (%.1f s)", time.perf_counter() - t0)

    # --- Clean up stale sessions from previous runs ---
    _cleanup_stale_sessions()

    # --- Select ports (dynamic fallback) ---
    t0 = time.perf_counter()
    total = len(SERVICES)
    for factory, preferred, label, key in SERVICES:
        chosen = _pick_port(preferred, label)
        if chosen == -1:
            log.error("Cannot find a free port for %s. "
                      "Close other programs and try again.", label)
            _safe_pause()
            sys.exit(1)
        _RUNTIME_PORTS[key] = chosen
    log.info("Ports selected: %s (%.1f s)",
             _RUNTIME_PORTS, time.perf_counter() - t0)

    # Build the health-check path map from the actual chosen ports
    for factory, preferred, label, key in SERVICES:
        port = _RUNTIME_PORTS[key]
        _HEALTH_PATHS[port] = "/" if key == "home" else "/api/health"

    # --- Create all apps sequentially (import isolation requires this) ---
    apps: list[tuple] = []
    for idx, (factory, preferred, label, key) in enumerate(SERVICES, 1):
        port = _RUNTIME_PORTS[key]
        print(f"  [{idx}/{total}] Loading {label} ...")
        t0 = time.perf_counter()
        try:
            app = factory()
            _add_config_endpoint(app)
        except Exception:
            log.exception("Failed to initialise %s", label)
            _safe_pause()
            sys.exit(1)
        elapsed = time.perf_counter() - t0
        log.info("  %s loaded in %.1f s", label, elapsed)
        apps.append((app, port, label))

    # Seal the import environment
    _post_load_cleanup()

    # --- Start each service in a daemon thread ---
    print(f"\n  Starting {total} servers ...")
    t0 = time.perf_counter()
    for app, port, label in apps:
        t = threading.Thread(target=_run_server, args=(app, port, label),
                             daemon=True, name=f"srv-{port}")
        t.start()
        _server_threads.append(t)

    # --- Health-check all services ---
    all_ports = [port for _, port, _ in apps]
    port_to_label = {port: label for _, port, label in apps}

    stalled = _wait_for_health(all_ports, timeout=60)

    if _server_errors:
        for label, port in _server_errors:
            log.error("  %s (port %d) failed to start.", label, port)
        print("\n  Some services failed to start. Check the log above.")
        _safe_pause()
        sys.exit(1)

    if stalled:
        for port in stalled:
            log.warning("  %s (port %d) slow — retrying (60 s more) ...",
                        port_to_label.get(port, "Unknown"), port)
        stalled = _wait_for_health(list(stalled), timeout=60)

    if stalled:
        for port in stalled:
            log.error("  %s (port %d) never became healthy.",
                      port_to_label.get(port, "Unknown"), port)
        print("\n  Some services failed to become healthy. The app cannot start.")
        _safe_pause()
        sys.exit(1)

    health_time = time.perf_counter() - t0
    log.info("All services ready (%.1f s)", health_time)
    print("  All services ready.\n")

    # --- Open browser ---
    home_port = _RUNTIME_PORTS.get("home", 3000)
    url = f"http://localhost:{home_port}"
    log.info("Opening %s in your browser…", url)
    try:
        webbrowser.open(url)
    except Exception:
        log.warning("Could not open browser automatically. "
                     "Navigate to %s manually.", url)

    print(f"  App running at {url}")
    print("  Close this window (or press Ctrl+C) to stop.\n")

    # --- Keep-alive loop with thread health monitoring ---
    try:
        health_check_interval = 30
        last_health_check = time.time()
        while True:
            time.sleep(1)
            now = time.time()
            if now - last_health_check >= health_check_interval:
                last_health_check = now
                for t in _server_threads:
                    if not t.is_alive():
                        log.warning("Server thread '%s' is no longer alive",
                                    t.name)
    except KeyboardInterrupt:
        log.info("Shutting down…")
        _cleanup()


if __name__ == "__main__":
    main()
