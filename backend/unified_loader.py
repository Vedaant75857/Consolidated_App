"""Import the independently-owned backend apps without sharing local modules.

The three backends predate the unified host and use names such as ``shared`` and
``routes`` as top-level imports.  Loading each app in a short-lived import
namespace keeps those names from being reused by the next app while leaving the
module source trees unchanged.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Iterable


_MODULE_PREFIXES = {
    "module1": (
        "appending", "data_loading", "data_quality_assessment", "db",
        "inventory", "merging", "preview_operations", "routes", "shared",
        "summary",
    ),
    "module2": ("agents", "db"),
    "module3": ("routes", "services", "shared"),
}

# Some legacy modules retain lazy imports.  Preserve only roots that are unique
# to the owning module after construction, so a future lazy import cannot bind
# to another module's similarly named package (notably ``shared``).
_RUNTIME_ALIASES: dict[str, dict[str, object]] = {}
_PERSISTENT_ALIAS_ROOTS = {
    "module1": ("shared",),
    "module3": ("services",),
}


def _is_owned(name: str, prefixes: Iterable[str]) -> bool:
    return name == "app" or any(name == prefix or name.startswith(f"{prefix}.") for prefix in prefixes)


def load_module_app(module_name: str):
    """Load one legacy app, then move its local imports out of global names."""
    try:
        prefixes = _MODULE_PREFIXES[module_name]
    except KeyError as exc:
        raise ValueError(f"Unknown backend module: {module_name}") from exc

    module_root = Path(__file__).with_name(module_name)
    if not module_root.is_dir():
        raise RuntimeError(f"Backend folder is missing: {module_root}")

    # The applications currently use top-level imports.  The aliases exist only
    # while their module graph is built; Flask endpoint functions retain their
    # imported collaborators after this function returns.
    global _RUNTIME_ALIASES

    previous_path = list(sys.path)
    stale = [name for name in tuple(sys.modules) if _is_owned(name, prefixes)]
    for name in stale:
        sys.modules.pop(name, None)
    sys.path.insert(0, str(module_root))
    try:
        app_module = importlib.import_module("app")
        # Existing modules create their standalone ``app`` at import time.  Use
        # that object so the host does not initialise routes and cleanup hooks a
        # second time.  Factories remain available for modules that add them
        # before defining a module-level app.
        app = getattr(app_module, "app", None)
        if app is None:
            app_factory = getattr(app_module, "create_app", None)
            if not callable(app_factory):
                raise RuntimeError(f"{module_name} does not expose a Flask app or create_app()")
            app = app_factory()
    finally:
        sys.path[:] = previous_path

    owned = [name for name in tuple(sys.modules) if _is_owned(name, prefixes)]
    persistent_roots = _PERSISTENT_ALIAS_ROOTS.get(module_name, ())
    if persistent_roots:
        _RUNTIME_ALIASES[module_name] = {
            name: sys.modules[name]
            for name in owned
            if any(name == root or name.startswith(f"{root}.") for root in persistent_roots)
        }
    runtime_prefix = f"_unified_{module_name}"
    for name in sorted(owned, key=len, reverse=True):
        loaded = sys.modules.pop(name)
        sys.modules[f"{runtime_prefix}.{name}"] = loaded
    for aliases in _RUNTIME_ALIASES.values():
        sys.modules.update(aliases)
    return app
