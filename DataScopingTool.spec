# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for DataScopingTool — single-file EXE."""

import os
from PyInstaller.utils.hooks import collect_dynamic_libs, collect_data_files

ROOT = os.path.abspath(".")

# ── Data files to bundle ──────────────────────────────────────────────
# (source_path, dest_path_inside_bundle)
# Pre-built frontend dist folders
datas = [
    (os.path.join(ROOT, "landing-page", "dist"),
     os.path.join("landing-page", "dist")),

    (os.path.join(ROOT, "DataConsolidationAppV7", "frontend", "dist"),
     os.path.join("DataConsolidationAppV7", "frontend", "dist")),

    (os.path.join(ROOT, "ProcIP_Module2-main", "frontend", "dist"),
     os.path.join("ProcIP_Module2-main", "frontend", "dist")),

    (os.path.join(ROOT, "SummarizationModule", "frontend", "dist"),
     os.path.join("SummarizationModule", "frontend", "dist")),

]

# FX reference workbook used by Module 2 currency conversion (optional)
_fx_path = os.path.join(ROOT, "FX_rates_table.xlsx")
if os.path.isfile(_fx_path):
    datas.append((_fx_path, "."))

# Collect DuckDB native libraries so the C-extension works inside the bundle
_duckdb_bins = collect_dynamic_libs("duckdb")
_duckdb_data = collect_data_files("duckdb")
datas.extend(_duckdb_data)

# Backend Python source trees (PyInstaller needs them as data because we
# load them dynamically via importlib at runtime).
backend_trees = [
    ("DataConsolidationAppV7/backend/module-1", "DataConsolidationAppV7/backend/module-1"),
    ("ProcIP_Module2-main/backend",             "ProcIP_Module2-main/backend"),
    ("SummarizationModule/backend",             "SummarizationModule/backend"),
]

_SKIP_DIRS = {"__pycache__", ".venv", ".sessions", "sessions"}
_SKIP_FILE_PREFIXES = (".env",)
_SKIP_FILE_EXTENSIONS = {".sqlite", ".sqlite3", ".sqlite-journal",
                         ".duckdb", ".wal",
                         ".log", ".pkl", ".pickle"}

for src_rel, dst_rel in backend_trees:
    src_abs = os.path.join(ROOT, src_rel)
    for dirpath, _dirs, files in os.walk(src_abs):
        if any(part in _SKIP_DIRS for part in dirpath.replace("\\", "/").split("/")):
            continue
        for f in files:
            if any(f.startswith(p) for p in _SKIP_FILE_PREFIXES):
                continue
            if os.path.splitext(f)[1].lower() in _SKIP_FILE_EXTENSIONS:
                continue
            full = os.path.join(dirpath, f)
            rel = os.path.relpath(dirpath, ROOT)
            datas.append((full, rel))

# ── Hidden imports ────────────────────────────────────────────────────
# Packages that PyInstaller's static analysis often misses.
hiddenimports = [
    "waitress",
    "flask",
    "flask_cors",
    "flask.json",
    "flask.json.provider",
    "dotenv",
    "pandas",
    "numpy",
    "openpyxl",
    "xlsxwriter",
    "requests",
    "pydantic",
    "portkey_ai",
    "openai",
    "httpx",
    "langdetect",
    "rapidfuzz",
    "rapidfuzz.fuzz",
    "reportlab",
    "reportlab.lib",
    "reportlab.lib.pagesizes",
    "reportlab.platypus",
    "reportlab.pdfgen",
    "PIL",
    "PIL.Image",
    "python_calamine",
    "engineio",
    "engineio.async_drivers",
    "duckdb",
    "json",
    "csv",
    "logging",
    "email",
    "email.mime",
    "email.mime.text",
    "email.mime.multipart",
]

# ── Analysis ──────────────────────────────────────────────────────────

a = Analysis(
    ["launcher.py"],
    pathex=[
        ROOT,
        os.path.join(ROOT, "DataConsolidationAppV7", "backend", "module-1"),
        os.path.join(ROOT, "ProcIP_Module2-main", "backend"),
        os.path.join(ROOT, "SummarizationModule", "backend"),
    ],
    binaries=_duckdb_bins,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["tkinter", "matplotlib", "scipy", "IPython", "notebook"],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="DataScopingTool",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,           # keep console visible so user sees status
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
