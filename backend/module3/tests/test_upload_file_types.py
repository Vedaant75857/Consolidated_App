import io
import os
import sys
import zipfile

import pytest
from flask import Flask


BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from routes import upload_routes
from services.upload import file_loader


class FakeConn:
    def execute(self, *args, **kwargs):
        return self

    def commit(self):
        return None


def test_excel_extensions_include_legacy_and_binary_formats():
    assert ".xls" in file_loader._EXCEL_EXTS
    assert ".xlsb" in file_loader._EXCEL_EXTS
    assert ".xltm" in file_loader._EXCEL_EXTS


def test_single_file_dispatches_xls_and_xlsb_to_excel_parser(monkeypatch):
    calls: list[str] = []

    def fake_parse_excel(data: bytes, filename: str):
        calls.append(filename)
        return {}

    monkeypatch.setattr(file_loader, "_parse_excel_bytes", fake_parse_excel)

    file_loader.load_single_file(FakeConn(), "legacy.xls", b"not-real-excel")
    file_loader.load_single_file(FakeConn(), "binary.xlsb", b"not-real-excel")

    assert calls == ["legacy.xls", "binary.xlsb"]


def test_zip_dispatches_nested_xlsb_to_excel_parser(monkeypatch):
    calls: list[str] = []

    def fake_parse_excel(data: bytes, filename: str):
        calls.append(filename)
        return {}

    monkeypatch.setattr(file_loader, "_parse_excel_bytes", fake_parse_excel)

    nested = io.BytesIO()
    with zipfile.ZipFile(nested, "w") as zf:
        zf.writestr("binary.xlsb", b"not-real-excel")

    outer = io.BytesIO()
    with zipfile.ZipFile(outer, "w") as zf:
        zf.writestr("nested.zip", nested.getvalue())

    file_loader.load_zip_to_session(FakeConn(), outer.getvalue())

    assert calls == ["binary.xlsb"]


def test_upload_route_does_not_treat_xlsx_zip_container_as_zip_archive(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(upload_routes, "get_session_db", lambda _session_id: FakeConn())
    monkeypatch.setattr(upload_routes, "set_meta", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(upload_routes, "collect_column_info", lambda _conn, _keys: [])
    monkeypatch.setattr(
        upload_routes,
        "build_inventory",
        lambda _conn: [{"table_key": "spend.xlsx::Sheet1", "rows": 1, "cols": 1}],
    )

    def fake_load_single_file(_conn, filename: str, _file_data: bytes):
        calls.append(filename)
        return ["spend.xlsx::Sheet1"], []

    def fake_load_zip_to_session(_conn, _file_data: bytes):
        pytest.fail("xlsx uploads must not be routed through ZIP extraction")

    monkeypatch.setattr(upload_routes, "load_single_file", fake_load_single_file)
    monkeypatch.setattr(upload_routes, "load_zip_to_session", fake_load_zip_to_session)

    xlsx_like_zip = io.BytesIO()
    with zipfile.ZipFile(xlsx_like_zip, "w") as zf:
        zf.writestr("[Content_Types].xml", "<Types />")

    app = Flask(__name__)
    app.register_blueprint(upload_routes.upload_bp, url_prefix="/api")
    with app.test_client() as client:
        response = client.post(
            "/api/upload",
            data={"file": (io.BytesIO(xlsx_like_zip.getvalue()), "spend.xlsx")},
            content_type="multipart/form-data",
        )

    assert response.status_code == 200
    assert calls == ["spend.xlsx"]
