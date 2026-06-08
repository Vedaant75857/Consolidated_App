import io
import os
import sys
import zipfile


BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

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
