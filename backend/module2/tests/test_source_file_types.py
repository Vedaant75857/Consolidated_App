import ast
from pathlib import Path


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def _constant_tuple(name: str) -> tuple[str, ...]:
    module = ast.parse(APP_PATH.read_text(encoding="utf-8"))
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    value = ast.literal_eval(node.value)
                    return tuple(value)
    raise AssertionError(f"{name} not found")


def test_source_excel_extensions_include_legacy_and_binary_formats():
    exts = _constant_tuple("SOURCE_EXCEL_EXTS")
    assert ".xls" in exts
    assert ".xlsb" in exts
    assert ".xltm" in exts
