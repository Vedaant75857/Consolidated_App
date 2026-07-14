import os
import shutil
import sys
import tempfile
import unittest

from flask import Flask

BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from routes.preview_routes import preview_bp
from shared import db


class PlaygroundPreviewRouteTests(unittest.TestCase):
    def setUp(self):
        self.original_sessions_dir = db.SESSIONS_DIR
        self.tmpdir = tempfile.mkdtemp(prefix="summarizer-preview-")
        db.SESSIONS_DIR = self.tmpdir
        self.session_id = "preview-session"

        with db.get_session_lock(self.session_id):
            conn = db.get_session_db(self.session_id)
            conn.execute(
                "CREATE TABLE _table_registry "
                "(table_key VARCHAR PRIMARY KEY, data_table VARCHAR, raw_table VARCHAR)"
            )
            conn.execute(
                "INSERT INTO _table_registry VALUES (?, ?, ?)",
                ("sample.csv::", "data__sample", "raw__sample"),
            )
            conn.execute(
                'CREATE TABLE "data__sample" '
                '("RECORD_ID" VARCHAR, "SUPPLIER" VARCHAR, "AMOUNT" VARCHAR)'
            )
            conn.execute('CREATE TABLE "raw__sample" ("RAW_0" VARCHAR, "RAW_1" VARCHAR)')
            conn.execute(
                'INSERT INTO "raw__sample" VALUES (?, ?)',
                ("Supplier Name", "Paid Amount"),
            )
            conn.executemany(
                'INSERT INTO "data__sample" VALUES (?, ?, ?)',
                [
                    ("1", "Acme", "20"),
                    ("2", "Bravo", "10"),
                    ("3", "Acme Supplies", "30"),
                    ("4", "Zero Co", "0"),
                ],
            )
            conn.commit()

    def tearDown(self):
        try:
            db.delete_session(self.session_id)
        finally:
            db.SESSIONS_DIR = self.original_sessions_dir
            shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _app(self):
        app = Flask(__name__)
        app.register_blueprint(preview_bp, url_prefix="/api")
        return app

    def test_preview_state_uses_table_key_and_hides_record_id(self):
        with self._app().test_client() as client:
            response = client.post("/api/preview/state", json={
                "sessionId": self.session_id,
                "tableKey": "sample.csv::",
                "offset": 0,
                "limit": 2,
            })

        self.assertEqual(200, response.status_code)
        body = response.get_json()
        self.assertEqual("sample.csv::", body["tableKey"])
        self.assertEqual(["COL_0", "COL_1"], [col["key"] for col in body["columns"]])
        self.assertEqual(["SUPPLIER", "AMOUNT"], [col["displayName"] for col in body["columns"]])
        self.assertNotIn("columnTypes", body)
        self.assertEqual(4, body["totalRows"])
        self.assertEqual(2, len(body["rows"]))
        self.assertEqual("1", body["rows"][0]["__row_id"])
        self.assertNotIn("RECORD_ID", body["rows"][0]["values"])
        self.assertEqual(0, body["undoDepth"])
        self.assertEqual(0, body["redoDepth"])
        self.assertFalse(body["dirty"])

    def test_preview_state_filters_searches_and_sorts_by_column_key(self):
        with self._app().test_client() as client:
            response = client.post("/api/preview/state", json={
                "sessionId": self.session_id,
                "tableKey": "sample.csv::",
                "search": "acme",
                "filters": [{"columnKey": "COL_0", "op": "contains", "value": "acme"}],
                "sort": [{"columnKey": "COL_1", "dir": "desc"}],
            })

        self.assertEqual(200, response.status_code)
        body = response.get_json()
        self.assertEqual(2, body["totalRows"])
        self.assertEqual(["30", "20"], [row["values"]["COL_1"] for row in body["rows"]])

    def test_preview_state_accepts_approved_filter_contract_ops(self):
        with self._app().test_client() as client:
            response = client.post("/api/preview/state", json={
                "sessionId": self.session_id,
                "tableKey": "sample.csv::",
                "filters": [
                    {"columnKey": "COL_0", "op": "startswith", "value": "acme"},
                    {"columnKey": "COL_1", "op": "in", "values": ["20", "30"]},
                ],
                "sort": [{"columnKey": "COL_1", "dir": "asc"}],
            })

        self.assertEqual(200, response.status_code)
        body = response.get_json()
        self.assertEqual(["20", "30"], [row["values"]["COL_1"] for row in body["rows"]])
        self.assertEqual(["TEXT", "TEXT"], [col["dataType"] for col in body["columns"]])

    def test_preview_state_preserves_falsy_filter_values(self):
        with self._app().test_client() as client:
            response = client.post("/api/preview/state", json={
                "sessionId": self.session_id,
                "tableKey": "sample.csv::",
                "filters": [{"columnKey": "COL_1", "op": "eq", "value": 0}],
            })

        self.assertEqual(200, response.status_code)
        body = response.get_json()
        self.assertEqual(1, body["totalRows"])
        self.assertEqual(["0"], [row["values"]["COL_1"] for row in body["rows"]])

    def test_preview_state_rejects_display_name_only_filter_refs(self):
        with self._app().test_client() as client:
            response = client.post("/api/preview/state", json={
                "sessionId": self.session_id,
                "tableKey": "sample.csv::",
                "filters": [{"displayName": "Supplier Name", "op": "eq", "value": "Acme"}],
            })

        self.assertEqual(400, response.status_code)
        self.assertEqual("VALIDATION_ERROR", response.get_json()["code"])

    def test_preview_state_rejects_physical_column_name_refs(self):
        with self._app().test_client() as client:
            response = client.post("/api/preview/state", json={
                "sessionId": self.session_id,
                "tableKey": "sample.csv::",
                "filters": [{"columnKey": "SUPPLIER", "op": "eq", "value": "Acme"}],
            })

        self.assertEqual(400, response.status_code)
        self.assertEqual("VALIDATION_ERROR", response.get_json()["code"])

    def test_preview_state_normalizes_supported_filter_aliases_before_service(self):
        with self._app().test_client() as client:
            response = client.post("/api/preview/state", json={
                "sessionId": self.session_id,
                "tableKey": "sample.csv::",
                "filters": [{"columnKey": "COL_0", "op": "starts_with", "value": "Acme"}],
                "sort": [{"columnKey": "COL_1", "dir": "DESC"}],
            })

        self.assertEqual(200, response.status_code)
        body = response.get_json()
        self.assertEqual(["30", "20"], [row["values"]["COL_1"] for row in body["rows"]])

    def test_preview_state_rejects_unknown_table_key(self):
        with self._app().test_client() as client:
            response = client.post("/api/preview/state", json={
                "sessionId": self.session_id,
                "tableKey": "data__sample",
            })

        self.assertEqual(400, response.status_code)
        self.assertEqual("VALIDATION_ERROR", response.get_json()["code"])

    def test_preview_state_rechecks_session_inside_route_before_opening_db(self):
        db.delete_session(self.session_id)

        with self._app().test_client() as client:
            response = client.post("/api/preview/state", json={
                "sessionId": self.session_id,
                "tableKey": "sample.csv::",
            })

        self.assertEqual(404, response.status_code)
        self.assertEqual("SESSION_NOT_FOUND", response.get_json()["code"])
        self.assertFalse(db.session_exists(self.session_id))

    def test_all_preview_routes_recheck_deleted_session_inside_route(self):
        db.delete_session(self.session_id)
        cases = [
            ("/api/preview/state", {"tableKey": "sample.csv::"}),
            ("/api/preview/column-values", {"tableKey": "sample.csv::", "columnKey": "COL_0"}),
            (
                "/api/preview/operation",
                {
                    "tableKey": "sample.csv::",
                    "op": "cell_edit",
                    "params": {"rowId": "1", "columnKey": "COL_0", "value": "X"},
                },
            ),
            ("/api/preview/undo", {"tableKey": "sample.csv::"}),
            ("/api/preview/redo", {"tableKey": "sample.csv::"}),
            ("/api/preview/refresh-inventory", {}),
        ]

        with self._app().test_client() as client:
            for path, payload in cases:
                with self.subTest(path=path):
                    response = client.post(path, json={"sessionId": self.session_id, **payload})
                    self.assertEqual(404, response.status_code)
                    self.assertEqual("SESSION_NOT_FOUND", response.get_json()["code"])
                    self.assertFalse(db.session_exists(self.session_id))

    def test_preview_state_validates_pagination_bounds(self):
        with self._app().test_client() as client:
            response = client.post("/api/preview/state", json={
                "sessionId": self.session_id,
                "tableKey": "sample.csv::",
                "limit": 1001,
            })

        self.assertEqual(400, response.status_code)
        self.assertEqual("VALIDATION_ERROR", response.get_json()["code"])

    def test_preview_state_rejects_fractional_pagination_values(self):
        with self._app().test_client() as client:
            response = client.post("/api/preview/state", json={
                "sessionId": self.session_id,
                "tableKey": "sample.csv::",
                "offset": 1.5,
            })

        self.assertEqual(400, response.status_code)
        self.assertEqual("VALIDATION_ERROR", response.get_json()["code"])


if __name__ == "__main__":
    unittest.main()
