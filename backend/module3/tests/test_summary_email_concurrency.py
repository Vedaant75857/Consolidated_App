import os
import shutil
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch

from flask import Flask

BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from routes.email_routes import email_bp
from routes.views_routes import views_bp
from shared import db


class SummaryEmailConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.original_sessions_dir = db.SESSIONS_DIR
        self.tmpdir = tempfile.mkdtemp(prefix="summarizer-concurrency-")
        db.SESSIONS_DIR = self.tmpdir
        self.session_id = "concurrent-session"

        with db.get_session_lock(self.session_id):
            conn = db.get_session_db(self.session_id)
            db.set_meta(conn, "view_results", [
                {"viewId": f"view-{i}", "title": f"View {i}", "rows": [{"value": i}]}
                for i in range(6)
            ])

    def tearDown(self):
        try:
            db.delete_session(self.session_id)
        finally:
            db.SESSIONS_DIR = self.original_sessions_dir
            shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _app(self):
        app = Flask(__name__)
        app.register_blueprint(views_bp, url_prefix="/api")
        app.register_blueprint(email_bp, url_prefix="/api")
        return app

    def test_concurrent_summary_and_email_requests_share_session_safely(self):
        app = self._app()

        def fake_summary(view, api_key):
            time.sleep(0.001)
            return f"summary for {view['viewId']}"

        def fake_email(view_results, context, api_key):
            time.sleep(0.001)
            return {
                "email": f"email from {len(view_results)} views",
                "subject": "Spend summary",
            }

        def post_summary(view_id):
            with app.test_client() as client:
                response = client.post("/api/generate-summary", json={
                    "sessionId": self.session_id,
                    "viewId": view_id,
                    "apiKey": "test-key",
                })
            return response.status_code, response.get_json()

        def post_email():
            with app.test_client() as client:
                response = client.post("/api/generate-email", json={
                    "sessionId": self.session_id,
                    "apiKey": "test-key",
                    "context": {"recipient": "CFO"},
                })
            return response.status_code, response.get_json()

        with patch("routes.views_routes.generate_summary_for_view", side_effect=fake_summary), patch(
            "routes.email_routes.generate_email",
            side_effect=fake_email,
        ):
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = []
                for i in range(48):
                    futures.append(executor.submit(post_summary, f"view-{i % 6}"))
                for _ in range(24):
                    futures.append(executor.submit(post_email))

                results = [future.result() for future in as_completed(futures)]

        failures = [(status, body) for status, body in results if status != 200]
        self.assertEqual([], failures)

        with db.get_session_lock(self.session_id):
            conn = db.get_session_db(self.session_id)
            view_results = db.get_meta(conn, "view_results")

        self.assertEqual(6, len(view_results))
        self.assertTrue(all(view.get("aiSummary") for view in view_results))

    def test_delete_session_keeps_existing_session_lock(self):
        lock = db.get_session_lock(self.session_id)

        db.delete_session(self.session_id)

        self.assertIs(lock, db.get_session_lock(self.session_id))
        self.assertFalse(db.session_exists(self.session_id))

    def test_waiting_summary_request_after_cleanup_does_not_recreate_session(self):
        app = self._app()
        lock = db.get_session_lock(self.session_id)

        def post_summary():
            with app.test_client() as client:
                response = client.post("/api/generate-summary", json={
                    "sessionId": self.session_id,
                    "viewId": "view-0",
                    "apiKey": "test-key",
                })
            return response.status_code, response.get_json()

        with ThreadPoolExecutor(max_workers=1) as executor:
            lock.acquire()
            try:
                future = executor.submit(post_summary)
                time.sleep(0.01)
                db.delete_session(self.session_id)
            finally:
                lock.release()

            status, body = future.result(timeout=3)

        self.assertEqual(400, status)
        self.assertEqual({"error": "Invalid session"}, body)
        self.assertFalse(db.session_exists(self.session_id))

    def test_post_cleanup_summary_and_email_return_invalid_without_recreating_session(self):
        app = self._app()

        db.delete_session(self.session_id)

        with app.test_client() as client:
            summary_response = client.post("/api/generate-summary", json={
                "sessionId": self.session_id,
                "viewId": "view-0",
                "apiKey": "test-key",
            })
            email_response = client.post("/api/generate-email", json={
                "sessionId": self.session_id,
                "apiKey": "test-key",
                "context": {"recipient": "CFO"},
            })

        self.assertEqual(400, summary_response.status_code)
        self.assertEqual({"error": "Invalid session"}, summary_response.get_json())
        self.assertEqual(400, email_response.status_code)
        self.assertEqual({"error": "Invalid session"}, email_response.get_json())
        self.assertFalse(db.session_exists(self.session_id))


if __name__ == "__main__":
    unittest.main()
