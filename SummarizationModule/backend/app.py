import json
import logging
import math
import os

from flask import Flask
from flask.json.provider import DefaultJSONProvider
from flask_cors import CORS
from dotenv import load_dotenv


def _nan_to_none(obj):
    """Recursively replace float NaN/Infinity with None for JSON safety."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _nan_to_none(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_nan_to_none(v) for v in obj]
    return obj


class SafeJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        kwargs.setdefault("default", self.default)
        return json.dumps(_nan_to_none(obj), allow_nan=False, **kwargs)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"))

from routes.upload_routes import upload_bp
from routes.mapping_routes import mapping_bp
from routes.views_routes import views_bp
from routes.export_routes import export_bp
from routes.email_routes import email_bp

app = Flask(__name__)
app.json = SafeJSONProvider(app)
CORS(app)

app.register_blueprint(upload_bp, url_prefix="/api")
app.register_blueprint(mapping_bp, url_prefix="/api")
app.register_blueprint(views_bp, url_prefix="/api")
app.register_blueprint(export_bp, url_prefix="/api")
app.register_blueprint(email_bp, url_prefix="/api")


@app.route("/api/health")
def health():
    return {"status": "ok"}


@app.route("/api/cleanup-session", methods=["POST"])
def cleanup_session():
    """Delete the session SQLite file. Called on tab/browser close."""
    from flask import request, jsonify
    from shared.db import delete_session, session_exists

    body = request.get_json(force=True, silent=True) or {}
    session_id = (body.get("sessionId") or "").strip()
    if not session_id:
        return jsonify({"error": "sessionId required"}), 400
    if session_exists(session_id):
        try:
            delete_session(session_id)
        except Exception:
            pass
    return jsonify({"status": "ok"})


@app.route("/api/test-key", methods=["POST"])
def test_key():
    """Minimal endpoint to verify a Portkey API key works."""
    from flask import request, jsonify
    body = request.get_json(force=True, silent=True) or {}
    api_key = (body.get("apiKey") or "").strip()
    if not api_key:
        return jsonify({"error": "Send {apiKey: '...'} in the body"}), 400
    try:
        from portkey_ai import Portkey
        base_url = os.getenv("PORTKEY_BASE_URL", "https://portkey.bain.dev/v1")
        model = os.getenv("PORTKEY_MODEL", "@personal-openai/gpt-5.4")
        client = Portkey(api_key=api_key, base_url=base_url)
        resp = client.chat.completions.create(
            messages=[{"role": "user", "content": "Say OK"}],
            model=model,
            max_tokens=5,
        )
        text = resp.choices[0].message.content if resp.choices else ""
        return jsonify({"status": "ok", "response": text, "model": model})
    except Exception as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "3005"))
    app.run(host="0.0.0.0", port=port, debug=True)
