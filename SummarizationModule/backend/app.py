import os
from flask import Flask
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"))

from routes.upload_routes import upload_bp
from routes.mapping_routes import mapping_bp
from routes.views_routes import views_bp
from routes.export_routes import export_bp

app = Flask(__name__)
CORS(app)

app.config["MAX_CONTENT_LENGTH"] = 300 * 1024 * 1024  # 300 MB

app.register_blueprint(upload_bp, url_prefix="/api")
app.register_blueprint(mapping_bp, url_prefix="/api")
app.register_blueprint(views_bp, url_prefix="/api")
app.register_blueprint(export_bp, url_prefix="/api")


@app.route("/api/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    port = int(os.getenv("PORT", "3005"))
    app.run(host="0.0.0.0", port=port, debug=True)
