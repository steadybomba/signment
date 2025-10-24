import eventlet
eventlet.monkey_patch()

# Standard library imports
import re
import os
import json
import random
import threading
import time
from datetime import datetime, timedelta

# Third-party imports
import requests
import smtplib
import logging
from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_socketio import SocketIO, emit
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
import validators
from sqlalchemy.exc import OperationalError
from sqlalchemy import inspect, text, func
from time import sleep
from telebot import TeleBot
from telebot.types import Update
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from flask_wtf import FlaskForm

# Local imports from utils.py
from utils import (
    BotConfig, redis_client, console, enqueue_notification,
    get_cached_route_templates, sanitize_tracking_number, validate_email,
    validate_location, validate_webhook_url, send_email_notification,
    cache_route_templates, get_bot,	save_shipment, invalidate_cache,
    register_bot_handlers
)

# ----------------------------------------------------------------------
# Flask / Core init
# ----------------------------------------------------------------------
app = Flask(__name__)

bot = get_bot()                     # Telegram bot instance

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
try:
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        redis_url=os.getenv("REDIS_URL"),
        redis_token=os.getenv("REDIS_TOKEN", ""),
        webhook_url=os.getenv(
            "WEBHOOK_URL",
            "https://signment-9a96.onrender.com/telegram/webhook"
        ),
        websocket_server=os.getenv(
            "WEBSOCKET_SERVER",
            "https://signment-9a96.onrender.com"
        ),
        allowed_admins=[
            int(uid) for uid in os.getenv("ALLOWED_ADMINS", "").split(",") if uid
        ],
        valid_statuses=os.getenv(
            "VALID_STATUSES",
            "Pending,In_Transit,Out_for_Delivery,Delivered,Returned,Delayed"
        ).split(","),
        route_templates=json.loads(
            os.getenv("ROUTE_TEMPLATES", '{"Lagos, NG": ["Lagos, NG"]}')
        ),
        smtp_host=os.getenv("SMTP_HOST", "smtp.gmail.com"),
        smtp_port=int(os.getenv("SMTP_PORT", 587)),
        smtp_user=os.getenv("SMTP_USER", ""),
        smtp_pass=os.getenv("SMTP_PASS", ""),
        smtp_from=os.getenv("SMTP_FROM", "no-reply@example.com"),
    )

    app.config.update(
        TELEGRAM_BOT_TOKEN=config.telegram_bot_token,
        REDIS_URL=config.redis_url,
        WEBSOCKET_SERVER=config.websocket_server,
        SECRET_KEY=os.getenv("SECRET_KEY", "default-secret-key"),
        SQLALCHEMY_DATABASE_URI=os.getenv(
            "SQLALCHEMY_DATABASE_URI", "sqlite:///shipments.db"
        ),
        SMTP_HOST=config.smtp_host,
        SMTP_PORT=config.smtp_port,
        SMTP_USER=config.smtp_user,
        SMTP_PASS=config.smtp_pass,
        SMTP_FROM=config.smtp_from,
        RECAPTCHA_SITE_KEY=os.getenv("RECAPTCHA_SITE_KEY", "your-site-key"),
        RECAPTCHA_SECRET_KEY=os.getenv("RECAPTCHA_SECRET_KEY", "your-secret-key"),
        RECAPTCHA_VERIFY_URL="https://www.google.com/recaptcha/api/siteverify",
        GEOCODING_API_KEY=os.getenv("GEOCODING_API_KEY", ""),
        TAWK_PROPERTY_ID=os.getenv("TAWK_PROPERTY_ID", ""),
        TAWK_WIDGET_ID=os.getenv("TAWK_WIDGET_ID", ""),
        RATELIMIT_DEFAULTS=['200 per day', '50 per hour'],
        RATELIMIT_STORAGE_URI=os.getenv(
            "RATELIMIT_STORAGE_URI",
            f"redis://{config.redis_url}" if config.redis_url else "memory://",
        ),
        GLOBAL_WEBHOOK_URL=os.getenv(
            "GLOBAL_WEBHOOK_URL", config.websocket_server
        ),
        STATUS_TRANSITIONS=json.loads(
            os.getenv(
                "STATUS_TRANSITIONS",
                '''
                {
                    "Pending": {"next": ["In_Transit"], "delay": [60, 300], "probabilities": [1.0], "events": {}},
                    "In_Transit": {"next": ["Out_for_Delivery", "Delayed"], "delay": [120, 600], "probabilities": [0.9, 0.1], "events": {"Delayed due to weather", "Customs inspection"}},
                    "Out_for_Delivery": {"next": ["Delivered"], "delay": [60, 300], "probabilities": [1.0], "events": {}},
                    "Delayed": {"next": ["Out_for_Delivery"], "delay": [300, 1200], "probabilities": [1.0], "events": {"Resolved delay"}},
                    "Delivered": {"next": [], "delay": [0, 0], "probabilities": [], "events": {}},
                    "Returned": {"next": [], "delay": [0, 0], "probabilities": [], "events": {}}
                }
                ''',
            )
        ),
    )
except Exception as e:
    console.print(
        Panel(
            f"[error]Configuration validation failed: {e}[/error]",
            title="Config Error",
            border_style="red",
        )
    )
    raise

db = SQLAlchemy(app)
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=app.config["RATELIMIT_DEFAULTS"],
    storage_uri=app.config["RATELIMIT_STORAGE_URI"],
)
socketio = SocketIO(app, cors_allowed_origins="*")

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
flask_logger = logging.getLogger("flask_app")
flask_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
flask_logger.addHandler(handler)

sim_logger = logging.getLogger("simulator")
sim_logger.setLevel(logging.INFO)
sim_logger.addHandler(handler)

# ----------------------------------------------------------------------
# In-memory caches (fallback when Redis is not available)
# ----------------------------------------------------------------------
geocode_cache = {}
in_memory_clients = {}

# ----------------------------------------------------------------------
# Critical env-var check
# ----------------------------------------------------------------------
required_vars = [
    "SECRET_KEY",
    "SQLALCHEMY_DATABASE_URI",
    "SMTP_USER",
    "SMTP_PASS",
    "TELEGRAM_BOT_TOKEN",
]
for var in required_vars:
    if not app.config.get(var):
        err = f"Missing required environment variable: {var}"
        flask_logger.error(err)
        console.print(Panel(f"[error]{err}[/error]", title="Config Error", border_style="red"))
        raise ValueError(err)

# ----------------------------------------------------------------------
# Forms (fallback if forms.py is missing)
# ----------------------------------------------------------------------
class TrackForm(FlaskForm):
    tracking_number = StringField("Tracking Number", validators=[DataRequired()])
    email = StringField("Email (Optional)")
    submit = SubmitField("Track")

# ----------------------------------------------------------------------
# DB Model
# ----------------------------------------------------------------------
class Shipment(db.Model):
    __tablename__ = "shipments"
    id = db.Column(db.Integer, primary_key=True)
    tracking_number = db.Column(db.String(50), unique=True, nullable=False)
    status = db.Column(db.String(50), nullable=False)
    checkpoints = db.Column(db.Text)
    delivery_location = db.Column(db.String(100), nullable=False)
    last_updated = db.Column(db.DateTime, nullable=False)
    recipient_email = db.Column(db.String(120))
    created_at = db.Column(db.DateTime, nullable=False)
    origin_location = db.Column(db.String(100))
    webhook_url = db.Column(db.String(200))
    email_notifications = db.Column(db.Boolean, default=True)

    def to_dict(self):
        return {
            "tracking_number": self.tracking_number,
            "status": self.status,
            "checkpoints": self.checkpoints,
            "delivery_location": self.delivery_location,
            "last_updated": self.last_updated.isoformat(),
            "recipient_email": self.recipient_email,
            "origin_location": self.origin_location,
            "webhook_url": self.webhook_url,
            "email_notifications": self.email_notifications,
        }

# ----------------------------------------------------------------------
# DB init (with retries)
# ----------------------------------------------------------------------
def init_db():
    flask_logger.info("Starting database initialization")
    console.print("[info]Starting database initialization[/info]")
    max_retries = 5
    for attempt in range(max_retries):
        try:
            with app.app_context():
                db.session.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS shipments (
                            id SERIAL PRIMARY KEY,
                            tracking_number VARCHAR(50) UNIQUE NOT NULL,
                            status VARCHAR(50) NOT NULL,
                            checkpoints TEXT,
                            delivery_location VARCHAR(100) NOT NULL,
                            last_updated TIMESTAMP NOT NULL,
                            recipient_email VARCHAR(120),
                            created_at TIMESTAMP NOT NULL,
                            origin_location VARCHAR(100),
                            webhook_url VARCHAR(200),
                            email_notifications BOOLEAN DEFAULT TRUE
                        )
                        """
                    )
                )
                db.session.commit()
                if not inspect(db.engine).has_table("shipments"):
                    raise Exception("Shipments table not created")
                db.session.execute(text("SELECT 1"))
                db.session.commit()
                flask_logger.info("Database initialized successfully")
                console.print("[info]Database initialized[/info]")
                return
        except OperationalError:
            if attempt < max_retries - 1:
                sleep(5 * (2 ** attempt))
            else:
                raise
        except Exception as e:
            raise

# ----------------------------------------------------------------------
# reCAPTCHA verification
# ----------------------------------------------------------------------
def verify_recaptcha(token):
    if not app.config["RECAPTCHA_SECRET_KEY"] or "your-secret-key" in app.config["RECAPTCHA_SECRET_KEY"]:
        return True
    try:
        payload = {"secret": app.config["RECAPTCHA_SECRET_KEY"], "response": token}
        r = requests.post(app.config["RECAPTCHA_VERIFY_URL"], data=payload, timeout=5)
        return r.json().get("success", False)
    except Exception:
        return False

# ----------------------------------------------------------------------
# Geocoding (cached)
# ----------------------------------------------------------------------
def geocode_locations(checkpoints):
    coords = []
    api_key = app.config["GEOCODING_API_KEY"]
    last_time = [0]

    for cp in checkpoints:
        if cp in geocode_cache:
            coords.append(geocode_cache[cp])
            continue

        parts = cp.split(" - ")
        if len(parts) < 2:
            continue
        loc = parts[1].strip()
        cache_key = f"geocode:{loc}"

        try:
            # Rate-limit to 1 req/sec for the free API
            if time.time() - last_time[0] < 1:
                time.sleep(1 - (time.time() - last_time[0]))
            last_time[0] = time.time()

            if redis_client:
                cached = redis_client.get(cache_key)
                if cached:
                    coord = json.loads(cached)
                    geocode_cache[cp] = coord
                    coords.append(coord)
                    continue

            url = f"https://geocode.maps.co/search?q={loc}&api_key={api_key}"
            r = requests.get(url, timeout=5)
            data = r.json()
            if data:
                res = data[0]
                coord = {"lat": float(res["lat"]), "lon": float(res["lon"]), "desc": cp}
                geocode_cache[cp] = coord
                if redis_client:
                    redis_client.setex(cache_key, 86400, json.dumps(coord))
                coords.append(coord)
        except Exception:
            pass
    return coords

# ----------------------------------------------------------------------
# WebSocket client management (Redis or in-memory)
# ----------------------------------------------------------------------
def add_client(tn, sid):
    if redis_client:
        redis_client.sadd(f"clients:{tn}", sid)
    else:
        in_memory_clients.setdefault(tn, set()).add(sid)

def remove_client(tn, sid):
    if redis_client:
        redis_client.srem(f"clients:{tn}", sid)
    else:
        in_memory_clients.get(tn, set()).discard(sid)

def get_clients(tn):
    if redis_client:
        return redis_client.smembers(f"clients:{tn}") or set()
    return in_memory_clients.get(tn, set())

# ----------------------------------------------------------------------
# Background threads
# ----------------------------------------------------------------------
def keep_alive():
    while True:
        try:
            requests.get(f"{app.config['WEBSOCKET_SERVER']}/health", timeout=10)
        except Exception:
            pass
        time.sleep(300)

def process_notification_queue():
    while True:
        if not redis_client:
            time.sleep(60)
            continue
        raw = redis  = redis_client.lpop("notifications")
        if not raw:
            time.sleep(1)
            continue
        data = json.loads(raw)
        tn = data["tracking_number"]
        typ = data["type"]
        payload = data["data"]

        if typ == "email" and payload.get("recipient_email"):
            send_email_notification(
                payload["recipient_email"],
                f"Shipment Update: {tn}",
                f"Status: {payload['status']}\nLocation: {payload['delivery_location']}\nCheckpoints: {payload['checkpoints']}",
            )
        elif typ == "webhook":
            try:
                requests.post(
                    payload["webhook_url"],
                    json={**payload, "tracking_number": tn},
                    timeout=10,
                )
            except Exception:
                pass

def cleanup_websocket_clients():
    while True:
        time.sleep(3600)
        if redis_client:
            for key in redis_client.scan_iter("clients:*"):
                tn = key.decode().split(":", 1)[1]
                for sid in redis_client.smembers(key):
                    try:
                        socketio.emit("ping", room=sid, callback=lambda: None)
                    except Exception:
                        remove_client(tn, sid.decode())
        else:
            for tn, clients in list(in_memory_clients.items()):
                for sid in list(clients):
                    try:
                        socketio.emit("ping", room=sid, callback=lambda: None)
                    except Exception:
                        remove_client(tn, sid)

# ----------------------------------------------------------------------
# Simulation
# ----------------------------------------------------------------------
def simulate_tracking(tracking_number):
    tn = sanitize_tracking_number(tracking_number)
    if not tn:
        return

    start = datetime.now()
    max_time = timedelta(days=30)

    with Progress(console=console) as progress:
        task = progress.add_task(f"Simulating {tn}", total=100)

        while datetime.now() - start < max_time:
            if redis_client and redis_client.hget("paused_simulations", tn) == b"true":
                eventlet.sleep(5)
                continue

            shipment = Shipment.query.filter_by(tracking_number=tn).first()
            if not shipment:
                break
            if shipment.status in ("Delivered", "Returned"):
                progress.update(task, advance=100)
                break

            transition = app.config["STATUS_TRANSITIONS"].get(shipment.status, {})
            next_states = transition.get("next", [])
            if next_states:
                new_status = random.choices(
                    next_states,
                    transition.get("probabilities", [1.0 / len(next_states)] * len(next_states)),
                )[0]
                if new_status != shipment.status:
                    shipment.status = new_status
                    if new_status == "Delivered":
                        shipment.checkpoints = (
                            (shipment.checkpoints or "")
                            + f";{datetime.now():%Y-%m-%d %H:%M} - {shipment.delivery_location} - Delivered"
                        )
                    db.session.commit()
                    invalidate_cache(tn)
                    broadcast_update(tn)

            eventlet.sleep(60)

def broadcast_update(tracking_number):
    tn = sanitize_tracking_number(tracking_number)
    if not tn:
        return
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return

    checkpoints = (shipment.checkpoints or "").split(";")
    coords = geocode_locations(checkpoints)

    data = {
        "tracking_number": tn,
        "status": shipment.status,
        "checkpoints": checkpoints,
        "delivery_location": shipment.delivery_location,
        "coords": [
            {"lat": c["lat"], "lon": c["lon"], "desc": c["desc"]} for c in coords
        ],
        "found": True,
        "paused": redis_client and redis_client.hget("paused_simulations", tn) == b"true",
        "speed_multiplier": float(
            redis_client.hget("sim_speed_multipliers", tn) or b"1.0"
        )
        if redis_client
        else 1.0,
    }

    for sid in get_clients(tn):
        socketio.emit("tracking_update", data, room=sid)

# ----------------------------------------------------------------------
# Flask routes
# ----------------------------------------------------------------------
@app.route("/")
def index():
    try:
        from forms import TrackForm as F
        form = F()
    except Exception:
        form = TrackForm()
    return render_template(
        "index.html",
        form=form,
        tawk_property_id=app.config["TAWK_PROPERTY_ID"],
        tawk_widget_id=app.config["TAWK_WIDGET_ID"],
        recaptcha_site_key=app.config["RECAPTCHA_SITE_KEY"],
    )

@app.route("/track", methods=["POST"])
@limiter.limit("10 per minute")
def track():
    try:
        from forms import TrackForm as F
        form = F()
    except Exception:
        form = TrackForm()

    if not form.validate_on_submit():
        return jsonify({"error": "Invalid form"}), 400

    recaptcha_token = request.form.get("g-recaptcha-response")
    if (
        app.config["RECAPTCHA_SITE_KEY"]
        and "your-site-key" not in app.config["RECAPTCHA_SITE_KEY"]
    ):
        if not verify_recaptcha(recaptcha_token):
            return jsonify({"error": "reCAPTCHA failed"}), 400

    tn = sanitize_tracking_number(form.tracking_number.data)
    if not tn:
        return render_template(
            "tracking_result.html", error="Invalid tracking number", coords=[]
        )

    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return render_template(
            "tracking_result.html", error="Shipment not found", coords=[]
        )

    if form.email.data and validate_email(form.email.data):
        shipment.recipient_email = form.email.data
        db.session.commit()
        invalidate_cache(tn)

    checkpoints = (shipment.checkpoints or "").split(";")
    coords = geocode_locations(checkpoints)

    if shipment.status not in ("Delivered", "Returned"):
        eventlet.spawn(simulate_tracking, tn)

    return render_template(
        "tracking_result.html",
        shipment=shipment,
        checkpoints=checkpoints,
        coords=coords,
        tawk_property_id=app.config["TAWK_PROPERTY_ID"],
        tawk_widget_id=app.config["TAWK_WIDGET_ID"],
    )

@app.route("/health")
def health_check():
    status = {"status": "healthy"}
    try:
        db.session.execute(text("SELECT 1"))
    except Exception:
        status["status"] = "unhealthy"
    return jsonify(status), 200 if status["status"] == "healthy" else 500

@app.route("/telegram/webhook", methods=["POST"])
def webhook():
    if request.headers.get("content-type") == "application/json":
        raw = request.get_data().decode("utf-8")
        update = Update.de_json(raw)
        bot.process_new_updates([update])
        return "", 200
    return "", 403

# ----------------------------------------------------------------------
# API Endpoints (admin-only)
# ----------------------------------------------------------------------
def _require_admin():
    from utils import is_admin
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or not is_admin(int(auth.split()[-1])):
        return jsonify({"error": "Unauthorized"}), 403
    return None

@app.route("/api/shipments", methods=["POST"])
@limiter.limit("5 per minute")
def create_shipment():
    err = _require_admin()
    if err:
        return err

    data = request.get_json()
    if not data or "tracking_number" not in data:
        return jsonify({"error": "Invalid payload"}), 400

    tn = sanitize_tracking_number(data["tracking_number"])
    if not tn:
        return jsonify({"error": "Invalid tracking number"}), 400

    if save_shipment(
        tn,
        data.get("status", "Pending"),
        data.get("checkpoints", ""),
        data.get("delivery_location", "Lagos, NG"),
        data.get("recipient_email", ""),
        data.get("origin_location", ""),
        data.get("webhook_url", ""),
    ):
        eventlet.spawn(simulate_tracking, tn)
        return jsonify({"message": f"Shipment {tn} created"}), 201
    return jsonify({"error": "Failed to create"}), 500

@app.route("/api/shipments/<tracking_number>", methods=["PUT"])
@limiter.limit("5 per minute")
def update_shipment(tracking_number):
    err = _require_admin()
    if err:
        return err

    tn = sanitize_tracking_number(tracking_number)
    if not tn:
        return jsonify({"error": "Invalid tracking number"}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid payload"}), 400

    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return jsonify({"error": "Shipment not found"}), 404

    if save_shipment(
        tn,
        data.get("status", shipment.status),
        data.get("checkpoints", shipment.checkpoints),
        data.get("delivery_location", shipment.delivery_location),
        data.get("recipient_email", shipment.recipient_email),
        data.get("origin_location", shipment.origin_location),
        data.get("webhook_url", shipment.webhook_url),
    ):
        shipment.email_notifications = data.get(
            "email_notifications", shipment.email_notifications
        )
        db.session.commit()
        invalidate_cache(tn)
        broadcast_update(tn)
        return jsonify({"message": f"Shipment {tn} updated"}), 200
    return jsonify({"error": "Failed to update"}), 500

@app.route("/api/shipments/<tracking_number>", methods=["DELETE"])
@limiter.limit("5 per minute")
def delete_shipment(tracking_number):
    err = _require_admin()
    if err:
        return err

    tn = sanitize_tracking_number(tracking_number)
    if not tn:
        return jsonify({"error": "Invalid tracking number"}), 400

    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return jsonify({"error": "Shipment not found"}), 404

    db.session.delete(shipment)
    db.session.commit()
    invalidate_cache(tn)
    if redis_client:
        redis_client.hdel("paused_simulations", tn)
        redis_client.hdel("sim_speed_multipliers", tn)
        redis_client.delete(f"clients:{tn}")
    return jsonify({"message": f"Shipment {tn} deleted"}), 200

@app.route("/api/shipments/bulk", methods=["POST"])
@limiter.limit("5 per minute")
def bulk_action():
    err = _require_admin()
    if err:
        return err

    data = request.get_json()
    if not data or "action" not in data or "tracking_numbers" not in data:
        return jsonify({"error": "Invalid payload"}), 400

    action = data["action"]
    tns = [sanitize_tracking_number(t) for t in data["tracking_numbers"]]
    results = []

    for tn in tns:
        if not tn:
            results.append({"tracking_number": tn, "status": "failed", "error": "Invalid"})
            continue
        shipment = Shipment.query.filter_by(tracking_number=tn).first()
        if not shipment:
            results.append({"tracking_number": tn, "status": "failed", "error": "Not found"})
            continue

        if action == "pause":
            if redis_client:
                redis_client.hset("paused_simulations", tn, "true")
            results.append({"tracking_number": tn, "status": "success", "message": "Paused"})
        elif action == "resume":
            if redis_client:
                redis_client.hdel("paused_simulations", tn)
            eventlet.spawn(simulate_tracking, tn)
            results.append({"tracking_number": tn, "status": "success", "message": "Resumed"})
        elif action == "delete":
            db.session.delete(shipment)
            invalidate_cache(tn)
            if redis_client:
                redis_client.hdel("paused_simulations", tn)
                redis_client.hdel("sim_speed_multipliers", tn)
                redis_client.delete(f"clients:{tn}")
            results.append({"tracking_number": tn, "status": "success", "message": "Deleted"})
        else:
            results.append({"tracking_number": tn, "status": "failed", "error": "Invalid action"})

    db.session.commit()
    return jsonify({"results": results}), 200

@app.route("/api/shipments/<tracking_number>/notify", methods=["POST"])
@limiter.limit("5 per minute")
def send_notification(tracking_number):
    err = _require_admin()
    if err:
        return err

    tn = sanitize_tracking_number(tracking_number)
    if not tn:
        return jsonify({"error": "Invalid tracking number"}), 400

    data = request.get_json()
    if not data or "type" not in data:
        return jsonify({"error": "Invalid payload"}), 400

    typ = data["type"]
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return jsonify({"error": "Shipment not found"}), 404

    if typ == "email" and shipment.email_notifications and shipment.recipient_email:
        enqueue_notification(
            {
                "tracking_number": tn,
                "type": "email",
                "data": {
                    "status": shipment.status,
                    "checkpoints": shipment.checkpoints or "",
                    "delivery_location": shipment.delivery_location,
                    "recipient_email": shipment.recipient_email,
                },
            }
        )
        return jsonify({"message": "Email enqueued"}), 200

    if typ == "webhook":
        enqueue_notification(
            {
                "tracking_number": tn,
                "type": "webhook",
                "data": {
                    "status": shipment.status,
                    "checkpoints": (shipment.checkpoints or "").split(";"),
                    "delivery_location": shipment.delivery_location,
                    "webhook_url": shipment.webhook_url or app.config["GLOBAL_WEBHOOK_URL"],
                },
            }
        )
        return jsonify({"message": "Webhook enqueued"}), 200

    return jsonify({"error": "Invalid notification type or disabled"}), 400

@app.route("/api/shipments/<tracking_number>/speed", methods=["POST"])
@limiter.limit("5 per minute")
def set_simulation_speed(tracking_number):
    err = _require_admin()
    if err:
        return err

    tn = sanitize_tracking_number(tracking_number)
    if not tn:
        return jsonify({"error": "Invalid tracking number"}), 400

    data = request.get_json()
    if not data or "speed_multiplier" not in data:
        return jsonify({"error": "Invalid payload"}), 400

    try:
        speed = float(data["speed_multiplier"])
        if not (0.1 <= speed <= 10.0):
            return jsonify({"error": "Speed must be 0.1 to 10.0"}), 400
        if redis_client:
            redis_client.hset("sim_speed_multipliers", tn, str(speed))
        invalidate_cache(tn)
        broadcast_update(tn)
        return jsonify({"message": f"Speed set to {speed}"}), 200
    except Exception:
        return jsonify({"error": "Invalid speed value"}), 400

@app.route("/api/stats", methods=["GET"])
@limiter.limit("5 per minute")
def get_stats():
    total = Shipment.query.count()
    counts = (
        db.session.query(Shipment.status, func.count(Shipment.id))
        .group_by(Shipment.status)
        .all()
    )
    status_counts = {s: c for s, c in counts}
    queue_len = redis_client.llen("notifications") if redis_client else 0
    return (
        jsonify(
            {
                "total_shipments": total,
                "status_counts": status_counts,
                "notification_queue_length": queue_len,
            }
        ),
        200,
    )

# ----------------------------------------------------------------------
# SocketIO events
# ----------------------------------------------------------------------
@socketio.on("connect")
def handle_connect():
    emit("status", {"message": "Connected"})

@socketio.on("request_tracking")
def handle_request_tracking(data):
    tn = sanitize_tracking_number(data.get("tracking_number"))
    if not tn:
        emit("tracking_update", {"error": "Invalid"})
        return
    add_client(tn, request.sid)
    broadcast_update(tn)

@socketio.on("disconnect")
def handle_disconnect():
    # Clean up in-memory only; Redis cleanup runs in background thread
    if not redis_client:
        for tn in list(in_memory_clients.keys()):
            remove_client(tn, request.sid)

# ----------------------------------------------------------------------
# Register Telegram bot handlers (critical)
# ----------------------------------------------------------------------
register_bot_handlers(bot)

# ----------------------------------------------------------------------
# Startup
# ----------------------------------------------------------------------
with app.app_context():
    db.create_all()
    init_db()
    cache_route_templates()

# ----------------------------------------------------------------------
# Run (Render uses gunicorn + eventlet)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    threading.Thread(target=keep_alive, daemon=True).start()
    threading.Thread(target=process_notification_queue, daemon=True).start()
    threading.Thread(target=cleanup_websocket_clients, daemon=True).start()
    socketio.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", 10000)),
        debug=os.getenv("FLASK_ENV") == "development",
    )
