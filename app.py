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
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import StringIO
import csv
from math import radians, cos, sin, sqrt, atan2

# Third-party imports
import requests
import smtplib
import logging
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash, Response
from flask_sqlalchemy import SQLAlchemy
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_socketio import SocketIO, emit
from rich.console import Console
from rich.panel import Panel
import validators
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy import inspect, text
from time import sleep
from telebot import TeleBot
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from flask_wtf import FlaskForm
from functools import wraps

# Local imports
from utils import (
    BotConfig, redis_client, console, get_app_modules, enqueue_notification,
    get_cached_route_templates, sanitize_tracking_number, validate_email,
    validate_location, validate_webhook_url, send_email_notification,
    check_bot_status, cache_route_templates, get_bot, get_shipment_list,
    get_shipment_details, save_shipment, invalidate_cache, is_admin
)

# Initialize Flask app
app = Flask(__name__)
bot = get_bot()

# Load config
try:
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        redis_url=os.getenv("REDIS_URL"),
        redis_token=os.getenv("REDIS_TOKEN", ""),
        webhook_url=os.getenv("WEBHOOK_URL", "https://signment-9a96.onrender.com/telegram/webhook"),
        websocket_server=os.getenv("WEBSOCKET_SERVER", "https://signment-9a96.onrender.com"),
        allowed_admins=[int(uid) for uid in os.getenv("ALLOWED_ADMINS", "").split(",") if uid],
        valid_statuses=os.getenv("VALID_STATUSES", "Pending,In_Transit,Out_for_Delivery,Delivered,Returned,Delayed").split(","),
        route_templates=json.loads(os.getenv("ROUTE_TEMPLATES", '{"Lagos, NG": ["Lagos, NG"]}')),
        smtp_host=os.getenv("SMTP_HOST", "smtp.gmail.com"),
        smtp_port=int(os.getenv("SMTP_PORT", 587)),
        smtp_user=os.getenv("SMTP_USER", ""),
        smtp_pass=os.getenv("SMTP_PASS", ""),
        smtp_from=os.getenv("SMTP_FROM", "no-reply@example.com")
    )
    app.config.update(
        TELEGRAM_BOT_TOKEN=config.telegram_bot_token,
        REDIS_URL=config.redis_url,
        WEBSOCKET_SERVER=config.websocket_server,
        SECRET_KEY=os.getenv("SECRET_KEY", "default-secret-key"),
        SQLALCHEMY_DATABASE_URI=os.getenv("SQLALCHEMY_DATABASE_URI", "sqlite:///shipments.db"),
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
        RATELIMIT_STORAGE_URI=os.getenv("RATELIMIT_STORAGE_URI", f"redis://{config.redis_url}" if config.redis_url else "memory://"),
        GLOBAL_WEBHOOK_URL=os.getenv("GLOBAL_WEBHOOK_URL", config.websocket_server),
        ADMIN_PASSWORD=os.getenv("ADMIN_PASSWORD", "admin123")
    )
except Exception as e:
    console.print(Panel(f"[error]Configuration failed: {e}[/error]", title="Config Error", border_style="red"))
    raise

# Core extensions
db = SQLAlchemy(app)
limiter = Limiter(get_remote_address, app=app, default_limits=app.config['RATELIMIT_DEFAULTS'], storage_uri=app.config['RATELIMIT_STORAGE_URI'])
socketio = SocketIO(app, cors_allowed_origins="*")

# Logging
flask_logger = logging.getLogger('flask_app')
flask_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
flask_logger.addHandler(handler)
sim_logger = logging.getLogger('simulator')
sim_logger.setLevel(logging.INFO)
sim_logger.addHandler(handler)

# Caches
geocode_cache = {}
in_memory_clients = {}

# Validate env
required = ['SECRET_KEY', 'SQLALCHEMY_DATABASE_URI', 'SMTP_USER', 'SMTP_PASS', 'TELEGRAM_BOT_TOKEN']
for var in required:
    if not app.config.get(var):
        raise ValueError(f"Missing: {var}")

# Forms
class TrackForm(FlaskForm):
    tracking_number = StringField('Tracking Number', validators=[DataRequired()])
    email = StringField('Email (Optional)')
    submit = SubmitField('Track')

# Models
class Shipment(db.Model):
    __tablename__ = 'shipments'
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
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

# DB Init
def init_db():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            with app.app_context():
                db.session.execute(text("""
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
                """))
                db.session.commit()
                if inspectors := inspect(db.engine):
                    if 'shipments' in inspectors.get_table_names():
                        flask_logger.info("DB initialized")
                        return
            sleep(5 * (2 ** attempt))
        except Exception as e:
            if attempt == max_retries - 1:
                raise
    raise Exception("DB init failed")

# reCAPTCHA
def verify_recaptcha(token):
    if 'your-secret-key' in app.config['RECAPTCHA_SECRET_KEY']:
        return True
    try:
        r = requests.post(app.config['RECAPTCHA_VERIFY_URL'], data={
            'secret': app.config['RECAPTCHA_SECRET_KEY'],
            'response': token
        }, timeout=5)
        return r.json().get('success', False)
    except:
        return False

# Geocoding
def geocode_locations(checkpoints):
    coords = []
    api_key = app.config['GEOCODING_API_KEY']
    last_time = [0]
    for cp in checkpoints:
        if cp in geocode_cache:
            coords.append(geocode_cache[cp])
            continue
        loc = cp.split(' - ')[1] if ' - ' in cp else cp
        cache_key = f"geocode:{loc}"
        try:
            if time.time() - last_time[0] < 1:
                time.sleep(1 - (time.time() - last_time[0]))
            last_time[0] = time.time()
            if redis_client and (cached := redis_client.get(cache_key)):
                coord = json.loads(cached)
                geocode_cache[cp] = coord
                coords.append(coord)
                continue
            url = f"https://geocode.maps.co/search?q={loc}&api_key={api_key}"
            res = requests.get(url, timeout=5).json()
            if res:
                c = res[0]
                coord = {'lat': float(c['lat']), 'lon': float(c['lon']), 'desc': cp}
                geocode_cache[cp] = coord
                if redis_client:
                    redis_client.setex(cache_key, 86400, json.dumps(coord))
                coords.append(coord)
        except:
            pass
    return coords

# WebSocket clients
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

# Background threads
def keep_alive():
    while True:
        try:
            requests.get(f"{app.config['WEBSOCKET_SERVER']}/health", timeout=10)
        except:
            pass
        time.sleep(300)

def process_notification_queue():
    while True:
        if not redis_client:
            time.sleep(60)
            continue
        notif = redis_client.lpop("notifications")
        if not notif:
            time.sleep(1)
            continue
        try:
            data = json.loads(notif)
            typ = data["type"]
            d = data["data"]
            if typ == "email":
                send_email_notification(
                    d["recipient_email"],
                    d.get("subject", "Shipment Update"),
                    d.get("html_body"),
                    d.get("plain_body")
                )
            elif typ == "webhook" and d.get("webhook_url"):
                requests.post(d["webhook_url"], json={**d, "tracking_number": data["tracking_number"]}, timeout=10)
        except Exception as e:
            flask_logger.error(f"Queue error: {e}")

def cleanup_websocket_clients():
    while True:
        time.sleep(3600)
        if redis_client:
            for key in redis_client.scan_iter("clients:*"):
                tn = key.decode().split(":", 1)[1]
                for sid in redis_client.smembers(key):
                    try:
                        socketio.emit('ping', room=sid)
                    except:
                        remove_client(tn, sid)

# === REALISTIC DISTANCE CALCULATION (50+ CITIES) ===
def estimate_distance(origin, dest):
    city_coords = {
        # Nigeria
        "Lagos, NG": (6.5244, 3.3792),
        "Abuja, NG": (9.0579, 7.4951),
        "Port Harcourt, NG": (4.8156, 7.0498),
        "Kano, NG": (12.0001, 8.5167),
        "Ibadan, NG": (7.3775, 3.9470),
        "Enugu, NG": (6.4584, 7.5170),
        "Kaduna, NG": (10.5105, 7.4165),
        "Benin City, NG": (6.3340, 5.6037),
        "Jos, NG": (9.8965, 8.8583),
        "Ilorin, NG": (8.4966, 4.5429),

        # United States
        "New York, NY": (40.7128, -74.0060),
        "Los Angeles, CA": (34.0522, -118.2437),
        "Chicago, IL": (41.8781, -87.6298),
        "Houston, TX": (29.7604, -95.3698),
        "Phoenix, AZ": (33.4484, -112.0740),
        "Philadelphia, PA": (39.9526, -75.1652),
        "San Antonio, TX": (29.4241, -98.4936),
        "San Diego, CA": (32.7157, -117.1611),
        "Dallas, TX": (32.7767, -96.7970),
        "San Francisco, CA": (37.7749, -122.4194),

        # United Kingdom
        "London, UK": (51.5074, -0.1278),
        "Manchester, UK": (53.4808, -2.2426),
        "Birmingham, UK": (52.4862, -1.8904),
        "Glasgow, UK": (55.8642, -4.2518),
        "Liverpool, UK": (53.4084, -2.9916),

        # Europe
        "Paris, FR": (48.8566, 2.3522),
        "Berlin, DE": (52.5200, 13.4050),
        "Madrid, ES": (40.4168, -3.7038),
        "Rome, IT": (41.9028, 12.4964),
        "Amsterdam, NL": (52.3676, 4.9041),
        "Vienna, AT": (48.2082, 16.3738),
        "Warsaw, PL": (52.2297, 21.0122),
        "Prague, CZ": (50.0755, 14.4378),

        # Asia
        "Dubai, UAE": (25.2048, 55.2708),
        "Mumbai, IN": (19.0760, 72.8777),
        "Delhi, IN": (28.7041, 77.1025),
        "Bangalore, IN": (12.9716, 77.5946),
        "Singapore, SG": (1.3521, 103.8198),
        "Beijing, CN": (39.9042, 116.4074),
        "Shanghai, CN": (31.2304, 121.4737),
        "Hong Kong, HK": (22.3193, 114.1694),
        "Tokyo, JP": (35.6762, 139.6503),
        "Seoul, KR": (37.5665, 126.9780),
        "Jakarta, ID": (-6.2088, 106.8456),
        "Bangkok, TH": (13.7563, 100.5018),

        # Africa
        "Johannesburg, ZA": (-26.2041, 28.0473),
        "Cape Town, ZA": (-33.9249, 18.4241),
        "Nairobi, KE": (-1.2921, 36.8219),
        "Accra, GH": (5.6037, -0.1870),
        "Cairo, EG": (30.0444, 31.2357),
        "Addis Ababa, ET": (8.9806, 38.7578),

        # Oceania
        "Sydney, AU": (-33.8688, 151.2093),
        "Melbourne, AU": (-37.8136, 144.9631),
        "Auckland, NZ": (-36.8485, 174.7633),

        # South America
        "São Paulo, BR": (-23.5505, -46.6333),
        "Buenos Aires, AR": (-34.6037, -58.3816),
        "Lima, PE": (-12.0464, -77.0428),
        "Santiago, CL": (-33.4489, -70.6693),
    }

    # Normalize city names
    origin_key = next((k for k in city_coords if origin.lower() in k.lower() or k.lower().startswith(origin.lower())), None)
    dest_key = next((k for k in city_coords if dest.lower() in k.lower() or k.lower().startswith(dest.lower())), None)

    if not origin_key or not dest_key:
        return 1000  # fallback distance in km

    lat1, lon1 = map(radians, city_coords[origin_key])
    lat2, lon2 = map(radians, city_coords[dest_key])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return round(6371 * c, 1)  # km, rounded to 1 decimal

# === REALISTIC SIMULATION ===
def simulate_tracking(tn):
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return

    origin = shipment.origin_location or "Lagos, NG"
    destination = shipment.delivery_location
    route_template = get_cached_route_templates().get(destination, get_cached_route_templates().get(origin, []))
    if not route_template:
        route_template = [origin, destination]

    checkpoints = (shipment.checkpoints or "").split(";") if shipment.checkpoints else []
    current_idx = len([c for c in checkpoints if "Processed" in c])
    start_time = datetime.now()

    # Distance-based realistic timing
    distance_km = estimate_distance(origin, destination)
    base_hours = max(24, min(120, distance_km / 100))  # 24h min, 5 days max
    speed_multiplier = float(redis_client.hget("sim_speed_multipliers", tn) or "1.0") if redis_client else 1.0
    speed_multiplier = max(0.1, min(10.0, speed_multiplier))

    while datetime.now() - start_time < timedelta(days=7):
        if redis_client and redis_client.hget("paused_simulations", tn) == "true":
            eventlet.sleep(10)
            continue

        try:
            # === UPDATE CHECKPOINT ===
            if current_idx < len(route_template) and shipment.status not in ["Delivered", "Returned"]:
                city = route_template[current_idx]
                events = [
                    "Processed", "In transit", "Arrived at sorting center",
                    "Departed facility", "Customs clearance", "On vehicle for delivery"
                ]
                event = random.choice(events)
                weather_delay = ""
                if random.random() < 0.1:
                    weather_delay = " | Weather delay"
                    eventlet.sleep(random.uniform(300, 900) / speed_multiplier)
                checkpoint = f"{datetime.now():%Y-%m-%d %H:%M} - {city} - {event}{weather_delay}"
                if checkpoint not in checkpoints:
                    checkpoints.append(checkpoint)
                    current_idx += 1

            # === STATUS TRANSITION ===
            current_status = shipment.status
            if current_status == "Pending" and current_idx > 0:
                current_status = "In_Transit"
            elif current_status == "In_Transit" and current_idx >= len(route_template) - 1:
                current_status = "Out_for_Delivery"
            elif current_status == "Out_for_Delivery" and random.random() < 0.7:
                current_status = "Delivered"
                checkpoints.append(f"{datetime.now():%Y-%m-%d %H:%M} - {destination} - Delivered")
            elif current_status == "In_Transit" and random.random() < 0.05:
                current_status = "Delayed"
            elif current_status == "Delayed" and random.random() < 0.8:
                current_status = "In_Transit"

            # === FINALIZE ===
            shipment.status = current_status
            shipment.checkpoints = ";".join(checkpoints[-50:])
            shipment.last_updated = datetime.now()
            db.session.commit()
            invalidate_cache(tn)

            # === NOTIFICATION ===
            if len(checkpoints) > 1 and random.random() < 0.6:
                enqueue_realistic_email(tn, current_status, checkpoints[-1], destination)

            broadcast_update(tn)

            # === DYNAMIC SLEEP ===
            base_sleep = base_hours * 3600 / len(route_template) / speed_multiplier
            jitter = random.uniform(0.7, 1.3)
            eventlet.sleep(base_sleep * jitter)

            if current_status in ["Delivered", "Returned"]:
                break

        except Exception as e:
            sim_logger.error(f"Sim error {tn}: {e}")
            eventlet.sleep(30)

# === REALISTIC EMAIL NOTIFICATION ===
def enqueue_realistic_email(tn, status, latest_checkpoint, destination):
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment or not shipment.recipient_email or not shipment.email_notifications:
        return

    subject = f"Shipment Update: {tn} — {status}"
    location = latest_checkpoint.split(' - ')[1] if ' - ' in latest_checkpoint else destination
    html_body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: auto; border: 1px solid #eee; border-radius: 12px; overflow: hidden;">
      <div style="background: #2563eb; color: white; padding: 1.5rem; text-align: center;">
        <h2 style="margin: 0;">Shipment Update</h2>
      </div>
      <div style="padding: 1.5rem;">
        <p><strong>Tracking #:</strong> <code>{tn}</code></p>
        <p><strong>Status:</strong> <span style="color: #dc2626; font-weight: bold;">{status}</span></p>
        <p><strong>Latest:</strong> {location}</p>
        <p><strong>Destination:</strong> {destination}</p>
        <hr>
        <p style="color: #666; font-size: 0.9rem;">
          This is an automated update. <a href="{app.config['WEBSOCKET_SERVER']}/track/{tn}">Track Live</a>
        </p>
      </div>
      <div style="background: #f8f9fa; padding: 1rem; text-align: center; font-size: 0.8rem; color: #888;">
        © {datetime.now().year} Your Logistics Co.
      </div>
    </div>
    """
    plain_body = f"Update: {tn}\nStatus: {status}\nLatest: {location}\nTrack: {app.config['WEBSOCKET_SERVER']}/track/{tn}"

    enqueue_notification({
        "tracking_number": tn,
        "type": "email",
        "data": {
            "recipient_email": shipment.recipient_email,
            "subject": subject,
            "html_body": html_body,
            "plain_body": plain_body
        }
    })

# === ROBUST EMAIL SENDER ===
def send_email_notification(recipient, subject, html_body=None, plain_body=None):
    if not all([app.config['SMTP_HOST'], app.config['SMTP_USER'], app.config['SMTP_PASS']]):
        flask_logger.warning("SMTP not configured")
        return False

    msg = MIMEMultipart("alternative")
    msg['From'] = app.config['SMTP_FROM']
    msg['To'] = recipient
    msg['Subject'] = subject

    if plain_body:
        msg.attach(MIMEText(plain_body, "plain"))
    if html_body:
        msg.attach(MIMEText(html_body, "html"))

    max_retries = 3
    for attempt in range(max_retries):
        try:
            with smtplib.SMTP(app.config['SMTP_HOST'], app.config['SMTP_PORT'], timeout=10) as server:
                server.starttls()
                server.login(app.config['SMTP_USER'], app.config['SMTP_PASS'])
                server.send_message(msg)
            flask_logger.info(f"Email sent to {recipient}")
            return True
        except Exception as e:
            flask_logger.error(f"Email attempt {attempt+1} failed: {e}")
            if attempt == max_retries - 1:
                console.print(Panel(f"[error]Failed to send email to {recipient}[/error]", title="Email Error"))
                return False
            time.sleep(2 ** attempt)
    return False

# Broadcast
def broadcast_update(tn):
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return
    speed = float(redis_client.hget("sim_speed_multipliers", tn) or "1.0") if redis_client else 1.0
    paused = redis_client and redis_client.hget("paused_simulations", tn) == "true"
    data = {
        "tracking_number": tn,
        "status": shipment.status,
        "delivery_location": shipment.delivery_location,
        "checkpoints": (shipment.checkpoints or "").split(";"),
        "last_updated": shipment.last_updated.isoformat(),
        "speed_multiplier": speed,
        "paused": paused
    }
    try:
        emit('tracking_update', data, broadcast=True)
    except:
        pass
    try:
        requests.post(f"{app.config['WEBSOCKET_SERVER']}/notify", json=data, timeout=3)
    except:
        pass

# Admin decorator
def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated

# Routes
@app.route('/')
def index():
    try:
        from forms import TrackForm as F
        form = F()
    except:
        form = TrackForm()
    return render_template('index.html', form=form, tawk_property_id=app.config['TAWK_PROPERTY_ID'],
                           tawk_widget_id=app.config['TAWK_WIDGET_ID'], recaptcha_site_key=app.config['RECAPTCHA_SITE_KEY'])

@app.route('/track', methods=['POST'])
@limiter.limit("10 per minute")
def track():
    try:
        from forms import TrackForm as F
        form = F()
    except:
        form = TrackForm()
    if not form.validate_on_submit():
        return jsonify({'error': 'Invalid form'}), 400

    recaptcha = request.form.get('g-recaptcha-response')
    if app.config['RECAPTCHA_SITE_KEY'] and 'your-site-key' not in app.config['RECAPTCHA_SITE_KEY']:
        if not verify_recaptcha(recaptcha):
            return jsonify({'error': 'reCAPTCHA failed'}), 400

    tn = sanitize_tracking_number(form.tracking_number.data)
    email = form.email.data
    if not tn:
        return render_template('tracking_result.html', error='Invalid tracking number', coords=[])

    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return render_template('tracking_result.html', error='Not found', coords=[])

    if email and validate_email(email):
        shipment.recipient_email = email
        db.session.commit()
        invalidate_cache(tn)

    checkpoints = (shipment.checkpoints or "").split(";")
    coords = geocode_locations(checkpoints)
    coords_list = [{'lat': c['lat'], 'lon': c['lon'], 'desc': c['desc']} for c in coords]

    if shipment.status not in ['Delivered', 'Returned']:
        eventlet.spawn(simulate_tracking, tn)

    return render_template('tracking_result.html', shipment=shipment, checkpoints=checkpoints, coords=coords_list,
                           tawk_property_id=app.config['TAWK_PROPERTY_ID'], tawk_widget_id=app.config['TAWK_WIDGET_ID'])

@app.route('/health')
def health_check():
    status = {'status': 'healthy', 'database': 'ok', 'redis': 'ok', 'smtp': 'ok'}
    try:
        db.session.execute(text('SELECT 1'))
    except:
        status['status'] = status['database'] = 'error'
    try:
        if redis_client:
            redis_client.ping()
        else:
            status['redis'] = 'unavailable'
    except:
        status['redis'] = 'error'
    try:
        with smtplib.SMTP(app.config['SMTP_HOST'], app.config['SMTP_PORT'], timeout=5) as s:
            s.starttls()
            s.login(app.config['SMTP_USER'], app.config['SMTP_PASS'])
    except:
        status['smtp'] = 'error'
    return jsonify(status), 200 if status['status'] == 'healthy' else 500

# Admin Routes
@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        if request.form.get('password') == app.config['ADMIN_PASSWORD']:
            session['admin_logged_in'] = True
            return redirect(url_for('admin_dashboard'))
        flash("Invalid password", "error")
    return render_template('admin_login.html')

@app.route('/admin/logout')
def admin_logout():
    session.pop('admin_logged_in', None)
    return redirect(url_for('index'))

@app.route('/admin')
@admin_required
def admin_dashboard():
    page = int(request.args.get('page', 1))
    per_page = 10
    tracking_numbers, total = get_shipment_list(page=page, per_page=per_page)
    shipments = []
    for tn in tracking_numbers:
        s = Shipment.query.filter_by(tracking_number=tn).first()
        if s:
            paused = redis_client and redis_client.hget("paused_simulations", tn) == "true"
            speed = float(redis_client.hget("sim_speed_multipliers", tn) or "1.0") if redis_client else 1.0
            shipments.append({
                'tracking_number': s.tracking_number,
                'status': s.status,
                'delivery_location': s.delivery_location,
                'last_updated': s.last_updated.strftime("%Y-%m-%d %H:%M"),
                'paused': paused,
                'speed': f"{speed:.1f}x"
            })
    total_pages = (total - 1) // per_page + 1
    return render_template('admin_dashboard.html',
                           total=total, queue_len=redis_client.llen("notifications") if redis_client else 0,
                           active_clients=len(redis_client.keys("clients:*")) if redis_client else 0,
                           shipments=shipments, page=page, total_pages=total_pages,
                           now=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))

@app.route('/admin/csv')
@admin_required
def admin_csv():
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Tracking Number", "Status", "Origin", "Destination", "Email", "Last Updated", "Created At"])
    for s in Shipment.query.order_by(Shipment.created_at.desc()).all():
        writer.writerow([s.tracking_number, s.status, s.origin_location or "-", s.delivery_location,
                         s.recipient_email or "-", s.last_updated.strftime("%Y-%m-%d %H:%M"), s.created_at.strftime("%Y-%m-%d %H:%M")])
    output.seek(0)
    return Response(output, mimetype="text/csv", headers={"Content-Disposition": f"attachment;filename=shipments_{datetime.utcnow().strftime('%Y%m%d')}.csv"})

@app.route('/admin/api/pause', methods=['POST'])
@admin_required
def admin_api_pause():
    data = request.get_json()
    tn = data.get('tracking_number')
    pause = data.get('pause')
    if not tn or pause is None:
        return jsonify({"error": "Invalid"}), 400
    if not redis_client:
        return jsonify({"error": "Redis down"}), 500
    if pause:
        redis_client.hset("paused_simulations", tn, "true")
    else:
        redis_client.hdel("paused_simulations", tn)
        eventlet.spawn(simulate_tracking, tn)
    invalidate_cache(tn)
    broadcast_update(tn)
    return jsonify({"success": True})

@app.route('/admin/api/speed', methods=['POST'])
@admin_required
def admin_api_speed():
    data = request.get_json()
    tn = data.get('tracking_number')
    speed = data.get('speed')
    if not tn or not (0.1 <= speed <= 10.0):
        return jsonify({"error": "Invalid"}), 400
    if not redis_client:
        return jsonify({"error": "Redis down"}), 500
    redis_client.hset("sim_speed_multipliers", tn, str(speed))
    invalidate_cache(tn)
    broadcast_update(tn)
    return jsonify({"success": True})

# SocketIO
@socketio.on('connect')
def on_connect():
    emit('status', {'message': 'Connected'})

@socketio.on('request_tracking')
def on_request(data):
    tn = sanitize_tracking_number(data.get('tracking_number'))
    if not tn:
        emit('tracking_update', {'error': 'Invalid'})
        return
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        emit('tracking_update', {'error': 'Not found'})
        return
    add_client(tn, request.sid)
    checkpoints = (shipment.checkpoints or "").split(";")
    coords = geocode_locations(checkpoints)
    speed = float(redis_client.hget("sim_speed_multipliers", tn) or "1.0") if redis_client else 1.0
    paused = redis_client and redis_client.hget("paused_simulations", tn) == "true"
    emit('tracking_update', {
        'tracking_number': tn, 'status': shipment.status, 'delivery_location': shipment.delivery_location,
        'checkpoints': checkpoints, 'coords': [{'lat': c['lat'], 'lon': c['lon'], 'desc': c['desc']} for c in coords],
        'speed_multiplier': speed, 'paused': paused
    })

@socketio.on('disconnect')
def on_disconnect():
    for tn in list(in_memory_clients.keys()):
        remove_client(tn, request.sid)
    if redis_client:
        for key in redis_client.scan_iter("clients:*"):
            tn = key.decode().split(":", 1)[1]
            remove_client(tn, request.sid)

# Start
if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    init_db()
    cache_route_templates()
    threading.Thread(target=keep_alive, daemon=True).start()
    threading.Thread(target=process_notification_queue, daemon=True).start()
    threading.Thread(target=cleanup_websocket_clients, daemon=True).start()
    socketio.run(app, host='0.0.0.0', port=10000, debug=os.getenv('FLASK_ENV') == 'development')
