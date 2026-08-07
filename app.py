import logging
import sys

def _get_eventlet():
    class _FallbackEventlet:
        @staticmethod
        def sleep(seconds):
            import time as _time
            _time.sleep(seconds)

        @staticmethod
        def spawn(func, *args, **kwargs):
            import threading
            thread = threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True)
            thread.start()
            return thread

    if sys.platform == 'win32':
        logging.warning("Windows platform detected; using threading fallback instead of eventlet.")
        return _FallbackEventlet()

    if sys.version_info >= (3, 14):
        logging.warning("Skipping eventlet on Python 3.14+ due to known incompatibility; using threading fallback.")
        return _FallbackEventlet()

    try:
        import eventlet
        try:
            eventlet.monkey_patch()
            return eventlet
        except Exception as e:
            logging.warning(f"Eventlet monkey_patch failed: {e}")
    except ImportError as e:
        logging.warning(f"Eventlet import failed: {e}")

    return _FallbackEventlet()

eventlet = _get_eventlet()

# Standard library imports
import re
import os
import sys
import json
import random
import threading
import time
import csv
import string
from io import StringIO
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from math import radians, cos, sin, sqrt, atan2, ceil

# Third-party imports
import requests
from requests.structures import CaseInsensitiveDict
import smtplib
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash, Response
from flask_sqlalchemy import SQLAlchemy
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_socketio import SocketIO, emit
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
import validators
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy import inspect, text, or_
from time import sleep
from urllib.parse import quote_plus
from telebot import TeleBot, types
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from flask_wtf import FlaskForm
from functools import wraps, lru_cache
from collections import deque

load_dotenv()

# Local imports
from utils import (
    BotConfig, redis_client, get_redis_client, console, enqueue_notification,
    email_throttle_cache, email_digest_cache,
    get_cached_route_templates, sanitize_tracking_number, validate_email,
    validate_location, validate_webhook_url,
    cache_route_templates, get_bot, get_shipment_list,
    get_shipment_details, save_shipment, invalidate_cache, is_admin,
    spawn_simulation, add_socket_event, recent_socket_events, add_client_error, recent_client_errors
)

# Initialize Flask app
app = Flask(__name__)

# Email configuration
EMAIL_TEST_MODE = os.getenv('EMAIL_TEST_MODE', 'false').lower() == 'true'
EMAIL_ENABLED = os.getenv('EMAIL_ENABLED', 'true').lower() == 'true'
AUTO_EMAIL_ENABLED = os.getenv('AUTO_EMAIL_ENABLED', 'true').lower() == 'true'
EMAIL_THROTTLE_MINUTES = int(os.getenv('EMAIL_THROTTLE_MINUTES', '60'))

# Load config
try:
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_TOKEN", "")),
        redis_url=os.getenv("REDIS_URL"),
        redis_token=os.getenv("REDIS_TOKEN", ""),
        webhook_url=os.getenv("WEBHOOK_URL", "https://signment-9a96.onrender.com/telegram/webhook"),
        websocket_server=os.getenv("WEBSOCKET_SERVER", ""),
        allowed_admins=[int(uid) for uid in os.getenv("ALLOWED_ADMINS", "").split(",") if uid],
        valid_statuses=os.getenv("VALID_STATUSES", "Pending,On_Hold,In_Transit,Out_for_Delivery,Delivered,Returned,Delayed").split(","),
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
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
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
        RATELIMIT_STORAGE_URI=(os.getenv("RATELIMIT_STORAGE_URI") or ("redis://" + config.redis_url if config.redis_url and bool(redis_client) else "memory://")),
        GLOBAL_WEBHOOK_URL=os.getenv("GLOBAL_WEBHOOK_URL", config.websocket_server),
        ADMIN_PASSWORD=os.getenv("ADMIN_PASSWORD", "admin123")
    )
except Exception as e:
    console.print(Panel(f"[error]Configuration failed: {e}[/error]", title="Config Error", border_style="red"))
    raise

# === DHL CARRIER CONFIG ===
DHL_CONFIG = {
    "name": "DHL Express",
    "primary_color": "#D40511",
    "secondary_color": "#FFCC00",
    "logo_url": "https://www.dhl.com/etc.clientlibs/dhl/clientlibs/clientlib-site/resources/images/dhl-logo.svg",
    "tracking_prefix": "JD",
    "tracking_format": r"^JD\d{10}$",
    "status_flow": {
        "Pending": {"next": ["In_Transit"], "delay": [60, 180]},
        "On_Hold": {"next": ["In_Transit", "Delayed"], "delay": [3600, 86400]},
        "In_Transit": {"next": ["Out_for_Delivery", "Delayed"], "delay": [120, 600], "probabilities": [0.92, 0.08]},
        "Out_for_Delivery": {"next": ["Delivered"], "delay": [60, 240]},
        "Delayed": {"next": ["Out_for_Delivery"], "delay": [300, 900]},
        "Delivered": {"next": [], "delay": [0, 0]},
        "Returned": {"next": [], "delay": [0, 0]}
    },
    "events": {
        "In_Transit": ["Shipment picked up", "Departed origin facility", "Arrived at sort facility", "Processed at hub"],
        "Out_for_Delivery": ["Out for delivery", "With delivery courier"],
        "Delayed": ["Held at customs", "Weather delay", "Routing delay"],
        "On_Hold": ["Held for customs clearance", "Awaiting documentation"]
    }
}

# Core extensions
db = SQLAlchemy(app)
limiter = Limiter(get_remote_address, app=app, default_limits=app.config['RATELIMIT_DEFAULTS'], storage_uri=app.config['RATELIMIT_STORAGE_URI'])
async_mode = 'eventlet' if hasattr(eventlet, 'sleep') and eventlet.__class__.__name__ != '_FallbackEventlet' else 'threading'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode=async_mode)

# Logging
flask_logger = logging.getLogger('flask_app')
sim_logger = logging.getLogger('simulator')

# Caches
geocode_cache = {}
in_memory_clients = {}
in_memory_sim = {}


def rget(field, tn, default=None):
    """Safe redis.hget wrapper that returns decoded value or default when redis missing or errors."""
    global redis_client
    try:
        if not redis_client:
            return in_memory_sim.get(tn, {}).get(field, default)
        val = redis_client.hget(field, tn)
        if val is None:
            return in_memory_sim.get(tn, {}).get(field, default)
        if isinstance(val, bytes):
            return val.decode('utf-8')
        return val
    except Exception:
        redis_client = None
        return in_memory_sim.get(tn, {}).get(field, default)


def rset(field, tn, value):
    """Safe redis.hset wrapper that disables redis_client on failure."""
    global redis_client
    if not redis_client:
        try:
            in_memory_sim.setdefault(tn, {})[field] = value
        except Exception:
            pass
        return
    try:
        redis_client.hset(field, tn, value)
    except Exception:
        redis_client = None


def rkeys(pattern):
    try:
        if not redis_client:
            return []
        keys = redis_client.keys(pattern) or []
        return [k.decode() if isinstance(k, bytes) else k for k in keys]
    except Exception as e:
        flask_logger.warning(f"Redis keys failed for pattern {pattern}: {e}")
        return []


def rhgetall(key):
    try:
        if not redis_client:
            return {}
        d = redis_client.hgetall(key) or {}
        return { (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in d.items() }
    except Exception as e:
        flask_logger.warning(f"Redis hgetall failed for {key}: {e}")
        return {}


def rlist_lpop(key):
    try:
        if not redis_client:
            return None
        v = redis_client.lpop(key)
        if v is None:
            return None
        return v.decode() if isinstance(v, bytes) else v
    except Exception as e:
        flask_logger.warning(f"Redis lpop failed for {key}: {e}")
        return None


def rexists(key):
    try:
        if not redis_client:
            return False
        return bool(redis_client.exists(key))
    except Exception as e:
        flask_logger.warning(f"Redis exists check failed for {key}: {e}")
        return False


def rhlen(key):
    try:
        if not redis_client:
            return 0
        return redis_client.hlen(key)
    except Exception as e:
        flask_logger.warning(f"Redis hlen failed for {key}: {e}")
        return 0


def densify_route_coords(route_coords, max_segment_km=1.0):
    """Densify a route represented as a list of [lat, lon] or dicts with lat/lon.
    Inserts intermediate points so that no segment is longer than max_segment_km.
    """
    if not route_coords or len(route_coords) < 2:
        return route_coords or []
    pairs = []
    for p in route_coords:
        if isinstance(p, dict):
            pairs.append([float(p.get('lat')), float(p.get('lon'))])
        elif isinstance(p, (list, tuple)) and len(p) >= 2:
            pairs.append([float(p[0]), float(p[1])])
    out = [{'lat': pairs[0][0], 'lon': pairs[0][1]}]
    for a, b in zip(pairs, pairs[1:]):
        dist_km = haversine_distance(a[0], a[1], b[0], b[1])
        if dist_km <= 0:
            continue
        segments = max(1, int(ceil(dist_km / float(max_segment_km))))
        for i in range(1, segments + 1):
            frac = i / float(segments)
            lat = a[0] + (b[0] - a[0]) * frac
            lon = a[1] + (b[1] - a[1]) * frac
            out.append({'lat': lat, 'lon': lon})
    return out

@app.before_request
def log_request():
    request.start_time = time.time()
    flask_logger.debug(
        "Request start: %s %s from %s query=%s json=%s",
        request.method,
        request.path,
        request.remote_addr,
        request.args.to_dict(flat=False),
        request.get_json(silent=True)
    )

@app.after_request
def log_response(response):
    duration = (time.time() - getattr(request, 'start_time', time.time())) * 1000
    flask_logger.debug(
        "Request complete: %s %s status=%s duration=%.1fms",
        request.method,
        request.path,
        response.status,
        duration
    )
    return response

@app.errorhandler(Exception)
def handle_app_exception(error):
    flask_logger.exception("Unhandled exception during request %s %s", request.method, request.path)
    if app.debug:
        raise error
    return jsonify({"error": "Internal server error"}), 500

# Validate env
required = ['SECRET_KEY', 'SQLALCHEMY_DATABASE_URI']
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
    last_updated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    recipient_email = db.Column(db.String(120))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    origin_location = db.Column(db.String(100))
    origin_lat = db.Column(db.Float)
    origin_lon = db.Column(db.Float)
    delivery_lat = db.Column(db.Float)
    delivery_lon = db.Column(db.Float)
    webhook_url = db.Column(db.String(200))
    email_notifications = db.Column(db.Boolean, default=True)
    carrier = db.Column(db.String(20), default="DHL")

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

# DB Init
def init_db():
    with app.app_context():
        db.create_all()
        engine = db.engine
        if engine.dialect.name == 'sqlite':
            try:
                conn = engine.connect()
                existing = {row['name'] for row in conn.execute(text("PRAGMA table_info(shipments)")).mappings()}
                alterations = [
                    ("carrier", "ALTER TABLE shipments ADD COLUMN carrier VARCHAR(20) DEFAULT 'DHL';"),
                    ("origin_lat", "ALTER TABLE shipments ADD COLUMN origin_lat REAL;"),
                    ("origin_lon", "ALTER TABLE shipments ADD COLUMN origin_lon REAL;"),
                    ("delivery_lat", "ALTER TABLE shipments ADD COLUMN delivery_lat REAL;"),
                    ("delivery_lon", "ALTER TABLE shipments ADD COLUMN delivery_lon REAL;")
                ]
                for col, stmt in alterations:
                    if col not in existing:
                        try:
                            conn.execute(text(stmt))
                        except Exception as e:
                            flask_logger.warning(f"SQLite column add failed for {col}: {e}")
                conn.commit()
                flask_logger.info("DB initialized using SQLite")
            except Exception as e:
                flask_logger.warning(f"SQLite DB init failed: {e}")
            return

        max_retries = 5
        for attempt in range(max_retries):
            try:
                if inspectors := inspect(engine):
                    if 'shipments' not in inspectors.get_table_names():
                        db.create_all()
                    db.session.execute(text("""
                        ALTER TABLE shipments ADD COLUMN IF NOT EXISTS carrier VARCHAR(20) DEFAULT 'DHL';
                    """))
                    db.session.execute(text("""
                        ALTER TABLE shipments ADD COLUMN IF NOT EXISTS origin_lat REAL;
                    """))
                    db.session.execute(text("""
                        ALTER TABLE shipments ADD COLUMN IF NOT EXISTS origin_lon REAL;
                    """))
                    db.session.execute(text("""
                        ALTER TABLE shipments ADD COLUMN IF NOT EXISTS delivery_lat REAL;
                    """))
                    db.session.execute(text("""
                        ALTER TABLE shipments ADD COLUMN IF NOT EXISTS delivery_lon REAL;
                    """))
                    db.session.commit()
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

# Geocoding functions
def geoapify_geocode(address):
    api_key = app.config.get('GEOCODING_API_KEY', '')
    if not api_key:
        return None
    try:
        url = f"https://api.geoapify.com/v1/geocode/search?text={quote_plus(address)}&apiKey={api_key}"
        headers = CaseInsensitiveDict({"Accept": "application/json"})
        resp = requests.get(url, headers=headers, timeout=6)
        if resp.status_code != 200:
            return None
        payload = resp.json()
        features = payload.get('features') or []
        if not features:
            return None
        props = features[0].get('properties', {})
        return {'lat': float(props.get('lat')), 'lon': float(props.get('lon')), 'desc': address}
    except Exception as e:
        flask_logger.debug(f"Geoapify geocode failed for {address}: {e}")
        return None

def geoapify_route(coords, mode='drive'):
    api_key = app.config.get('GEOCODING_API_KEY', '')
    if not api_key or len(coords) < 2:
        return coords
    try:
        waypoints = '|'.join(f"{c['lat']},{c['lon']}" for c in coords)
        url = f"https://api.geoapify.com/v1/routing?waypoints={quote_plus(waypoints)}&mode={mode}&apiKey={api_key}"
        headers = CaseInsensitiveDict({"Accept": "application/json"})
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return coords
        payload = resp.json()
        features = payload.get('features') or []
        if not features:
            return coords
        geometry = features[0].get('geometry', {})
        if geometry.get('type') != 'LineString':
            return coords
        return [
            {'lat': float(lat), 'lon': float(lon), 'desc': f"Route point {idx+1}"}
            for idx, (lon, lat) in enumerate(geometry.get('coordinates', []))
        ]
    except Exception as e:
        flask_logger.debug(f"Geoapify routing failed: {e}")
        return coords

@lru_cache(maxsize=1000)
def cached_geocode(location):
    return geoapify_geocode(location)

def build_route_from_checkpoints(checkpoint_coords, mode='drive'):
    if len(checkpoint_coords) < 2:
        return checkpoint_coords
    return geoapify_route(checkpoint_coords, mode=mode)

def geocode_locations(checkpoints):
    coords = []
    api_key = app.config.get('GEOCODING_API_KEY')
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
            coord = cached_geocode(loc) if api_key else None
            if not coord:
                url = f"https://geocode.maps.co/search?q={loc}&api_key={api_key}"
                res = requests.get(url, timeout=5).json()
                if res:
                    c = res[0]
                    coord = {'lat': float(c['lat']), 'lon': float(c['lon']), 'desc': cp}
            if coord:
                geocode_cache[cp] = coord
                if redis_client:
                    redis_client.set(cache_key, json.dumps(coord), ex=86400)
                coords.append(coord)
        except Exception:
            pass
    return coords


def normalize_location(loc):
    """Normalize a free-text location into a readable 'City, CC' or fallback to a cleaned string.
    Uses Geoapify when API key is available and caches results in Redis when possible.
    Returns the normalized string.
    """
    if not loc:
        return loc
    loc = loc.strip()
    cache_key = f"normloc:{loc}"
    try:
        if redis_client and (cached := redis_client.get(cache_key)):
            return cached.decode('utf-8')
    except Exception:
        pass

    normalized = loc
    api_key = app.config.get('GEOCODING_API_KEY', '')
    try:
        if api_key:
            url = f"https://api.geoapify.com/v1/geocode/search?text={quote_plus(loc)}&apiKey={api_key}"
            resp = requests.get(url, timeout=6)
            if resp.status_code == 200:
                payload = resp.json()
                features = payload.get('features') or []
                if features:
                    props = features[0].get('properties', {})
                    city = props.get('city') or props.get('town') or props.get('village') or props.get('county')
                    country_code = props.get('country_code')
                    display = props.get('formatted') or props.get('address_line1') or props.get('name') or props.get('display_name')
                    if city and country_code:
                        normalized = f"{city}, {country_code.upper()}"
                    elif display:
                        normalized = display
        else:
            url = f"https://geocode.maps.co/search?q={quote_plus(loc)}"
            res = requests.get(url, timeout=5).json()
            if res:
                item = res[0]
                display = item.get('display_name')
                if display:
                    parts = [p.strip() for p in display.split(',')]
                    normalized = f"{parts[0]}, {parts[1]}" if len(parts) >= 2 else display
    except Exception:
        normalized = loc

    try:
        if redis_client:
            redis_client.set(cache_key, normalized, ex=86400)
    except Exception:
        pass

    return normalized


def resolve_location(loc):
    """Return a normalized location string and geographic coordinates for a free-text location."""
    if not loc:
        return loc, None
    loc = loc.strip()
    cache_key = f"resloc:{loc}"
    try:
        if redis_client and (cached := redis_client.get(cache_key)):
            val = json.loads(cached)
            name = val.get('name') or loc
            lat = val.get('lat')
            lon = val.get('lon')
            coords = {'lat': float(lat), 'lon': float(lon)} if lat is not None and lon is not None else None
            return name, coords
    except Exception:
        pass

    name = normalize_location(loc)
    coords = None
    api_key = app.config.get('GEOCODING_API_KEY', '')
    try:
        if api_key:
            url = f"https://api.geoapify.com/v1/geocode/search?text={quote_plus(loc)}&apiKey={api_key}"
            resp = requests.get(url, timeout=6)
            if resp.status_code == 200:
                payload = resp.json()
                features = payload.get('features') or []
                if features:
                    props = features[0].get('properties', {})
                    city = props.get('city') or props.get('town') or props.get('village') or props.get('county')
                    country_code = props.get('country_code')
                    lat = props.get('lat') or props.get('latitude')
                    lon = props.get('lon') or props.get('longitude')
                    display = props.get('formatted') or props.get('display_name') or props.get('name')
                    if city and country_code:
                        name = f"{city}, {country_code.upper()}"
                    elif display:
                        name = display
                    if lat is not None and lon is not None:
                        coords = {'lat': float(lat), 'lon': float(lon)}
        else:
            url = f"https://geocode.maps.co/search?q={quote_plus(loc)}"
            res = requests.get(url, timeout=6).json()
            if res:
                item = res[0]
                display = item.get('display_name')
                lat = item.get('lat')
                lon = item.get('lon')
                if display:
                    parts = [p.strip() for p in display.split(',')]
                    name = f"{parts[0]}, {parts[1]}" if len(parts) >= 2 else display
                if lat is not None and lon is not None:
                    coords = {'lat': float(lat), 'lon': float(lon)}
    except Exception:
        coords = None

    try:
        if redis_client:
            redis_client.set(cache_key, json.dumps({
                'name': name,
                'lat': coords.get('lat') if coords else None,
                'lon': coords.get('lon') if coords else None
            }), ex=86400)
    except Exception:
        pass

    return name, coords

# WebSocket clients
def add_client(tn, sid):
    try:
        if redis_client:
            redis_client.sadd(f"clients:{tn}", sid)
        else:
            in_memory_clients.setdefault(tn, set()).add(sid)
    except Exception as e:
        flask_logger.warning(f"Redis add_client failed: {e}")
        try:
            in_memory_clients.setdefault(tn, set()).add(sid)
        except Exception:
            pass

def remove_client(tn, sid):
    try:
        if redis_client:
            redis_client.srem(f"clients:{tn}", sid)
        else:
            in_memory_clients.get(tn, set()).discard(sid)
    except Exception as e:
        flask_logger.warning(f"Redis remove_client failed: {e}")
        try:
            in_memory_clients.get(tn, set()).discard(sid)
        except Exception:
            pass

def get_clients(tn):
    try:
        if redis_client:
            return redis_client.smembers(f"clients:{tn}") or set()
        return in_memory_clients.get(tn, set())
    except Exception as e:
        flask_logger.warning(f"Redis get_clients failed: {e}")
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
        try:
            notif = rlist_lpop("notifications")
        except Exception as e:
            flask_logger.error(f"Notification pop failed: {e}")
            notif = None
        if not notif:
            time.sleep(1)
            continue
        try:
            data = json.loads(notif)
            typ = data.get("type")
            d = data.get("data", {})
            if typ == "email":
                send_email_notification(
                    d.get("recipient_email"),
                    d.get("subject", "Shipment Update"),
                    d.get("html_body"),
                    d.get("plain_body")
                )
            elif typ == "webhook" and d.get("webhook_url"):
                try:
                    requests.post(d.get("webhook_url"), json={**d, "tracking_number": data.get("tracking_number")}, timeout=10)
                except Exception as e:
                    flask_logger.debug(f"Webhook notify failed in queue: {e}")
        except Exception as e:
            flask_logger.error(f"Queue error: {e}")

def cleanup_websocket_clients():
    while True:
        time.sleep(3600)
        try:
            if redis_client:
                for key in redis_client.scan_iter("clients:*"):
                    try:
                        if isinstance(key, bytes):
                            tn = key.decode().split(":", 1)[1]
                        else:
                            tn = str(key).split(":", 1)[1]
                        for sid in redis_client.smembers(key):
                            try:
                                socketio.emit('ping', room=sid)
                            except Exception:
                                remove_client(tn, sid)
                    except Exception:
                        continue
        except Exception as e:
            flask_logger.warning(f"cleanup_websocket_clients failed: {e}")

# === REALISTIC DISTANCE FUNCTION ===
def haversine_distance(lat1, lon1, lat2, lon2):
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    return round(6371 * 2 * atan2(sqrt(a), sqrt(1 - a)), 1)


def estimate_distance(origin, dest):
    if not origin or not dest:
        return 1000

    origin_norm, origin_coords = resolve_location(origin)
    dest_norm, dest_coords = resolve_location(dest)
    if origin_coords and dest_coords:
        return haversine_distance(
            origin_coords['lat'], origin_coords['lon'],
            dest_coords['lat'], dest_coords['lon']
        )

    city_coords = {
        "Lagos, NG": (6.5244, 3.3792), "Abuja, NG": (9.0579, 7.4951), "Port Harcourt, NG": (4.8156, 7.0498),
        "Kano, NG": (12.0001, 8.5167), "Ibadan, NG": (7.3775, 3.9470), "Enugu, NG": (6.4584, 7.5170),
        "New York, NY": (40.7128, -74.0060), "Los Angeles, CA": (34.0522, -118.2437), "London, UK": (51.5074, -0.1278),
        "Dubai, UAE": (25.2048, 55.2708), "Tokyo, JP": (35.6762, 139.6503), "Sydney, AU": (-33.8688, 151.2093),
        "Paris, FR": (48.8566, 2.3522), "Berlin, DE": (52.5200, 13.4050), "Mumbai, IN": (19.0760, 72.8777),
        "Singapore, SG": (1.3521, 103.8198), "Hong Kong, HK": (22.3193, 114.1694), "São Paulo, BR": (-23.5505, -46.6333),
        "Johannesburg, ZA": (-26.2041, 28.0473), "Cairo, EG": (30.0444, 31.2357), "Moscow, RU": (55.7558, 37.6173),
        "Toronto, CA": (43.6532, -79.3832), "Mexico City, MX": (19.4326, -99.1332), "Seoul, KR": (37.5665, 126.9780),
        "Bangkok, TH": (13.7563, 100.5018), "Jakarta, ID": (-6.2088, 106.8456), "Delhi, IN": (28.7041, 77.1025),
        "Beijing, CN": (39.9042, 116.4074), "Shanghai, CN": (31.2304, 121.4737), "Istanbul, TR": (41.0082, 28.9784),
        "Karachi, PK": (24.8607, 67.0011), "Buenos Aires, AR": (-34.6037, -58.3816), "Rio de Janeiro, BR": (-22.9068, -43.1729),
        "Lima, PE": (-12.0464, -77.0428), "Bogotá, CO": (4.7110, -74.0721), "Santiago, CL": (-33.4489, -70.6693),
        "Cape Town, ZA": (-33.9249, 18.4241), "Nairobi, KE": (-1.2921, 36.8219), "Accra, GH": (5.6037, -0.1870),
        "Addis Ababa, ET": (8.9806, 38.7578), "Kuala Lumpur, MY": (3.1390, 101.6869), "Hanoi, VN": (21.0285, 105.8342),
        "Manila, PH": (14.5995, 120.9842), "Taipei, TW": (25.0330, 121.5654), "Riyadh, SA": (24.7136, 46.6753),
        "Tel Aviv, IL": (32.0853, 34.7818), "Athens, GR": (37.9838, 23.7275), "Lisbon, PT": (38.7223, -9.1393),
        "Stockholm, SE": (59.3293, 18.0686), "Oslo, NO": (59.9139, 10.7522), "Helsinki, FI": (60.1699, 24.9384),
        "Warsaw, PL": (52.2297, 21.0122), "Prague, CZ": (50.0755, 14.4378), "Budapest, HU": (47.4979, 19.0402),
        "Vienna, AT": (48.2082, 16.3738), "Zurich, CH": (47.3769, 8.5417), "Amsterdam, NL": (52.3676, 4.9041),
        "Brussels, BE": (50.8476, 4.3572), "Dublin, IE": (53.3498, -6.2603), "Madrid, ES": (40.4168, -3.7038),
        "Rome, IT": (41.9028, 12.4964), "Milan, IT": (45.4642, 9.1900), "Barcelona, ES": (41.3851, 2.1734)
    }
    origin_lower = origin.lower()
    dest_lower = dest.lower()
    origin_key = next((k for k in city_coords if origin_lower in k.lower() or k.lower().startswith(origin_lower)), None)
    dest_key = next((k for k in city_coords if dest_lower in k.lower() or k.lower().startswith(dest_lower)), None)
    if not origin_key or not dest_key:
        return 1000
    lat1, lon1 = city_coords[origin_key]
    lat2, lon2 = city_coords[dest_key]
    return haversine_distance(lat1, lon1, lat2, lon2)

# === DHL REALISTIC SIMULATOR ===
class DHLRealisticSimulator:
    STATUS_CODES = {
        "Pending": "Shipment information received",
        "In_Transit": "In transit",
        "Out_for_Delivery": "Out for delivery",
        "Delivered": "Delivered",
        "Delayed": "Delayed",
        "Exception": "Exception - Contact DHL",
        "Customs_Clearance": "Customs clearance in progress",
        "Arrived_Destination": "Arrived at destination country",
        "Departed_Origin": "Departed origin facility"
    }

    REALISTIC_DELAYS = {
        "pickup": (30, 120),
        "sorting": (60, 240),
        "transit_local": (120, 600),
        "transit_international": (360, 2880),
        "customs": (180, 1440),
        "delivery": (60, 480),
        "delay_factor": (1.2, 3.0)
    }

    EVENT_MESSAGES = {
        "Pending": [
            "Shipment information received from shipper",
            "Electronic shipment data received",
            "Shipment details uploaded"
        ],
        "In_Transit": [
            "Processed at {location}",
            "Departed {location} facility",
            "Arrived at {location} sort facility",
            "Shipment in transit to {destination}",
            "Transferred through {location} hub",
            "In transit through {location}",
            "Processed through customs at {location}"
        ],
        "Out_for_Delivery": [
            "With delivery courier for final delivery",
            "Out for delivery from {location}",
            "Loaded onto delivery vehicle",
            "Scheduled for delivery today"
        ],
        "Delivered": [
            "Delivered successfully to recipient",
            "Signed by: {recipient}",
            "Delivered to {location} at {time}",
            "Proof of delivery available"
        ],
        "Delayed": [
            "Shipment delayed due to weather conditions at {location}",
            "Customs clearance delay at {location}",
            "Operational delay - rescheduled delivery",
            "Held for inspection at {location}",
            "Delay due to high shipment volume"
        ],
        "Exception": [
            "Shipment held - contact DHL for more information",
            "Delivery attempted - recipient not available",
            "Address issue - correction required",
            "Shipment damaged - inspection in progress"
        ]
    }

    SERVICE_LEVELS = {
        "DHL Express 9:00": {"premium": True, "delivery_window": "by 9:00 AM"},
        "DHL Express 12:00": {"premium": True, "delivery_window": "by 12:00 PM"},
        "DHL Express": {"premium": True, "delivery_window": "end of day"},
        "DHL Economy Select": {"premium": False, "delivery_window": "1-3 days"}
    }

    DHL_HUBS = {
        "Leipzig, DE": {"zone": "CET", "lat": 51.3397, "lon": 12.3731},
        "Hong Kong, HK": {"zone": "HKT", "lat": 22.3193, "lon": 114.1694},
        "Cincinnati, OH": {"zone": "EST", "lat": 39.1031, "lon": -84.5120},
        "Dubai, UAE": {"zone": "GST", "lat": 25.2048, "lon": 55.2708},
        "London, UK": {"zone": "GMT", "lat": 51.5074, "lon": -0.1278},
        "Frankfurt, DE": {"zone": "CET", "lat": 50.1109, "lon": 8.6821},
        "Singapore, SG": {"zone": "SGT", "lat": 1.3521, "lon": 103.8198},
        "Brussels, BE": {"zone": "CET", "lat": 50.8476, "lon": 4.3572},
        "Miami, FL": {"zone": "EST", "lat": 25.7617, "lon": -80.1918},
        "Tokyo, JP": {"zone": "JST", "lat": 35.6762, "lon": 139.6503}
    }

    @staticmethod
    def is_business_hours(dt):
        return 9 <= dt.hour < 18 and dt.weekday() < 5

    @staticmethod
    def get_service_level(distance, is_business):
        if distance < 500 and is_business:
            return random.choices(
                ["DHL Express 9:00", "DHL Express 12:00", "DHL Express"],
                weights=[0.2, 0.3, 0.5]
            )[0]
        elif distance < 2000:
            return random.choices(
                ["DHL Express", "DHL Economy Select"],
                weights=[0.7, 0.3]
            )[0]
        return "DHL Express"

    @staticmethod
    def get_delivery_window(service_level, distance):
        now = datetime.now()
        if service_level == "DHL Express 9:00":
            delivery_date = now + timedelta(days=1)
            return f"{delivery_date.strftime('%B %d')} by 9:00 AM"
        elif service_level == "DHL Express 12:00":
            delivery_date = now + timedelta(days=1)
            return f"{delivery_date.strftime('%B %d')} by 12:00 PM"
        elif distance < 500:
            delivery_date = now + timedelta(days=1)
            return f"{delivery_date.strftime('%B %d')} (end of day)"
        elif distance < 2000:
            delivery_date = now + timedelta(days=2)
            return f"{delivery_date.strftime('%B %d')} (end of day)"
        else:
            delivery_date = now + timedelta(days=3)
            return f"{delivery_date.strftime('%B %d')} (end of day)"

    @staticmethod
    def generate_pod_info():
        names = ["J. SMITH", "M. JOHNSON", "R. WILLIAMS", "A. BROWN", "T. DAVIS"]
        signatures = [
            f"Signature: {random.choice(names)}",
            f"Signed by: {random.choice(['Front desk', 'Reception', 'Security', random.choice(names)])}",
            f"Delivery confirmation: {random.randint(1000, 9999)}"
        ]
        return random.choice(signatures)

    @staticmethod
    def get_closest_hubs(coords, count=2):
        if not coords:
            return []
        hubs = sorted(
            DHLRealisticSimulator.DHL_HUBS.items(),
            key=lambda item: haversine_distance(coords['lat'], coords['lon'], item[1]['lat'], item[1]['lon'])
        )
        return [name for name, _ in hubs[:count]]

    @staticmethod
    def build_route_hubs(origin_coords, dest_coords, distance_km):
        if not origin_coords or not dest_coords or distance_km < 800:
            return []
        origin_hubs = DHLRealisticSimulator.get_closest_hubs(origin_coords, count=1)
        dest_hubs = DHLRealisticSimulator.get_closest_hubs(dest_coords, count=1)
        if distance_km < 2000:
            return [hub for hub in origin_hubs + dest_hubs if hub not in origin_hubs or hub not in dest_hubs]
        middle_hub = "Frankfurt, DE" if abs(origin_coords['lon']) < 60 and abs(dest_coords['lon']) < 60 else "Dubai, UAE"
        hubs = []
        for hub in origin_hubs + [middle_hub] + dest_hubs:
            if hub and hub not in hubs:
                hubs.append(hub)
        return hubs

    @staticmethod
    def generate_realistic_checkpoint(city, status, tracking_number, **kwargs):
        now = datetime.now()
        time_str = now.strftime("%Y-%m-%d %H:%M")
        events = DHLRealisticSimulator.EVENT_MESSAGES.get(status, ["Shipment processed"])
        event_template = random.choice(events)
        event = event_template.format(
            location=city,
            destination=kwargs.get('destination', city),
            recipient=kwargs.get('recipient', 'CUSTOMER'),
            time=now.strftime("%I:%M %p")
        )
        facility_code = f"DHL{random.randint(100, 999)}"
        tracking_id = f"{facility_code}-{tracking_number[-4:]}"
        temp_info = ""
        if random.random() < 0.15:
            temp_info = f" | Temp-controlled: {random.randint(2, 8)}°C" if random.random() < 0.5 else ""
        return f"{time_str} - {city} - {event} [Ref: {tracking_id}]{temp_info}"

    @staticmethod
    def estimate_realistic_delivery_time(origin, destination):
        distance = estimate_distance(origin, destination)
        if distance <= 500:
            base_time = 24
            factor = 1
        elif distance <= 2000:
            base_time = 48
            factor = 1.5
        elif distance <= 8000:
            base_time = 72
            factor = 2
        else:
            base_time = 120
            factor = 3
        customs_delay = random.randint(12, 48) if distance > 1000 and random.random() < 0.3 else 0
        total_hours = base_time * factor + customs_delay
        return timedelta(hours=total_hours), total_hours / 9 * 1.4

    @staticmethod
    def generate_pickup_location(origin):
        pickup_locations = {
            "Lagos, NG": ["Ikeja Industrial Zone", "Apapa Port Complex", "Victoria Island Business District"],
            "Abuja, NG": ["Central Business District", "Garki Industrial Area", "Wuse Commercial Zone"],
            "Dubai, UAE": ["Jebel Ali Free Zone", "Dubai Airport Freezone", "Business Bay"],
            "London, UK": ["Heathrow Cargo Area", "Canary Wharf", "London City Business Park"],
            "New York, NY": ["JFK Cargo Area", "Times Square District", "Brooklyn Industrial Zone"],
            "Sydney, AU": ["Sydney Airport Cargo", "Parramatta Industrial", "CBD Business District"]
        }
        locations = pickup_locations.get(origin, ["Industrial Zone", "Business District"])
        return random.choice(locations)

    @staticmethod
    def generate_delivery_location(destination):
        delivery_locations = {
            "Lagos, NG": ["Adeola Odeku Street, VI", "Bourdillon Road, Ikoyi", "Awolowo Road, Ikoyi"],
            "Abuja, NG": ["Gana Street, Maitama", "Lagos Street, Garki", "Mambilla Street, Wuse"],
            "Dubai, UAE": ["Sheikh Zayed Road, Dubai Marina", "Jumeirah Beach Road", "Al Barsha District"],
            "London, UK": ["Brick Lane, Shoreditch", "King's Road, Chelsea", "Oxford Street, Mayfair"],
            "New York, NY": ["5th Avenue, Manhattan", "Wall Street, Financial District", "Broadway, Soho"],
            "Sydney, AU": ["George Street, CBD", "Bondi Beach Road", "Kings Cross, Potts Point"]
        }
        locations = delivery_locations.get(destination, ["Main Street", "City Center"])
        return random.choice(locations)

# === SIMULATION FUNCTIONS ===
def track_metrics(tn, event_type, data):
    sim_logger.info(f"SIM_METRIC|{tn}|{event_type}|{json.dumps(data)}")
    if redis_client:
        key = f"metrics:{tn}"
        redis_client.lpush(key, json.dumps({
            "timestamp": datetime.now().isoformat(),
            "event": event_type,
            "data": data
        }))
        redis_client.ltrim(key, 0, 100)

def handle_exception(tn, shipment, reason):
    if not shipment:
        return
    exception_checkpoints = [
        f"{datetime.now():%Y-%m-%d %H:%M} - {shipment.delivery_location} - Delivery attempted - {reason}",
        f"{datetime.now():%Y-%m-%d %H:%M} - {shipment.delivery_location} - Address correction required",
        f"{datetime.now():%Y-%m-%d %H:%M} - {shipment.delivery_location} - Shipment held for inspection"
    ]
    checkpoint = random.choice(exception_checkpoints)
    shipment.status = "Exception"
    existing = shipment.checkpoints or ""
    shipment.checkpoints = f"{existing};{checkpoint}" if existing else checkpoint
    shipment.last_updated = datetime.now()
    db.session.commit()
    invalidate_cache(tn)
    enqueue_dhl_email(tn, "Exception", checkpoint, shipment.delivery_location)
    broadcast_update(tn)
    eventlet.sleep(3600)

# === EMAIL THROTTLING ===
def should_send_email(tn, status, checkpoints):
    if not AUTO_EMAIL_ENABLED:
        return False
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment or not shipment.recipient_email or not shipment.email_notifications:
        return False
    important_statuses = {"Pending", "In_Transit", "Out_for_Delivery", "Delivered", "Exception", "Delayed"}
    final_statuses = {"Delivered", "Exception"}
    if status not in important_statuses:
        return False
    if isinstance(checkpoints, str):
        checkpoints_list = [c for c in checkpoints.split(";") if c]
    else:
        checkpoints_list = [c for c in checkpoints if c]
    if not checkpoints_list:
        return False
    latest_checkpoint = checkpoints_list[-1].strip()
    digest = f"{status}:{latest_checkpoint}"

    client = get_redis_client()
    last_email_key = f"last_email:{tn}"
    digest_key = f"last_email_digest:{tn}"
    now = datetime.now()

    if client:
        try:
            current_digest = client.get(digest_key)
            if current_digest:
                current_digest = current_digest.decode() if isinstance(current_digest, bytes) else current_digest
                if current_digest == digest:
                    return False
        except Exception:
            pass
        if status not in final_statuses:
            try:
                last_sent = client.get(last_email_key)
                if last_sent:
                    last_time = datetime.fromisoformat(last_sent.decode() if isinstance(last_sent, bytes) else last_sent)
                    if now - last_time < timedelta(minutes=EMAIL_THROTTLE_MINUTES):
                        return False
            except Exception:
                pass
    else:
        current_digest_entry = email_digest_cache.get(tn)
        if current_digest_entry:
            current_digest, expiry = current_digest_entry
            if expiry and now < expiry and current_digest == digest:
                return False
        last_sent = email_throttle_cache.get(tn)
        if status not in final_statuses and last_sent and now - last_sent < timedelta(minutes=EMAIL_THROTTLE_MINUTES):
            return False

    if client:
        try:
            ttl = 86400 if status in final_statuses else EMAIL_THROTTLE_MINUTES * 60
            client.set(last_email_key, now.isoformat(), ex=ttl)
            client.set(digest_key, digest, ex=6 * 3600)
        except Exception:
            pass
    else:
        email_throttle_cache[tn] = now
        email_digest_cache[tn] = (digest, now + timedelta(hours=6))

    return True

def enhanced_full_simulate_tracking(tn):
    with app.app_context():
        shipment = Shipment.query.filter_by(tracking_number=tn).first()
        if not shipment:
            return
        origin = shipment.origin_location or "Lagos, NG"
        destination = shipment.delivery_location
        origin_norm, origin_coords = resolve_location(origin)
        destination_norm, dest_coords = resolve_location(destination)
        pickup_location = DHLRealisticSimulator.generate_pickup_location(origin_norm)
        delivery_address = DHLRealisticSimulator.generate_delivery_location(destination_norm)
        if redis_client:
            rset("pickup_location", tn, pickup_location)
            rset("delivery_address", tn, delivery_address)
        estimated_duration, _ = DHLRealisticSimulator.estimate_realistic_delivery_time(origin_norm, destination_norm)
        speed_multiplier = float(rget("sim_speed_multipliers", tn, "1.0") or "1.0")
        speed_multiplier = max(0.1, min(5.0, speed_multiplier))
        distance_km = estimate_distance(origin_norm, destination_norm)
        hubs = DHLRealisticSimulator.build_route_hubs(origin_coords, dest_coords, distance_km)
        route_template = [origin_norm] + hubs + [destination_norm]
        checkpoints = (shipment.checkpoints or "").split(";") if shipment.checkpoints else []
        current_status = shipment.status
        last_route_index = -1
        last_checkpoint_progress = 0.0
        sim_min_checkpoint_delta = float(os.getenv('SIM_MIN_CHECKPOINT_DELTA', '0.05') or '0.05')
        start_time = datetime.now()
        delivery_attempts = 0
        max_attempts = 3 if random.random() < 0.15 else 1
        stage = "pickup"
        sim_accel = float(os.getenv('SIM_ACCEL', '1.0') or '1.0')
        while datetime.now() - start_time < timedelta(days=10):
            if rget("paused_simulations", tn, "false") == "true":
                eventlet.sleep(10)
                continue
            try:
                elapsed_hours = (datetime.now() - start_time).total_seconds() / 3600
                total_hours = max(1, estimated_duration.total_seconds() / 3600)
                progress = min(elapsed_hours * speed_multiplier * sim_accel / total_hours, 1.0)
                if progress < 0.1:
                    stage = "pickup"
                    new_status = "Pending"
                elif progress < 0.35:
                    stage = "pickup"
                    new_status = "In_Transit"
                elif progress < 0.55:
                    stage = "transit"
                    new_status = "Customs_Clearance" if random.random() < 0.12 else "In_Transit"
                elif progress < 0.75:
                    stage = "transit"
                    new_status = "In_Transit"
                elif progress < 0.9:
                    stage = "delivery"
                    new_status = "Out_for_Delivery"
                else:
                    if delivery_attempts >= max_attempts:
                        new_status = "Delivered"
                        stage = "delivered"
                    elif random.random() < 0.15:
                        new_status = "Exception"
                        delivery_attempts += 1
                    else:
                        new_status = "Delivered"
                        stage = "delivered"

                try:
                    route_index = min(int(progress * len(route_template)), len(route_template) - 1)
                except Exception:
                    route_index = 0

                checkpoint = None
                if stage == "transit":
                    if route_index != last_route_index and (progress - last_checkpoint_progress) >= sim_min_checkpoint_delta:
                        city = route_template[route_index] if route_template else destination_norm
                        checkpoint = DHLRealisticSimulator.generate_realistic_checkpoint(
                            city, new_status, tn, destination=destination_norm
                        )
                        last_route_index = route_index
                        last_checkpoint_progress = progress
                    elif random.random() < 0.02 and (progress - last_checkpoint_progress) >= sim_min_checkpoint_delta:
                        city = route_template[route_index] if route_template else destination_norm
                        checkpoint = DHLRealisticSimulator.generate_realistic_checkpoint(
                            city, new_status, tn, destination=destination_norm
                        )
                        last_checkpoint_progress = progress
                elif new_status != current_status or random.random() < 0.12:
                    if stage == "pickup":
                        pickup_events = [
                            f"{datetime.now():%Y-%m-%d %H:%M} - {pickup_location} - Pickup request received from shipper",
                            f"{datetime.now():%Y-%m-%d %H:%M} - {pickup_location} - DHL courier en route for pickup",
                            f"{datetime.now():%Y-%m-%d %H:%M} - {pickup_location} - Package collected from shipper",
                            f"{datetime.now():%Y-%m-%d %H:%M} - {origin} - Shipment processed at DHL origin facility",
                            f"{datetime.now():%Y-%m-%d %H:%M} - {origin} - Package scanned and weighed at DHL facility"
                        ]
                        checkpoint = random.choice(pickup_events)
                    elif stage == "transit":
                        route_index = min(int(progress * len(route_template)), len(route_template) - 1)
                        city = route_template[route_index] if route_template else destination_norm
                        checkpoint = DHLRealisticSimulator.generate_realistic_checkpoint(
                            city, new_status, tn, destination=destination_norm
                        )
                    elif stage == "delivery":
                        delivery_events = [
                            f"{datetime.now():%Y-%m-%d %H:%M} - {destination} - Shipment arrived at destination DHL facility",
                            f"{datetime.now():%Y-%m-%d %H:%M} - {destination} - Package sorted for delivery route",
                            f"{datetime.now():%Y-%m-%d %H:%M} - {delivery_address} - Package loaded onto delivery vehicle",
                            f"{datetime.now():%Y-%m-%d %H:%M} - {delivery_address} - Out for delivery to recipient address",
                            f"{datetime.now():%Y-%m-%d %H:%M} - {delivery_address} - Delivery attempted - recipient not available"
                        ]
                        if new_status == "Delivered":
                            delivery_events.append(
                                f"{datetime.now():%Y-%m-%d %H:%M} - {delivery_address} - Delivered successfully - Signed by: {DHLRealisticSimulator.generate_pod_info()}"
                            )
                        checkpoint = random.choice(delivery_events)
                    if checkpoint and checkpoint not in checkpoints:
                        if (progress - last_checkpoint_progress) >= sim_min_checkpoint_delta:
                            checkpoints.append(checkpoint)
                            last_checkpoint_progress = progress
                        current_status = new_status
                        track_metrics(tn, "checkpoint_added", {
                            "status": current_status,
                            "stage": stage,
                            "checkpoint": checkpoint,
                            "progress": round(progress, 2)
                        })
                if current_status == "Exception":
                    handle_exception(tn, shipment, "delivery attempt failed")
                    track_metrics(tn, "exception", {
                        "status": current_status,
                        "reason": "delivery attempt failed",
                        "distance_km": distance_km,
                        "stage": stage
                    })
                    break
                shipment.status = current_status
                shipment.checkpoints = ";".join(checkpoints[-50:])
                shipment.last_updated = datetime.now()
                db.session.commit()

                try:
                    route_coords = []
                    for city_name in route_template:
                        hub = DHLRealisticSimulator.DHL_HUBS.get(city_name)
                        if hub:
                            route_coords.append({'lat': hub['lat'], 'lon': hub['lon']})
                        else:
                            _n, coords = resolve_location(city_name)
                            if coords:
                                route_coords.append(coords)
                    if not route_coords and origin_coords and dest_coords:
                        route_coords = [origin_coords, dest_coords]
                    try:
                        dens_km = float(os.getenv('SIM_ROUTE_DENSIFY_KM', '1.0') or '1.0')
                        route_coords = densify_route_coords(route_coords, dens_km)
                    except Exception:
                        pass

                    current_lat = None
                    current_lon = None
                    if route_coords and len(route_coords) >= 2:
                        segments = len(route_coords) - 1
                        frac = min(max(progress, 0.0), 1.0) * segments
                        seg_idx = min(int(frac), segments - 1)
                        local_frac = frac - seg_idx
                        a = route_coords[seg_idx]
                        b = route_coords[seg_idx + 1]
                        current_lat = a['lat'] + (b['lat'] - a['lat']) * local_frac
                        current_lon = a['lon'] + (b['lon'] - a['lon']) * local_frac
                    else:
                        if dest_coords:
                            current_lat = dest_coords.get('lat')
                            current_lon = dest_coords.get('lon')

                    try:
                        rset('progress', tn, str(progress))
                        rset('stage', tn, stage)
                        rset('current_location', tn, city if 'city' in locals() and city else (destination_norm or ''))
                        if current_lat is not None and current_lon is not None:
                            rset('current_lat', tn, str(current_lat))
                            rset('current_lon', tn, str(current_lon))
                    except Exception:
                        pass

                    sim_logger.info(f"SIM_UPDATE|{tn}|progress={progress:.3f}|route_idx={route_index if 'route_index' in locals() else -1}|lat={current_lat}|lon={current_lon}")
                except Exception:
                    pass

                shipment = reload_shipment(tn)
                if shipment:
                    checkpoints = (shipment.checkpoints or "").split(";") if shipment.checkpoints else checkpoints
                    current_status = shipment.status
                invalidate_cache(tn)
                if len(checkpoints) > 1:
                    if should_send_email(tn, current_status, checkpoints):
                        enqueue_dhl_email(tn, current_status, checkpoints[-1], destination)
                        track_metrics(tn, "email_sent", {
                            "status": current_status,
                            "checkpoint": checkpoints[-1],
                            "stage": stage
                        })
                broadcast_update(tn)
                if current_status in ["Delivered", "Returned"]:
                    final_checkpoint = f"{datetime.now():%Y-%m-%d %H:%M} - {delivery_address} - Delivery confirmed - Signed by: {DHLRealisticSimulator.generate_pod_info()}"
                    if final_checkpoint not in checkpoints:
                        checkpoints.append(final_checkpoint)
                        if shipment:
                            shipment.checkpoints = ";".join(checkpoints[-50:])
                            db.session.commit()
                            reset_db_session()
                            shipment = reload_shipment(tn)
                    break
                wait_seconds = random.uniform(15, 60) / speed_multiplier
                if DHLRealisticSimulator.is_business_hours(datetime.now()):
                    wait_seconds *= 0.7
                if stage in ["pickup", "delivery"]:
                    wait_seconds *= 0.5
                eventlet.sleep(min(max(wait_seconds, 5), 120))
            except Exception as e:
                sim_logger.error(f"Enhanced full simulation error for {tn}: {e}")
                reset_db_session()
                shipment = reload_shipment(tn)
                if not shipment:
                    sim_logger.error(f"Shipment {tn} no longer exists after error, aborting simulation")
                    break
                checkpoints = (shipment.checkpoints or "").split(";") if shipment.checkpoints else checkpoints
                current_status = shipment.status
                eventlet.sleep(30)
        if shipment.status not in ["Delivered", "Returned"]:
            shipment.status = "Delivered" if delivery_attempts < max_attempts else "Exception"
            shipment.last_updated = datetime.now()
            try:
                db.session.commit()
            except Exception as e:
                sim_logger.error(f"Final commit failed for {tn}: {e}")
                try:
                    db.session.rollback()
                except Exception:
                    pass
                reset_db_session()
            else:
                reset_db_session()
            invalidate_cache(tn)
            broadcast_update(tn)

def simulate_tracking(tn):
    with app.app_context():
        try:
            enhanced_full_simulate_tracking(tn)
        except Exception as e:
            sim_logger.error(f"Enhanced full simulation failed for {tn}: {e}")
            basic_simulate_tracking(tn)

def basic_simulate_tracking(tn):
    with app.app_context():
        shipment = Shipment.query.filter_by(tracking_number=tn).first()
        if not shipment:
            return
        carrier = shipment.carrier or "DHL"
    config = DHL_CONFIG if carrier == "DHL" else app.config.get('STATUS_TRANSITIONS', {})
    origin = shipment.origin_location or "Lagos, NG"
    destination = shipment.delivery_location
    origin_norm, origin_coords = resolve_location(origin)
    destination_norm, dest_coords = resolve_location(destination)
    distance_km = estimate_distance(origin_norm, destination_norm)
    default_mode = "air" if distance_km > 1000 else "ground"
    transport_mode = rget("transport_mode", tn, default_mode)
    transport_mode = (transport_mode or default_mode).lower()
    hubs = DHLRealisticSimulator.build_route_hubs(origin_coords, dest_coords, distance_km)
    if transport_mode == "air" and hubs:
        route_template = [origin_norm] + hubs + [destination_norm]
    else:
        route_template = [origin_norm, destination_norm] if not hubs else [origin_norm] + hubs + [destination_norm]
    checkpoints = (shipment.checkpoints or "").split(";") if shipment.checkpoints else []
    event_phrases = []
    for values in config.get("events", {}).values():
        if isinstance(values, list):
            event_phrases.extend(values)
        elif isinstance(values, str):
            event_phrases.append(values)
    current_idx = len([c for c in checkpoints if any(phrase in c for phrase in event_phrases)])
    start_time = datetime.now()
    if transport_mode == "air":
        base_hours = max(6, min(48, distance_km / 850))
    else:
        base_hours = max(24, min(120, distance_km / 90))
    speed_multiplier = float(rget("sim_speed_multipliers", tn, "1.0") or "1.0")
    speed_multiplier = max(0.1, min(10.0, speed_multiplier))
    while datetime.now() - start_time < timedelta(days=7):
        if rget("paused_simulations", tn, "false") == "true":
            eventlet.sleep(10)
            continue
        try:
            current_status = shipment.status
            if current_idx < len(route_template) and current_status not in ["Delivered", "Returned"]:
                city = route_template[current_idx]
                event_pool = config["events"].get(current_status, ["Processed at facility"])
                event = random.choice(event_pool)
                delay_note = ""
                if random.random() < 0.07:
                    delay_note = " | " + random.choice(["Customs clearance", "Weather delay", "High volume"])
                    eventlet.sleep(random.uniform(600, 1800) / speed_multiplier)
                checkpoint = f"{datetime.now():%Y-%m-%d %H:%M} - {city} - {event}{delay_note}"
                if checkpoint not in checkpoints:
                    checkpoints.append(checkpoint)
                    current_idx += 1
            transition = config.get("status_flow", {}).get(current_status, {})
            next_states = transition.get("next", [])
            if next_states and current_status not in ["Delivered", "Returned"]:
                probs = transition.get("probabilities", [1.0/len(next_states)]*len(next_states))
                new_status = random.choices(next_states, probs)[0]
                if new_status != current_status:
                    current_status = new_status
                    if new_status == "Delivered":
                        checkpoints.append(f"{datetime.now():%Y-%m-%d %H:%M} - {destination} - Delivered successfully")
                    elif new_status == "Returned":
                        checkpoints.append(f"{datetime.now():%Y-%m-%d %H:%M} - {origin} - Returned to shipper")
            shipment.status = current_status
            shipment.checkpoints = ";".join(checkpoints[-50:])
            shipment.last_updated = datetime.now()
            db.session.commit()
            invalidate_cache(tn)
            if len(checkpoints) > 1:
                if should_send_email(tn, current_status, checkpoints):
                    enqueue_dhl_email(tn, current_status, checkpoints[-1], destination)
            broadcast_update(tn)
            steps = max(1, len(route_template))
            base_sleep = base_hours * 3600 / steps / speed_multiplier
            eventlet.sleep(base_sleep * random.uniform(0.7, 1.3))
            if current_status in ["Delivered", "Returned"]:
                break
        except Exception as e:
            sim_logger.error(f"DHL Sim error {tn}: {e}")
            eventlet.sleep(30)

# === DHL EMAIL ===
def build_dhl_email_html(tn, status, latest_checkpoint, destination, service_level=None, delivery_window=None):
    location = latest_checkpoint.split(' - ')[1] if ' - ' in latest_checkpoint else destination
    service_text = f"Service: {service_level or 'DHL Express'}"
    delivery_info = ""
    if status in ["In_Transit", "Out_for_Delivery", "Delayed"]:
        if delivery_window:
            delivery_info = f"<p><strong>Estimated Delivery:</strong> {delivery_window}</p>"
        else:
            delivery_info = "<p><strong>Estimated Delivery:</strong> Pending</p>"
    hold_info = ""
    if status in ["On_Hold"]:
        hold_info = (
            "<p style='color:#b45309;'><strong>Important:</strong> Your shipment is on hold for customs "
            "clearance. Additional documentation or actions may be required. We will notify you when the "
            "shipment is released. If you have questions, please contact support.</p>"
        )
    return f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: auto; border: 1px solid #eee; border-radius: 8px; overflow: hidden;">
      <div style="background: #D40511; padding: 1rem; text-align: center;">
        <img src="{DHL_CONFIG['logo_url']}" alt="DHL" width="120">
      </div>
      <div style="padding: 1.5rem; background: #fff;">
        <h3 style="color: #D40511; margin-top: 0;">Shipment Update</h3>
        <p><strong>Waybill:</strong> <code style="background:#f5f5f5;padding:2px 6px;border-radius:4px;">{tn}</code></p>
        <p><strong>Status:</strong> <span style="color:#D40511;font-weight:bold;">{status}</span></p>
        <p><strong>Location:</strong> {location}</p>
        <p><strong>Destination:</strong> {destination}</p>
        {delivery_info}
        {hold_info}
        <p><strong>{service_text}</strong></p>
        <hr style="border:0;border-top:1px solid #eee;margin:1.5rem 0;">
        <div style="text-align: center;">
          <a href="{app.config['WEBSOCKET_SERVER']}/track/{tn}" style="background:#D40511;color:white;padding:12px 24px;text-decoration:none;border-radius:4px;display:inline-block;">
            Track Shipment
          </a>
        </div>
        <p style="font-size:0.9rem;color:#666;margin-top:1rem;">
          Need help? Contact DHL Express Support
        </p>
      </div>
      <div style="background:#FFCC00;padding:0.8rem;text-align:center;font-size:0.8rem;color:#000;">
        © {datetime.now().year} DHL International GmbH. All rights reserved.
      </div>
    </div>
    """

def enqueue_dhl_email(tn, status, latest_checkpoint, destination, service_level=None, delivery_window=None):
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment or not shipment.recipient_email or not shipment.email_notifications:
        return
    distance_km = estimate_distance(shipment.origin_location or "Lagos, NG", destination)
    service_level = service_level or DHLRealisticSimulator.get_service_level(
        distance_km, DHLRealisticSimulator.is_business_hours(datetime.now())
    )
    if delivery_window is None and status in ["In_Transit", "Out_for_Delivery", "Delayed"]:
        delivery_window = DHLRealisticSimulator.get_delivery_window(service_level, distance_km)
    location = latest_checkpoint.split(' - ')[1] if ' - ' in latest_checkpoint else destination
    subject = f"DHL Shipment {tn} - {status}"
    html_body = build_dhl_email_html(tn, status, latest_checkpoint, destination, service_level, delivery_window)
    plain_body = f"DHL Update: {tn}\nStatus: {status}\nLocation: {location}\nService: {service_level}\nEstimated Delivery: {delivery_window or 'Pending'}\nTrack: {app.config['WEBSOCKET_SERVER']}/track/{tn}"
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

# === EMAIL SENDER ===
def send_email_notification(recipient, subject, html_body=None, plain_body=None, tracking_number=None, email_type=None, message=None):
    if EMAIL_TEST_MODE:
        flask_logger.info(f"📧 TEST MODE - Email would be sent to: {recipient}")
        flask_logger.info(f"   Subject: {subject}")
        flask_logger.info(f"   Tracking: {tracking_number}")
        return True
    if not EMAIL_ENABLED:
        flask_logger.info(f"📧 Email disabled - would send to {recipient}: {subject}")
        return True
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
            if tracking_number:
                try:
                    history_key = f"email_history:{tracking_number}"
                    entry = {
                        'timestamp': datetime.now().isoformat(),
                        'type': email_type or 'status_update',
                        'recipient': recipient,
                        'subject': subject,
                        'message': message
                    }
                    if redis_client:
                        redis_client.lpush(history_key, json.dumps(entry))
                        redis_client.ltrim(history_key, 0, 99)
                except Exception as history_exc:
                    flask_logger.warning('Failed to store email history for %s: %s', tracking_number, history_exc)
            return True
        except Exception as e:
            flask_logger.error(f"Email attempt {attempt+1} failed: {e}")
            if attempt == max_retries - 1:
                console.print(Panel(f"[error]Failed to send email to {recipient}[/error]", title="Email Error"))
                return False
            time.sleep(2 ** attempt)
    return False

# === BROADCAST UPDATE ===
def broadcast_update(tn):
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return
    speed = float(rget("sim_speed_multipliers", tn, "1.0") or "1.0")
    paused = rget("paused_simulations", tn, "false") == "true"
    try:
        coords = geocode_locations((shipment.checkpoints or "").split(";"))
        route_coords = build_route_from_checkpoints(coords, mode='drive')
        try:
            dens_km = float(os.getenv('SIM_ROUTE_DENSIFY_KM', '1.0') or '1.0')
            route_coords = densify_route_coords(route_coords, dens_km)
        except Exception:
            pass
    except Exception as e:
        flask_logger.warning(f"Geocoding failed for {tn}: {e}")
        coords = []
        route_coords = []
    progress = float(rget("progress", tn, "0") or "0")
    service_level = rget("service_level", tn, "DHL Express") or "DHL Express"
    delivery_window = rget("delivery_window", tn, "Calculating...") or "Calculating..."
    proof_of_delivery = rget("proof_of_delivery", tn, "Pending") or "Pending"
    data = {
        "tracking_number": tn,
        "status": shipment.status,
        "delivery_location": shipment.delivery_location,
        "checkpoints": (shipment.checkpoints or "").split(";"),
        "coords": [{'lat': c['lat'], 'lon': c['lon'], 'desc': c['desc']} for c in coords],
        "route_coords": route_coords,
        "last_updated": shipment.last_updated.isoformat(),
        "progress": progress,
        "service_level": service_level,
        "delivery_window": delivery_window,
        "proof_of_delivery": proof_of_delivery,
        "speed_multiplier": speed,
        "paused": paused,
        "carrier": shipment.carrier
    }
    try:
        socketio.emit('tracking_update', data, broadcast=True, namespace='/')
    except TypeError:
        try:
            socketio.emit('tracking_update', data, namespace='/')
        except Exception as e:
            flask_logger.warning(f"Socket emit failed for {tn}: {e}")
    webhook_url = f"{app.config['WEBSOCKET_SERVER']}/notify"
    if 'localhost' not in webhook_url and '127.0.0.1' not in webhook_url:
        try:
            requests.post(webhook_url, json=data, timeout=2)
        except Exception as e:
            flask_logger.debug(f"Webhook notify skipped for {tn}: {e}")

# Admin decorator
def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated

# === FAVICON ROUTE ===
@app.route('/favicon.ico')
def favicon():
    try:
        return redirect('https://www.dhl.com/favicon.ico')
    except:
        return '', 204

# === PUBLIC ROUTES ===
@app.route('/')
def index():
    form = TrackForm()
    recaptcha_key = app.config.get('RECAPTCHA_SITE_KEY', '')
    host = request.host or ''
    if app.debug or app.config.get('FLASK_ENV') == 'development' or 'your-site-key' in (recaptcha_key or '') or 'localhost' in host or '127.0.0.1' in host:
        recaptcha_key = ''
    return render_template('index.html', form=form, tawk_property_id=app.config['TAWK_PROPERTY_ID'],
                           tawk_widget_id=app.config['TAWK_WIDGET_ID'], recaptcha_site_key=recaptcha_key)

def _render_tracking_response(rendered_html, status_code=200):
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return jsonify({'html': rendered_html}), status_code
    return rendered_html, status_code

@app.route('/track', methods=['POST'])
@limiter.limit("10 per minute")
def track():
    try:
        from forms import TrackForm as F
        form = F()
    except:
        form = TrackForm()
    if not form.validate_on_submit():
        if app.testing or request.form.get('submit') == 'Track' or request.form.get('tracking_number'):
            form.tracking_number.data = request.form.get('tracking_number', '')
            form.email.data = request.form.get('email', '')
        else:
            return _render_tracking_response(render_template('tracking_result.html', error='Invalid form submission', coords=[]), 400)
    
    # reCAPTCHA REMOVED - Already on main page
    # recaptcha = request.form.get('g-recaptcha-response')
    # if app.config['RECAPTCHA_SITE_KEY'] and 'your-site-key' not in app.config['RECAPTCHA_SITE_KEY']:
    #     if not verify_recaptcha(recaptcha):
    #         return _render_tracking_response(render_template('tracking_result.html', error='reCAPTCHA failed', coords=[]), 400)
    
    tn = sanitize_tracking_number(form.tracking_number.data)
    email = form.email.data
    if not tn:
        return _render_tracking_response(render_template('tracking_result.html', error='Invalid tracking number', coords=[]), 400)
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return _render_tracking_response(render_template('tracking_result.html', error='Not found', coords=[]), 404)
    if email and validate_email(email):
        shipment.recipient_email = email
        db.session.commit()
        invalidate_cache(tn)
    checkpoints = (shipment.checkpoints or "").split(";")
    coords = geocode_locations(checkpoints)
    coords_list = [{'lat': c['lat'], 'lon': c['lon'], 'desc': c['desc']} for c in coords]
    route_coords = build_route_from_checkpoints(coords_list, mode='drive')
    try:
        dens_km = float(os.getenv('SIM_ROUTE_DENSIFY_KM', '1.0') or '1.0')
        route_coords = densify_route_coords(route_coords, dens_km)
    except Exception:
        pass
    distance_km = estimate_distance(shipment.origin_location or "Lagos, NG", shipment.delivery_location)
    service_level = DHLRealisticSimulator.get_service_level(
        distance_km, DHLRealisticSimulator.is_business_hours(datetime.now())
    )
    delivery_window = DHLRealisticSimulator.get_delivery_window(service_level, distance_km)
    proof_of_delivery = DHLRealisticSimulator.generate_pod_info()
    if shipment.status not in ['Delivered', 'Returned']:
        try:
            spawn_simulation(tn)
        except Exception:
            try:
                eventlet.spawn(simulate_tracking, tn)
            except Exception:
                threading.Thread(target=simulate_tracking, args=(tn,), daemon=True).start()
    progress = float(rget('progress', tn, '0') or '0')
    
    # Check if this is an AJAX request
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        result_html = render_template(
            'tracking_result_content.html',
            shipment=shipment,
            checkpoints=checkpoints,
            coords=coords_list,
            route_coords=route_coords,
            service_level=service_level,
            delivery_window=delivery_window,
            proof_of_delivery=proof_of_delivery,
            progress=progress
        )
        return jsonify({'html': result_html})
    
    rendered = render_template(
        'tracking_result.html', shipment=shipment, checkpoints=checkpoints, coords=coords_list,
        route_coords=route_coords, service_level=service_level, delivery_window=delivery_window,
        proof_of_delivery=proof_of_delivery, progress=progress,
        tawk_property_id=app.config['TAWK_PROPERTY_ID'], tawk_widget_id=app.config['TAWK_WIDGET_ID']
    )
    return _render_tracking_response(rendered, 200)

@app.route('/track/<tracking_number>')
def track_direct(tracking_number):
    tn = sanitize_tracking_number(tracking_number)
    if not tn:
        return render_template('tracking_result.html', error='Invalid tracking number', coords=[])
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return render_template('tracking_result.html', error='Not found', coords=[])
    checkpoints = (shipment.checkpoints or "").split(";")
    coords = geocode_locations(checkpoints)
    coords_list = [{'lat': c['lat'], 'lon': c['lon'], 'desc': c['desc']} for c in coords]
    route_coords = build_route_from_checkpoints(coords_list, mode='drive')
    try:
        dens_km = float(os.getenv('SIM_ROUTE_DENSIFY_KM', '1.0') or '1.0')
        route_coords = densify_route_coords(route_coords, dens_km)
    except Exception:
        pass
    distance_km = estimate_distance(shipment.origin_location or "Lagos, NG", shipment.delivery_location)
    service_level = DHLRealisticSimulator.get_service_level(
        distance_km, DHLRealisticSimulator.is_business_hours(datetime.now())
    )
    delivery_window = DHLRealisticSimulator.get_delivery_window(service_level, distance_km)
    proof_of_delivery = DHLRealisticSimulator.generate_pod_info()
    if shipment.status not in ['Delivered', 'Returned']:
        try:
            spawn_simulation(tn)
        except Exception:
            try:
                eventlet.spawn(simulate_tracking, tn)
            except Exception:
                threading.Thread(target=simulate_tracking, args=(tn,), daemon=True).start()
    progress = float(rget('progress', tn, '0') or '0')
    return render_template(
        'tracking_result.html', shipment=shipment, checkpoints=checkpoints, coords=coords_list,
        route_coords=route_coords, service_level=service_level, delivery_window=delivery_window,
        proof_of_delivery=proof_of_delivery, progress=progress,
        tawk_property_id=app.config['TAWK_PROPERTY_ID'], tawk_widget_id=app.config['TAWK_WIDGET_ID']
    )

@app.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    try:
        bot = get_bot()
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({'error': 'Invalid JSON payload'}), 400
        update = types.Update.de_json(data)
        bot.process_new_updates([update])
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        flask_logger.exception('Telegram webhook processing failed: %s', e)
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health_check():
    status = {'status': 'healthy', 'database': 'ok', 'redis': 'ok', 'smtp': 'ok'}
    try:
        db.session.execute(text('SELECT 1'))
    except Exception as e:
        status['status'] = status['database'] = 'error'
        flask_logger.exception("Health check database failed: %s", e)
    try:
        if redis_client:
            redis_client.ping()
        else:
            status['redis'] = 'unavailable'
    except Exception as e:
        status['redis'] = 'error'
        flask_logger.exception("Health check redis failed: %s", e)
    try:
        with smtplib.SMTP(app.config['SMTP_HOST'], app.config['SMTP_PORT'], timeout=5) as s:
            s.starttls()
            s.login(app.config['SMTP_USER'], app.config['SMTP_PASS'])
    except Exception as e:
        status['smtp'] = 'error'
        flask_logger.exception("Health check smtp failed: %s", e)
    return jsonify(status), 200 if status['status'] == 'healthy' else 500

@app.route('/debug')
def debug_info():
    status = {
        'app_debug': app.debug,
        'flask_env': app.config.get('FLASK_ENV'),
        'services_started': services_started,
        'database': 'unknown',
        'redis': 'unknown',
        'smtp': 'unknown',
        'bot': 'unknown',
        'webhook_url': config.webhook_url,
        'webhook_configured': bool(config.webhook_url),
        'redis_configured': bool(config.redis_url),
        'smtp_configured': bool(app.config.get('SMTP_HOST') and app.config.get('SMTP_USER') and app.config.get('SMTP_PASS'))
    }
    try:
        db.session.execute(text('SELECT 1'))
        status['database'] = 'ok'
    except Exception as e:
        status['database'] = 'error'
        flask_logger.exception("Debug database check failed: %s", e)
    try:
        if redis_client:
            redis_client.ping()
            status['redis'] = 'ok'
        else:
            status['redis'] = 'unavailable'
    except Exception as e:
        status['redis'] = 'error'
        flask_logger.exception("Debug redis check failed: %s", e)
    try:
        if status['smtp_configured']:
            with smtplib.SMTP(app.config['SMTP_HOST'], app.config['SMTP_PORT'], timeout=5) as s:
                s.starttls()
                s.login(app.config['SMTP_USER'], app.config['SMTP_PASS'])
            status['smtp'] = 'ok'
        else:
            status['smtp'] = 'unconfigured'
    except Exception as e:
        status['smtp'] = 'error'
        flask_logger.exception("Debug smtp check failed: %s", e)
    try:
        webhook_info = bot.get_webhook_info()
        status['bot'] = {
            'class': bot.__class__.__name__,
            'webhook_url': getattr(webhook_info, 'url', None)
        }
    except Exception as e:
        status['bot'] = 'error'
        flask_logger.exception("Debug bot webhook info failed: %s", e)
    debug_config = {
        'SQLALCHEMY_DATABASE_URI': app.config.get('SQLALCHEMY_DATABASE_URI'),
        'WEBSOCKET_SERVER': app.config.get('WEBSOCKET_SERVER'),
        'GLOBAL_WEBHOOK_URL': app.config.get('GLOBAL_WEBHOOK_URL'),
        'WEBHOOK_URL': config.webhook_url,
        'ALLOWED_ADMINS': config.allowed_admins,
        'VALID_STATUSES': config.valid_statuses,
        'SMTP_HOST': app.config.get('SMTP_HOST'),
        'SMTP_PORT': app.config.get('SMTP_PORT'),
        'SMTP_FROM': app.config.get('SMTP_FROM'),
        'GEOCODING_API_KEY_SET': bool(app.config.get('GEOCODING_API_KEY')),
        'TAWK_PROPERTY_ID': app.config.get('TAWK_PROPERTY_ID'),
        'TAWK_WIDGET_ID': app.config.get('TAWK_WIDGET_ID')
    }
    return jsonify({'status': status, 'config': debug_config})

# === ADMIN ROUTES ===
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

@app.route('/admin/metrics')
@admin_required
def admin_metrics():
    metrics = {}
    try:
        active_keys = rkeys("clients:*")
        metrics['active_simulations'] = len(active_keys)
        speeds = rhgetall("sim_speed_multipliers") or {}
        if speeds:
            try:
                metrics['avg_speed'] = round(sum(float(v) for v in speeds.values()) / len(speeds), 2)
            except Exception:
                metrics['avg_speed'] = 0.0
        else:
            metrics['avg_speed'] = 0.0
        metrics['paused_simulations'] = rhlen("paused_simulations") if rexists("paused_simulations") else 0
    except Exception:
        metrics['active_simulations'] = 0
        metrics['avg_speed'] = 0.0
        metrics['paused_simulations'] = 0
    try:
        statuses = Shipment.query.with_entities(
            Shipment.status, db.func.count()
        ).group_by(Shipment.status).all()
        metrics['status_distribution'] = {s: c for s, c in statuses}
    except Exception:
        metrics['status_distribution'] = {}
    return jsonify(metrics)

# ============================================================
# FIXED ADMIN DASHBOARD - Uses direct database queries
# ============================================================
@app.route('/admin')
@admin_required
def admin_dashboard():
    page = int(request.args.get('page', 1))
    per_page = 10
    
    try:
        total = Shipment.query.count()
        flask_logger.info(f"Total shipments found: {total}")
        
        shipments_query = Shipment.query.order_by(
            Shipment.created_at.desc()
        ).paginate(page=page, per_page=per_page, error_out=False)
        
        shipments_data = []
        for s in shipments_query.items:
            try:
                paused = False
                speed = 1.0
                mode = "ground"
                progress = 0
                stage = "pickup"
                service_level = "DHL Express"
                delivery_window = "Calculating..."
                proof_of_delivery = "Pending"
                
                if redis_client:
                    try:
                        paused = rget("paused_simulations", s.tracking_number, "false") == "true"
                        speed = float(rget("sim_speed_multipliers", s.tracking_number, "1.0") or "1.0")
                        mode = rget("transport_mode", s.tracking_number, "ground") or "ground"
                        progress = float(rget("progress", s.tracking_number, "0") or "0")
                        stage = rget("stage", s.tracking_number, "pickup") or "pickup"
                        
                        service_level = rget("service_level", s.tracking_number, "DHL Express") or "DHL Express"
                        delivery_window = rget("delivery_window", s.tracking_number, "Calculating...") or "Calculating..."
                        proof_of_delivery = rget("proof_of_delivery", s.tracking_number, "Pending") or "Pending"
                    except Exception as redis_err:
                        flask_logger.warning(f"Redis error for {s.tracking_number}: {redis_err}")
                
                if isinstance(stage, bytes):
                    stage = stage.decode('utf-8')
                if isinstance(mode, bytes):
                    mode = mode.decode('utf-8')
                
                shipments_data.append({
                    'tracking_number': s.tracking_number,
                    'status': s.status,
                    'delivery_location': s.delivery_location,
                    'origin_location': s.origin_location,
                    'last_updated': s.last_updated.strftime("%Y-%m-%d %H:%M"),
                    'paused': paused,
                    'speed': f"{speed:.1f}x",
                    'mode': mode,
                    'carrier': s.carrier or 'DHL',
                    'service_level': service_level,
                    'delivery_window': delivery_window,
                    'proof_of_delivery': proof_of_delivery,
                    'recipient_email': s.recipient_email,
                    'progress_percent': progress * 100,
                    'stage': stage,
                    'email_notifications': s.email_notifications
                })
            except Exception as row_err:
                flask_logger.error(f"Error processing shipment {s.tracking_number}: {row_err}")
                continue
        
        total_pages = (total - 1) // per_page + 1 if total > 0 else 1
        
        return render_template('admin_dashboard.html',
                               total=total,
                               queue_len=redis_client.llen("notifications") if redis_client else 0,
                               active_clients=len(redis_client.keys("clients:*")) if redis_client else 0,
                               shipments=shipments_data,
                               page=page,
                               total_pages=total_pages,
                               now=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
    except Exception as e:
        flask_logger.error(f"Error in admin_dashboard: {e}")
        return render_template('admin_dashboard.html',
                               total=0,
                               queue_len=0,
                               active_clients=0,
                               shipments=[],
                               page=1,
                               total_pages=1,
                               error=str(e),
                               now=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))

@app.route('/admin/csv')
@admin_required
def admin_csv():
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Tracking Number", "Status", "Origin", "Destination", "Email", "Carrier", "Service Level", "Delivery Window", "Proof of Delivery", "Last Updated", "Created At"])
    for s in Shipment.query.order_by(Shipment.created_at.desc()).all():
        distance_km = estimate_distance(s.origin_location or "Lagos, NG", s.delivery_location)
        service_level = DHLRealisticSimulator.get_service_level(
            distance_km, DHLRealisticSimulator.is_business_hours(datetime.now())
        )
        delivery_window = DHLRealisticSimulator.get_delivery_window(service_level, distance_km)
        proof_of_delivery = DHLRealisticSimulator.generate_pod_info()
        writer.writerow([s.tracking_number, s.status, s.origin_location or "-", s.delivery_location,
                         s.recipient_email or "-", s.carrier, service_level, delivery_window, proof_of_delivery,
                         s.last_updated.strftime("%Y-%m-%d %H:%M"), s.created_at.strftime("%Y-%m-%d %H:%M")])
    output.seek(0)
    return Response(output, mimetype="text/csv", headers={"Content-Disposition": f"attachment;filename=shipments_{datetime.utcnow().strftime('%Y%m%d')}.csv"})

def generate_dhl_tracking():
    prefix = "JD"
    digits = ''.join(random.choices(string.digits, k=10))
    return f"{prefix}{digits}"

# ============================================================
# ADMIN API ENDPOINTS
# ============================================================

@app.route('/admin/api/shipment/<tn>')
@admin_required
def api_shipment_detail(tn):
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return jsonify({'error': 'Not found'}), 404
    
    speed = float(rget("sim_speed_multipliers", tn, "1.0") or "1.0")
    paused = rget("paused_simulations", tn, "false") == "true"
    mode = rget("transport_mode", tn, "ground") or "ground"
    delivery_attempt = int(rget("delivery_attempts", tn, "0") or "0")
    max_attempts = int(rget("max_attempts", tn, "3") or "3")
    progress = float(rget("progress", tn, "0") or "0")
    checkpoints = (shipment.checkpoints or "").split(";") if shipment.checkpoints else []
    
    service_level = rget("service_level", tn, "DHL Express") or "DHL Express"
    delivery_window = rget("delivery_window", tn, "") or ""
    proof_of_delivery = rget("proof_of_delivery", tn, "") or ""
    temperature = rget("temperature", tn, None)
    current_lat = rget('current_lat', tn, None)
    current_lon = rget('current_lon', tn, None)
    
    return jsonify({
        'tracking_number': shipment.tracking_number,
        'status': shipment.status,
        'origin_location': shipment.origin_location,
        'origin_lat': shipment.origin_lat,
        'origin_lon': shipment.origin_lon,
        'delivery_location': shipment.delivery_location,
        'delivery_lat': shipment.delivery_lat,
        'delivery_lon': shipment.delivery_lon,
        'carrier': shipment.carrier,
        'recipient_email': shipment.recipient_email,
        'checkpoints': checkpoints,
        'last_updated': shipment.last_updated.isoformat(),
        'speed_multiplier': speed,
        'paused': paused,
        'mode': mode,
        'delivery_attempt': delivery_attempt,
        'max_attempts': max_attempts,
        'service_level': service_level,
        'delivery_window': delivery_window,
        'proof_of_delivery': proof_of_delivery,
        'temperature': temperature,
        'progress': progress,
        'current_lat': float(current_lat) if current_lat is not None else None,
        'current_lon': float(current_lon) if current_lon is not None else None
    })


def purge_shipment_cache(tn):
    if not redis_client or not tn:
        return
    try:
        redis_client.hdel(
            'paused_simulations', tn,
            'sim_speed_multipliers', tn,
            'transport_mode', tn,
            'delivery_attempts', tn,
            'max_attempts', tn,
            'progress', tn,
            'stage', tn,
            'delivery_window', tn,
            'proof_of_delivery', tn,
            'service_level', tn
        )
        redis_client.delete(f'email_history:{tn}', f'clients:{tn}')
    except Exception:
        pass


def reset_db_session():
    try:
        db.session.rollback()
    except Exception:
        pass
    try:
        db.session.remove()
    except Exception:
        pass


def reload_shipment(tn):
    if not tn:
        return None
    reset_db_session()
    try:
        return Shipment.query.filter_by(tracking_number=tn).first()
    except Exception:
        return None

@app.route('/admin/api/shipment/<tn>/update', methods=['POST'])
@admin_required
def api_shipment_update(tn):
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return jsonify({'error': 'Not found'}), 404

    data = request.get_json() or {}
    editable = {
        'status', 'stage', 'service_level', 'delivery_window', 'proof_of_delivery',
        'recipient_email', 'delivery_location', 'paused', 'speed', 'email_notifications', 'checkpoints'
    }

    updated = {}
    try:
        for k, v in data.items():
            if k not in editable:
                continue
            if k == 'paused':
                if redis_client:
                    rset('paused_simulations', tn, 'true' if v else 'false')
                updated['paused'] = bool(v)
                continue
            if k == 'speed':
                try:
                    speed = float(v)
                except Exception:
                    speed = 1.0
                if redis_client:
                    rset('sim_speed_multipliers', tn, str(speed))
                updated['speed'] = speed
                continue
            if k == 'service_level' and redis_client:
                rset('service_level', tn, v)
            if k == 'delivery_window' and redis_client:
                rset('delivery_window', tn, v)
            if k == 'proof_of_delivery' and redis_client:
                rset('proof_of_delivery', tn, v)

            if k == 'checkpoints':
                if isinstance(v, list):
                    shipment.checkpoints = ';'.join(v)
                    updated['checkpoints'] = v
                continue

            if hasattr(shipment, k):
                if k in ('delivery_location', 'origin_location') and v:
                    name, coords = resolve_location(v)
                    v = name
                    if coords:
                        if k == 'delivery_location':
                            shipment.delivery_lat = coords.get('lat')
                            shipment.delivery_lon = coords.get('lon')
                        else:
                            shipment.origin_lat = coords.get('lat')
                            shipment.origin_lon = coords.get('lon')
                    else:
                        return jsonify({'error': f'Could not resolve location for {k}'}), 400
                setattr(shipment, k, v)
                updated[k] = v

        shipment.last_updated = datetime.now()
        db.session.commit()
        invalidate_cache(tn)
        try:
            broadcast_update(tn)
        except Exception:
            pass
        return jsonify({'success': True, 'updated': updated})
    except Exception as e:
        flask_logger.exception('Failed to update shipment %s: %s', tn, e)
        return jsonify({'error': str(e)}), 500

@app.route('/admin/api/shipment/<tn>/delete', methods=['POST'])
@admin_required
def api_delete_shipment(tn):
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return jsonify({'error': 'Not found'}), 404
    try:
        db.session.delete(shipment)
        db.session.commit()
        purge_shipment_cache(tn)
        return jsonify({'success': True, 'deleted': tn})
    except Exception as e:
        db.session.rollback()
        flask_logger.exception('Failed to delete shipment %s: %s', tn, e)
        return jsonify({'error': 'Failed to delete shipment'}), 500

@app.route('/admin/api/cities')
@admin_required
def api_cities():
    cities = sorted(list(DHLRealisticSimulator.DHL_HUBS.keys()) + [
        "Lagos, NG", "Abuja, NG", "Port Harcourt, NG", "Kano, NG", "Ibadan, NG",
        "New York, NY", "Los Angeles, CA", "London, UK", "Dubai, UAE",
        "Tokyo, JP", "Sydney, AU", "Paris, FR", "Berlin, DE", "Mumbai, IN",
        "Singapore, SG", "Hong Kong, HK", "São Paulo, BR", "Johannesburg, ZA",
        "Cairo, EG", "Moscow, RU", "Toronto, CA", "Mexico City, MX", "Seoul, KR",
        "Bangkok, TH", "Jakarta, ID", "Delhi, IN", "Beijing, CN", "Shanghai, CN",
        "Istanbul, TR", "Karachi, PK", "Buenos Aires, AR", "Rio de Janeiro, BR",
        "Tel Aviv, IL", "Athens, GR", "Lisbon, PT", "Stockholm, SE", "Oslo, NO",
        "Helsinki, FI", "Warsaw, PL", "Prague, CZ", "Budapest, HU", "Vienna, AT",
        "Zurich, CH", "Amsterdam, NL", "Brussels, BE", "Dublin, IE", "Madrid, ES",
        "Rome, IT", "Milan, IT", "Barcelona, ES", "Cincinnati, OH", "Miami, FL",
        "Frankfurt, DE", "Leipzig, DE"
    ])
    return jsonify(cities)

def create_shipment_record(origin, destination, recipient_email=None, service_level='DHL Express'):
    if not origin or not destination:
        return {'error': 'Origin and destination required'}, 400

    valid_service_levels = set(DHLRealisticSimulator.SERVICE_LEVELS.keys())
    if service_level not in valid_service_levels:
        return {'error': 'Invalid service_level', 'allowed': sorted(valid_service_levels)}, 400

    if recipient_email and not validate_email(recipient_email):
        return {'error': 'Invalid recipient_email'}, 400

    tracking_number = generate_dhl_tracking()
    while Shipment.query.filter_by(tracking_number=tracking_number).first():
        tracking_number = generate_dhl_tracking()

    now = datetime.now()
    norm_origin, origin_coords = resolve_location(origin)
    norm_destination, dest_coords = resolve_location(destination)

    if not origin_coords or not dest_coords:
        return {'error': 'Unable to resolve origin or destination to geographic coordinates'}, 400

    checkpoints = f"{now.strftime('%Y-%m-%d %H:%M')} - {norm_origin} - Shipment information received"

    shipment = Shipment(
        tracking_number=tracking_number,
        status='Pending',
        checkpoints=checkpoints,
        origin_location=norm_origin,
        origin_lat=origin_coords.get('lat') if origin_coords else None,
        origin_lon=origin_coords.get('lon') if origin_coords else None,
        delivery_location=norm_destination,
        delivery_lat=dest_coords.get('lat') if dest_coords else None,
        delivery_lon=dest_coords.get('lon') if dest_coords else None,
        last_updated=now,
        recipient_email=recipient_email or '',
        created_at=now,
        carrier='DHL',
        email_notifications=bool(recipient_email)
    )

    try:
        db.session.add(shipment)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        flask_logger.error(f"Failed to save shipment {tracking_number}: {e}")
        return {'error': 'Failed to save shipment to database'}, 500

    distance = None
    try:
        if origin_coords and dest_coords:
            lat1, lon1 = origin_coords['lat'], origin_coords['lon']
            lat2, lon2 = dest_coords['lat'], dest_coords['lon']
            from math import radians, sin, cos, sqrt, atan2
            rlat1, rlon1, rlat2, rlon2 = map(radians, (lat1, lon1, lat2, lon2))
            dlon = rlon2 - rlon1
            dlat = rlat2 - rlat1
            a = sin(dlat/2)**2 + cos(rlat1) * cos(rlat2) * sin(dlon/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            distance = round(6371 * c, 1)
        else:
            distance = estimate_distance(norm_origin, norm_destination)
    except Exception:
        distance = estimate_distance(norm_origin, norm_destination)

    mode = 'air' if distance > 1000 else 'ground'
    max_attempts = 3 if random.random() < 0.15 else 1
    delivery_window = DHLRealisticSimulator.get_delivery_window(service_level, distance)

    if redis_client:
        try:
            rset('service_level', tracking_number, service_level)
            rset('transport_mode', tracking_number, mode)
            rset('delivery_attempts', tracking_number, '0')
            rset('max_attempts', tracking_number, str(max_attempts))
            rset('progress', tracking_number, '0')
            rset('stage', tracking_number, 'pickup')
            rset('delivery_window', tracking_number, delivery_window)
            rset('proof_of_delivery', tracking_number, 'Pending')
        except Exception as redis_err:
            flask_logger.warning(f"Redis error for {tracking_number}: {redis_err}")

    try:
        spawn_simulation(tracking_number)
    except Exception:
        try:
            eventlet.spawn(simulate_tracking, tracking_number)
        except Exception as sim_err:
            flask_logger.warning(f"Simulation start error for {tracking_number}: {sim_err}")

    return {
        'success': True,
        'tracking_number': tracking_number,
        'shipment': {
            'tracking_number': tracking_number,
            'status': 'Pending',
            'origin': origin,
            'destination': destination,
            'service_level': service_level,
            'mode': mode,
            'delivery_window': delivery_window if delivery_window else 'Calculating...'
        }
    }, 201

@app.route('/admin/api/create_shipment', methods=['POST'])
@admin_required
def api_create_shipment():
    data = request.get_json() or {}
    result, status_code = create_shipment_record(
        data.get('origin'),
        data.get('destination'),
        data.get('recipient_email'),
        data.get('service_level', 'DHL Express')
    )
    return jsonify(result), status_code

@app.route('/admin/api/bulk_create', methods=['POST'])
@admin_required
def api_bulk_create():
    payload = request.get_json() or {}
    shipments = payload.get('shipments') or []
    if not isinstance(shipments, list) or not shipments:
        return jsonify({'error': 'Shipments list required'}), 400

    created = []
    errors = []
    for index, shipment_data in enumerate(shipments):
        if not isinstance(shipment_data, dict):
            errors.append({'index': index, 'error': 'Invalid shipment object'})
            continue

        origin = shipment_data.get('origin')
        destination = shipment_data.get('destination')
        recipient_email = shipment_data.get('recipient_email')
        service_level = shipment_data.get('service_level', 'DHL Express')

        result, status_code = create_shipment_record(origin, destination, recipient_email, service_level)
        if result.get('success'):
            created.append(result['tracking_number'])
        else:
            errors.append({'index': index, 'error': result.get('error', 'Unknown error'), 'status': status_code})

    return jsonify({
        'success': len(errors) == 0,
        'created': created,
        'errors': errors,
        'total_created': len(created),
        'total_errors': len(errors)
    }), 200

@app.route('/admin/api/shipments/email-history/<tn>')
@admin_required
def api_email_history(tn):
    if not tn:
        return jsonify([])
    history_key = f"email_history:{tn}"
    entries = []
    if redis_client:
        try:
            raw = redis_client.lrange(history_key, 0, 99) or []
            for item in raw:
                if isinstance(item, bytes):
                    item = item.decode('utf-8')
                try:
                    entries.append(json.loads(item))
                except Exception:
                    continue
        except Exception:
            pass
    return jsonify(entries)

@app.route('/admin/api/send_email', methods=['POST'])
@admin_required
def api_send_email():
    data = request.get_json() or {}
    tn = data.get('tracking_number')
    email_type = data.get('email_type', 'status_update')
    custom_message = data.get('custom_message', '')

    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return jsonify({'error': 'Shipment not found'}), 404
    if not shipment.recipient_email:
        return jsonify({'error': 'No recipient email on file'}), 400

    subject = f"DHL Shipment {tn} - {email_type.replace('_', ' ').title()}"
    message = custom_message or f"Here is an update for your shipment {tn}."
    html_body = f"<p>{message}</p><p>Track your shipment <a href='{app.config['WEBSOCKET_SERVER']}/track/{tn}'>here</a>.</p>"
    plain_body = f"{message}\nTrack your shipment: {app.config['WEBSOCKET_SERVER']}/track/{tn}"

    success = send_email_notification(
        shipment.recipient_email,
        subject,
        html_body=html_body,
        plain_body=plain_body,
        tracking_number=tn,
        email_type=email_type,
        message=message
    )

    if not success:
        return jsonify({'error': 'Failed to send email'}), 500
    return jsonify({'success': True, 'recipient': shipment.recipient_email})

@app.route('/admin/api/pause', methods=['POST'])
@admin_required
def api_pause_simulation():
    data = request.get_json() or {}
    tn = data.get('tracking_number')
    pause = data.get('pause')
    if not tn or pause is None:
        return jsonify({'error': 'tracking_number and pause required'}), 400

    if redis_client:
        try:
            rset('paused_simulations', tn, 'true' if bool(pause) else 'false')
            invalidate_cache(tn)
            try:
                broadcast_update(tn)
            except Exception:
                pass
        except Exception as e:
            flask_logger.warning(f"Failed to pause/resume shipment {tn}: {e}")
            return jsonify({'error': 'Failed to update pause state'}), 500

    return jsonify({'success': True, 'paused': bool(pause)})

@app.route('/admin/api/speed', methods=['POST'])
@admin_required
def api_update_speed():
    data = request.get_json() or {}
    tn = data.get('tracking_number')
    speed = data.get('speed')
    if not tn or speed is None:
        return jsonify({'error': 'tracking_number and speed required'}), 400

    try:
        speed_value = float(speed)
    except (TypeError, ValueError):
        return jsonify({'error': 'Invalid speed value'}), 400

    speed_value = max(0.1, min(10.0, speed_value))
    if redis_client:
        try:
            rset('sim_speed_multipliers', tn, str(speed_value))
            invalidate_cache(tn)
            try:
                broadcast_update(tn)
            except Exception:
                pass
        except Exception as e:
            flask_logger.warning(f"Failed to update speed for {tn}: {e}")
            return jsonify({'error': 'Failed to update speed state'}), 500

    return jsonify({'success': True, 'speed': speed_value})

# SocketIO - Merged disconnect handlers
@socketio.on('connect')
def on_connect():
    sid = getattr(request, 'sid', None)
    try:
        headers = dict(request.headers)
    except Exception:
        headers = {}
    transport = request.args.get('transport') or (request.environ.get('wsgi.websocket') and 'websocket') or 'polling'
    details = {
        'event': 'connect',
        'sid': sid,
        'addr': request.remote_addr,
        'transport': transport,
        'headers': {k: headers.get(k) for k in ['User-Agent', 'Origin', 'Referer'] if headers.get(k)},
        'query': request.args.to_dict(flat=False)
    }
    flask_logger.info("SocketIO connect: %s", details)
    try:
        add_socket_event(details)
    except Exception:
        pass
    emit('status', {'message': 'Connected'})


@socketio.on('disconnect')
def on_disconnect():
    sid = getattr(request, 'sid', None)
    details = {'event': 'disconnect', 'sid': sid, 'addr': request.remote_addr}
    flask_logger.info("SocketIO disconnect: %s", details)
    try:
        add_socket_event(details)
    except Exception:
        pass
    
    # Clean up clients
    for tn in list(in_memory_clients.keys()):
        remove_client(tn, request.sid)
    if redis_client:
        for key in redis_client.scan_iter("clients:*"):
            try:
                tn = key.decode().split(":", 1)[1]
                remove_client(tn, request.sid)
            except Exception:
                continue


@app.route('/admin/client_error', methods=['POST'])
@admin_required
def admin_client_error():
    payload = request.get_json(silent=True) or {}
    payload['remote_addr'] = request.remote_addr
    try:
        add_client_error(payload)
    except Exception:
        pass
    flask_logger.error('Client-side error reported: %s', payload)
    return jsonify({'success': True})

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
    route_coords = build_route_from_checkpoints(coords, mode='drive')
    try:
        dens_km = float(os.getenv('SIM_ROUTE_DENSIFY_KM', '1.0') or '1.0')
        route_coords = densify_route_coords(route_coords, dens_km)
    except Exception:
        pass
    speed = float(rget("sim_speed_multipliers", tn, "1.0") or "1.0")
    paused = rget("paused_simulations", tn, "false") == "true"
    progress = float(rget("progress", tn, "0") or "0")
    mode = rget("transport_mode", tn) or ("air" if estimate_distance(shipment.origin_location or "Lagos, NG", shipment.delivery_location) > 1000 else "ground")
    distance_km = estimate_distance(shipment.origin_location or "Lagos, NG", shipment.delivery_location)
    service_level = DHLRealisticSimulator.get_service_level(
        distance_km, DHLRealisticSimulator.is_business_hours(datetime.now())
    )
    delivery_window = DHLRealisticSimulator.get_delivery_window(service_level, distance_km)
    proof_of_delivery = DHLRealisticSimulator.generate_pod_info()
    emit('tracking_update', {
        'tracking_number': tn, 'status': shipment.status, 'delivery_location': shipment.delivery_location,
        'checkpoints': checkpoints, 'coords': [{'lat': c['lat'], 'lon': c['lon'], 'desc': c['desc']} for c in coords],
        'route_coords': route_coords, 'service_level': service_level, 'delivery_window': delivery_window,
        'proof_of_delivery': proof_of_delivery, 'progress': progress,
        'speed_multiplier': speed, 'paused': paused, 'mode': mode, 'carrier': shipment.carrier
    })

services_started = False
services_started_lock = threading.Lock()

def start_background_services():
    global services_started
    with services_started_lock:
        if services_started:
            flask_logger.debug("Background services already started")
            return
        services_started = True

    try:
        flask_logger.info("Starting background services")
        with app.app_context():
            db.create_all()
        init_db()
        cache_route_templates()
        try:
            with app.app_context():
                active_shipments = Shipment.query.filter(Shipment.status.notin_(["Delivered", "Returned"]))
                for s in active_shipments:
                    try:
                        flask_logger.info(f"Resuming simulation for {s.tracking_number}")
                        eventlet.spawn(simulate_tracking, s.tracking_number)
                    except Exception as e:
                        flask_logger.warning(f"Failed to spawn simulation for {s.tracking_number}: {e}")
        except Exception:
            pass
        threading.Thread(target=keep_alive, daemon=True).start()
        threading.Thread(target=process_notification_queue, daemon=True).start()
        threading.Thread(target=cleanup_websocket_clients, daemon=True).start()
    except Exception:
        with services_started_lock:
            services_started = False
        raise

@app.before_request
def ensure_background_services():
    if not services_started:
        start_background_services()


@app.route('/admin/debug')
@admin_required
def admin_debug():
    """Return quick health/status info useful for debugging the admin UI."""
    info = {
        'services_started': services_started,
        'sqlite_url': app.config.get('SQLALCHEMY_DATABASE_URI'),
        'webrtc_server': app.config.get('WEBSOCKET_SERVER'),
        'redis_configured': bool(redis_client),
        'active_clients_in_memory': len(in_memory_clients) if in_memory_clients else 0,
        'shipments_count': None,
    }
    try:
        info['shipments_count'] = Shipment.query.count()
    except Exception as e:
        info['shipments_count'] = f'error: {e}'
    try:
        if redis_client:
            info['redis_paused_count'] = redis_client.hlen('paused_simulations') if redis_client.exists('paused_simulations') else 0
    except Exception:
        info['redis_paused_count'] = 'error'
    try:
        info['recent_socket_events'] = list(recent_socket_events)[:50]
    except Exception:
        info['recent_socket_events'] = []
    try:
        info['recent_client_errors'] = list(recent_client_errors)[:50]
    except Exception:
        info['recent_client_errors'] = []
    return jsonify(info)

# Start
if __name__ == '__main__':
    start_background_services()
    socketio.run(app, host='0.0.0.0', port=10000, debug=os.getenv('FLASK_ENV') == 'development')
