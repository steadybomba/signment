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

# Third-party imports
import requests
import smtplib
import logging
from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_socketio import SocketIO, emit, disconnect
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
import validators
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy import inspect, text
from time import sleep
from telebot import TeleBot
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from flask_wtf import FlaskForm

# Local imports from utils.py
from utils import (
    BotConfig, redis_client, console, get_app_modules, enqueue_notification,
    get_cached_route_templates, sanitize_tracking_number, validate_email,
    validate_location, validate_webhook_url, send_email_notification,
    check_bot_status, cache_route_templates, get_bot, get_shipment_list,
    get_shipment_details, save_shipment, invalidate_cache
)

# Initialize Flask app and core components
app = Flask(__name__)

# Initialize bot
bot = get_bot()

# Load configuration from utils.py
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
        STATUS_TRANSITIONS=json.loads(os.getenv("STATUS_TRANSITIONS", '''
            {
                "Pending": {"next": ["In_Transit"], "delay": [60, 300], "probabilities": [1.0], "events": {}},
                "In_Transit": {"next": ["Out_for_Delivery", "Delayed"], "delay": [120, 600], "probabilities": [0.9, 0.1], "events": {"Delayed due to weather", "Customs inspection"}},
                "Out_for_Delivery": {"next": ["Delivered"], "delay": [60, 300], "probabilities": [1.0], "events": {}},
                "Delayed": {"next": ["Out_for_Delivery"], "delay": [300, 1200], "probabilities": [1.0], "events": {"Resolved delay"}},
                "Delivered": {"next": [], "delay": [0, 0], "probabilities": [], "events": {}},
                "Returned": {"next": [], "delay": [0, 0], "probabilities": [], "events": {}}
            }
        '''))
    )
except Exception as e:
    console.print(Panel(f"[error]Configuration validation failed: {e}[/error]", title="Config Error", border_style="red"))
    raise

db = SQLAlchemy(app)
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=app.config['RATELIMIT_DEFAULTS'],
    storage_uri=app.config['RATELIMIT_STORAGE_URI']
)
socketio = SocketIO(app, cors_allowed_origins="*")

# Logging setup
flask_logger = logging.getLogger('flask_app')
flask_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
flask_logger.addHandler(handler)

sim_logger = logging.getLogger('simulator')
sim_logger.setLevel(logging.INFO)
sim_logger.addHandler(handler)

# In-memory caches
geocode_cache = {}
in_memory_clients = {}

# Validate critical environment variables
required_vars = ['SECRET_KEY', 'SQLALCHEMY_DATABASE_URI', 'SMTP_USER', 'SMTP_PASS', 'TELEGRAM_BOT_TOKEN']
for var in required_vars:
    if not app.config.get(var):
        error_msg = f"Missing required environment variable: {var}"
        flask_logger.error(error_msg)
        console.print(Panel(f"[error]{error_msg}[/error]", title="Config Error", border_style="red"))
        raise ValueError(error_msg)

# Fallback TrackForm definition
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
        return {
            'tracking_number': self.tracking_number,
            'status': self.status,
            'checkpoints': self.checkpoints,
            'delivery_location': self.delivery_location,
            'last_updated': self.last_updated.isoformat(),
            'recipient_email': self.recipient_email,
            'origin_location': self.origin_location,
            'webhook_url': self.webhook_url,
            'email_notifications': self.email_notifications
        }

def init_db():
    """Initialize database with retries and table creation."""
    flask_logger.info("Starting database initialization")
    console.print("[info]Starting database initialization[/info]")
    max_retries = 5
    retry_delay = 5  # seconds
    for attempt in range(max_retries):
        try:
            with app.app_context():
                flask_logger.debug(f"Attempting to create shipments table (attempt {attempt + 1})")
                console.print(f"[info]Attempting to create shipments table (attempt {attempt + 1})[/info]")
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
                inspector = inspect(db.engine)
                if not inspector.has_table('shipments'):
                    flask_logger.error("Shipments table not created after explicit creation attempt")
                    console.print(Panel("[error]Shipments table not created after explicit attempt[/error]", title="Database Error", border_style="red"))
                    raise Exception("Failed to create shipments table")
                db.session.execute(text('SELECT 1'))
                db.session.commit()
                flask_logger.info("Database initialized successfully, shipments table verified")
                console.print("[info]Database initialized, shipments table verified[/info]")
                return
        except OperationalError as e:
            flask_logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
            console.print(Panel(f"[error]Database connection attempt {attempt + 1} failed: {e}[/error]", title="Database Error", border_style="red"))
            if attempt < max_retries - 1:
                sleep(retry_delay * (2 ** attempt))
            continue
        except SQLAlchemyError as e:
            flask_logger.error(f"Database initialization failed: {e}")
            console.print(Panel(f"[error]Database initialization failed: {e}[/error]", title="Database Error", border_style="red"))
            raise
        except Exception as e:
            flask_logger.error(f"Unexpected error during database initialization: {e}")
            console.print(Panel(f"[error]Unexpected error during database initialization: {e}[/error]", title="Database Error", border_style="red"))
            raise
    flask_logger.critical("Failed to initialize database after max retries")
    console.print(Panel("[critical]Failed to initialize database after max retries[/critical]", title="Database Error", border_style="red"))
    raise Exception("Database initialization failed")

def verify_recaptcha(response_token):
    """Verify reCAPTCHA response with Google API."""
    if not app.config['RECAPTCHA_SECRET_KEY'] or 'your-secret-key' in app.config['RECAPTCHA_SECRET_KEY']:
        flask_logger.debug("reCAPTCHA disabled due to missing or default secret key")
        return True
    try:
        payload = {
            'secret': app.config['RECAPTCHA_SECRET_KEY'],
            'response': response_token
        }
        response = requests.post(app.config['RECAPTCHA_VERIFY_URL'], data=payload, timeout=5)
        result = response.json()
        if result.get('success') and result.get('score', 1.0) >= 0.5:
            flask_logger.debug(f"reCAPTCHA verification successful: score={result.get('score')}", extra={'tracking_number': ''})
            return True
        flask_logger.warning(f"reCAPTCHA verification failed: {result}", extra={'tracking_number': ''})
        console.print(Panel(f"[warning]reCAPTCHA verification failed: {result}[/warning]", title="reCAPTCHA Warning", border_style="yellow"))
        return False
    except requests.RequestException as e:
        flask_logger.error(f"reCAPTCHA verification error: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]reCAPTCHA error: {e}[/error]", title="reCAPTCHA Error", border_style="red"))
        return False

def geocode_locations(checkpoints):
    """Geocode checkpoint locations using external API."""
    coords = []
    api_key = app.config['GEOCODING_API_KEY']
    last_request_time = [0]

    for checkpoint in checkpoints:
        if checkpoint in geocode_cache:
            coords.append(geocode_cache[checkpoint])
            continue
        parts = checkpoint.split(' - ')
        if len(parts) >= 2:
            location = parts[1].strip()
            cache_key = f"geocode:{location}"
            try:
                current_time = time.time()
                if current_time - last_request_time[0] < 1:
                    time.sleep(1 - (current_time - last_request_time[0]))
                last_request_time[0] = time.time()
                if redis_client:
                    try:
                        cached_result = redis_client.get(cache_key)
                        if cached_result:
                            coord = json.loads(cached_result)
                            geocode_cache[checkpoint] = coord
                            coords.append(coord)
                            flask_logger.debug(f"Geocode cache hit for {location}: {coord}", extra={'tracking_number': ''})
                            continue
                    except Exception as e:
                        flask_logger.warning(f"Redis get error for {cache_key}: {e}", extra={'tracking_number': ''})
                url = f"https://geocode.maps.co/search?q={location}&api_key={api_key}"
                response = requests.get(url, timeout=5)
                response.raise_for_status()
                results = response.json()
                if results and isinstance(results, list) and len(results) > 0:
                    result = results[0]
                    coord = {
                        'lat': float(result.get('lat', 0)),
                        'lon': float(result.get('lon', 0)),
                        'desc': checkpoint
                    }
                    geocode_cache[checkpoint] = coord
                    if redis_client:
                        try:
                            redis_client.setex(cache_key, 86400, json.dumps(coord))
                        except Exception as e:
                            flask_logger.warning(f"Failed to cache geocode result: {e}", extra={'tracking_number': ''})
                    coords.append(coord)
                    flask_logger.debug(f"Geocoded {location}: {coord}", extra={'tracking_number': ''})
                else:
                    flask_logger.warning(f"No geocode results for {location}", extra={'tracking_number': ''})
            except requests.RequestException as e:
                flask_logger.warning(f"Geocoding failed for {location}: {e}", extra={'tracking_number': ''})
                console.print(Panel(f"[warning]Geocoding failed for {location}: {e}[/warning]", title="Geocode Error", border_style="yellow"))
    return coords

def add_client(tracking_number, sid):
    """Add WebSocket client to tracking number's client set."""
    if redis_client:
        try:
            redis_client.sadd(f"clients:{tracking_number}", sid)
            flask_logger.debug(f"Added client {sid}", extra={'tracking_number': tracking_number})
        except Exception as e:
            flask_logger.error(f"Redis error adding client {sid}: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[error]Redis error for {tracking_number}: {e}[/error]", title="Redis Error", border_style="red"))
    else:
        if tracking_number not in in_memory_clients:
            in_memory_clients[tracking_number] = set()
        in_memory_clients[tracking_number].add(sid)
        flask_logger.debug(f"Added client {sid} to in-memory store", extra={'tracking_number': tracking_number})

def remove_client(tracking_number, sid):
    """Remove WebSocket client from tracking number's client set."""
    if redis_client:
        try:
            redis_client.srem(f"clients:{tracking_number}", sid)
            flask_logger.debug(f"Removed client {sid}", extra={'tracking_number': tracking_number})
        except Exception as e:
            flask_logger.error(f"Redis error removing client {sid}: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[error]Redis error for {tracking_number}: {e}[/error]", title="Redis Error", border_style="red"))
    else:
        if tracking_number in in_memory_clients:
            in_memory_clients[tracking_number].discard(sid)
            flask_logger.debug(f"Removed client {sid} from in-memory store", extra={'tracking_number': tracking_number})

def get_clients(tracking_number):
    """Retrieve all WebSocket clients for a tracking number."""
    if redis_client:
        try:
            clients = redis_client.smembers(f"clients:{tracking_number}")
            flask_logger.debug(f"Fetched clients: {clients}", extra={'tracking_number': tracking_number})
            return clients
        except Exception as e:
            flask_logger.error(f"Redis error fetching clients: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[error]Redis error for {tracking_number}: {e}[/error]", title="Redis Error", border_style="red"))
            return set()
    else:
        clients = in_memory_clients.get(tracking_number, set())
        flask_logger.debug(f"Fetched clients from in-memory store: {clients}", extra={'tracking_number': tracking_number})
        return clients

def keep_alive():
    """Periodically ping WebSocket server to ensure it remains responsive."""
    max_retries = 3
    retry_delay = 10
    while True:
        for attempt in range(max_retries):
            try:
                response = requests.get(f"{app.config['WEBSOCKET_SERVER']}/health", timeout=10)
                if response.status_code == 200:
                    flask_logger.info("Keep-alive ping successful")
                    console.print(f"[info]Keep-alive ping successful: {response.json()['status']}[/info]")
                    break
                else:
                    flask_logger.warning(f"Keep-alive ping failed: {response.status_code}")
                    console.print(Panel(f"[warning]Keep-alive ping failed: {response.status_code}[/warning]", title="Keep-Alive Warning", border_style="yellow"))
            except requests.RequestException as e:
                flask_logger.error(f"Keep-alive ping error: {e}")
                console.print(Panel(f"[error]Keep-alive ping error: {e}[/error]", title="Keep-Alive Error", border_style="red"))
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (2 ** attempt))
            else:
                flask_logger.error("Max retries exceeded for keep-alive ping")
                console.print(Panel("[error]Max retries exceeded for keep-alive ping[/error]", title="Keep-Alive Error", border_style="red"))
        time.sleep(300)

def process_notification_queue():
    """Process notifications from the Redis queue."""
    max_retries = 3
    retry_delay = 5
    while True:
        if not redis_client:
            flask_logger.error("Redis client not available for notification queue processing")
            console.print(Panel("[error]Redis client not available for notification queue[/error]", title="Queue Error", border_style="red"))
            time.sleep(60)
            continue
        try:
            notification = redis_client.lpop("notifications")
            if not notification:
                time.sleep(1)
                continue
            data = json.loads(notification)
            tracking_number = data.get("tracking_number")
            notification_type = data.get("type")
            notification_data = data.get("data", {})
            
            if notification_type == "email":
                recipient_email = notification_data.get("recipient_email")
                status = notification_data.get("status")
                checkpoints = notification_data.get("checkpoints", "")
                delivery_location = notification_data.get("delivery_location")
                subject = f"Shipment Update: {tracking_number}"
                body = f"Tracking Number: {tracking_number}\nStatus: {status}\nDelivery Location: {delivery_location}\nCheckpoints:\n{checkpoints}"
                for attempt in range(max_retries):
                    try:
                        if send_email_notification(recipient_email, subject, body):
                            flask_logger.info(f"Processed email notification for {tracking_number}")
                            console.print(f"[info]Processed email notification for {tracking_number}[/info]")
                            break
                    except Exception as e:
                        flask_logger.error(f"Email notification attempt {attempt + 1} failed for {tracking_number}: {e}")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay * (2 ** attempt))
                        else:
                            flask_logger.error(f"Max retries exceeded for email notification {tracking_number}")
                            console.print(Panel(f"[error]Max retries exceeded for email notification {tracking_number}[/error]", title="Notification Error", border_style="red"))
            elif notification_type == "webhook":
                webhook_url = notification_data.get("webhook_url")
                payload = {
                    "tracking_number": tracking_number,
                    "status": notification_data.get("status"),
                    "checkpoints": notification_data.get("checkpoints", []),
                    "delivery_location": notification_data.get("delivery_location"),
                    "timestamp": data.get("timestamp")
                }
                for attempt in range(max_retries):
                    try:
                        response = requests.post(webhook_url, json=payload, timeout=10)
                        response.raise_for_status()
                        flask_logger.info(f"Processed webhook notification for {tracking_number} to {webhook_url}")
                        console.print(f"[info]Processed webhook notification for {tracking_number}[/info]")
                        break
                    except requests.RequestException as e:
                        flask_logger.error(f"Webhook notification attempt {attempt + 1} failed for {tracking_number}: {e}")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay * (2 ** attempt))
                        else:
                            flask_logger.error(f"Max retries exceeded for webhook notification {tracking_number}")
                            console.print(Panel(f"[error]Max retries exceeded for webhook notification {tracking_number}[/error]", title="Notification Error", border_style="red"))
        except Exception as e:
            flask_logger.error(f"Unexpected error processing notification queue: {e}")
            console.print(Panel(f"[error]Unexpected error processing notification queue: {e}[/error]", title="Queue Error", border_style="red"))
            time.sleep(5)

def cleanup_websocket_clients():
    """Periodically clean up stale WebSocket clients."""
    cleanup_interval = 3600  # 1 hour
    while True:
        try:
            if redis_client:
                for key in redis_client.scan_iter("clients:*"):
                    tracking_number = key.split(":", 1)[1]
                    clients = redis_client.smembers(key)
                    for sid in clients:
                        try:
                            socketio.emit('ping', room=sid, callback=lambda: None)
                        except Exception:
                            remove_client(tracking_number, sid)
                            flask_logger.debug(f"Removed stale client {sid} for {tracking_number}")
                            console.print(f"[info]Removed stale client {sid} for {tracking_number}[/info]")
            else:
                for tracking_number in list(in_memory_clients.keys()):
                    for sid in in_memory_clients[tracking_number].copy():
                        try:
                            socketio.emit('ping', room=sid, callback=lambda: None)
                        except Exception:
                            remove_client(tracking_number, sid)
                            flask_logger.debug(f"Removed stale client {sid} from in-memory store for {tracking_number}")
                            console.print(f"[info]Removed stale client {sid} for {tracking_number}[/info]")
            time.sleep(cleanup_interval)
        except Exception as e:
            flask_logger.error(f"Error cleaning up WebSocket clients: {e}")
            console.print(Panel(f"[error]Error cleaning up WebSocket clients: {e}[/error]", title="WebSocket Error", border_style="red"))
            time.sleep(cleanup_interval)

def simulate_tracking(tracking_number):
    """Simulate shipment tracking with status updates and notifications."""
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        sim_logger.error("Invalid tracking number", extra={'tracking_number': str(tracking_number)})
        console.print(Panel(f"[error]Invalid tracking number: {tracking_number}[/error]", title="Simulation Error", border_style="red"))
        return

    retries = 0
    max_retries = 5
    max_simulation_time = timedelta(days=30)
    start_time = datetime.now()

    console.print(f"[info]Starting simulation for {sanitized_tn}[/info]")
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console
    ) as progress:
        task = progress.add_task(f"Simulating {sanitized_tn}", total=100)
        
        while datetime.now() - start_time < max_simulation_time:
            if redis_client and redis_client.hget("paused_simulations", sanitized_tn) == "true":
                sim_logger.debug(f"Simulation paused for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
                console.print(f"[info]Simulation paused for {sanitized_tn}[/info]")
                eventlet.sleep(5)
                continue

            try:
                shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
                if not shipment:
                    sim_logger.warning("Shipment not found", extra={'tracking_number': sanitized_tn})
                    console.print(f"[warning]Shipment not found: {sanitized_tn}[/warning]")
                    break
                status = shipment.status
                checkpoints_str = shipment.checkpoints or ''
                checkpoints = checkpoints_str.split(';') if checkpoints_str else []
                delivery_location = shipment.delivery_location
                origin_location = shipment.origin_location or delivery_location
                webhook_url = shipment.webhook_url or app.config['GLOBAL_WEBHOOK_URL']
                recipient_email = shipment.recipient_email

                if status in ['Delivered', 'Returned']:
                    progress.update(task, advance=100, description=f"Completed {sanitized_tn}: {status}")
                    sim_logger.info(f"Simulation completed: {status}", extra={'tracking_number': sanitized_tn})
                    console.print(f"[info]Simulation completed for {sanitized_tn}: {status}[/info]")
                    break

                current_time = datetime.now()
                template = get_cached_route_templates().get(delivery_location, get_cached_route_templates().get(origin_location, ['Lagos, NG']))
                transition = app.config['STATUS_TRANSITIONS'].get(status, {
                    'next': ['Delivered'],
                    'delay': (60, 300),
                    'probabilities': [1.0],
                    'events': {}
                })
                delay_range = transition['delay']
                next_states = transition['next']
                probabilities = transition.get('probabilities', [1.0 / len(next_states)] * len(next_states)) if next_states else [1.0]
                events = transition.get('events', {})

                route_length = len(template)
                delay_multiplier = 1 + (route_length / 10)
                speed_multiplier = float(redis_client.hget("sim_speed_multipliers", sanitized_tn) or 1.0) if redis_client else 1.0
                speed_multiplier = max(0.1, min(10.0, speed_multiplier))
                adjusted_delay = random.uniform(delay_range[0], delay_range[1]) * delay_multiplier / speed_multiplier

                if status not in ['Out_for_Delivery', 'Delivered']:
                    next_index = min(len(checkpoints), len(template) - 1)
                    next_checkpoint = f"{current_time.strftime('%Y-%m-%d %H:%M')} - {template[next_index]} - Processed"
                    if next_checkpoint not in checkpoints:
                        checkpoints.append(next_checkpoint)
                        sim_logger.debug(f"Added checkpoint: {next_checkpoint}", extra={'tracking_number': sanitized_tn})

                new_status = random.choices(next_states, probabilities)[0] if next_states else status
                send_notification = new_status != status
                if new_status != status:
                    status = new_status
                    if events and isinstance(events, set):
                        event_msg = random.choice(list(events))
                        checkpoints.append(f"{current_time.strftime('%Y-%m-%d %H:%M')} - {delivery_location} - {event_msg}")
                        sim_logger.info(f"Event triggered: {event_msg}", extra={'tracking_number': sanitized_tn})
                    if status == 'Delivered':
                        checkpoints.append(f"{current_time.strftime('%Y-%m-%d %H:%M')} - {delivery_location} - Delivered")
                    if status == 'Returned':
                        checkpoints.append(f"{current_time.strftime('%Y-%m-%d %H:%M')} - {origin_location} - Returned")
                    sim_logger.info(f"Status changed to {status}", extra={'tracking_number': sanitized_tn})
                    progress.update(task, advance=20)

                shipment.status = status
                shipment.checkpoints = ';'.join(checkpoints)
                shipment.last_updated = current_time
                db.session.commit()
                invalidate_cache(sanitized_tn)
                sim_logger.debug(f"Updated shipment: status={status}, checkpoints={len(checkpoints)}", extra={'tracking_number': sanitized_tn})

                if send_notification and recipient_email and shipment.email_notifications:
                    queue_length = redis_client.llen("notifications") if redis_client else 0
                    sim_logger.debug(f"Enqueuing notification, queue length: {queue_length}", extra={'tracking_number': sanitized_tn})
                    console.print(f"[info]Enqueuing notification for {sanitized_tn}, queue length: {queue_length}[/info]")
                    enqueue_notification({
                        "tracking_number": sanitized_tn,
                        "type": "email",
                        "data": {
                            "status": status,
                            "checkpoints": ';'.join(checkpoints),
                            "delivery_location": delivery_location,
                            "recipient_email": recipient_email
                        }
                    })

                if webhook_url:
                    queue_length = redis_client.llen("notifications") if redis_client else 0
                    sim_logger.debug(f"Enqueuing webhook, queue_length: {queue_length}", extra={'tracking_number': sanitized_tn})
                    console.print(f"[info]Enqueuing webhook for {sanitized_tn}, queue length: {queue_length}[/info]")
                    enqueue_notification({
                        "tracking_number": sanitized_tn,
                        "type": "webhook",
                        "data": {
                            "status": status,
                            "checkpoints": checkpoints,
                            "delivery_location": delivery_location,
                            "webhook_url": webhook_url
                        }
                    })

                broadcast_update(sanitized_tn)

                sim_logger.debug(f"Sleeping for {adjusted_delay:.2f} seconds with speed multiplier {speed_multiplier}", extra={'tracking_number': sanitized_tn})
                eventlet.sleep(adjusted_delay)
                retries = 0
            except SQLAlchemyError as e:
                db.session.rollback()
                sim_logger.error(f"Database error: {e}", extra={'tracking_number': sanitized_tn})
                console.print(Panel(f"[error]Database error for {sanitized_tn}: {e}[/error]", title="Simulation Error", border_style="red"))
                retries += 1
                if retries >= max_retries:
                    sim_logger.critical(f"Max retries exceeded. Stopping simulation.", extra={'tracking_number': sanitized_tn})
                    console.print(Panel(f"[critical]Max retries exceeded for {sanitized_tn}[/critical]", title="Simulation Error", border_style="red"))
                    break
                eventlet.sleep(2 ** retries)
            except Exception as e:
                sim_logger.error(f"Unexpected error in simulation: {e}", extra={'tracking_number': sanitized_tn})
                console.print(Panel(f"[error]Unexpected simulation error for {sanitized_tn}: {e}[/error]", title="Simulation Error", border_style="red"))
                break

def broadcast_update(tracking_number):
    """Broadcast shipment updates to connected WebSocket clients."""
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.error("Invalid tracking number for broadcast", extra={'tracking_number': str(tracking_number)})
        console.print(Panel(f"[error]Invalid tracking number for broadcast: {tracking_number}[/error]", title="Broadcast Error", border_style="red"))
        return

    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        update_data = {
            'tracking_number': sanitized_tn,
            'found': False,
            'error': 'Tracking number not found.'
        }
        if shipment:
            status = shipment.status
            checkpoints_str = shipment.checkpoints
            delivery_location = shipment.delivery_location
            checkpoints = checkpoints_str.split(';') if checkpoints_str else []
            coords = geocode_locations(checkpoints)
            coords_list = [{'lat': c['lat'], 'lon': c['lon'], 'desc': c['desc']} for c in coords]
            update_data = {
                'tracking_number': sanitized_tn,
                'status': status,
                'checkpoints': checkpoints,
                'delivery_location': delivery_location,
                'coords': coords_list,
                'found': True,
                'paused': redis_client.hget("paused_simulations", sanitized_tn) == "true" if redis_client else False,
                'speed_multiplier': float(redis_client.hget("sim_speed_multipliers", sanitized_tn) or 1.0) if redis_client else 1.0
            }
        clients = get_clients(sanitized_tn)
        if clients:
            for sid in clients:
                try:
                    socketio.emit('tracking_update', update_data, room=sid)
                    flask_logger.debug(f"Broadcast update to {sid}", extra={'tracking_number': sanitized_tn})
                except Exception as e:
                    flask_logger.error(f"Failed to emit to {sid}: {e}", extra={'tracking_number': sanitized_tn})
                    remove_client(sanitized_tn, sid)
        else:
            flask_logger.debug(f"No clients for broadcast: {sanitized_tn}", extra={'tracking_number': sanitized_tn})
    except SQLAlchemyError as e:
        db.session.rollback()
        flask_logger.error(f"Database error during broadcast: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error during broadcast for {sanitized_tn}: {e}[/error]", title="Broadcast Error", border_style="red"))
    except Exception as e:
        flask_logger.error(f"Unexpected broadcast error: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Unexpected broadcast error: {e}[/error]", title="Broadcast Error", border_style="red"))

# Flask routes
@app.route('/')
def index():
    """Serve the main index page with tracking form."""
    if request.method == 'HEAD':
        return '', 200
    try:
        from forms import TrackForm as ExternalTrackForm
        form = ExternalTrackForm()
    except ImportError as e:
        flask_logger.warning(f"forms.py not found, using fallback TrackForm: {e}")
        console.print(Panel(f"[warning]forms.py not found, using fallback TrackForm: {e}[/warning]", title="Form Warning", border_style="yellow"))
        form = TrackForm()
    flask_logger.info("Serving index page", extra={'tracking_number': ''})
    console.print("[info]Serving index page[/info]")
    return render_template('index.html', 
                         form=form, 
                         tawk_property_id=app.config['TAWK_PROPERTY_ID'], 
                         tawk_widget_id=app.config['TAWK_WIDGET_ID'], 
                         recaptcha_site_key=app.config['RECAPTCHA_SITE_KEY'])

@app.route('/track', methods=['POST'])
@limiter.limit("10 per minute")
def track():
    """Handle tracking form submission and initiate simulation."""
    try:
        from forms import TrackForm as ExternalTrackForm
        form = ExternalTrackForm()
    except ImportError as e:
        flask_logger.warning(f"forms.py not found, using fallback TrackForm: {e}")
        console.print(Panel(f"[warning]forms.py not found, using fallback TrackForm: {e}[/warning]", title="Form Warning", border_style="yellow"))
        form = TrackForm()
    if not form.validate_on_submit():
        flask_logger.warning("Form validation failed", extra={'tracking_number': ''})
        return jsonify({'error': 'Invalid form data'}), 400

    recaptcha_response = request.form.get('g-recaptcha-response')
    if app.config['RECAPTCHA_SITE_KEY'] and 'your-site-key' not in app.config['RECAPTCHA_SITE_KEY'] and not verify_recaptcha(recaptcha_response):
        flask_logger.warning("reCAPTCHA verification failed", extra={'tracking_number': ''})
        return jsonify({'error': 'reCAPTCHA verification failed'}), 400

    tracking_number = form.tracking_number.data
    email = form.email.data
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number submitted: {tracking_number}", extra={'tracking_number': str(tracking_number)})
        return render_template('tracking_result.html', 
                             error='Invalid tracking number', 
                             coords=[],
                             tawk_property_id=app.config['TAWK_PROPERTY_ID'], 
                             tawk_widget_id=app.config['TAWK_WIDGET_ID'])

    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        if not shipment:
            flask_logger.warning(f"Shipment not found: {sanitized_tn}", extra={'tracking_number': sanitized_tn})
            return render_template('tracking_result.html', 
                                 error='Shipment not found', 
                                 coords=[],
                                 tawk_property_id=app.config['TAWK_PROPERTY_ID'], 
                                 tawk_widget_id=app.config['TAWK_WIDGET_ID'])

        if email and validate_email(email):
            shipment.recipient_email = email
            db.session.commit()
            invalidate_cache(sanitized_tn)
            flask_logger.info(f"Updated recipient email to {email}", extra={'tracking_number': sanitized_tn})
            console.print(f"[info]Updated recipient email to {email} for {sanitized_tn}[/info]")

        checkpoints_str = shipment.checkpoints or ''
        checkpoints = checkpoints_str.split(';') if checkpoints_str else []
        coords = geocode_locations(checkpoints)
        coords_list = [{'lat': c['lat'], 'lon': c['lon'], 'desc': c['desc']} for c in coords]
        
        if shipment.status not in ['Delivered', 'Returned']:
            eventlet.spawn(simulate_tracking, sanitized_tn)
        flask_logger.info(f"Started tracking simulation", extra={'tracking_number': sanitized_tn})
        console.print(f"[info]Started tracking simulation for {sanitized_tn}[/info]")
        return render_template('tracking_result.html', 
                             shipment=shipment, 
                             checkpoints=checkpoints, 
                             coords=coords_list, 
                             tawk_property_id=app.config['TAWK_PROPERTY_ID'], 
                             tawk_widget_id=app.config['TAWK_WIDGET_ID'])
    except SQLAlchemyError as e:
        db.session.rollback()
        flask_logger.error(f"Database error: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error for {sanitized_tn}: {e}[/error]", title="Database Error", border_style="red"))
        return render_template('tracking_result.html', 
                             error='Database error occurred', 
                             coords=[],
                             tawk_property_id=app.config['TAWK_PROPERTY_ID'], 
                             tawk_widget_id=app.config['TAWK_WIDGET_ID'])
    except Exception as e:
        flask_logger.error(f"Unexpected error in track: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Unexpected error in track: {e}[/error]", title="Track Error", border_style="red"))
        return render_template('tracking_result.html', 
                             error='Unexpected error', 
                             coords=[],
                             tawk_property_id=app.config['TAWK_PROPERTY_ID'], 
                             tawk_widget_id=app.config['TAWK_WIDGET_ID'])

@app.route('/broadcast/<tracking_number>')
@limiter.limit("10 per minute")
def trigger_broadcast(tracking_number):
    """Trigger a broadcast update for a specific tracking number."""
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number for broadcast: {tracking_number}", extra={'tracking_number': str(tracking_number)})
        return jsonify({'error': 'Invalid tracking number'}), 400
    eventlet.spawn(broadcast_update, sanitized_tn)
    flask_logger.info(f"Triggered broadcast", extra={'tracking_number': sanitized_tn})
    return '', 204

@app.route('/health', methods=['GET'])
@limiter.limit("5 per minute")
def health_check():
    """Perform health check for application components."""
    status = {
        'status': 'healthy',
        'database': 'ok',
        'redis': 'unavailable',
        'smtp': 'ok',
        'telegram': 'unavailable',
        'notification_queue': 'unavailable'
    }
    try:
        inspector = inspect(db.engine)
        status['database'] = 'ok' if inspector.has_table('shipments') else 'shipments table missing'
        db.session.execute(text('SELECT 1'))
    except SQLAlchemyError as e:
        status['status'] = 'unhealthy'
        status['database'] = str(e)
    try:
        if redis_client:
            redis_client.set("test", "ping")
            redis_client.delete("test")
            status['redis'] = 'ok'
            status['notification_queue'] = f"length: {redis_client.llen('notifications')}"
    except Exception as e:
        status['redis'] = str(e)
        status['notification_queue'] = str(e)
    try:
        with smtplib.SMTP(app.config['SMTP_HOST'], app.config['SMTP_PORT'], timeout=5) as server:
            server.starttls()
            server.login(app.config['SMTP_USER'], app.config['SMTP_PASS'])
        status['smtp'] = 'ok'
    except smtplib.SMTPException as e:
        status['smtp'] = str(e)
    try:
        status['telegram'] = 'ok' if check_bot_status() else 'unavailable'
    except Exception as e:
        status['telegram'] = str(e)
    if status['status'] == 'healthy' and any(v != 'ok' for v in [status['database'], 'smtp', 'redis']):
        status['status'] = 'unhealthy'
    flask_logger.info("Health check", extra=status)
    console.print(f"[info]Health check: {status}[/info]")
    return jsonify(status), 200 if status['status'] == 'healthy' else 500

@app.route('/telegram/webhook', methods=['POST'])
def webhook():
    """Handle Telegram webhook updates."""
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = bot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        flask_logger.info("Processed Telegram webhook update", extra={'tracking_number': ''})
        return '', 200
    flask_logger.warning("Invalid Telegram webhook request", extra={'tracking_number': ''})
    return '', 403

@app.route('/api/shipments', methods=['POST'])
@limiter.limit("5 per minute")
def create_shipment():
    """Create a new shipment (admin only)."""
    from utils import is_admin
    auth_token = request.headers.get('Authorization')
    if not auth_token or not is_admin(int(auth_token.split()[-1]) if auth_token.startswith('Bearer ') else 0):
        flask_logger.warning("Unauthorized attempt to create shipment", extra={'tracking_number': ''})
        return jsonify({'error': 'Unauthorized'}), 403

    data = request.get_json()
    if not data:
        flask_logger.warning("Invalid JSON payload for create shipment", extra={'tracking_number': ''})
        return jsonify({'error': 'Invalid payload'}), 400

    tracking_number = data.get('tracking_number')
    status = data.get('status', 'Pending')
    checkpoints = data.get('checkpoints', '')
    delivery_location = data.get('delivery_location')
    recipient_email = data.get('recipient_email', '')
    origin_location = data.get('origin_location', '')
    webhook_url = data.get('webhook_url', '')

    try:
        if save_shipment(tracking_number, status, checkpoints, delivery_location, 
                        recipient_email, origin_location, webhook_url):
            eventlet.spawn(simulate_tracking, tracking_number)
            flask_logger.info(f"Created shipment {tracking_number}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Created shipment {tracking_number}[/info]")
            return jsonify({'message': f'Shipment {tracking_number} created', 'tracking_number': tracking_number}), 201
        else:
            flask_logger.error(f"Failed to create shipment {tracking_number}", extra={'tracking_number': tracking_number})
            return jsonify({'error': 'Failed to create shipment'}), 500
    except ValueError as e:
        flask_logger.warning(f"Validation error: {e}", extra={'tracking_number': tracking_number})
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        flask_logger.error(f"Unexpected error creating shipment: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Unexpected error creating shipment {tracking_number}: {e}[/error]", title="Server Error", border_style="red"))
        return jsonify({'error': 'Server error'}), 500

@app.route('/api/shipments/<tracking_number>', methods=['PUT'])
@limiter.limit("5 per minute")
def update_shipment(tracking_number):
    """Update an existing shipment (admin only)."""
    from utils import is_admin
    auth_token = request.headers.get('Authorization')
    if not auth_token or not is_admin(int(auth_token.split()[-1]) if auth_token.startswith('Bearer ') else 0):
        flask_logger.warning("Unauthorized attempt to update shipment", extra={'tracking_number': tracking_number})
        return jsonify({'error': 'Unauthorized'}), 403

    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number: {tracking_number}", extra={'tracking_number': tracking_number})
        return jsonify({'error': 'Invalid tracking number'}), 400

    data = request.get_json()
    if not data:
        flask_logger.warning("Invalid JSON payload for update shipment", extra={'tracking_number': sanitized_tn})
        return jsonify({'error': 'Invalid payload'}), 400

    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        if not shipment:
            flask_logger.warning(f"Shipment not found: {sanitized_tn}", extra={'tracking_number': sanitized_tn})
            return jsonify({'error': 'Shipment not found'}), 404

        status = data.get('status', shipment.status)
        checkpoints = data.get('checkpoints', shipment.checkpoints or '')
        delivery_location = data.get('delivery_location', shipment.delivery_location)
        recipient_email = data.get('recipient_email', shipment.recipient_email or '')
        origin_location = data.get('origin_location', shipment.origin_location or '')
        webhook_url = data.get('webhook_url', shipment.webhook_url or '')
        email_notifications = data.get('email_notifications', shipment.email_notifications)

        if save_shipment(sanitized_tn, status, checkpoints, delivery_location, 
                        recipient_email, origin_location, webhook_url):
            shipment.email_notifications = email_notifications
            db.session.commit()
            invalidate_cache(sanitized_tn)
            eventlet.spawn(broadcast_update, sanitized_tn)
            flask_logger.info(f"Updated shipment {sanitized_tn}", extra={'tracking_number': sanitized_tn})
            console.print(f"[info]Updated shipment {sanitized_tn}[/info]")
            return jsonify({'message': f'Shipment {sanitized_tn} updated'}), 200
        else:
            flask_logger.error(f"Failed to update shipment {sanitized_tn}", extra={'tracking_number': sanitized_tn})
            return jsonify({'error': 'Failed to update shipment'}), 500
    except ValueError as e:
        flask_logger.warning(f"Validation error: {e}", extra={'tracking_number': sanitized_tn})
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        flask_logger.error(f"Unexpected error updating shipment: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Unexpected error updating shipment {sanitized_tn}: {e}[/error]", title="Server Error", border_style="red"))
        return jsonify({'error': 'Server error'}), 500

@app.route('/api/shipments/<tracking_number>', methods=['DELETE'])
@limiter.limit("5 per minute")
def delete_shipment(tracking_number):
    """Delete a shipment (admin only)."""
    from utils import is_admin
    auth_token = request.headers.get('Authorization')
    if not auth_token or not is_admin(int(auth_token.split()[-1]) if auth_token.startswith('Bearer ') else 0):
        flask_logger.warning("Unauthorized attempt to delete shipment", extra={'tracking_number': tracking_number})
        return jsonify({'error': 'Unauthorized'}), 403

    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number: {tracking_number}", extra={'tracking_number': tracking_number})
        return jsonify({'error': 'Invalid tracking number'}), 400

    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        if not shipment:
            flask_logger.warning(f"Shipment not found: {sanitized_tn}", extra={'tracking_number': sanitized_tn})
            return jsonify({'error': 'Shipment not found'}), 404

        db.session.delete(shipment)
        db.session.commit()
        invalidate_cache(sanitized_tn)
        if redis_client:
            redis_client.hdel("paused_simulations", sanitized_tn)
            redis_client.hdel("sim_speed_multipliers", sanitized_tn)
            redis_client.delete(f"clients:{sanitized_tn}")
        flask_logger.info(f"Deleted shipment {sanitized_tn}", extra={'tracking_number': sanitized_tn})
        console.print(f"[info]Deleted shipment {sanitized_tn}[/info]")
        return jsonify({'message': f'Shipment {sanitized_tn} deleted'}), 200
    except SQLAlchemyError as e:
        db.session.rollback()
        flask_logger.error(f"Database error deleting shipment: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error deleting shipment {sanitized_tn}: {e}[/error]", title="Database Error", border_style="red"))
        return jsonify({'error': 'Database error'}), 500
    except Exception as e:
        flask_logger.error(f"Unexpected error deleting shipment: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Unexpected error deleting shipment {sanitized_tn}: {e}[/error]", title="Server Error", border_style="red"))
        return jsonify({'error': 'Server error'}), 500

@app.route('/api/shipments/bulk', methods=['POST'])
@limiter.limit("5 per minute")
def bulk_action():
    """Perform bulk actions on shipments (pause/resume/delete, admin only)."""
    from utils import is_admin
    auth_token = request.headers.get('Authorization')
    if not auth_token or not is_admin(int(auth_token.split()[-1]) if auth_token.startswith('Bearer ') else 0):
        flask_logger.warning("Unauthorized attempt to perform bulk action", extra={'tracking_number': ''})
        return jsonify({'error': 'Unauthorized'}), 403

    data = request.get_json()
    if not data or 'action' not in data or 'tracking_numbers' not in data:
        flask_logger.warning("Invalid JSON payload for bulk action", extra={'tracking_number': ''})
        return jsonify({'error': 'Invalid payload'}), 400

    action = data['action']
    tracking_numbers = data['tracking_numbers']
    if not isinstance(tracking_numbers, list):
        flask_logger.warning("Tracking numbers must be a list", extra={'tracking_number': ''})
        return jsonify({'error': 'Tracking numbers must be a list'}), 400

    results = []
    try:
        for tn in tracking_numbers:
            sanitized_tn = sanitize_tracking_number(tn)
            if not sanitized_tn:
                results.append({'tracking_number': tn, 'status': 'failed', 'error': 'Invalid tracking number'})
                continue

            shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
            if not shipment:
                results.append({'tracking_number': sanitized_tn, 'status': 'failed', 'error': 'Shipment not found'})
                continue

            if action == 'pause':
                if redis_client:
                    redis_client.hset("paused_simulations", sanitized_tn, "true")
                invalidate_cache(sanitized_tn)
                results.append({'tracking_number': sanitized_tn, 'status': 'success', 'message': 'Simulation paused'})
                flask_logger.info(f"Paused simulation for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
                console.print(f"[info]Paused simulation for {sanitized_tn}[/info]")
            elif action == 'resume':
                if redis_client:
                    redis_client.hdel("paused_simulations", sanitized_tn)
                invalidate_cache(sanitized_tn)
                eventlet.spawn(simulate_tracking, sanitized_tn)
                results.append({'tracking_number': sanitized_tn, 'status': 'success', 'message': 'Simulation resumed'})
                flask_logger.info(f"Resumed simulation for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
                console.print(f"[info]Resumed simulation for {sanitized_tn}[/info]")
            elif action == 'delete':
                db.session.delete(shipment)
                invalidate_cache(sanitized_tn)
                if redis_client:
                    redis_client.hdel("paused_simulations", sanitized_tn)
                    redis_client.hdel("sim_speed_multipliers", sanitized_tn)
                    redis_client.delete(f"clients:{sanitized_tn}")
                results.append({'tracking_number': sanitized_tn, 'status': 'success', 'message': 'Shipment deleted'})
                flask_logger.info(f"Deleted shipment {sanitized_tn}", extra={'tracking_number': sanitized_tn})
                console.print(f"[info]Deleted shipment {sanitized_tn}[/info]")
            else:
                results.append({'tracking_number': sanitized_tn, 'status': 'failed', 'error': f'Invalid action: {action}'})

        db.session.commit()
        return jsonify({'results': results}), 200
    except SQLAlchemyError as e:
        db.session.rollback()
        flask_logger.error(f"Database error during bulk action: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Database error during bulk action: {e}[/error]", title="Database Error", border_style="red"))
        return jsonify({'error': 'Database error'}), 500
    except Exception as e:
        flask_logger.error(f"Unexpected error during bulk action: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Unexpected error during bulk action: {e}[/error]", title="Server Error", border_style="red"))
        return jsonify({'error': 'Server error'}), 500

@app.route('/api/shipments/<tracking_number>/notify', methods=['POST'])
@limiter.limit("5 per minute")
def send_notification(tracking_number):
    """Send manual notification for a shipment (admin only)."""
    from utils import is_admin
    auth_token = request.headers.get('Authorization')
    if not auth_token or not is_admin(int(auth_token.split()[-1]) if auth_token.startswith('Bearer ') else 0):
        flask_logger.warning("Unauthorized attempt to send notification", extra={'tracking_number': tracking_number})
        return jsonify({'error': 'Unauthorized'}), 403

    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number: {tracking_number}", extra={'tracking_number': tracking_number})
        return jsonify({'error': 'Invalid tracking number'}), 400

    data = request.get_json()
    if not data or 'type' not in data:
        flask_logger.warning("Invalid JSON payload for notification", extra={'tracking_number': sanitized_tn})
        return jsonify({'error': 'Invalid payload'}), 400

    notification_type = data['type']
    if notification_type not in ['email', 'webhook']:
        flask_logger.warning(f"Invalid notification type: {notification_type}", extra={'tracking_number': sanitized_tn})
        return jsonify({'error': 'Invalid notification type'}), 400

    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        if not shipment:
            flask_logger.warning(f"Shipment not found: {sanitized_tn}", extra={'tracking_number': sanitized_tn})
            return jsonify({'error': 'Shipment not found'}), 404

        if notification_type == 'email' and shipment.email_notifications and shipment.recipient_email:
            success = enqueue_notification({
                "tracking_number": sanitized_tn,
                "type": "email",
                "data": {
                    "status": shipment.status,
                    "checkpoints": shipment.checkpoints or '',
                    "delivery_location": shipment.delivery_location,
                    "recipient_email": shipment.recipient_email
                }
            })
            if success:
                flask_logger.info(f"Enqueued email notification for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
                console.print(f"[info]Enqueued email notification for {sanitized_tn}[/info]")
                return jsonify({'message': f'Email notification enqueued for {sanitized_tn}'}), 200
            else:
                flask_logger.error(f"Failed to enqueue email notification for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
                return jsonify({'error': 'Failed to enqueue email notification'}), 500
        elif notification_type == 'webhook':
            webhook_url = shipment.webhook_url or app.config['GLOBAL_WEBHOOK_URL']
            success = enqueue_notification({
                "tracking_number": sanitized_tn,
                "type": "webhook",
                "data": {
                    "status": shipment.status,
                    "checkpoints": shipment.checkpoints.split(';') if shipment.checkpoints else [],
                    "delivery_location": shipment.delivery_location,
                    "webhook_url": webhook_url
                }
            })
            if success:
                flask_logger.info(f"Enqueued webhook notification for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
                console.print(f"[info]Enqueued webhook notification for {sanitized_tn}[/info]")
                return jsonify({'message': f'Webhook notification enqueued for {sanitized_tn}'}), 200
            else:
                flask_logger.error(f"Failed to enqueue webhook notification for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
                return jsonify({'error': 'Failed to enqueue webhook notification'}), 500
        else:
            flask_logger.warning(f"Email notifications disabled or no recipient for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
            return jsonify({'error': 'Email notifications disabled or no recipient'}), 400
    except Exception as e:
        flask_logger.error(f"Unexpected error sending notification: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Unexpected error sending notification for {sanitized_tn}: {e}[/error]", title="Server Error", border_style="red"))
        return jsonify({'error': 'Server error'}), 500

@app.route('/api/shipments/<tracking_number>/speed', methods=['POST'])
@limiter.limit("5 per minute")
def set_simulation_speed(tracking_number):
    """Set simulation speed multiplier for a shipment (admin only)."""
    from utils import is_admin
    auth_token = request.headers.get('Authorization')
    if not auth_token or not is_admin(int(auth_token.split()[-1]) if auth_token.startswith('Bearer ') else 0):
        flask_logger.warning("Unauthorized attempt to set simulation speed", extra={'tracking_number': tracking_number})
        return jsonify({'error': 'Unauthorized'}), 403

    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number: {tracking_number}", extra={'tracking_number': tracking_number})
        return jsonify({'error': 'Invalid tracking number'}), 400

    data = request.get_json()
    if not data or 'speed_multiplier' not in data:
        flask_logger.warning("Invalid JSON payload for set speed", extra={'tracking_number': sanitized_tn})
        return jsonify({'error': 'Invalid payload'}), 400

    try:
        speed_multiplier = float(data['speed_multiplier'])
        if not (0.1 <= speed_multiplier <= 10.0):
            flask_logger.warning(f"Invalid speed multiplier: {speed_multiplier}", extra={'tracking_number": sanitized_tn})
            return jsonify({'error': 'Speed multiplier must be between 0.1 and 10.0'}), 400

        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        if not shipment:
            flask_logger.warning(f"Shipment not found: {sanitized_tn}", extra={'tracking_number': sanitized_tn})
            return jsonify({'error': 'Shipment not found'}), 404

        if redis_client:
            redis_client.hset("sim_speed_multipliers", sanitized_tn, str(speed_multiplier))
        invalidate_cache(sanitized_tn)
        eventlet.spawn(broadcast_update, sanitized_tn)
        flask_logger.info(f"Set speed multiplier to {speed_multiplier} for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
        console.print(f"[info]Set speed multiplier to {speed_multiplier} for {sanitized_tn}[/info]")
        return jsonify({'message': f'Speed multiplier set to {speed_multiplier} for {sanitized_tn}'}), 200
    except ValueError as e:
        flask_logger.warning(f"Invalid speed multiplier format: {e}", extra={'tracking_number': sanitized_tn})
        return jsonify({'error': 'Invalid speed multiplier format'}), 400
    except Exception as e:
        flask_logger.error(f"Unexpected error setting speed: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Unexpected error setting speed for {sanitized_tn}: {e}[/error]", title="Server Error", border_style="red"))
        return jsonify({'error': 'Server error'}), 500

@app.route('/api/stats', methods=['GET'])
@limiter.limit("5 per minute")
def get_stats():
    """Retrieve system statistics."""
    try:
        total_shipments = Shipment.query.count()
        status_counts = db.session.query(Shipment.status, db.func.count(Shipment.id))\
                                .group_by(Shipment.status).all()
        status_counts = {status: count for status, count in status_counts}
        queue_length = redis_client.llen("notifications") if redis_client else 0
        active_simulations = sum(1 for key in (redis_client.scan_iter("paused_simulations") if redis_client else []) 
                                if redis_client.hget("paused_simulations", key.split(':')[-1]) != "true")
        stats = {
            'total_shipments': total_shipments,
            'status_counts': status_counts,
            'notification_queue_length': queue_length,
            'active_simulations': active_simulations,
            'redis_status': 'ok' if redis_client else 'unavailable'
        }
        flask_logger.info("Retrieved system statistics", extra={'tracking_number': ''})
        console.print(f"[info]Retrieved system statistics: {stats}[/info]")
        return jsonify(stats), 200
    except SQLAlchemyError as e:
        flask_logger.error(f"Database error retrieving stats: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Database error retrieving stats: {e}[/error]", title="Database Error", border_style="red"))
        return jsonify({'error': 'Database error'}), 500
    except Exception as e:
        flask_logger.error(f"Unexpected error retrieving stats: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Unexpected error retrieving stats: {e}[/error]", title="Server Error", border_style="red"))
        return jsonify({'error': 'Server error'}), 500

# SocketIO handlers
@socketio.on('connect')
def handle_connect():
    """Handle WebSocket client connection."""
    flask_logger.debug(f"Client connected: {request.sid}", extra={'tracking_number': ''})
    try:
        emit('status', {'message': 'Connected to tracking service'}, broadcast=False)
    except Exception as e:
        flask_logger.error(f"Failed to emit connect status: {e}", extra={'tracking_number': ''})

@socketio.on('request_tracking')
def handle_request_tracking(data):
    """Handle WebSocket tracking request."""
    tracking_number = data.get('tracking_number')
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number in WebSocket request: {tracking_number}", extra={'tracking_number': str(tracking_number)})
        try:
            emit('tracking_update', {'error': 'Invalid tracking number'}, room=request.sid)
        except Exception as e:
            flask_logger.error(f"Failed to emit error: {e}", extra={'tracking_number': str(tracking_number)})
        disconnect(request.sid)
        return

    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        update_data = {
            'tracking_number': sanitized_tn,
            'found': False,
            'error': 'Tracking number not found.'
        }
        if shipment:
            status = shipment.status
            checkpoints_str = shipment.checkpoints
            delivery_location = shipment.delivery_location
            checkpoints = checkpoints_str.split(';') if checkpoints_str else []
            coords = geocode_locations(checkpoints)
            coords_list = [{'lat': c['lat'], 'lon': c['lon'], 'desc': c['desc']} for c in coords]
            add_client(sanitized_tn, request.sid)
            update_data = {
                'tracking_number': sanitized_tn,
                'status': status,
                'checkpoints': checkpoints,
                'delivery_location': delivery_location,
                'coords': coords_list,
                'found': True,
                'paused': redis_client.hget("paused_simulations", sanitized_tn) == "true" if redis_client else False,
                'speed_multiplier': float(redis_client.hget("sim_speed_multipliers", sanitized_tn) or 1.0) if redis_client else 1.0
            }
        try:
            emit('tracking_update', update_data, room=request.sid)
            flask_logger.info(f"Sent tracking update to {request.sid}", extra={'tracking_number': sanitized_tn})
        except Exception as e:
            flask_logger.error(f"Failed to emit tracking update to {request.sid}: {e}", extra={'tracking_number': sanitized_tn})
            remove_client(sanitized_tn, request.sid)
            disconnect(request.sid)
    except SQLAlchemyError as e:
        db.session.rollback()
        flask_logger.error(f"Database error: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error for {sanitized_tn}: {e}[/error]", title="WebSocket Error", border_style="red"))
        try:
            emit('tracking_update', {'error': 'Database error'}, room=request.sid)
        except Exception:
            pass
        disconnect(request.sid)
    except Exception as e:
        flask_logger.error(f"Unexpected WebSocket error: {e}", extra={'tracking_number': sanitized_tn})
        try:
            emit('tracking_update', {'error': 'Server error'}, room=request.sid)
        except Exception:
            pass
        disconnect(request.sid)

@socketio.on('disconnect')
def handle_disconnect():
    """Handle WebSocket client disconnection."""
    try:
        if redis_client:
            for key in redis_client.scan_iter("clients:*"):
                tracking_number = key.split(':', 1)[1]
                remove_client(tracking_number, request.sid)
        else:
            for tracking_number in list(in_memory_clients.keys()):
                remove_client(tracking_number, request.sid)
        flask_logger.debug(f"Client disconnected: {request.sid}", extra={'tracking_number': ''})
    except Exception as e:
        flask_logger.error(f"Error on disconnect: {e}", extra={'tracking_number': ''})

# Initialize database on startup
try:
    with app.app_context():
        db.create_all()
    init_db()
except Exception as e:
    flask_logger.critical(f"Failed to initialize database on startup: {e}")
    console.print(Panel(f"[critical]Failed to initialize database on startup: {e}[/critical]", title="Startup Error", border_style="red"))
    raise

if __name__ == '__main__':
    try:
        cache_route_templates()
        flask_logger.info("Route templates cached successfully")
        console.print("[info]Route templates cached successfully[/info]")
    except Exception as e:
        flask_logger.error(f"Failed to cache route templates: {e}")
        console.print(Panel(f"[error]Route templates cache failed: {e}[/error]", title="Cache Error", border_style="red"))
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
    console.print("[info]Keep-alive thread started[/info]")
    notification_thread = threading.Thread(target=process_notification_queue, daemon=True)
    notification_thread.start()
    console.print("[info]Notification queue processing thread started[/info]")
    cleanup_thread = threading.Thread(target=cleanup_websocket_clients, daemon=True)
    cleanup_thread.start()
    console.print("[info]WebSocket cleanup thread started[/info]")
    socketio.run(app, host='0.0.0.0', port=10000, debug=app.config.get('FLASK_ENV') == 'development')
