# app.py
import eventlet
eventlet.monkey_patch()  # Patch for gevent/gunicorn compatibility

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

# Local imports from utils.py
from utils import (
    BotConfig, redis_client, console, get_app_modules, enqueue_notification,
    get_cached_route_templates, sanitize_tracking_number, validate_email,
    validate_location, validate_webhook_url, send_email_notification,
    check_bot_status, cache_route_templates, get_bot, get_shipment_details
)

# Logging setup
import logging
bot_logger = logging.getLogger('telegram_bot')
bot_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
bot_logger.addHandler(handler)

flask_logger = logging.getLogger('flask_app')
flask_logger.setLevel(logging.INFO)
flask_logger.addHandler(handler)

sim_logger = logging.getLogger('simulator')
sim_logger.setLevel(logging.INFO)
sim_logger.addHandler(handler)

# Initialize Flask app and core components
app = Flask(__name__)

# Load configuration from utils.py
try:
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        webhook_url=os.getenv("WEBHOOK_URL", "https://signment-9a96.onrender.com/telegram/webhook"),
        websocket_server=os.getenv("WEBSOCKET_SERVER", "https://signment-9a96.onrender.com"),
        allowed_admins=[int(uid) for uid in os.getenv("ALLOWED_ADMINS", "").split(",") if uid],
        valid_statuses=os.getenv("VALID_STATUSES", "Pending,In_Transit,Out_for_Delivery,Delivered,Returned,Delayed").split(","),
        route_templates=json.loads(os.getenv("ROUTE_TEMPLATES", '{"Lagos, NG": ["Lagos, NG"]}'))
    )
    app.config.update(
        TELEGRAM_BOT_TOKEN=config.telegram_bot_token,
        REDIS_URL=config.redis_url,
        WEBSOCKET_SERVER=config.websocket_server,
        SECRET_KEY=os.getenv("SECRET_KEY", "default-secret-key"),
        SQLALCHEMY_DATABASE_URI=os.getenv("SQLALCHEMY_DATABASE_URI", "sqlite:///shipments.db"),
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        SQLALCHEMY_POOL_SIZE=5,
        SQLALCHEMY_MAX_OVERFLOW=10,
        SMTP_HOST=os.getenv("SMTP_HOST", "smtp.gmail.com"),
        SMTP_PORT=int(os.getenv("SMTP_PORT", 587)),
        SMTP_USER=os.getenv("SMTP_USER", ""),
        SMTP_PASS=os.getenv("SMTP_PASS", ""),
        SMTP_FROM=os.getenv("SMTP_FROM", "no-reply@example.com"),
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
    flask_logger.error(f"Configuration validation failed: {e}")
    raise

db = SQLAlchemy(app)
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=app.config['RATELIMIT_DEFAULTS'],
    storage_uri=app.config['RATELIMIT_STORAGE_URI']
)
socketio = SocketIO(app, async_mode='eventlet', cors_allowed_origins="*")

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
            'created_at': self.created_at.isoformat(),
            'origin_location': self.origin_location,
            'webhook_url': self.webhook_url,
            'email_notifications': self.email_notifications
        }

# Database initialization
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

# reCAPTCHA verification
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

# Geocoding
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

# WebSocket client management
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

# Keep-alive mechanism
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
        time.sleep(60)

# Simulation logic
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
                sim_logger.debug(f"Updated shipment: status={status}, checkpoints={len(checkpoints)}", extra={'tracking_number': sanitized_tn})

                if send_notification and recipient_email:
                    enqueue_notification(sanitized_tn, "email", {
                        "status": status,
                        "checkpoints": ';'.join(checkpoints),
                        "delivery_location": delivery_location,
                        "recipient_email": recipient_email
                    })

                if webhook_url:
                    enqueue_notification(sanitized_tn, "webhook", {
                        "status": status,
                        "checkpoints": checkpoints,
                        "delivery_location": delivery_location,
                        "webhook_url": webhook_url
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

# Broadcast updates
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
        from forms import TrackForm
    except ImportError as e:
        flask_logger.error(f"forms.py not found: {e}")
        console.print(Panel(f"[error]forms.py not found: {e}[/error]", title="Server Error", border_style="red"))
        return "Server configuration error", 500
    form = TrackForm()
    flask_logger.info("Serving index page", extra={'tracking_number': ''})
    console.print("[info]Serving index page[/info]")
    return render_template('index.html', 
                         form=form, 
                         tawk_property_id=app.config['TAWK_PROPERTY_ID'], 
                         tawk_widget_id=app.config['TAWK_WIDGET_ID'], 
                         recaptcha_site_key=app.config['RECAPTCHA_SITE_KEY'])

@app.route('/track', methods=['POST'])
def track():
    """Handle tracking form submission and initiate simulation."""
    tracking_number = request.form.get('tracking_number') or (request.get_json(silent=True).get('tracking_number') if request.is_json else None)
    if not tracking_number:
        flask_logger.warning("Missing tracking number in request", extra={'tracking_number': ''})
        return jsonify({'success': False, 'error-codes': ['missing-input']}), 400
    sanitized_tn = sanitize_tracking_number(sanitize_input(tracking_number))
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number: {tracking_number}", extra={'tracking_number': str(tracking_number)})
        return jsonify({'success': False, 'error-codes': ['invalid-input-response']}), 400
    try:
        shipment = get_shipment_details(sanitized_tn)
        if not shipment:
            flask_logger.warning(f"Shipment not found: {sanitized_tn}", extra={'tracking_number': sanitized_tn})
            return jsonify({'success': False, 'error-codes': ['not-found']}), 404
        if request.form.get('email') and validate_email(request.form.get('email')):
            shipment_obj = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
            shipment_obj.recipient_email = request.form.get('email')
            db.session.commit()
            flask_logger.info(f"Updated recipient email for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
        if shipment['status'] not in ['Delivered', 'Returned']:
            eventlet.spawn(simulate_tracking, sanitized_tn)
        flask_logger.info(f"Tracking request processed", extra={'tracking_number': sanitized_tn})
        return jsonify({'success': True, 'data': shipment})
    except SQLAlchemyError as e:
        db.session.rollback()
        flask_logger.error(f"Database error: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error for {sanitized_tn}: {e}[/error]", title="Database Error", border_style="red"))
        return jsonify({'success': False, 'error-codes': ['database-error']}), 500
    except Exception as e:
        flask_logger.error(f"Unexpected error in track: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Unexpected error in track: {e}[/error]", title="Track Error", border_style="red"))
        return jsonify({'success': False, 'error-codes': ['server-error']}), 500

@app.route('/broadcast/<tracking_number>')
def trigger_broadcast(tracking_number):
    """Trigger a broadcast update for a specific tracking number."""
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number for broadcast: {tracking_number}", extra={'tracking_number': str(tracking_number)})
        return jsonify({'success': False, 'error-codes': ['invalid-input-response']}), 400
    eventlet.spawn(broadcast_update, sanitized_tn)
    flask_logger.info(f"Triggered broadcast", extra={'tracking_number': sanitized_tn})
    return '', 204

@app.route('/health', methods=['GET'])
def health_check():
    """Perform health check for application components."""
    status = {'status': 'healthy', 'database': 'ok', 'redis': 'unavailable', 'smtp': 'ok', 'telegram': 'unavailable'}
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
    except Exception as e:
        status['redis'] = str(e)
    try:
        with smtplib.SMTP(app.config['SMTP_HOST'], app.config['SMTP_PORT'], timeout=5) as server:
            server.starttls()
            server.login(app.config['SMTP_USER'], app.config['SMTP_PASS'])
        status['smtp'] = 'ok'
    except smtplib.SMTPException as e:
        status['smtp'] = str(e)
    try:
        bot = get_bot()
        bot.get_me()
        status['telegram'] = 'ok'
    except Exception as e:
        status['telegram'] = str(e)
    if status['status'] == 'healthy' and any(v != 'ok' for v in [status['database'], 'smtp']):
        status['status'] = 'unhealthy'
    flask_logger.info("Health check", extra=status)
    console.print(f"[info]Health check: {status}[/info]")
    return jsonify(status), 200 if status['status'] == 'healthy' else 500

@app.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    """Handle Telegram webhook updates."""
    try:
        update = request.get_json()
        if not update:
            bot_logger.error("Invalid webhook payload received")
            return '', 400
        bot = get_bot()
        bot.process_update(update)
        bot_logger.info("Processed Telegram webhook update")
        return '', 204
    except Exception as e:
        bot_logger.error(f"Error processing webhook update: {e}")
        return '', 500

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
            emit('tracking_update', {'success': False, 'error-codes': ['invalid-input-response']}, room=request.sid)
        except Exception as e:
            flask_logger.error(f"Failed to emit error: {e}", extra={'tracking_number': str(tracking_number)})
        disconnect(request.sid)
        return

    try:
        shipment = get_shipment_details(sanitized_tn)
        update_data = {
            'tracking_number': sanitized_tn,
            'found': False,
            'error': 'Tracking number not found.',
            'error-codes': ['not-found']
        }
        if shipment:
            status = shipment['status']
            checkpoints = shipment.get('checkpoints', '').split(';') if shipment.get('checkpoints') else []
            delivery_location = shipment['delivery_location']
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
                'speed_multiplier': float(redis_client.hget("sim_speed_multipliers", sanitized_tn) or 1.0) if redis_client else 1.0,
                'success': True
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
            emit('tracking_update', {'success': False, 'error-codes': ['database-error']}, room=request.sid)
        except Exception:
            pass
        disconnect(request.sid)
    except Exception as e:
        flask_logger.error(f"Unexpected WebSocket error: {e}", extra={'tracking_number': sanitized_tn})
        try:
            emit('tracking_update', {'success': False, 'error-codes': ['server-error']}, room=request.sid)
        except Exception:
            pass
        disconnect(request.sid)

@socketio.on('unsubscribe')
def handle_unsubscribe(data):
    """Handle WebSocket client unsubscription."""
    tracking_number = data.get('tracking_number')
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if sanitized_tn:
        remove_client(sanitized_tn, request.sid)
        flask_logger.debug(f"Client unsubscribed from {sanitized_tn}", extra={'tracking_number': sanitized_tn})

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
    socketio.run(app, host='0.0.0.0', port=5000, debug=app.config.get('FLASK_ENV') == 'development')
