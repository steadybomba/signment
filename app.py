from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit, disconnect
from flask_sqlalchemy import SQLAlchemy
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import eventlet
import json
import random
import time
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv
import redis
import logging
from logging.handlers import RotatingFileHandler
from pythonjsonlogger import jsonlogger
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ForceReply
import threading
import string
from sqlalchemy.exc import SQLAlchemyError
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.theme import Theme
import importlib.metadata

# Initialize rich console with custom theme
custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "critical": "bold red underline",
    "debug": "dim green"
})
console = Console(theme=custom_theme)

# Load environment variables
load_dotenv()

# Configure logging for Flask
flask_logger = logging.getLogger('flask_app')
flask_logger.setLevel(logging.DEBUG)
flask_file_handler = RotatingFileHandler('tracking_app.log', maxBytes=1000000, backupCount=5)
flask_file_handler.setFormatter(jsonlogger.JsonFormatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s %(funcName)s %(tracking_number)s'
))
flask_console_handler = RichHandler(console=console, markup=True, rich_tracebacks=True)
flask_console_handler.setLevel(logging.INFO)
flask_logger.addHandler(flask_file_handler)
flask_logger.addHandler(flask_console_handler)

# Configure logging for Telegram bot
bot_logger = logging.getLogger('telegram_bot')
bot_logger.setLevel(logging.DEBUG)
bot_file_handler = RotatingFileHandler('admin_bot.log', maxBytes=1000000, backupCount=5)
bot_file_handler.setFormatter(jsonlogger.JsonFormatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s %(funcName)s %(tracking_number)s'
))
bot_console_handler = RichHandler(console=console, markup=True, rich_tracebacks=True)
bot_console_handler.setLevel(logging.INFO)
bot_logger.addHandler(bot_file_handler)
bot_logger.addHandler(bot_console_handler)

# Configure logging for simulation
sim_logger = logging.getLogger('simulation')
sim_logger.setLevel(logging.DEBUG)
sim_file_handler = RotatingFileHandler('simulation.log', maxBytes=1000000, backupCount=5)
sim_file_handler.setFormatter(jsonlogger.JsonFormatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s %(funcName)s %(tracking_number)s'
))
sim_console_handler = RichHandler(console=console, markup=True, rich_tracebacks=True)
sim_console_handler.setLevel(logging.INFO)
sim_logger.addHandler(sim_file_handler)
sim_logger.addHandler(sim_console_handler)

# Validate environment variables
required_env_vars = [
    'SECRET_KEY', 'SQLALCHEMY_DATABASE_URI', 'SMTP_HOST', 'SMTP_USER',
    'SMTP_PASS', 'SMTP_FROM', 'RECAPTCHA_SITE_KEY', 'RECAPTCHA_SECRET_KEY',
    'TELEGRAM_BOT_TOKEN'
]
for var in required_env_vars:
    if not os.getenv(var):
        flask_logger.critical(f"Missing required environment variable: {var}", extra={'tracking_number': ''})
        bot_logger.critical(f"Missing required environment variable: {var}", extra={'tracking_number': ''})
        console.print(Panel(f"[critical]Missing environment variable: {var}[/critical]", title="Startup Error", border_style="red"))
        raise ValueError(f"Missing required environment variable: {var}")

# Display startup banner
try:
    flask_version = importlib.metadata.version('flask')
except importlib.metadata.PackageNotFoundError:
    flask_version = "unknown"
console.print(Panel(
    f"[bold cyan]Tracking App Starting[/bold cyan]\n"
    f"Flask Version: {flask_version}\n"
    f"Environment: {os.getenv('FLASK_ENV', 'production')}\n"
    f"Database: PostgreSQL\n"
    f"SMTP: {os.getenv('SMTP_HOST')}\n"
    f"Redis: {os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', 6379)}\n"
    f"Telegram Bot: Enabled",
    title="Tracking App", border_style="green"
))

# Flask and SocketIO setup
app = Flask(__name__, static_url_path='/static', static_folder='static')
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('SQLALCHEMY_DATABASE_URI')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
socketio = SocketIO(app, async_mode='eventlet', ping_timeout=20, ping_interval=10)

# Rate limiting
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)

# Telegram bot setup
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
ALLOWED_ADMINS = [int(id) for id in os.getenv('ADMIN_USER_IDS', '').split(',') if id.strip().isdigit()]
bot = telebot.TeleBot(BOT_TOKEN)

# Shared configurations
SMTP_HOST = os.getenv('SMTP_HOST')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SMTP_USER = os.getenv('SMTP_USER')
SMTP_PASS = os.getenv('SMTP_PASS')
SMTP_FROM = os.getenv('SMTP_FROM')
RECAPTCHA_SITE_KEY = os.getenv('RECAPTCHA_SITE_KEY')
RECAPTCHA_SECRET_KEY = os.getenv('RECAPTCHA_SECRET_KEY')
RECAPTCHA_VERIFY_URL = "https://www.google.com/recaptcha/api/siteverify"
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
WEBSOCKET_SERVER = os.getenv('WEBSOCKET_SERVER', 'http://localhost:5000')
TAWK_PROPERTY_ID = os.getenv('TAWK_PROPERTY_ID')
TAWK_WIDGET_ID = os.getenv('TAWK_WIDGET_ID')
GLOBAL_WEBHOOK_URL = os.getenv('GLOBAL_WEBHOOK_URL')

# Redis client
redis_client = None
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=0,
        decode_responses=True
    )
    redis_client.ping()
    console.print("[info]Redis connection established[/info]")
except redis.RedisError as e:
    flask_logger.warning(f"Redis connection failed: {e}. Falling back to in-memory client tracking.", extra={'tracking_number': ''})
    console.print(Panel(f"[warning]Redis connection failed: {e}. Using in-memory client tracking.[/warning]", title="Redis Warning", border_style="yellow"))
    redis_client = None

# In-memory fallback for client tracking
in_memory_clients = {}

# Valid statuses
VALID_STATUSES = ['Pending', 'In_Transit', 'Delayed', 'Customs_Hold', 'Out_for_Delivery', 'Delivered', 'Returned']

# Simulation constants
STATUS_TRANSITIONS = {
    'Pending': {'next': ['In_Transit'], 'delay': (30, 120), 'events': {}},
    'In_Transit': {'next': ['Out_for_Delivery', 'Delayed', 'Customs_Hold'], 'delay': (120, 600), 'probabilities': [0.8, 0.1, 0.1]},
    'Delayed': {'next': ['In_Transit'], 'delay': (300, 900), 'events': {'Delayed due to weather', 'Delayed due to traffic'}},
    'Customs_Hold': {'next': ['In_Transit'], 'delay': (600, 1200), 'events': {'Held at customs for inspection'}},
    'Out_for_Delivery': {'next': ['Delivered', 'Returned'], 'delay': (60, 180), 'probabilities': [0.95, 0.05]},
    'Returned': {'next': [], 'delay': (0, 0), 'events': {'Returned to sender'}},
    'Delivered': {'next': [], 'delay': (0, 0), 'events': {}}
}

# Route templates
ROUTE_TEMPLATES = {
    'Lagos, NG': ['Lagos, NG - Origin Sorting', 'Accra, GH - Transit Hub', 'New York, NY - Port of Entry', 'Newark, NJ - Customs Clearance', 'New York, NY - Regional Distribution', 'New York, NY - Out for Delivery'],
    'Nairobi, KE': ['Nairobi, KE - Origin Sorting', 'Dar es Salaam, TZ - Transit Hub', 'Dubai, AE - International Gateway', 'Los Angeles, CA - Port of Entry', 'Los Angeles, CA - Customs Clearance', 'Los Angeles, CA - Regional Distribution', 'Los Angeles, CA - Out for Delivery'],
    'London, UK': ['London, UK - Origin Sorting', 'Rotterdam, NL - Transit Hub', 'Singapore, SG - Port of Entry', 'Singapore, SG - Customs Clearance', 'Shanghai, CN - Customs Clearance', 'Shanghai, CN - Regional Distribution', 'Shanghai, CN - Out for Delivery'],
    'Paris, FR': ['Paris, FR - Origin Sorting', 'Hamburg, DE - Transit Hub', 'Hong Kong, HK - Port of Entry', 'Hong Kong, HK - Customs Clearance', 'Guangzhou, CN - Customs Clearance', 'Guangzhou, CN - Regional Distribution', 'Guangzhou, CN - Out for Delivery'],
    'New York, NY': ['New York, NY - Origin Sorting', 'Miami, FL - Transit Hub', 'Rotterdam, NL - Port of Entry', 'Rotterdam, NL - Customs Clearance', 'Antwerp, BE - Customs Clearance', 'London, UK - Regional Distribution', 'London, UK - Out for Delivery'],
    'Los Angeles, CA': ['Los Angeles, CA - Origin Sorting', 'Seattle, WA - Transit Hub', 'Tokyo, JP - International Gateway', 'Sydney, AU - Port of Entry', 'Sydney, AU - Customs Clearance', 'Melbourne, AU - Regional Distribution', 'Melbourne, AU - Out for Delivery'],
    'Tokyo, JP': ['Tokyo, JP - Origin Sorting', 'Shanghai, CN - Transit Hub', 'Dubai, AE - International Gateway', 'Nairobi, KE - Port of Entry', 'Nairobi, KE - Customs Clearance', 'Lagos, NG - Customs Clearance', 'Lagos, NG - Regional Distribution', 'Lagos, NG - Out for Delivery'],
    'Shanghai, CN': ['Shanghai, CN - Origin Sorting', 'Hong Kong, HK - Transit Hub', 'Mumbai, IN - International Gateway', 'Cape Town, ZA - Port of Entry', 'Cape Town, ZA - Customs Clearance', 'Johannesburg, ZA - Regional Distribution', 'Johannesburg, ZA - Out for Delivery']
}

# Cache geocoded locations
geocode_cache = {}

# Database model
class Shipment(db.Model):
    __tablename__ = 'shipments'
    tracking_number = db.Column(db.String(50), primary_key=True)
    status = db.Column(db.String(50))
    checkpoints = db.Column(db.Text)
    delivery_location = db.Column(db.Text)
    last_updated = db.Column(db.DateTime)
    recipient_email = db.Column(db.String(255))
    created_at = db.Column(db.DateTime)
    origin_location = db.Column(db.Text)
    webhook_url = db.Column(db.Text)
    email_notifications = db.Column(db.Boolean, default=True)

    def to_dict(self):
        return {
            'tracking_number': self.tracking_number,
            'status': self.status,
            'checkpoints': self.checkpoints,
            'delivery_location': self.delivery_location,
            'last_updated': self.last_updated,
            'recipient_email': self.recipient_email,
            'created_at': self.created_at,
            'origin_location': self.origin_location,
            'webhook_url': self.webhook_url,
            'email_notifications': self.email_notifications
        }

# Shared utility functions
def sanitize_tracking_number(tracking_number):
    if not tracking_number or not isinstance(tracking_number, str):
        sim_logger.debug("Invalid tracking number provided", extra={'tracking_number': str(tracking_number)})
        console.print(f"[error]Invalid tracking number: {tracking_number}[/error]")
        return None
    sanitized = re.sub(r'[^a-zA-Z0-9\-]', '', tracking_number.strip())[:50]
    return sanitized if sanitized else None

def send_email_notification(tracking_number, status, checkpoints, delivery_location, recipient_email):
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment or not shipment.email_notifications:
            flask_logger.debug("Email notifications disabled", extra={'tracking_number': tracking_number})
            return
    except SQLAlchemyError as e:
        flask_logger.error(f"Database error checking email notifications: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Database error for {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
        return

    if not recipient_email:
        flask_logger.warning("No recipient email provided", extra={'tracking_number': tracking_number})
        return
    subject = f"Shipment Update: Tracking {tracking_number}"
    body = f"""
    Dear Customer,

    Your shipment with tracking number {tracking_number} has been updated.

    Status: {status}
    Delivery Location: {delivery_location}
    Latest Checkpoint: {checkpoints.split(';')[-1] if checkpoints else 'No checkpoints available'}

    For more details, visit our tracking portal.

    Best regards,
    Tracking Service Team
    """
    msg = MIMEMultipart()
    msg['From'] = SMTP_FROM
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
            flask_logger.info(f"Email sent to {recipient_email} for status: {status}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Email sent for {tracking_number}[/info]")
    except smtplib.SMTPException as e:
        flask_logger.error(f"Failed to send email: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Email failed for {tracking_number}: {e}[/error]", title="Email Error", border_style="red"))

def init_db():
    try:
        db.create_all()
        flask_logger.info("Database initialized successfully", extra={'tracking_number': ''})
        console.print("[info]Database initialized[/info]")
    except SQLAlchemyError as e:
        flask_logger.error(f"Database initialization failed: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Database initialization failed: {e}[/error]", title="Database Error", border_style="red"))
        raise

# Flask-specific functions
def verify_recaptcha(response_token):
    try:
        payload = {
            'secret': RECAPTCHA_SECRET_KEY,
            'response': response_token
        }
        response = requests.post(RECAPTCHA_VERIFY_URL, data=payload, timeout=5)
        result = response.json()
        if result.get('success') and result.get('score', 1.0) >= 0.5:
            flask_logger.debug(f"reCAPTCHA verification successful: score={result.get('score')}", extra={'tracking_number': ''})
            return True
        else:
            flask_logger.warning(f"reCAPTCHA verification failed: {result}", extra={'tracking_number': ''})
            return False
    except requests.RequestException as e:
        flask_logger.error(f"reCAPTCHA verification error: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]reCAPTCHA error: {e}[/error]", title="reCAPTCHA Error", border_style="red"))
        return False

def geocode_locations(checkpoints):
    geolocator = Nominatim(user_agent="tracking_simulator")
    coords = []
    for checkpoint in checkpoints:
        if checkpoint in geocode_cache:
            coords.append(geocode_cache[checkpoint])
            continue
        parts = checkpoint.split(' - ')
        if len(parts) >= 2:
            location = parts[1].strip()
            try:
                geo = geolocator.geocode(location, timeout=5)
                if geo:
                    coord = (geo.latitude, geo.longitude, checkpoint)
                    geocode_cache[checkpoint] = coord
                    coords.append(coord)
                    flask_logger.debug(f"Geocoded {location}: {coord}", extra={'tracking_number': ''})
            except GeocoderTimedOut:
                flask_logger.warning(f"Geocoding timed out for {location}", extra={'tracking_number': ''})
    return coords

def add_client(tracking_number, sid):
    if redis_client:
        try:
            redis_client.sadd(f"clients:{tracking_number}", sid)
            flask_logger.debug(f"Added client {sid}", extra={'tracking_number': tracking_number})
        except redis.RedisError as e:
            flask_logger.error(f"Redis error adding client {sid}: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[error]Redis error for {tracking_number}: {e}[/error]", title="Redis Error", border_style="red"))
    else:
        if tracking_number not in in_memory_clients:
            in_memory_clients[tracking_number] = set()
        in_memory_clients[tracking_number].add(sid)
        flask_logger.debug(f"Added client {sid} to in-memory store", extra={'tracking_number': tracking_number})

def remove_client(tracking_number, sid):
    if redis_client:
        try:
            redis_client.srem(f"clients:{tracking_number}", sid)
            flask_logger.debug(f"Removed client {sid}", extra={'tracking_number': tracking_number})
        except redis.RedisError as e:
            flask_logger.error(f"Redis error removing client {sid}: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[error]Redis error for {tracking_number}: {e}[/error]", title="Redis Error", border_style="red"))
    else:
        if tracking_number in in_memory_clients:
            in_memory_clients[tracking_number].discard(sid)
            flask_logger.debug(f"Removed client {sid} from in-memory store", extra={'tracking_number': tracking_number})

def get_clients(tracking_number):
    if redis_client:
        try:
            clients = redis_client.smembers(f"clients:{tracking_number}")
            flask_logger.debug(f"Fetched clients: {clients}", extra={'tracking_number': tracking_number})
            return clients
        except redis.RedisError as e:
            flask_logger.error(f"Redis error fetching clients: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[error]Redis error for {tracking_number}: {e}[/error]", title="Redis Error", border_style="red"))
            return set()
    else:
        clients = in_memory_clients.get(tracking_number, set())
        flask_logger.debug(f"Fetched clients from in-memory store: {clients}", extra={'tracking_number': tracking_number})
        return clients

def simulate_tracking(tracking_number):
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
                webhook_url = shipment.webhook_url or GLOBAL_WEBHOOK_URL
                recipient_email = shipment.recipient_email

                if status in ['Delivered', 'Returned']:
                    progress.update(task, advance=100, description=f"Completed {sanitized_tn}: {status}")
                    sim_logger.info(f"Simulation completed: {status}", extra={'tracking_number': sanitized_tn})
                    console.print(f"[info]Simulation completed for {sanitized_tn}: {status}[/info]")
                    break

                current_time = datetime.now()
                template = ROUTE_TEMPLATES.get(delivery_location, ROUTE_TEMPLATES.get(origin_location, ROUTE_TEMPLATES['Lagos, NG']))
                transition = STATUS_TRANSITIONS.get(status, {'next': ['Delivered'], 'delay': (60, 300), 'probabilities': [1.0], 'events': {}})
                delay_range = transition['delay']
                next_states = transition['next']
                probabilities = transition.get('probabilities', [1.0 / len(next_states)] * len(next_states))
                events = transition.get('events', {})

                route_length = len(template)
                delay_multiplier = 1 + (route_length / 10)
                delay = random.uniform(delay_range[0], delay_range[1]) * delay_multiplier

                if 'Out for Delivery' not in status and 'Delivered' not in status:
                    next_index = min(len(checkpoints), len(template) - 1)
                    next_checkpoint = f"{current_time.strftime('%Y-%m-%d %H:%M')} - {template[next_index]} - Processed"
                    if next_checkpoint not in checkpoints:
                        checkpoints.append(next_checkpoint)
                        sim_logger.debug(f"Added checkpoint: {next_checkpoint}", extra={'tracking_number': sanitized_tn})

                new_status = random.choices(next_states, probabilities)[0]
                send_notification = new_status != status
                if new_status != status:
                    status = new_status
                    if status in events:
                        event_msg = random.choice(list(events)) if isinstance(events, set) else random.choice(events)
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
                    eventlet.spawn(send_email_notification, sanitized_tn, status, ';'.join(checkpoints), delivery_location, recipient_email)

                payload = {
                    'tracking_number': sanitized_tn,
                    'status': status,
                    'checkpoints': checkpoints,
                    'delivery_location': delivery_location,
                    'last_updated': current_time.isoformat(),
                    'event': 'status_update'
                }
                try:
                    response = requests.post(webhook_url, json=payload, timeout=5)
                    if response.status_code != 200:
                        sim_logger.warning(f"Webhook failed: {response.status_code} - {response.text}", extra={'tracking_number': sanitized_tn})
                    else:
                        sim_logger.debug(f"Webhook sent: {payload}", extra={'tracking_number': sanitized_tn})
                except requests.RequestException as e:
                    sim_logger.error(f"Webhook error: {e}", extra={'tracking_number': sanitized_tn})

                broadcast_update(sanitized_tn)

                sim_logger.debug(f"Sleeping for {delay:.2f} seconds", extra={'tracking_number': sanitized_tn})
                eventlet.sleep(delay)
                retries = 0
            except SQLAlchemyError as e:
                sim_logger.error(f"Database error: {e}", extra={'tracking_number': sanitized_tn})
                console.print(Panel(f"[error]Database error for {sanitized_tn}: {e}[/error]", title="Simulation Error", border_style="red"))
                retries += 1
                if retries >= max_retries:
                    sim_logger.critical(f"Max retries exceeded. Stopping simulation.", extra={'tracking_number': sanitized_tn})
                    console.print(Panel(f"[critical]Max retries exceeded for {sanitized_tn}[/critical]", title="Simulation Error", border_style="red"))
                    break
                eventlet.sleep(2 ** retries)

def broadcast_update(tracking_number):
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.error("Invalid tracking number for broadcast", extra={'tracking_number': str(tracking_number)})
        console.print(Panel(f"[error]Invalid tracking number for broadcast: {tracking_number}[/error]", title="Broadcast Error", border_style="red"))
        return

    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        if shipment:
            status = shipment.status
            checkpoints_str = shipment.checkpoints
            delivery_location = shipment.delivery_location
            checkpoints = checkpoints_str.split(';') if checkpoints_str else []
            coords = geocode_locations(checkpoints)
            coords_list = [{'lat': lat, 'lon': lon, 'desc': desc} for lat, lon, desc in coords]
            update_data = json.dumps({
                'tracking_number': sanitized_tn,
                'status': status,
                'checkpoints': checkpoints,
                'delivery_location': delivery_location,
                'coords': coords_list,
                'found': True
            }).encode('utf-8')
            for sid in get_clients(sanitized_tn):
                socketio.emit('tracking_update', update_data, room=sid, namespace='/', binary=True)
                flask_logger.debug(f"Broadcast update to {sid}", extra={'tracking_number': sanitized_tn})
        else:
            for sid in get_clients(sanitized_tn):
                socketio.emit('tracking_update', {
                    'tracking_number': sanitized_tn,
                    'found': False,
                    'error': 'Tracking number not found.'
                }, room=sid)
                flask_logger.warning("Tracking number not found for broadcast", extra={'tracking_number': sanitized_tn})
                console.print(f"[warning]Tracking number not found: {sanitized_tn}[/warning]")
    except SQLAlchemyError as e:
        flask_logger.error(f"Database error during broadcast: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error during broadcast for {sanitized_tn}: {e}[/error]", title="Broadcast Error", border_style="red"))

# Flask routes
@app.route('/')
@limiter.limit("100 per hour")
def index():
    flask_logger.info("Serving index page", extra={'tracking_number': ''})
    console.print("[info]Serving index page[/info]")
    return render_template('index.html', tawk_property_id=TAWK_PROPERTY_ID, tawk_widget_id=TAWK_WIDGET_ID, recaptcha_site_key=RECAPTCHA_SITE_KEY)

@app.route('/track', methods=['POST'])
@limiter.limit("50 per hour")
def track():
    recaptcha_response = request.form.get('g-recaptcha-response')
    if not recaptcha_response:
        flask_logger.warning("No reCAPTCHA response provided", extra={'tracking_number': ''})
        return jsonify({'error': 'reCAPTCHA verification required'}), 400
    if not verify_recaptcha(recaptcha_response):
        flask_logger.warning("reCAPTCHA verification failed", extra={'tracking_number': ''})
        return jsonify({'error': 'reCAPTCHA verification failed'}), 400

    tracking_number = request.form.get('tracking_number')
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number submitted: {tracking_number}", extra={'tracking_number': str(tracking_number)})
        return jsonify({'error': 'Invalid tracking number'}), 400
    eventlet.spawn(simulate_tracking, sanitized_tn)
    flask_logger.info(f"Started tracking simulation", extra={'tracking_number': sanitized_tn})
    console.print(f"[info]Started tracking simulation for {sanitized_tn}[/info]")
    return render_template('tracking_result.html', tracking_number=sanitized_tn, tawk_property_id=TAWK_PROPERTY_ID, tawk_widget_id=TAWK_WIDGET_ID)

@app.route('/broadcast/<tracking_number>')
@limiter.limit("20 per hour")
def trigger_broadcast(tracking_number):
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number for broadcast: {tracking_number}", extra={'tracking_number': str(tracking_number)})
        return jsonify({'error': 'Invalid tracking number'}), 400
    eventlet.spawn(broadcast_update, sanitized_tn)
    flask_logger.info(f"Triggered broadcast", extra={'tracking_number': sanitized_tn})
    return '', 204

@app.route('/health', methods=['GET'])
@limiter.limit("100 per hour")
def health_check():
    try:
        db.session.execute('SELECT 1')
        redis_status = 'ok' if redis_client and redis_client.ping() else 'unavailable'
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
        flask_logger.debug("Health check passed", extra={'tracking_number': ''})
        console.print(f"[info]Health check passed: DB=ok, Redis={redis_status}, SMTP=ok[/info]")
        return jsonify({'status': 'healthy', 'database': 'ok', 'redis': redis_status, 'smtp': 'ok'}), 200
    except (SQLAlchemyError, redis.RedisError, smtplib.SMTPException) as e:
        flask_logger.error(f"Health check failed: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Health check failed: {e}[/error]", title="Health Check Error", border_style="red"))
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

# SocketIO handlers
@socketio.on('connect')
def handle_connect():
    flask_logger.debug(f"Client connected: {request.sid}", extra={'tracking_number': ''})
    emit('status', {'message': 'Connected to tracking service'}, broadcast=False)

@socketio.on('request_tracking')
def handle_request_tracking(data):
    tracking_number = data.get('tracking_number')
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number in WebSocket request: {tracking_number}", extra={'tracking_number': str(tracking_number)})
        emit('tracking_update', {'error': 'Invalid tracking number'}, room=request.sid)
        disconnect(request.sid)
        return

    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        if shipment:
            status = shipment.status
            checkpoints_str = shipment.checkpoints
            delivery_location = shipment.delivery_location
            checkpoints = checkpoints_str.split(';') if checkpoints_str else []
            coords = geocode_locations(checkpoints)
            coords_list = [{'lat': lat, 'lon': lon, 'desc': desc} for lat, lon, desc in coords]
            add_client(sanitized_tn, request.sid)
            emit('tracking_update', {
                'tracking_number': sanitized_tn,
                'status': status,
                'checkpoints': checkpoints,
                'delivery_location': delivery_location,
                'coords': coords_list,
                'found': True
            }, room=request.sid)
            flask_logger.info(f"Sent tracking update to {request.sid}", extra={'tracking_number': sanitized_tn})
        else:
            emit('tracking_update', {
                'tracking_number': sanitized_tn,
                'found': False,
                'error': 'Tracking number not found.'
            }, room=request.sid)
            flask_logger.warning("Tracking number not found", extra={'tracking_number': sanitized_tn})
            disconnect(request.sid)
    except SQLAlchemyError as e:
        flask_logger.error(f"Database error: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error for {sanitized_tn}: {e}[/error]", title="WebSocket Error", border_style="red"))
        emit('tracking_update', {'error': 'Database error'}, room=request.sid)
        disconnect(request.sid)

@socketio.on('disconnect')
def handle_disconnect():
    for tracking_number in (in_memory_clients.keys() if not redis_client else redis_client.scan_iter("clients:*")):
        key = tracking_number.replace("clients:", "") if redis_client else tracking_number
        remove_client(key, request.sid)
    flask_logger.debug(f"Client disconnected: {request.sid}", extra={'tracking_number': ''})

# Telegram bot functions
def is_admin(user_id):
    is_admin_user = user_id in ALLOWED_ADMINS
    bot_logger.debug(f"Checked admin status for user {user_id}: {is_admin_user}", extra={'tracking_number': ''})
    return is_admin_user

def generate_unique_id():
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    random_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    new_id = f"TRK{timestamp}{random_str}"
    bot_logger.debug(f"Generated ID: {new_id}", extra={'tracking_number': new_id})
    console.print(f"[info]Generated tracking ID: {new_id}[/info]")
    return new_id

def get_shipment_list():
    try:
        shipments = Shipment.query.with_entities(Shipment.tracking_number).all()
        bot_logger.debug(f"Fetched shipment list: {len(shipments)} shipments", extra={'tracking_number': ''})
        return [s.tracking_number for s in shipments]
    except SQLAlchemyError as e:
        bot_logger.error(f"Database error fetching shipment list: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Database error fetching shipment list: {e}[/error]", title="Database Error", border_style="red"))
        return []

def get_shipment_details(tracking_number):
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        bot_logger.debug(f"Fetched details for {tracking_number}", extra={'tracking_number': tracking_number})
        return shipment.to_dict() if shipment else None
    except SQLAlchemyError as e:
        bot_logger.error(f"Database error fetching details: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Database error fetching details for {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
        return None

def save_shipment(tracking_number, status, checkpoints, delivery_location, recipient_email='', origin_location=None, webhook_url=None, email_notifications=True):
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        bot_logger.error("Invalid tracking number", extra={'tracking_number': str(tracking_number)})
        console.print(Panel(f"[error]Invalid tracking number: {tracking_number}[/error]", title="Database Error", border_style="red"))
        raise ValueError("Invalid tracking number")
    if status not in VALID_STATUSES:
        bot_logger.error(f"Invalid status: {status}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Invalid status for {sanitized_tn}: {status}[/error]", title="Database Error", border_style="red"))
        raise ValueError("Invalid status")
    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        last_updated = datetime.now()
        origin_location = origin_location or delivery_location
        webhook_url = webhook_url or None
        if shipment:
            shipment.status = status
            shipment.checkpoints = checkpoints
            shipment.delivery_location = delivery_location
            shipment.last_updated = last_updated
            shipment.recipient_email = recipient_email
            shipment.origin_location = origin_location
            shipment.webhook_url = webhook_url
            shipment.email_notifications = email_notifications
        else:
            shipment = Shipment(
                tracking_number=sanitized_tn,
                status=status,
                checkpoints=checkpoints,
                delivery_location=delivery_location,
                last_updated=last_updated,
                recipient_email=recipient_email,
                created_at=last_updated,
                origin_location=origin_location,
                webhook_url=webhook_url,
                email_notifications=email_notifications
            )
            db.session.add(shipment)
        db.session.commit()
        bot_logger.info(f"Saved shipment: status={status}, delivery_location={delivery_location}", extra={'tracking_number': sanitized_tn})
        console.print(f"[info]Saved shipment {sanitized_tn}: {status}[/info]")
        eventlet.spawn(send_email_notification, sanitized_tn, status, checkpoints, delivery_location, recipient_email)
        try:
            response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{sanitized_tn}', timeout=5)
            if response.status_code != 204:
                bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': sanitized_tn})
        except requests.RequestException as e:
            bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': sanitized_tn})
    except SQLAlchemyError as e:
        db.session.rollback()
        bot_logger.error(f"Database error saving shipment: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error saving {sanitized_tn}: {e}[/error]", title="Database Error", border_style="red"))
        raise

def send_dynamic_menu(chat_id, message_id=None):
    markup = InlineKeyboardMarkup()
    markup.row_width = 2
    markup.add(
        InlineKeyboardButton("Generate ID", callback_data="generate_id"),
        InlineKeyboardButton("Add Shipment", callback_data="add")
    )
    shipments = get_shipment_list()
    if shipments:
        markup.add(
            InlineKeyboardButton("View Shipment", callback_data="view_menu"),
            InlineKeyboardButton("Update Shipment", callback_data="update_menu")
        )
        markup.add(
            InlineKeyboardButton("Delete Shipment", callback_data="delete_menu"),
            InlineKeyboardButton("Batch Delete", callback_data="batch_delete_menu")
        )
        markup.add(
            InlineKeyboardButton("Trigger Broadcast", callback_data="broadcast_menu"),
            InlineKeyboardButton("Toggle Email", callback_data="toggle_email_menu")
        )
        markup.add(InlineKeyboardButton("List Shipments", callback_data="list"))
    markup.add(
        InlineKeyboardButton("Settings", callback_data="settings"),
        InlineKeyboardButton("Help", callback_data="help")
    )
    try:
        if message_id:
            bot.edit_message_text("Choose an action:", chat_id=chat_id, message_id=message_id, reply_markup=markup)
        else:
            bot.send_message(chat_id, "Choose an action:", reply_markup=markup)
        bot_logger.debug("Sent dynamic menu", extra={'tracking_number': ''})
    except telebot.apihelper.ApiTelegramException as e:
        bot_logger.error(f"Telegram API error sending menu: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Telegram API error sending menu to {chat_id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['myid'])
def get_my_id(message):
    bot.reply_to(message, f"Your Telegram user ID: {message.from_user.id}")
    bot_logger.info(f"User requested their ID", extra={'tracking_number': ''})

@bot.message_handler(commands=['start', 'menu'])
def send_menu(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning("Access denied for user", extra={'tracking_number': ''})
        return
    send_dynamic_menu(message.chat.id)
    bot_logger.info("Menu sent to admin", extra={'tracking_number': ''})
    console.print(f"[info]Menu sent to admin {message.from_user.id}[/info]")

@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    bot.last_call = call
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        bot_logger.warning("Access denied for callback", extra={'tracking_number': ''})
        return

    try:
        if call.data == "generate_id":
            new_id = generate_unique_id()
            bot.answer_callback_query(call.id, f"Generated ID: {new_id}")
            bot_logger.info(f"Generated ID: {new_id}", extra={'tracking_number': new_id})
        elif call.data == "add":
            msg = bot.send_message(
                call.message.chat.id,
                "Enter shipment details:\nFormat: [tracking_number] <status> \"<checkpoints>\" <delivery_location> <recipient_email> [origin_location] [webhook_url]",
                reply_markup=ForceReply(selective=True)
            )
            bot.register_next_step_handler(msg, handle_add_input)
            bot_logger.debug("Prompted to add shipment", extra={'tracking_number': ''})
            console.print(f"[info]Admin {call.from_user.id} prompted to add shipment[/info]")
        elif call.data == "view_menu":
            shipments = get_shipment_list()
            if shipments:
                markup = InlineKeyboardMarkup(row_width=1)
                for tn in shipments:
                    markup.add(InlineKeyboardButton(tn, callback_data=f"view_{tn}"))
                markup.add(InlineKeyboardButton("Back", callback_data="menu"))
                bot.edit_message_text("Select shipment to view:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
                bot_logger.debug("View menu sent", extra={'tracking_number': ''})
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug("No shipments for view menu", extra={'tracking_number': ''})
        elif call.data.startswith("view_"):
            tracking_number = sanitize_tracking_number(call.data.replace("view_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error("Invalid tracking number for view", extra={'tracking_number': str(tracking_number)})
                return
            details = get_shipment_details(tracking_number)
            if details:
                response = (
                    f"Shipment: {details['tracking_number']}\n"
                    f"Status: {details['status']}\n"
                    f"Delivery Location: {details['delivery_location']}\n"
                    f"Origin Location: {details['origin_location']}\n"
                    f"Recipient Email: {details['recipient_email'] or 'None'}\n"
                    f"Checkpoints: {details['checkpoints'] or 'None'}\n"
                    f"Webhook URL: {details['webhook_url'] or 'Default'}\n"
                    f"Email Notifications: {'Enabled' if details['email_notifications'] else 'Disabled'}\n"
                    f"Last Updated: {details['last_updated']}"
                )
                bot.answer_callback_query(call.id)
                bot.send_message(call.message.chat.id, response)
                bot_logger.info(f"Sent details for {tracking_number}", extra={'tracking_number': tracking_number})
            else:
                bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.")
                bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
        elif call.data == "update_menu":
            shipments = get_shipment_list()
            if shipments:
                markup = InlineKeyboardMarkup(row_width=1)
                for tn in shipments:
                    markup.add(InlineKeyboardButton(tn, callback_data=f"update_{tn}"))
                markup.add(InlineKeyboardButton("Back", callback_data="menu"))
                bot.edit_message_text("Select shipment to update:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
                bot_logger.debug("Update menu sent", extra={'tracking_number': ''})
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug("No shipments for update menu", extra={'tracking_number': ''})
        elif call.data.startswith("update_"):
            tracking_number = sanitize_tracking_number(call.data.replace("update_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error("Invalid tracking number for update", extra={'tracking_number': str(tracking_number)})
                return
            msg = bot.send_message(
                call.message.chat.id,
                f"Enter updates for {tracking_number}:\nFormat: <field=value> ... (e.g., status=In_Transit delivery_location=\"New York, NY\")",
                reply_markup=ForceReply(selective=True)
            )
            bot.register_next_step_handler(msg, lambda m: handle_update_input(m, tracking_number))
            bot_logger.debug(f"Prompted to update {tracking_number}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Admin {call.from_user.id} prompted to update {tracking_number}[/info]")
        elif call.data == "delete_menu":
            shipments = get_shipment_list()
            if shipments:
                markup = InlineKeyboardMarkup(row_width=1)
                for tn in shipments:
                    markup.add(InlineKeyboardButton(tn, callback_data=f"delete_{tn}"))
                markup.add(InlineKeyboardButton("Back", callback_data="menu"))
                bot.edit_message_text("Select shipment to delete:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
                bot_logger.debug("Delete menu sent", extra={'tracking_number': ''})
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug("No shipments for delete menu", extra={'tracking_number': ''})
        elif call.data.startswith("delete_"):
            tracking_number = sanitize_tracking_number(call.data.replace("delete_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error("Invalid tracking number for delete", extra={'tracking_number': str(tracking_number)})
                return
            try:
                shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
                if shipment:
                    db.session.delete(shipment)
                    db.session.commit()
                    bot_logger.info(f"Deleted shipment {tracking_number}", extra={'tracking_number': tracking_number})
                    console.print(f"[info]Deleted shipment {tracking_number} by admin {call.from_user.id}[/info]")
                    try:
                        response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
                        if response.status_code != 204:
                            bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
                    except requests.RequestException as e:
                        bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
                    bot.answer_callback_query(call.id, f"Deleted {tracking_number}")
                    send_dynamic_menu(call.message.chat.id, call.message.message_id)
                else:
                    bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.")
                    bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            except SQLAlchemyError as e:
                db.session.rollback()
                bot_logger.error(f"Database error deleting shipment: {e}", extra={'tracking_number': tracking_number})
                console.print(Panel(f"[error]Database error deleting {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
                bot.answer_callback_query(call.id, f"Error deleting {tracking_number}: {e}")
        elif call.data == "batch_delete_menu":
            shipments = get_shipment_list()
            if shipments:
                markup = InlineKeyboardMarkup(row_width=1)
                for tn in shipments:
                    markup.add(InlineKeyboardButton(tn, callback_data=f"batch_select_{tn}"))
                markup.add(
                    InlineKeyboardButton("Confirm Delete", callback_data="batch_delete_confirm"),
                    InlineKeyboardButton("Back", callback_data="menu")
                )
                bot.edit_message_text("Select shipments to delete:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
                bot.set_chat_data(call.message.chat.id, 'batch_delete', [])
                bot_logger.debug("Batch delete menu sent", extra={'tracking_number': ''})
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug("No shipments for batch delete menu", extra={'tracking_number': ''})
        elif call.data.startswith("batch_select_"):
            tracking_number = sanitize_tracking_number(call.data.replace("batch_select_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error("Invalid tracking number for batch select", extra={'tracking_number': str(tracking_number)})
                return
            batch_list = bot.get_chat_data(call.message.chat.id, 'batch_delete', [])
            if tracking_number in batch_list:
                batch_list.remove(tracking_number)
                bot.answer_callback_query(call.id, f"Deselected {tracking_number}")
            else:
                batch_list.append(tracking_number)
                bot.answer_callback_query(call.id, f"Selected {tracking_number}")
            bot.set_chat_data(call.message.chat.id, 'batch_delete', batch_list)
            bot_logger.debug(f"Updated batch delete list: {batch_list}", extra={'tracking_number': tracking_number})
        elif call.data == "batch_delete_confirm":
            batch_list = bot.get_chat_data(call.message.chat.id, 'batch_delete', [])
            if not batch_list:
                bot.answer_callback_query(call.id, "No shipments selected.")
                bot_logger.debug("No shipments selected for batch delete", extra={'tracking_number': ''})
                return
            try:
                for tn in batch_list:
                    shipment = Shipment.query.filter_by(tracking_number=tn).first()
                    if shipment:
                        db.session.delete(shipment)
                        try:
                            response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tn}', timeout=5)
                            if response.status_code != 204:
                                bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tn})
                        except requests.RequestException as e:
                            bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tn})
                db.session.commit()
                bot.answer_callback_query(call.id, f"Deleted {len(batch_list)} shipments")
                bot_logger.info(f"Batch deleted {len(batch_list)} shipments: {batch_list}", extra={'tracking_number': ''})
                console.print(f"[info]Batch deleted {len(batch_list)} shipments by admin {call.from_user.id}[/info]")
                bot.set_chat_data(call.message.chat.id, 'batch_delete', [])
                send_dynamic_menu(call.message.chat.id, call.message.message_id)
            except SQLAlchemyError as e:
                db.session.rollback()
                bot_logger.error(f"Database error in batch delete: {e}", extra={'tracking_number': ''})
                console.print(Panel(f"[error]Database error in batch delete: {e}[/error]", title="Database Error", border_style="red"))
                bot.answer_callback_query(call.id, f"Error deleting shipments: {e}")
        elif call.data == "broadcast_menu":
            shipments = get_shipment_list()
            if shipments:
                markup = InlineKeyboardMarkup(row_width=1)
                for tn in shipments:
                    markup.add(InlineKeyboardButton(tn, callback_data=f"broadcast_{tn}"))
                markup.add(InlineKeyboardButton("Back", callback_data="menu"))
                bot.edit_message_text("Select shipment to broadcast:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
                bot_logger.debug("Broadcast menu sent", extra={'tracking_number': ''})
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug("No shipments for broadcast menu", extra={'tracking_number': ''})
        elif call.data.startswith("broadcast_"):
            tracking_number = sanitize_tracking_number(call.data.replace("broadcast_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error("Invalid tracking number for broadcast", extra={'tracking_number': str(tracking_number)})
                return
            try:
                response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
                if response.status_code == 204:
                    bot.answer_callback_query(call.id, f"Broadcast triggered for {tracking_number}")
                    bot_logger.info(f"Broadcast triggered for {tracking_number}", extra={'tracking_number': tracking_number})
                    console.print(f"[info]Broadcast triggered for {tracking_number} by admin {call.from_user.id}[/info]")
                else:
                    bot.answer_callback_query(call.id, f"Broadcast failed: {response.status_code}")
                    bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
            except requests.RequestException as e:
                bot.answer_callback_query(call.id, f"Broadcast error: {e}")
                bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
        elif call.data == "toggle_email_menu":
            shipments = get_shipment_list()
            if shipments:
                markup = InlineKeyboardMarkup(row_width=1)
                for tn in shipments:
                    markup.add(InlineKeyboardButton(tn, callback_data=f"toggle_email_{tn}"))
                markup.add(InlineKeyboardButton("Back", callback_data="menu"))
                bot.edit_message_text("Select shipment to toggle email notifications:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
                bot_logger.debug("Toggle email menu sent", extra={'tracking_number': ''})
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug("No shipments for toggle email menu", extra={'tracking_number': ''})
        elif call.data.startswith("toggle_email_"):
            tracking_number = sanitize_tracking_number(call.data.replace("toggle_email_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error("Invalid tracking number for toggle email", extra={'tracking_number': str(tracking_number)})
                return
            try:
                shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
                if shipment:
                    shipment.email_notifications = not shipment.email_notifications
                    db.session.commit()
                    status = "enabled" if shipment.email_notifications else "disabled"
                    bot.answer_callback_query(call.id, f"Email notifications {status} for {tracking_number}")
                    bot_logger.info(f"Toggled email notifications to {status}", extra={'tracking_number': tracking_number})
                    console.print(f"[info]Email notifications {status} for {tracking_number} by admin {call.from_user.id}[/info]")
                else:
                    bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.")
                    bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            except SQLAlchemyError as e:
                db.session.rollback()
                bot_logger.error(f"Database error toggling email: {e}", extra={'tracking_number': tracking_number})
                console.print(Panel(f"[error]Database error toggling email for {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
                bot.answer_callback_query(call.id, f"Error toggling email notifications: {e}")
        elif call.data == "settings":
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(
                InlineKeyboardButton("View Admins", callback_data="view_admins"),
                InlineKeyboardButton("Back", callback_data="menu")
            )
            bot.edit_message_text("Settings:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
            bot_logger.debug("Settings menu sent", extra={'tracking_number': ''})
        elif call.data == "view_admins":
            bot.answer_callback_query(call.id)
            bot.send_message(call.message.chat.id, f"Allowed Admins: {', '.join(map(str, ALLOWED_ADMINS)) or 'None'}")
            bot_logger.info("Sent admin list", extra={'tracking_number': ''})
        elif call.data == "list":
            shipments = get_shipment_list()
            if shipments:
                bot.answer_callback_query(call.id)
                bot.send_message(call.message.chat.id, f"Shipments:\n{', '.join(shipments)}")
                bot_logger.info(f"Sent shipment list: {len(shipments)} shipments", extra={'tracking_number': ''})
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug("No shipments for list", extra={'tracking_number': ''})
        elif call.data == "help":
            bot.answer_callback_query(call.id)
            help_text = (
                "/myid - Get your Telegram user ID\n"
                "/start or /menu - Open the admin menu\n"
                "/generate_id - Generate a unique tracking ID\n"
                "/add - Add a new shipment\n"
                "/update <tracking_number> <field=value> - Update a shipment\n"
                "/delete <tracking_number> - Delete a shipment\n"
                "Use the menu for interactive options."
            )
            bot.send_message(call.message.chat.id, help_text)
            bot_logger.info("Sent help text", extra={'tracking_number': ''})
        elif call.data == "menu":
            send_dynamic_menu(call.message.chat.id, call.message.message_id)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}")
        bot_logger.error(f"Callback query error: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Callback query error for {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(content_types=['text'], func=lambda message: message.reply_to_message and message.reply_to_message.text.startswith("Enter shipment details:"))
def handle_add_input(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning("Access denied for add input", extra={'tracking_number': ''})
        return
    try:
        parts = message.text.split(maxsplit=7)
        if len(parts) < 4:
            bot.reply_to(message, "Usage: [tracking_number] <status> \"<checkpoints>\" <delivery_location> <recipient_email> [origin_location] [webhook_url]")
            bot_logger.warning("Invalid add input format", extra={'tracking_number': ''})
            return
        tracking_number = sanitize_tracking_number(parts[0].strip()) if len(parts) > 4 else generate_unique_id()
        status = parts[1].strip() if len(parts) > 1 else 'Pending'
        checkpoints = parts[2].strip('"') if len(parts) > 2 else ''
        delivery_location = parts[3].strip() if len(parts) > 3 else 'Unknown'
        recipient_email = parts[4].strip() if len(parts) > 4 else ''
        origin_location = parts[5].strip() if len(parts) > 5 else None
        webhook_url = parts[6].strip() if len(parts) > 6 else None
        if status not in VALID_STATUSES:
            bot.reply_to(message, f"Invalid status. Must be one of: {', '.join(VALID_STATUSES)}")
            bot_logger.warning(f"Invalid status: {status}", extra={'tracking_number': tracking_number})
            return
        save_shipment(tracking_number, status, checkpoints, delivery_location, recipient_email, origin_location, webhook_url)
        bot.reply_to(message, f"Added {tracking_number}. Email to {recipient_email}. Webhook: {webhook_url or 'default'}.")
        bot_logger.info(f"Added shipment {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Added shipment {tracking_number} by admin {message.from_user.id}[/info]")
        send_dynamic_menu(message.chat.id)
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in add input: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Error in add input for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(content_types=['text'], func=lambda message: message.reply_to_message and message.reply_to_message.text.startswith("Enter updates for"))
def handle_update_input(message, tracking_number):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning("Access denied for update input", extra={'tracking_number': tracking_number})
        return
    try:
        tracking_number = sanitize_tracking_number(tracking_number)
        if not tracking_number:
            bot.reply_to(message, "Invalid tracking number.")
            bot_logger.error("Invalid tracking number", extra={'tracking_number': str(tracking_number)})
            return
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.reply_to(message, f"Tracking {tracking_number} not found.")
            bot_logger.warning(f"Tracking not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        updates = message.text.strip()
        update_dict = {k: v.strip('"') if v.startswith('"') and v.endswith('"') else v for k, v in (pair.split('=', 1) for pair in updates.split()) if k in ['status', 'checkpoints', 'delivery_location', 'recipient_email', 'origin_location', 'webhook_url', 'email_notifications']}
        new_status = update_dict.get('status', shipment.status)
        new_checkpoints = update_dict.get('checkpoints', shipment.checkpoints)
        new_location = update_dict.get('delivery_location', shipment.delivery_location)
        new_email = update_dict.get('recipient_email', shipment.recipient_email)
        new_origin = update_dict.get('origin_location', shipment.origin_location)
        new_webhook = update_dict.get('webhook_url', shipment.webhook_url)
        new_email_notifications = update_dict.get('email_notifications', str(shipment.email_notifications)).lower() in ('true', '1', 'yes')
        if new_status not in VALID_STATUSES:
            bot.reply_to(message, f"Invalid status. Must be one of: {', '.join(VALID_STATUSES)}")
            bot_logger.warning(f"Invalid status: {new_status}", extra={'tracking_number': tracking_number})
            return
        save_shipment(tracking_number, new_status, new_checkpoints, new_location, new_email, new_origin, new_webhook, new_email_notifications)
        bot.reply_to(message, f"Updated {tracking_number}. Email to {new_email}. Webhook: {new_webhook or 'default'}. Email Notifications: {'Enabled' if new_email_notifications else 'Disabled'}.")
        bot_logger.info(f"Updated shipment {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Updated shipment {tracking_number} by admin {message.from_user.id}[/info]")
        send_dynamic_menu(message.chat.id)
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in update input: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in update input for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['generate_id'])
def generate_id_command(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning("Access denied for /generate_id", extra={'tracking_number': ''})
        return
    new_id = generate_unique_id()
    bot.reply_to(message, f"Generated ID: {new_id}")
    bot_logger.info(f"Generated ID: {new_id}", extra={'tracking_number': new_id})

@bot.message_handler(commands=['add'])
def add_shipment(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning("Access denied for /add", extra={'tracking_number': ''})
        return
    msg = bot.reply_to(
        message,
        "Enter shipment details:\nFormat: [tracking_number] <status> \"<checkpoints>\" <delivery_location> <recipient_email> [origin_location] [webhook_url]",
        reply_markup=ForceReply(selective=True)
    )
    bot.register_next_step_handler(msg, handle_add_input)
    bot_logger.debug("Prompted to add shipment", extra={'tracking_number': ''})
    console.print(f"[info]Admin {message.from_user.id} prompted to add shipment[/info]")

@bot.message_handler(commands=['update'])
def update_shipment(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning("Access denied for /update", extra={'tracking_number': ''})
        return
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /update <tracking_number> <field=value> ...")
            bot_logger.warning("Invalid /update command format", extra={'tracking_number': ''})
            return
        tracking_number = sanitize_tracking_number(parts[1].split()[0].strip())
        if not tracking_number:
            bot.reply_to(message, "Invalid tracking number.")
            bot_logger.error("Invalid tracking number", extra={'tracking_number': str(tracking_number)})
            return
        msg = bot.reply_to(
            message,
            f"Enter updates for {tracking_number}:\nFormat: <field=value> ... (e.g., status=In_Transit delivery_location=\"New York, NY\")",
            reply_markup=ForceReply(selective=True)
        )
        bot.register_next_step_handler(msg, lambda m: handle_update_input(m, tracking_number))
        bot_logger.debug(f"Prompted to update {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Admin {message.from_user.id} prompted to update {tracking_number}[/info]")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in /update: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Error in /update for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['delete'])
def delete_shipment(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning("Access denied for /delete", extra={'tracking_number': ''})
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "Usage: /delete <tracking_number>")
            bot_logger.warning("Invalid /delete command format", extra={'tracking_number': ''})
            return
        tracking_number = sanitize_tracking_number(parts[1].strip())
        if not tracking_number:
            bot.reply_to(message, "Invalid tracking number.")
            bot_logger.error("Invalid tracking number", extra={'tracking_number': str(tracking_number)})
            return
        try:
            shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
            if shipment:
                db.session.delete(shipment)
                db.session.commit()
                bot_logger.info(f"Deleted shipment {tracking_number}", extra={'tracking_number': tracking_number})
                console.print(f"[info]Deleted shipment {tracking_number} by admin {message.from_user.id}[/info]")
                try:
                    response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
                    if response.status_code != 204:
                        bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
                except requests.RequestException as e:
                    bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
                bot.reply_to(message, f"Deleted {tracking_number}.")
                send_dynamic_menu(message.chat.id)
            else:
                bot.reply_to(message, f"Shipment {tracking_number} not found.")
                bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
        except SQLAlchemyError as e:
            db.session.rollback()
            bot_logger.error(f"Database error deleting {tracking_number}: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[error]Database error deleting {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
            bot.reply_to(message, f"Database error: {e}")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in /delete: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Error in /delete for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['list'])
def list_shipments(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning("Access denied for /list", extra={'tracking_number': ''})
        return
    shipments = get_shipment_list()
    if shipments:
        bot.reply_to(message, f"Shipments:\n{', '.join(shipments)}")
        bot_logger.info(f"Sent shipment list: {len(shipments)} shipments", extra={'tracking_number': ''})
    else:
        bot.reply_to(message, "No shipments.")
        bot_logger.debug("No shipments for /list", extra={'tracking_number': ''})

@bot.message_handler(commands=['help'])
def help_command(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning("Access denied for /help", extra={'tracking_number': ''})
        return
    help_text = (
        "/myid - Get your Telegram user ID\n"
        "/start or /menu - Open the admin menu\n"
        "/generate_id - Generate a unique tracking ID\n"
        "/add - Add a new shipment\n"
        "/update <tracking_number> <field=value> - Update a shipment\n"
        "/delete <tracking_number> - Delete a shipment\n"
        "/list - List all shipments\n"
        "Use the menu for interactive options."
    )
    bot.reply_to(message, help_text)
    bot_logger.info("Sent help text", extra={'tracking_number': ''})

def start_bot():
    console.print("[info]Starting Telegram bot polling[/info]")
    bot_logger.info("Starting Telegram bot polling", extra={'tracking_number': ''})
    try:
        bot.infinity_polling()
    except Exception as e:
        bot_logger.error(f"Bot polling error: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Bot polling error: {e}[/error]", title="Telegram Error", border_style="red"))
        time.sleep(5)
        start_bot()

# Initialize application
if __name__ == '__main__':
    init_db()
    eventlet.spawn(start_bot)
    socketio.run(app, host='0.0.0.0', port=5000, debug=os.getenv('FLASK_ENV') == 'development')
