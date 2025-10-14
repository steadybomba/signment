from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit, disconnect
from flask_sqlalchemy import SQLAlchemy
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import eventlet
import json
from collections import defaultdict
import random
import time
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv
import redis
import logging
from logging.handlers import RotatingFileHandler
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

# Load environment variables
load_dotenv()

# Configure logging for Flask
flask_logger = logging.getLogger('flask_app')
flask_logger.setLevel(logging.INFO)
flask_handler = RotatingFileHandler('tracking_app.log', maxBytes=1000000, backupCount=5)
flask_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
flask_logger.addHandler(flask_handler)

# Configure logging for Telegram bot
bot_logger = logging.getLogger('telegram_bot')
bot_logger.setLevel(logging.INFO)
bot_handler = RotatingFileHandler('admin_bot.log', maxBytes=1000000, backupCount=5)
bot_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
bot_logger.addHandler(bot_handler)

# Validate environment variables
required_env_vars = [
    'SECRET_KEY', 'SQLALCHEMY_DATABASE_URI', 'SMTP_HOST', 'SMTP_USER',
    'SMTP_PASS', 'SMTP_FROM', 'RECAPTCHA_SITE_KEY', 'RECAPTCHA_SECRET_KEY',
    'TELEGRAM_BOT_TOKEN'
]
for var in required_env_vars:
    if not os.getenv(var):
        flask_logger.critical(f"Missing required environment variable: {var}")
        bot_logger.critical(f"Missing required environment variable: {var}")
        raise ValueError(f"Missing required environment variable: {var}")

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
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)

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
        return None
    sanitized = re.sub(r'[^a-zA-Z0-9\-]', '', tracking_number.strip())[:50]
    return sanitized if sanitized else None

def send_email_notification(tracking_number, status, checkpoints, delivery_location, recipient_email):
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment or not shipment.email_notifications:
            flask_logger.debug(f"Email notifications disabled for {tracking_number}")
            bot_logger.debug(f"Email notifications disabled for {tracking_number}")
            return
    except SQLAlchemyError as e:
        flask_logger.error(f"Database error checking email notifications for {tracking_number}: {e}")
        bot_logger.error(f"Database error checking email notifications for {tracking_number}: {e}")
        return

    if not recipient_email:
        flask_logger.warning(f"No recipient email for tracking {tracking_number}")
        bot_logger.warning(f"No recipient email for tracking {tracking_number}")
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
            flask_logger.info(f"Email sent to {recipient_email} for tracking {tracking_number}, status: {status}")
            bot_logger.info(f"Email sent to {recipient_email} for tracking {tracking_number}, status: {status}")
    except smtplib.SMTPException as e:
        flask_logger.error(f"Failed to send email to {recipient_email} for {tracking_number}: {e}")
        bot_logger.error(f"Failed to send email to {recipient_email} for {tracking_number}: {e}")

def init_db():
    try:
        db.create_all()
        flask_logger.info("Database initialized successfully")
        bot_logger.info("Database initialized successfully")
    except SQLAlchemyError as e:
        flask_logger.error(f"Database initialization failed: {e}")
        bot_logger.error(f"Database initialization failed: {e}")
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
            flask_logger.debug(f"reCAPTCHA verification successful: score={result.get('score')}")
            return True
        else:
            flask_logger.warning(f"reCAPTCHA verification failed: {result}")
            return False
    except requests.RequestException as e:
        flask_logger.error(f"reCAPTCHA verification error: {e}")
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
                    flask_logger.debug(f"Geocoded {location}: {coord}")
            except GeocoderTimedOut:
                flask_logger.warning(f"Geocoding timed out for {location}")
    return coords

def add_client(tracking_number, sid):
    try:
        redis_client.sadd(f"clients:{tracking_number}", sid)
        flask_logger.debug(f"Added client {sid} for tracking {tracking_number}")
    except redis.RedisError as e:
        flask_logger.error(f"Redis error adding client {sid}: {e}")

def remove_client(tracking_number, sid):
    try:
        redis_client.srem(f"clients:{tracking_number}", sid)
        flask_logger.debug(f"Removed client {sid} for tracking {tracking_number}")
    except redis.RedisError as e:
        flask_logger.error(f"Redis error removing client {sid}: {e}")

def get_clients(tracking_number):
    try:
        return redis_client.smembers(f"clients:{tracking_number}")
    except redis.RedisError as e:
        flask_logger.error(f"Redis error fetching clients for {tracking_number}: {e}")
        return set()

def simulate_tracking(tracking_number):
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.error(f"Invalid tracking number: {tracking_number}")
        return

    retries = 0
    max_retries = 5
    max_simulation_time = timedelta(days=30)  # Prevent infinite simulation
    start_time = datetime.now()

    while datetime.now() - start_time < max_simulation_time:
        try:
            shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
            if not shipment:
                flask_logger.warning(f"Shipment not found: {sanitized_tn}")
                break
            status = shipment.status
            checkpoints_str = shipment.checkpoints or ''
            checkpoints = checkpoints_str.split(';') if checkpoints_str else []
            delivery_location = shipment.delivery_location
            origin_location = shipment.origin_location or delivery_location
            webhook_url = shipment.webhook_url or GLOBAL_WEBHOOK_URL
            recipient_email = shipment.recipient_email

            if status in ['Delivered', 'Returned']:
                break

            current_time = datetime.now()
            template = ROUTE_TEMPLATES.get(delivery_location, ROUTE_TEMPLATES.get(origin_location, ROUTE_TEMPLATES['Lagos, NG']))
            transition = STATUS_TRANSITIONS.get(status, {'next': ['Delivered'], 'delay': (60, 300), 'probabilities': [1.0], 'events': {}})
            delay_range = transition['delay']
            next_states = transition['next']
            probabilities = transition.get('probabilities', [1.0 / len(next_states)] * len(next_states))
            events = transition.get('events', {})

            # Calculate delay based on route length
            route_length = len(template)
            delay_multiplier = 1 + (route_length / 10)  # Longer routes have longer delays
            delay = random.uniform(delay_range[0], delay_range[1]) * delay_multiplier

            # Add checkpoint if applicable
            if 'Out for Delivery' not in status and 'Delivered' not in status:
                next_index = min(len(checkpoints), len(template) - 1)
                next_checkpoint = f"{current_time.strftime('%Y-%m-%d %H:%M')} - {template[next_index]} - Processed"
                if next_checkpoint not in checkpoints:
                    checkpoints.append(next_checkpoint)

            # Transition to next status with probability
            new_status = random.choices(next_states, probabilities)[0]
            send_notification = new_status != status
            if new_status != status:
                status = new_status
                if status in events:
                    event_msg = random.choice(list(events)) if isinstance(events, set) else random.choice(events)
                    checkpoints.append(f"{current_time.strftime('%Y-%m-%d %H:%M')} - {delivery_location} - {event_msg}")
                if status == 'Delivered':
                    checkpoints.append(f"{current_time.strftime('%Y-%m-%d %H:%M')} - {delivery_location} - Delivered")
                if status == 'Returned':
                    checkpoints.append(f"{current_time.strftime('%Y-%m-%d %H:%M')} - {origin_location} - Returned")

            shipment.status = status
            shipment.checkpoints = ';'.join(checkpoints)
            shipment.last_updated = current_time
            db.session.commit()
            flask_logger.info(f"Updated shipment {sanitized_tn}: status={status}, checkpoints={checkpoints}")

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
                    flask_logger.warning(f"Webhook failed for {sanitized_tn}: {response.status_code} - {response.text}")
                else:
                    flask_logger.debug(f"Webhook sent for {sanitized_tn}: {payload}")
            except requests.RequestException as e:
                flask_logger.error(f"Webhook error for {sanitized_tn}: {e}")

            broadcast_update(sanitized_tn)

            flask_logger.debug(f"Sleeping for {delay} seconds for {sanitized_tn}")
            eventlet.sleep(delay)
            retries = 0  # Reset retries on success
        except SQLAlchemyError as e:
            flask_logger.error(f"Database error for {sanitized_tn}: {e}")
            retries += 1
            if retries >= max_retries:
                flask_logger.critical(f"Max retries exceeded for {sanitized_tn}. Stopping simulation.")
                break
            eventlet.sleep(2 ** retries)  # Exponential backoff

def broadcast_update(tracking_number):
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.error(f"Invalid tracking number for broadcast: {tracking_number}")
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
                flask_logger.debug(f"Broadcast update for {sanitized_tn} to {sid}")
        else:
            for sid in get_clients(sanitized_tn):
                socketio.emit('tracking_update', {
                    'tracking_number': sanitized_tn,
                    'found': False,
                    'error': 'Tracking number not found.'
                }, room=sid)
                flask_logger.warning(f"Tracking number not found for broadcast: {sanitized_tn}")
    except SQLAlchemyError as e:
        flask_logger.error(f"Database error during broadcast for {sanitized_tn}: {e}")

# Flask routes
@app.route('/')
@limiter.limit("100 per hour")
def index():
    return render_template('index.html', tawk_property_id=TAWK_PROPERTY_ID, tawk_widget_id=TAWK_WIDGET_ID, recaptcha_site_key=RECAPTCHA_SITE_KEY)

@app.route('/track', methods=['POST'])
@limiter.limit("50 per hour")
def track():
    recaptcha_response = request.form.get('g-recaptcha-response')
    if not recaptcha_response:
        flask_logger.warning("No reCAPTCHA response provided")
        return jsonify({'error': 'reCAPTCHA verification required'}), 400
    if not verify_recaptcha(recaptcha_response):
        flask_logger.warning("reCAPTCHA verification failed")
        return jsonify({'error': 'reCAPTCHA verification failed'}), 400

    tracking_number = request.form.get('tracking_number')
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number submitted: {tracking_number}")
        return jsonify({'error': 'Invalid tracking number'}), 400
    eventlet.spawn(simulate_tracking, sanitized_tn)
    flask_logger.info(f"Started tracking simulation for {sanitized_tn}")
    return render_template('tracking_result.html', tracking_number=sanitized_tn, tawk_property_id=TAWK_PROPERTY_ID, tawk_widget_id=TAWK_WIDGET_ID)

@app.route('/broadcast/<tracking_number>')
@limiter.limit("20 per hour")
def trigger_broadcast(tracking_number):
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number for broadcast: {tracking_number}")
        return jsonify({'error': 'Invalid tracking number'}), 400
    eventlet.spawn(broadcast_update, sanitized_tn)
    flask_logger.info(f"Triggered broadcast for {sanitized_tn}")
    return '', 204

@app.route('/health', methods=['GET'])
@limiter.limit("100 per hour")
def health_check():
    try:
        db.session.execute('SELECT 1')
        redis_client.ping()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
        flask_logger.debug("Health check passed")
        return jsonify({'status': 'healthy', 'database': 'ok', 'redis': 'ok', 'smtp': 'ok'}), 200
    except (SQLAlchemyError, redis.RedisError, smtplib.SMTPException) as e:
        flask_logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

# SocketIO handlers
@socketio.on('connect')
def handle_connect():
    flask_logger.debug(f"Client connected: {request.sid}")
    emit('status', {'message': 'Connected to tracking service'}, broadcast=False)

@socketio.on('request_tracking')
def handle_request_tracking(data):
    tracking_number = data.get('tracking_number')
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number in WebSocket request: {tracking_number}")
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
            flask_logger.info(f"Sent tracking update for {sanitized_tn} to {request.sid}")
        else:
            emit('tracking_update', {
                'tracking_number': sanitized_tn,
                'found': False,
                'error': 'Tracking number not found.'
            }, room=request.sid)
            flask_logger.warning(f"Tracking number not found: {sanitized_tn}")
            disconnect(request.sid)
    except SQLAlchemyError as e:
        flask_logger.error(f"Database error for {sanitized_tn}: {e}")
        emit('tracking_update', {'error': 'Database error'}, room=request.sid)
        disconnect(request.sid)

@socketio.on('disconnect')
def handle_disconnect():
    for key in redis_client.scan_iter("clients:*"):
        remove_client(key.replace("clients:", ""), request.sid)
    flask_logger.debug(f"Client disconnected: {request.sid}")

# Telegram bot functions
def is_admin(user_id):
    return user_id in ALLOWED_ADMINS

def generate_unique_id():
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    random_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"TRK{timestamp}{random_str}"

def get_shipment_list():
    try:
        shipments = Shipment.query.with_entities(Shipment.tracking_number).all()
        return [s.tracking_number for s in shipments]
    except SQLAlchemyError as e:
        bot_logger.error(f"Database error fetching shipment list: {e}")
        return []

def get_shipment_details(tracking_number):
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        return shipment.to_dict() if shipment else None
    except SQLAlchemyError as e:
        bot_logger.error(f"Database error fetching details for {tracking_number}: {e}")
        return None

def save_shipment(tracking_number, status, checkpoints, delivery_location, recipient_email='', origin_location=None, webhook_url=None, email_notifications=True):
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        bot_logger.error(f"Invalid tracking number: {tracking_number}")
        raise ValueError("Invalid tracking number")
    if status not in VALID_STATUSES:
        bot_logger.error(f"Invalid status for {sanitized_tn}: {status}")
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
        bot_logger.info(f"Saved shipment {sanitized_tn}: status={status}, delivery_location={delivery_location}")
        eventlet.spawn(send_email_notification, sanitized_tn, status, checkpoints, delivery_location, recipient_email)
        try:
            response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{sanitized_tn}', timeout=5)
            if response.status_code != 204:
                bot_logger.warning(f"Broadcast failed for {sanitized_tn}: {response.status_code}")
        except requests.RequestException as e:
            bot_logger.error(f"Broadcast error for {sanitized_tn}: {e}")
    except SQLAlchemyError as e:
        db.session.rollback()
        bot_logger.error(f"Database error saving shipment {sanitized_tn}: {e}")
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
        bot_logger.debug(f"Sent dynamic menu to chat {chat_id}")
    except telebot.apihelper.ApiTelegramException as e:
        bot_logger.error(f"Telegram API error sending menu to {chat_id}: {e}")

@bot.message_handler(commands=['myid'])
def get_my_id(message):
    bot.reply_to(message, f"Your Telegram user ID: {message.from_user.id}")
    bot_logger.info(f"User {message.from_user.id} requested their ID")

@bot.message_handler(commands=['start', 'menu'])
def send_menu(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for user {message.from_user.id}")
        return
    send_dynamic_menu(message.chat.id)
    bot_logger.info(f"Menu sent to admin {message.from_user.id}")

@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    bot.last_call = call
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        bot_logger.warning(f"Access denied for callback from user {call.from_user.id}")
        return

    try:
        if call.data == "generate_id":
            new_id = generate_unique_id()
            bot.answer_callback_query(call.id, f"Generated ID: {new_id}")
            bot_logger.info(f"Generated ID {new_id} for admin {call.from_user.id}")
        elif call.data == "add":
            msg = bot.send_message(
                call.message.chat.id,
                "Enter shipment details:\nFormat: [tracking_number] <status> \"<checkpoints>\" <delivery_location> <recipient_email> [origin_location] [webhook_url]",
                reply_markup=ForceReply(selective=True)
            )
            bot.register_next_step_handler(msg, handle_add_input)
            bot_logger.debug(f"Admin {call.from_user.id} prompted to add shipment")
        elif call.data == "view_menu":
            shipments = get_shipment_list()
            if shipments:
                markup = InlineKeyboardMarkup(row_width=1)
                for tn in shipments:
                    markup.add(InlineKeyboardButton(tn, callback_data=f"view_{tn}"))
                markup.add(InlineKeyboardButton("Back", callback_data="menu"))
                bot.edit_message_text("Select shipment to view:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
                bot_logger.debug(f"View menu sent to admin {call.from_user.id}")
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug(f"No shipments for view menu for admin {call.from_user.id}")
        elif call.data.startswith("view_"):
            tracking_number = sanitize_tracking_number(call.data.replace("view_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error(f"Invalid tracking number for view: {call.data}")
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
                bot_logger.info(f"Sent details for {tracking_number} to admin {call.from_user.id}")
            else:
                bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.")
                bot_logger.warning(f"Shipment {tracking_number} not found for admin {call.from_user.id}")
        elif call.data == "update_menu":
            shipments = get_shipment_list()
            if shipments:
                markup = InlineKeyboardMarkup(row_width=1)
                for tn in shipments:
                    markup.add(InlineKeyboardButton(tn, callback_data=f"update_{tn}"))
                markup.add(InlineKeyboardButton("Back", callback_data="menu"))
                bot.edit_message_text("Select shipment to update:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
                bot_logger.debug(f"Update menu sent to admin {call.from_user.id}")
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug(f"No shipments for update menu for admin {call.from_user.id}")
        elif call.data.startswith("update_"):
            tracking_number = sanitize_tracking_number(call.data.replace("update_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error(f"Invalid tracking number for update: {call.data}")
                return
            msg = bot.send_message(
                call.message.chat.id,
                f"Enter updates for {tracking_number}:\nFormat: <field=value> ... (e.g., status=In_Transit delivery_location=\"New York, NY\")",
                reply_markup=ForceReply(selective=True)
            )
            bot.register_next_step_handler(msg, lambda m: handle_update_input(m, tracking_number))
            bot_logger.debug(f"Admin {call.from_user.id} prompted to update {tracking_number}")
        elif call.data == "delete_menu":
            shipments = get_shipment_list()
            if shipments:
                markup = InlineKeyboardMarkup(row_width=1)
                for tn in shipments:
                    markup.add(InlineKeyboardButton(tn, callback_data=f"delete_{tn}"))
                markup.add(InlineKeyboardButton("Back", callback_data="menu"))
                bot.edit_message_text("Select shipment to delete:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
                bot_logger.debug(f"Delete menu sent to admin {call.from_user.id}")
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug(f"No shipments for delete menu for admin {call.from_user.id}")
        elif call.data.startswith("delete_"):
            tracking_number = sanitize_tracking_number(call.data.replace("delete_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error(f"Invalid tracking number for delete: {call.data}")
                return
            try:
                shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
                if shipment:
                    db.session.delete(shipment)
                    db.session.commit()
                    bot_logger.info(f"Deleted shipment {tracking_number} by admin {call.from_user.id}")
                    try:
                        response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
                        if response.status_code != 204:
                            bot_logger.warning(f"Broadcast failed for {tracking_number}: {response.status_code}")
                    except requests.RequestException as e:
                        bot_logger.error(f"Broadcast error for {tracking_number}: {e}")
                    bot.answer_callback_query(call.id, f"Deleted {tracking_number}")
                    send_dynamic_menu(call.message.chat.id, call.message.message_id)
                else:
                    bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.")
                    bot_logger.warning(f"Shipment {tracking_number} not found for admin {call.from_user.id}")
            except SQLAlchemyError as e:
                db.session.rollback()
                bot.answer_callback_query(call.id, f"Error deleting {tracking_number}: {e}")
                bot_logger.error(f"Database error deleting {tracking_number}: {e}")
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
                bot_logger.debug(f"Batch delete menu sent to admin {call.from_user.id}")
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug(f"No shipments for batch delete menu for admin {call.from_user.id}")
        elif call.data.startswith("batch_select_"):
            tracking_number = sanitize_tracking_number(call.data.replace("batch_select_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error(f"Invalid tracking number for batch select: {call.data}")
                return
            batch_list = bot.get_chat_data(call.message.chat.id, 'batch_delete', [])
            if tracking_number in batch_list:
                batch_list.remove(tracking_number)
                bot.answer_callback_query(call.id, f"Deselected {tracking_number}")
            else:
                batch_list.append(tracking_number)
                bot.answer_callback_query(call.id, f"Selected {tracking_number}")
            bot.set_chat_data(call.message.chat.id, 'batch_delete', batch_list)
            bot_logger.debug(f"Updated batch delete list for admin {call.from_user.id}: {batch_list}")
        elif call.data == "batch_delete_confirm":
            batch_list = bot.get_chat_data(call.message.chat.id, 'batch_delete', [])
            if not batch_list:
                bot.answer_callback_query(call.id, "No shipments selected.")
                bot_logger.debug(f"No shipments selected for batch delete by admin {call.from_user.id}")
                return
            try:
                for tn in batch_list:
                    shipment = Shipment.query.filter_by(tracking_number=tn).first()
                    if shipment:
                        db.session.delete(shipment)
                        try:
                            response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tn}', timeout=5)
                            if response.status_code != 204:
                                bot_logger.warning(f"Broadcast failed for {tn}: {response.status_code}")
                        except requests.RequestException as e:
                            bot_logger.error(f"Broadcast error for {tn}: {e}")
                db.session.commit()
                bot.answer_callback_query(call.id, f"Deleted {len(batch_list)} shipments")
                bot_logger.info(f"Batch deleted {len(batch_list)} shipments by admin {call.from_user.id}: {batch_list}")
                bot.set_chat_data(call.message.chat.id, 'batch_delete', [])
                send_dynamic_menu(call.message.chat.id, call.message.message_id)
            except SQLAlchemyError as e:
                db.session.rollback()
                bot.answer_callback_query(call.id, f"Error deleting shipments: {e}")
                bot_logger.error(f"Database error in batch delete: {e}")
        elif call.data == "broadcast_menu":
            shipments = get_shipment_list()
            if shipments:
                markup = InlineKeyboardMarkup(row_width=1)
                for tn in shipments:
                    markup.add(InlineKeyboardButton(tn, callback_data=f"broadcast_{tn}"))
                markup.add(InlineKeyboardButton("Back", callback_data="menu"))
                bot.edit_message_text("Select shipment to broadcast:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
                bot_logger.debug(f"Broadcast menu sent to admin {call.from_user.id}")
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug(f"No shipments for broadcast menu for admin {call.from_user.id}")
        elif call.data.startswith("broadcast_"):
            tracking_number = sanitize_tracking_number(call.data.replace("broadcast_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error(f"Invalid tracking number for broadcast: {call.data}")
                return
            try:
                response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
                if response.status_code == 204:
                    bot.answer_callback_query(call.id, f"Broadcast triggered for {tracking_number}")
                    bot_logger.info(f"Broadcast triggered for {tracking_number} by admin {call.from_user.id}")
                else:
                    bot.answer_callback_query(call.id, f"Broadcast failed: {response.status_code}")
                    bot_logger.warning(f"Broadcast failed for {tracking_number}: {response.status_code}")
            except requests.RequestException as e:
                bot.answer_callback_query(call.id, f"Broadcast error: {e}")
                bot_logger.error(f"Broadcast error for {tracking_number}: {e}")
        elif call.data == "toggle_email_menu":
            shipments = get_shipment_list()
            if shipments:
                markup = InlineKeyboardMarkup(row_width=1)
                for tn in shipments:
                    markup.add(InlineKeyboardButton(tn, callback_data=f"toggle_email_{tn}"))
                markup.add(InlineKeyboardButton("Back", callback_data="menu"))
                bot.edit_message_text("Select shipment to toggle email notifications:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
                bot_logger.debug(f"Toggle email menu sent to admin {call.from_user.id}")
            else:
                bot.answer_callback_query(call.id, "No shipments.")
                bot_logger.debug(f"No shipments for toggle email menu for admin {call.from_user.id}")
        elif call.data.startswith("toggle_email_"):
            tracking_number = sanitize_tracking_number(call.data.replace("toggle_email_", ""))
            if not tracking_number:
                bot.answer_callback_query(call.id, "Invalid tracking number.")
                bot_logger.error(f"Invalid tracking number for toggle email: {call.data}")
                return
            try:
                shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
                if shipment:
                    shipment.email_notifications = not shipment.email_notifications
                    db.session.commit()
                    status = "enabled" if shipment.email_notifications else "disabled"
                    bot.answer_callback_query(call.id, f"Email notifications {status} for {tracking_number}")
                    bot_logger.info(f"Toggled email notifications to {status} for {tracking_number} by admin {call.from_user.id}")
                else:
                    bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.")
                    bot_logger.warning(f"Shipment {tracking_number} not found for admin {call.from_user.id}")
            except SQLAlchemyError as e:
                db.session.rollback()
                bot.answer_callback_query(call.id, f"Error toggling email notifications: {e}")
                bot_logger.error(f"Database error toggling email for {tracking_number}: {e}")
        elif call.data == "settings":
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(
                InlineKeyboardButton("View Admins", callback_data="view_admins"),
                InlineKeyboardButton("Back", callback_data="menu")
            )
            bot.edit_message_text("Settings:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
            bot_logger.debug(f"Settings menu sent to admin {call.from_user.id}")
        elif call.data == "view_admins":
            bot.answer_callback_query(call.id)
            bot.send_message(call.message.chat.id, f"Allowed Admins: {', '.join(map(str, ALLOWED_ADMINS)) or 'None'}")
            bot_logger.info(f"Sent admin list to admin {call.from_user.id}")
        elif call.data == "help":
            show_help(call.message)
        elif call.data == "menu":
            send_dynamic_menu(call.message.chat.id, call.message.message_id)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}")
        bot_logger.error(f"Callback query error for {call.from_user.id}: {e}")

@bot.message_handler(commands=['generate_id'])
def generate_id_command(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /generate_id by user {message.from_user.id}")
        return
    new_id = generate_unique_id()
    bot.reply_to(message, f"Generated ID: {new_id}")
    bot_logger.info(f"Generated ID {new_id} for admin {message.from_user.id}")

@bot.message_handler(commands=['add'])
def add_shipment(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /add by user {message.from_user.id}")
        return
    msg = bot.reply_to(
        message,
        "Enter shipment details:\nFormat: [tracking_number] <status> \"<checkpoints>\" <delivery_location> <recipient_email> [origin_location] [webhook_url]",
        reply_markup=ForceReply(selective=True)
    )
    bot.register_next_step_handler(msg, handle_add_input)
    bot_logger.debug(f"Admin {message.from_user.id} prompted to add shipment")

@bot.message_handler(commands=['update'])
def update_shipment(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /update by user {message.from_user.id}")
        return
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /update <tracking_number> <field=value> ...")
            bot_logger.warning(f"Invalid /update command format by admin {message.from_user.id}")
            return
        tracking_number = sanitize_tracking_number(parts[1].split()[0].strip())
        if not tracking_number:
            bot.reply_to(message, "Invalid tracking number.")
            bot_logger.error(f"Invalid tracking number in /update by admin {message.from_user.id}")
            return
        msg = bot.reply_to(
            message,
            f"Enter updates for {tracking_number}:\nFormat: <field=value> ... (e.g., status=In_Transit delivery_location=\"New York, NY\")",
            reply_markup=ForceReply(selective=True)
        )
        bot.register_next_step_handler(msg, lambda m: handle_update_input(m, tracking_number))
        bot_logger.debug(f"Admin {message.from_user.id} prompted to update {tracking_number}")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in /update for admin {message.from_user.id}: {e}")

@bot.message_handler(commands=['delete'])
def delete_shipment(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /delete by user {message.from_user.id}")
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "Usage: /delete <tracking_number>")
            bot_logger.warning(f"Invalid /delete command format by admin {message.from_user.id}")
            return
        tracking_number = sanitize_tracking_number(parts[1].strip())
        if not tracking_number:
            bot.reply_to(message, "Invalid tracking number.")
            bot_logger.error(f"Invalid tracking number in /delete by admin {message.from_user.id}")
            return
        try:
            shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
            if shipment:
                db.session.delete(shipment)
                db.session.commit()
                bot_logger.info(f"Deleted shipment {tracking_number} by admin {message.from_user.id}")
                try:
                    response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
                    if response.status_code != 204:
                        bot_logger.warning(f"Broadcast failed for {tracking_number}: {response.status_code}")
                except requests.RequestException as e:
                    bot_logger.error(f"Broadcast error for {tracking_number}: {e}")
                bot.reply_to(message, f"Deleted {tracking_number}.")
                send_dynamic_menu(message.chat.id)
            else:
                bot.reply_to(message, f"Shipment {tracking_number} not found.")
                bot_logger.warning(f"Shipment {tracking_number} not found for admin {message.from_user.id}")
        except SQLAlchemyError as e:
            db.session.rollback()
            bot.reply_to(message, f"Database error: {e}")
            bot_logger.error(f"Database error deleting {tracking_number}: {e}")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in /delete for admin {message.from_user.id}: {e}")

@bot.message_handler(commands=['list'])
def list_shipments(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /list by user {message.from_user.id}")
        return
    try:
        shipments = Shipment.query.with_entities(Shipment.tracking_number, Shipment.status, Shipment.last_updated).all()
        response = "Shipments:\n" + "\n".join([f"{s.tracking_number}: {s.status} (Updated: {s.last_updated})" for s in shipments]) if shipments else "No shipments."
        bot.reply_to(message, response)
        bot_logger.info(f"Listed shipments for admin {message.from_user.id}")
    except SQLAlchemyError as e:
        bot.reply_to(message, f"Database error: {e}")
        bot_logger.error(f"Database error in /list for admin {message.from_user.id}: {e}")

@bot.message_handler(commands=['help'])
def show_help(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /help by user {message.from_user.id}")
        return
    help_text = f"""
    Commands:
    /myid - Show your Telegram user ID
    /generate_id - Generate a tracking ID
    /add [tracking_number] <status> "<checkpoints>" <delivery_location> <recipient_email> [origin_location] [webhook_url] - Add shipment
    /update <tracking_number> <field=value> ... - Update fields
    /delete <tracking_number> - Delete shipment
    /list - List shipments
    /help - Show this

    Inline Menu:
    - Generate ID: Create a new tracking ID
    - Add Shipment: Prompt to add a new shipment
    - View Shipment: View shipment details
    - Update Shipment: Update an existing shipment
    - Delete Shipment: Delete a single shipment
    - Batch Delete: Select multiple shipments to delete
    - Trigger Broadcast: Send WebSocket update for a shipment
    - Toggle Email: Enable/disable email notifications
    - Settings: View admin IDs
    - Help: Show this help

    Valid statuses: {', '.join(VALID_STATUSES)}
    Example: /add TRK202510130950ABCD Pending "" "New York, NY" user@example.com
    """
    bot.reply_to(message, help_text)
    bot_logger.info(f"Help text sent to admin {message.from_user.id}")

# Run Telegram bot in a separate thread
def run_bot():
    try:
        bot_logger.info("Starting Telegram bot polling")
        bot.polling()
    except Exception as e:
        bot_logger.critical(f"Telegram bot polling failed: {e}")
        raise

if __name__ == '__main__':
    try:
        with app.app_context():
            init_db()
        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()
        flask_logger.info("Starting Flask server")
        socketio.run(app, host='0.0.0.0', port=5000)
    except Exception as e:
        flask_logger.critical(f"Application startup failed: {e}")
        bot_logger.critical(f"Application startup failed: {e}")
        raise
