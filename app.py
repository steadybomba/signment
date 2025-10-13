from flask import Flask, render_template, request, redirect, url_for, jsonify
from flask_socketio import SocketIO, emit, disconnect
import mysql.connector
from mysql.connector import pooling
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

# Load env vars
load_dotenv()

# Configure structured logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = RotatingFileHandler('tracking_app.log', maxBytes=1000000, backupCount=5)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

app = Flask(__name__, static_url_path='/static', static_folder='static')
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
socketio = SocketIO(app, async_mode='eventlet', ping_timeout=20, ping_interval=10)

# Rate limiting
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)

# DB config from env
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}

# MySQL connection pool
try:
    db_pool = pooling.MySQLConnectionPool(
        pool_name="tracking_pool",
        pool_size=10,
        pool_reset_session=True,
        **DB_CONFIG
    )
    logger.info("MySQL connection pool initialized successfully")
except mysql.connector.Error as e:
    logger.error(f"Failed to initialize MySQL connection pool: {e}")
    raise

# Redis config
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True
)

# Webhook and Tawk.to from env
GLOBAL_WEBHOOK_URL = os.getenv('GLOBAL_WEBHOOK_URL')
TAWK_PROPERTY_ID = os.getenv('TAWK_PROPERTY_ID')
TAWK_WIDGET_ID = os.getenv('TAWK_WIDGET_ID')

# Email config from env
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SMTP_USERNAME = os.getenv('SMTP_USERNAME')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')
SMTP_FROM = os.getenv('SMTP_FROM')

# reCAPTCHA config from env
RECAPTCHA_SITE_KEY = os.getenv('RECAPTCHA_SITE_KEY')
RECAPTCHA_SECRET_KEY = os.getenv('RECAPTCHA_SECRET_KEY')
RECAPTCHA_VERIFY_URL = "https://www.google.com/recaptcha/api/siteverify"

# Input sanitization
def sanitize_tracking_number(tracking_number):
    if not tracking_number or not isinstance(tracking_number, str):
        return None
    # Allow alphanumeric and dashes, max 50 chars
    sanitized = re.sub(r'[^a-zA-Z0-9\-]', '', tracking_number.strip())[:50]
    return sanitized if sanitized else None

# Verify reCAPTCHA
def verify_recaptcha(response_token):
    try:
        payload = {
            'secret': RECAPTCHA_SECRET_KEY,
            'response': response_token
        }
        response = requests.post(RECAPTCHA_VERIFY_URL, data=payload, timeout=5)
        result = response.json()
        if result.get('success') and result.get('score', 1.0) >= 0.5:
            logger.debug(f"reCAPTCHA verification successful: score={result.get('score')}")
            return True
        else:
            logger.warning(f"reCAPTCHA verification failed: {result}")
            return False
    except requests.RequestException as e:
        logger.error(f"reCAPTCHA verification error: {e}")
        return False

# Send email notification
def send_email(recipient, tracking_number, status, checkpoints, delivery_location):
    if not recipient:
        logger.warning(f"No recipient email for tracking {tracking_number}")
        return

    subject = f"Shipment Update: Tracking {tracking_number}"
    body = f"""
    Dear Customer,

    Your shipment with tracking number {tracking_number} has been updated.

    Status: {status}
    Delivery Location: {delivery_location}
    Latest Checkpoint: {checkpoints[-1] if checkpoints else 'No checkpoints available'}

    For more details, visit our tracking portal.

    Best regards,
    Tracking Service Team
    """

    msg = MIMEMultipart()
    msg['From'] = SMTP_FROM
    msg['To'] = recipient
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
            logger.info(f"Email sent to {recipient} for tracking {tracking_number}, status: {status}")
    except smtplib.SMTPException as e:
        logger.error(f"Failed to send email to {recipient} for {tracking_number}: {e}")

# Init DB
def init_db():
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS shipments (
                tracking_number VARCHAR(50) PRIMARY KEY,
                status VARCHAR(50),
                checkpoints TEXT,
                delivery_location TEXT,
                last_updated DATETIME,
                recipient_email VARCHAR(255),
                created_at DATETIME,
                origin_location TEXT,
                webhook_url TEXT
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tracking_number ON shipments (tracking_number)')
        conn.commit()
        logger.info("Database initialized successfully")
    except mysql.connector.Error as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

init_db()

# Cache geocoded locations
geocode_cache = {}

# Geocode checkpoints
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
                    logger.debug(f"Geocoded {location}: {coord}")
            except GeocoderTimedOut:
                logger.warning(f"Geocoding timed out for {location}")
    return coords

# Track clients in Redis
def add_client(tracking_number, sid):
    try:
        redis_client.sadd(f"clients:{tracking_number}", sid)
        logger.debug(f"Added client {sid} for tracking {tracking_number}")
    except redis.RedisError as e:
        logger.error(f"Redis error adding client {sid}: {e}")

def remove_client(tracking_number, sid):
    try:
        redis_client.srem(f"clients:{tracking_number}", sid)
        logger.debug(f"Removed client {sid} for tracking {tracking_number}")
    except redis.RedisError as e:
        logger.error(f"Redis error removing client {sid}: {e}")

def get_clients(tracking_number):
    try:
        return redis_client.smembers(f"clients:{tracking_number}")
    except redis.RedisError as e:
        logger.error(f"Redis error fetching clients for {tracking_number}: {e}")
        return set()

# Simulation constants
STATUS_TRANSITIONS = {
    'Pending': {'next': ['In_Transit'], 'delay': (30, 120)},  # Shorter for initial processing
    'In_Transit': {'next': ['Out_for_Delivery'], 'delay': (120, 600)},  # Longer for transit
    'Out_for_Delivery': {'next': ['Delivered'], 'delay': (60, 180)}  # Medium for final delivery
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

# Simulate tracking
def simulate_tracking(tracking_number):
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        logger.error(f"Invalid tracking number: {tracking_number}")
        return

    while True:
        try:
            conn = db_pool.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT status, checkpoints, delivery_location, origin_location, webhook_url, recipient_email FROM shipments WHERE tracking_number = %s", (sanitized_tn,))
            result = cursor.fetchone()
        except mysql.connector.Error as e:
            logger.error(f"Database error for {sanitized_tn}: {e}")
            eventlet.sleep(60)
            continue
        finally:
            cursor.close()
            conn.close()

        if result:
            status = result['status']
            checkpoints_str = result['checkpoints'] or ''
            checkpoints = checkpoints_str.split(';') if checkpoints_str else []
            delivery_location = result['delivery_location']
            origin_location = result['origin_location'] or delivery_location
            webhook_url = result['webhook_url'] or GLOBAL_WEBHOOK_URL
            recipient_email = result['recipient_email']

            if status != 'Delivered':
                current_time = datetime.now()
                template = ROUTE_TEMPLATES.get(delivery_location, ROUTE_TEMPLATES.get(origin_location, ROUTE_TEMPLATES['Lagos, NG']))
                if not checkpoints or checkpoints[-1].split(' - ')[1] != delivery_location:
                    next_index = min(len(checkpoints), len(template) - 1)
                    next_checkpoint = f"{current_time.strftime('%Y-%m-%d %H:%M')} - {template[next_index]} - Processed"
                    if next_checkpoint not in checkpoints:
                        checkpoints.append(next_checkpoint)
                        transition = STATUS_TRANSITIONS.get(status, {'next': ['Delivered'], 'delay': (60, 300)})
                        new_status = random.choice(transition['next']) if transition['next'] else status
                        send_notification = new_status != status
                        if new_status != status:
                            status = new_status
                            if status == 'Delivered':
                                checkpoints.append(f"{current_time.strftime('%Y-%m-%d %H:%M')} - {delivery_location} - Delivered")

                        try:
                            conn = db_pool.get_connection()
                            cursor = conn.cursor()
                            cursor.execute('UPDATE shipments SET status = %s, checkpoints = %s, last_updated = %s WHERE tracking_number = %s', (status, ';'.join(checkpoints), current_time, sanitized_tn))
                            conn.commit()
                            logger.info(f"Updated shipment {sanitized_tn}: status={status}, checkpoints={checkpoints}")
                        except mysql.connector.Error as e:
                            logger.error(f"Database update error for {sanitized_tn}: {e}")
                        finally:
                            cursor.close()
                            conn.close()

                        # Send email notification on status change
                        if send_notification and recipient_email:
                            eventlet.spawn(send_email, recipient_email, sanitized_tn, status, checkpoints, delivery_location)

                        # Send webhook
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
                                logger.warning(f"Webhook failed for {sanitized_tn}: {response.status_code} - {response.text}")
                            else:
                                logger.debug(f"Webhook sent for {sanitized_tn}: {payload}")
                        except requests.RequestException as e:
                            logger.error(f"Webhook error for {sanitized_tn}: {e}")

                        broadcast_update(sanitized_tn)

                # Variable delay based on status
                delay = random.uniform(*transition['delay'])
                logger.debug(f"Sleeping for {delay} seconds for {sanitized_tn}")
                eventlet.sleep(delay)
            else:
                break
        else:
            logger.warning(f"Shipment not found: {sanitized_tn}")
            break

@app.route('/')
@limiter.limit("100 per hour")
def index():
    return render_template('index.html', tawk_property_id=TAWK_PROPERTY_ID, tawk_widget_id=TAWK_WIDGET_ID, recaptcha_site_key=RECAPTCHA_SITE_KEY)

@app.route('/track', methods=['POST'])
@limiter.limit("50 per hour")
def track():
    # Verify reCAPTCHA
    recaptcha_response = request.form.get('g-recaptcha-response')
    if not recaptcha_response:
        logger.warning("No reCAPTCHA response provided")
        return jsonify({'error': 'reCAPTCHA verification required'}), 400
    if not verify_recaptcha(recaptcha_response):
        logger.warning("reCAPTCHA verification failed")
        return jsonify({'error': 'reCAPTCHA verification failed'}), 400

    tracking_number = request.form.get('tracking_number')
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        logger.warning(f"Invalid tracking number submitted: {tracking_number}")
        return jsonify({'error': 'Invalid tracking number'}), 400
    eventlet.spawn(simulate_tracking, sanitized_tn)
    logger.info(f"Started tracking simulation for {sanitized_tn}")
    return render_template('tracking_result.html', tracking_number=sanitized_tn, tawk_property_id=TAWK_PROPERTY_ID, tawk_widget_id=TAWK_WIDGET_ID)

@socketio.on('connect')
def handle_connect():
    logger.debug(f"Client connected: {request.sid}")
    emit('status', {'message': 'Connected to tracking service'}, broadcast=False)

@socketio.on('request_tracking')
def handle_request_tracking(data):
    tracking_number = data.get('tracking_number')
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        logger.warning(f"Invalid tracking number in WebSocket request: {tracking_number}")
        emit('tracking_update', {'error': 'Invalid tracking number'}, room=request.sid)
        disconnect(request.sid)
        return

    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT status, checkpoints, delivery_location FROM shipments WHERE tracking_number = %s", (sanitized_tn,))
        result = cursor.fetchone()
    except mysql.connector.Error as e:
        logger.error(f"Database error for {sanitized_tn}: {e}")
        emit('tracking_update', {'error': 'Database error'}, room=request.sid)
        disconnect(request.sid)
        return
    finally:
        cursor.close()
        conn.close()

    if result:
        status, checkpoints_str, delivery_location = result['status'], result['checkpoints'], result['delivery_location']
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
        logger.info(f"Sent tracking update for {sanitized_tn} to {request.sid}")
    else:
        emit('tracking_update', {
            'tracking_number': sanitized_tn,
            'found': False,
            'error': 'Tracking number not found.'
        }, room=request.sid)
        logger.warning(f"Tracking number not found: {sanitized_tn}")
        disconnect(request.sid)

@socketio.on('disconnect')
def handle_disconnect():
    for key in redis_client.scan_iter("clients:*"):
        remove_client(key.replace("clients:", ""), request.sid)
    logger.debug(f"Client disconnected: {request.sid}")

# Broadcast updates
def broadcast_update(tracking_number):
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        logger.error(f"Invalid tracking number for broadcast: {tracking_number}")
        return

    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT status, checkpoints, delivery_location FROM shipments WHERE tracking_number = %s", (sanitized_tn,))
        result = cursor.fetchone()
    except mysql.connector.Error as e:
        logger.error(f"Database error during broadcast for {sanitized_tn}: {e}")
        return
    finally:
        cursor.close()
        conn.close()

    if result:
        status, checkpoints_str, delivery_location = result['status'], result['checkpoints'], result['delivery_location']
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
            logger.debug(f"Broadcast update for {sanitized_tn} to {sid}")
    else:
        for sid in get_clients(sanitized_tn):
            socketio.emit('tracking_update', {
                'tracking_number': sanitized_tn,
                'found': False,
                'error': 'Tracking number not found.'
            }, room=sid)
            logger.warning(f"Tracking number not found for broadcast: {sanitized_tn}")

@app.route('/broadcast/<tracking_number>')
@limiter.limit("20 per hour")
def trigger_broadcast(tracking_number):
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        logger.warning(f"Invalid tracking number for broadcast: {tracking_number}")
        return jsonify({'error': 'Invalid tracking number'}), 400
    eventlet.spawn(broadcast_update, sanitized_tn)
    logger.info(f"Triggered broadcast for {sanitized_tn}")
    return '', 204

# Health check
@app.route('/health', methods=['GET'])
@limiter.limit("100 per hour")
def health_check():
    try:
        conn = db_pool.get_connection()
        conn.ping(reconnect=True)
        conn.close()
        redis_client.ping()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
        logger.debug("Health check passed")
        return jsonify({'status': 'healthy', 'database': 'ok', 'redis': 'ok', 'smtp': 'ok'}), 200
    except (mysql.connector.Error, redis.RedisError, smtplib.SMTPException) as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

if __name__ == '__main__':
    try:
        init_db()
        socketio.run(app, host='0.0.0.0', port=5000, debug=True)
    except Exception as e:
        logger.critical(f"Application startup failed: {e}")
        raise
