import re
import os
import json
import random
import eventlet
import requests
import smtplib
import threading
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_socketio import SocketIO, emit, disconnect
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
import validators
import redis
import logging

app = Flask(__name__)
app.config.from_object('config.Config')
db = SQLAlchemy(app)
limiter = Limiter(app, key_func=get_remote_address)
socketio = SocketIO(app, cors_allowed_origins="*")
console = Console()

# Logging setup
flask_logger = logging.getLogger('flask_app')
sim_logger = logging.getLogger('simulator')
geocode_cache = {}
in_memory_clients = {}
paused_simulations = {}  # Dictionary to track paused simulations
sim_speed_multipliers = {}  # Dictionary to track simulation speed multipliers

# Configuration imports
from config import (
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM,
    RECAPTCHA_SECRET_KEY, RECAPTCHA_VERIFY_URL,
    WEBSOCKET_SERVER, GLOBAL_WEBHOOK_URL,
    STATUS_TRANSITIONS, VALID_STATUSES
)

# Redis setup
redis_client = redis.Redis.from_url(app.config.get('REDIS_URL'), decode_responses=True) if app.config.get('REDIS_URL') else None

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

# Shared utility functions
def sanitize_tracking_number(tracking_number):
    if not tracking_number or not isinstance(tracking_number, str):
        sim_logger.debug("Invalid tracking number provided", extra={'tracking_number': str(tracking_number)})
        console.print(f"[error]Invalid tracking number: {tracking_number}[/error]")
        return None
    sanitized = re.sub(r'[^a-zA-Z0-9\-]', '', tracking_number.strip())[:50]
    return sanitized if sanitized else None

def validate_email(email):
    try:
        return validators.email(email)
    except validators.ValidationFailure:
        return False

def validate_location(location):
    from telegram import get_cached_route_templates
    route_templates = get_cached_route_templates()
    return location in route_templates

def validate_webhook_url(url):
    if not url:
        return True
    try:
        return validators.url(url)
    except validators.ValidationFailure:
        return False

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

    if not recipient_email or not validate_email(recipient_email):
        flask_logger.warning(f"Invalid or no recipient email provided: {recipient_email}", extra={'tracking_number': tracking_number})
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
                    coord = {'lat': geo.latitude, 'lon': geo.longitude, 'desc': checkpoint}
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

def keep_alive():
    """Periodically ping the /health endpoint to maintain uptime."""
    while True:
        try:
            response = requests.get(f'http://localhost:5000/health', timeout=5)
            if response.status_code == 200:
                flask_logger.info("Keep-alive ping successful", extra={'tracking_number': ''})
                console.print(f"[info]Keep-alive ping successful: {response.json()['status']}[/info]")
            else:
                flask_logger.warning(f"Keep-alive ping failed: {response.status_code}", extra={'tracking_number': ''})
                console.print(Panel(f"[warning]Keep-alive ping failed: {response.status_code}[/warning]", title="Keep-Alive Warning", border_style="yellow"))
        except requests.RequestException as e:
            flask_logger.error(f"Keep-alive ping error: {e}", extra={'tracking_number': ''})
            console.print(Panel(f"[error]Keep-alive ping error: {e}[/error]", title="Keep-Alive Error", border_style="red"))
        time.sleep(300)  # Ping every 5 minutes

def simulate_tracking(tracking_number):
    from telegram import get_cached_route_templates
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        sim_logger.error("Invalid tracking number", extra={'tracking_number': str(tracking_number)})
        console.print(Panel(f"[error]Invalid tracking number: {tracking_number}[/error]", title="Simulation Error", border_style="red"))
        return

    retries = 0
    max_retries = 5
    max_simulation_time = timedelta(days=30)
    start_time = datetime.now()

    console.print(f"[info]Starting simulation for {sanitized_tn} with speed multiplier {sim_speed_multipliers.get(sanitized_tn, 1.0)}[/info]")
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console
    ) as progress:
        task = progress.add_task(f"Simulating {sanitized_tn}", total=100)
        
        while datetime.now() - start_time < max_simulation_time:
            if paused_simulations.get(sanitized_tn, False):
                sim_logger.debug(f"Simulation paused for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
                console.print(f"[info]Simulation paused for {sanitized_tn}[/info]")
                eventlet.sleep(5)  # Check pause state every 5 seconds
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
                webhook_url = shipment.webhook_url or GLOBAL_WEBHOOK_URL
                recipient_email = shipment.recipient_email

                if status in ['Delivered', 'Returned']:
                    progress.update(task, advance=100, description=f"Completed {sanitized_tn}: {status}")
                    sim_logger.info(f"Simulation completed: {status}", extra={'tracking_number': sanitized_tn})
                    console.print(f"[info]Simulation completed for {sanitized_tn}: {status}[/info]")
                    break

                current_time = datetime.now()
                template = get_cached_route_templates().get(delivery_location, get_cached_route_templates().get(origin_location, get_cached_route_templates()['Lagos, NG']))
                transition = STATUS_TRANSITIONS.get(status, {'next': ['Delivered'], 'delay': (60, 300), 'probabilities': [1.0], 'events': {}})
                delay_range = transition['delay']
                next_states = transition['next']
                probabilities = transition.get('probabilities', [1.0 / len(next_states)] * len(next_states))
                events = transition.get('events', {})

                route_length = len(template)
                delay_multiplier = 1 + (route_length / 10)
                speed_multiplier = sim_speed_multipliers.get(sanitized_tn, 1.0)  # Default speed is 1.0 (normal)
                adjusted_delay = random.uniform(delay_range[0], delay_range[1]) * delay_multiplier / speed_multiplier

                if 'Out_for_Delivery' not in status and 'Delivered' not in status:
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

                sim_logger.debug(f"Sleeping for {adjusted_delay:.2f} seconds with speed multiplier {speed_multiplier}", extra={'tracking_number': sanitized_tn})
                eventlet.sleep(adjusted_delay)
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
            update_data = {
                'tracking_number': sanitized_tn,
                'status': status,
                'checkpoints': checkpoints,
                'delivery_location': delivery_location,
                'coords': coords_list,
                'found': True,
                'paused': paused_simulations.get(sanitized_tn, False),
                'speed_multiplier': sim_speed_multipliers.get(sanitized_tn, 1.0)
            }
            for sid in get_clients(sanitized_tn):
                socketio.emit('tracking_update', update_data, room=sid)
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
    from forms import TrackForm
    form = TrackForm()
    flask_logger.info("Serving index page", extra={'tracking_number': ''})
    console.print("[info]Serving index page[/info]")
    return render_template('index.html', form=form, tawk_property_id=app.config['TAWK_PROPERTY_ID'], tawk_widget_id=app.config['TAWK_WIDGET_ID'], recaptcha_site_key=app.config['RECAPTCHA_SITE_KEY'])

@app.route('/track', methods=['POST'])
@limiter.limit("50 per hour")
def track():
    from forms import TrackForm
    form = TrackForm()
    if not form.validate_on_submit():
        flask_logger.warning("Form validation failed", extra={'tracking_number': ''})
        return jsonify({'error': 'Invalid form data'}), 400

    recaptcha_response = request.form.get('g-recaptcha-response')
    if not recaptcha_response:
        flask_logger.warning("No reCAPTCHA response provided", extra={'tracking_number': ''})
        return jsonify({'error': 'reCAPTCHA verification required'}), 400
    if not verify_recaptcha(recaptcha_response):
        flask_logger.warning("reCAPTCHA verification failed", extra={'tracking_number': ''})
        return jsonify({'error': 'reCAPTCHA verification failed'}), 400

    tracking_number = form.tracking_number.data
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        flask_logger.warning(f"Invalid tracking number submitted: {tracking_number}", extra={'tracking_number': str(tracking_number)})
        return render_template('tracking_result.html', error='Invalid tracking number', tawk_property_id=app.config['TAWK_PROPERTY_ID'], tawk_widget_id=app.config['TAWK_WIDGET_ID'])

    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        if not shipment:
            flask_logger.warning(f"Shipment not found: {sanitized_tn}", extra={'tracking_number': sanitized_tn})
            return render_template('tracking_result.html', error='Shipment not found', tawk_property_id=app.config['TAWK_PROPERTY_ID'], tawk_widget_id=app.config['TAWK_WIDGET_ID'])

        checkpoints_str = shipment.checkpoints or ''
        checkpoints = checkpoints_str.split(';') if checkpoints_str else []
        coords = geocode_locations(checkpoints)
        coords_list = [{'lat': lat, 'lon': lon, 'desc': desc} for lat, lon, desc in coords]
        
        sim_speed_multipliers[sanitized_tn] = sim_speed_multipliers.get(sanitized_tn, 1.0)  # Ensure default speed
        eventlet.spawn(simulate_tracking, sanitized_tn)
        flask_logger.info(f"Started tracking simulation", extra={'tracking_number': sanitized_tn})
        console.print(f"[info]Started tracking simulation for {sanitized_tn} with speed multiplier {sim_speed_multipliers[sanitized_tn]}[/info]")
        return render_template('tracking_result.html', 
                             shipment=shipment, 
                             checkpoints=checkpoints, 
                             coords=coords_list, 
                             tawk_property_id=app.config['TAWK_PROPERTY_ID'], 
                             tawk_widget_id=app.config['TAWK_WIDGET_ID'])
    except SQLAlchemyError as e:
        flask_logger.error(f"Database error: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error for {sanitized_tn}: {e}[/error]", title="Database Error", border_style="red"))
        return render_template('tracking_result.html', error='Database error occurred', tawk_property_id=app.config['TAWK_PROPERTY_ID'], tawk_widget_id=app.config['TAWK_WIDGET_ID'])

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
                'found': True,
                'paused': paused_simulations.get(sanitized_tn, False),
                'speed_multiplier': sim_speed_multipliers.get(sanitized_tn, 1.0)
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

if __name__ == '__main__':
    init_db()
    from telegram import cache_route_templates, start_bot
    cache_route_templates()
    # Start keep-alive thread
    keep_alive_thread = threading.Thread(target=keep_alive)
    keep_alive_thread.daemon = True
    keep_alive_thread.start()
    console.print("[info]Keep-alive thread started[/info]")
    bot_thread = threading.Thread(target=start_bot)
    bot_thread.daemon = True
    bot_thread.start()
    console.print("[info]Telegram bot started in background thread[/info]")
    socketio.run(app, host='0.0.0.0', port=5000, debug=os.getenv('FLASK_ENV') == 'development')
