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
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash, Response
from flask_sqlalchemy import SQLAlchemy
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_socketio import SocketIO, emit
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
import validators
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy import inspect, text, func
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
        ADMIN_PASSWORD=os.getenv("ADMIN_PASSWORD", "admin123"),
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
        data = json.loads(notif)
        tn = data["tracking_number"]
        typ = data["type"]
        d = data["data"]
        if typ == "email" and d.get("recipient_email"):
            send_email_notification(d["recipient_email"], f"Update: {tn}", f"Status: {d['status']}")
        elif typ == "webhook" and d.get("webhook_url"):
            requests.post(d["webhook_url"], json={**d, "tracking_number": tn}, timeout=10)

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

# Simulation
def simulate_tracking(tn):
    shipment = Shipment.query.filter_by(tracking_number=tn).first()
    if not shipment:
        return
    start = datetime.now()
    while datetime.now() - start < timedelta(days=30):
        if redis_client and redis_client.hget("paused_simulations", tn) == "true":
            time.sleep(5)
            continue
        try:
            status = shipment.status
            checkpoints = (shipment.checkpoints or "").split(";")
            delivery_loc = shipment.delivery_location
            origin = shipment.origin_location or delivery_loc
            webhook = shipment.webhook_url or app.config['GLOBAL_WEBHOOK_URL']
            email = shipment.recipient_email

            if status in ['Delivered', 'Returned']:
                break

            transition = app.config['STATUS_TRANSITIONS'].get(status, {})
            next_states = transition.get('next', ['Delivered'])
            delay = random.uniform(*transition.get('delay', (60, 300)))
            speed = float(redis_client.hget("sim_speed_multipliers", tn) or 1.0) if redis_client else 1.0
            speed = max(0.1, min(10.0, speed))
            delay /= speed

            if status != 'Out_for_Delivery':
                template = get_cached_route_templates().get(delivery_loc, ['Lagos, NG'])
                idx = min(len(checkpoints), len(template) - 1)
                cp = f"{datetime.now():%Y-%m-%d %H:%M} - {template[idx]} - Processed"
                if cp not in checkpoints:
                    checkpoints.append(cp)

            new_status = random.choices(next_states, transition.get('probabilities', [1.0]))[0] if next_states else status
            if new_status != status:
                if transition.get('events'):
                    event = random.choice(list(transition['events']))
                    checkpoints.append(f"{datetime.now():%Y-%m-%d %H:%M} - {delivery_loc} - {event}")
                if new_status == 'Delivered':
                    checkpoints.append(f"{datetime.now():%Y-%m-%d %H:%M} - {delivery_loc} - Delivered")
                if new_status == 'Returned':
                    checkpoints.append(f"{datetime.now():%Y-%m-%d %H:%M} - {origin} - Returned")

            shipment.status = new_status
            shipment.checkpoints = ";".join(checkpoints)
            shipment.last_updated = datetime.now()
            db.session.commit()
            invalidate_cache(tn)

            if new_status != status and email and shipment.email_notifications:
                enqueue_notification({
                    "tracking_number": tn,
                    "type": "email",
                    "data": {"status": new_status, "checkpoints": ";".join(checkpoints), "delivery_location": delivery_loc, "recipient_email": email}
                })
            if webhook:
                enqueue_notification({
                    "tracking_number": tn,
                    "type": "webhook",
                    "data": {"status": new_status, "checkpoints": checkpoints, "delivery_location": delivery_loc, "webhook_url": webhook}
                })

            broadcast_update(tn)
            eventlet.sleep(delay)
        except Exception as e:
            sim_logger.error(f"Sim error {tn}: {e}")
            break

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
