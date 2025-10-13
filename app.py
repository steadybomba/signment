# app.py - Flask app for tracking system

from flask import Flask, render_template, request, redirect, url_for, jsonify
from flask_socketio import SocketIO, emit, disconnect
import mysql.connector
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

# Load env vars
load_dotenv()

app = Flask(__name__, static_url_path='/static', static_folder='static')
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
socketio = SocketIO(app, async_mode='eventlet', ping_timeout=20, ping_interval=10)

# DB config from env
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}

# Webhook and Tawk.to from env
GLOBAL_WEBHOOK_URL = os.getenv('GLOBAL_WEBHOOK_URL')
TAWK_PROPERTY_ID = os.getenv('TAWK_PROPERTY_ID')
TAWK_WIDGET_ID = os.getenv('TAWK_WIDGET_ID')

# Init DB
def init_db():
    conn = mysql.connector.connect(**DB_CONFIG)
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
            except GeocoderTimedOut:
                pass
    return coords

# Track clients
connected_clients = defaultdict(set)

# Simulation constants
SIMULATION_DELAY = 300
STATUS_TRANSITIONS = {
    'Pending': ['In_Transit'],
    'In_Transit': ['Out_for_Delivery'],
    'Out_for_Delivery': ['Delivered']
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
    while True:
        conn = mysql.connector.connect(**DB_CONFIG, connect_timeout=5)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT status, checkpoints, delivery_location, origin_location, webhook_url FROM shipments WHERE tracking_number = %s", (tracking_number,))
        result = cursor.fetchone()
        conn.close()

        if result:
            status = result['status']
            checkpoints_str = result['checkpoints'] or ''
            checkpoints = checkpoints_str.split(';') if checkpoints_str else []
            delivery_location = result['delivery_location']
            origin_location = result['origin_location'] or delivery_location
            webhook_url = result['webhook_url'] or GLOBAL_WEBHOOK_URL

            if status != 'Delivered':
                current_time = datetime.now()
                template = ROUTE_TEMPLATES.get(delivery_location, ROUTE_TEMPLATES.get(origin_location, ROUTE_TEMPLATES['Lagos, NG']))
                if not checkpoints or checkpoints[-1].split(' - ')[1] != delivery_location:
                    next_index = min(len(checkpoints), len(template) - 1)
                    next_checkpoint = f"{current_time.strftime('%Y-%m-%d %H:%M')} - {template[next_index]} - Processed"
                    if next_checkpoint not in checkpoints:
                        checkpoints.append(next_checkpoint)
                        new_status = STATUS_TRANSITIONS.get(status, ['Delivered'])[0] if status in STATUS_TRANSITIONS else status
                        if new_status != status:
                            status = new_status
                            if status == 'Delivered':
                                checkpoints.append(f"{current_time.strftime('%Y-%m-%d %H:%M')} - {delivery_location} - Delivered")

                        conn = mysql.connector.connect(**DB_CONFIG, connect_timeout=5)
                        cursor = conn.cursor()
                        cursor.execute('UPDATE shipments SET status = %s, checkpoints = %s, last_updated = %s WHERE tracking_number = %s', (status, ';'.join(checkpoints), current_time, tracking_number))
                        conn.commit()
                        conn.close()

                        # Send webhook
                        payload = {'tracking_number': tracking_number, 'status': status, 'checkpoints': checkpoints, 'delivery_location': delivery_location, 'last_updated': current_time.isoformat(), 'event': 'status_update'}
                        try:
                            response = requests.post(webhook_url, json=payload, timeout=5)
                            if response.status_code != 200:
                                print(f"Webhook failed for {tracking_number}: {response.text}")
                        except requests.RequestException as e:
                            print(f"Webhook error for {tracking_number}: {e}")

                        broadcast_update(tracking_number)

            eventlet.sleep(random.uniform(60, SIMULATION_DELAY))
        else:
            break

@app.route('/')
def index():
    return render_template('index.html', tawk_property_id=TAWK_PROPERTY_ID, tawk_widget_id=TAWK_WIDGET_ID)

@app.route('/track', methods=['POST'])
def track():
    tracking_number = request.form.get('tracking_number')
    if not tracking_number:
        return redirect(url_for('index'))
    eventlet.spawn(simulate_tracking, tracking_number)
    return render_template('tracking_result.html', tracking_number=tracking_number, tawk_property_id=TAWK_PROPERTY_ID, tawk_widget_id=TAWK_WIDGET_ID)

@socketio.on('connect')
def handle_connect():
    emit('status', {'message': 'Connected to tracking service'}, broadcast=False)

@socketio.on('request_tracking')
def handle_request_tracking(data):
    tracking_number = data.get('tracking_number')
    if not tracking_number:
        emit('tracking_update', {'error': 'No tracking number provided'}, room=request.sid)
        disconnect(request.sid)
        return

    conn = mysql.connector.connect(**DB_CONFIG, connect_timeout=5)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT status, checkpoints, delivery_location FROM shipments WHERE tracking_number = %s", (tracking_number,))
    result = cursor.fetchone()
    conn.close()

    if result:
        status, checkpoints_str, delivery_location = result['status'], result['checkpoints'], result['delivery_location']
        checkpoints = checkpoints_str.split(';') if checkpoints_str else []
        coords = geocode_locations(checkpoints)
        coords_list = [{'lat': lat, 'lon': lon, 'desc': desc} for lat, lon, desc in coords]
        connected_clients[tracking_number].add(request.sid)
        emit('tracking_update', {'tracking_number': tracking_number, 'status': status, 'checkpoints': checkpoints, 'delivery_location': delivery_location, 'coords': coords_list, 'found': True}, room=request.sid)
    else:
        emit('tracking_update', {'tracking_number': tracking_number, 'found': False, 'error': 'Tracking number not found.'}, room=request.sid)
        disconnect(request.sid)

@socketio.on('disconnect')
def handle_disconnect():
    for tn, sids in connected_clients.items():
        if request.sid in sids:
            sids.remove(request.sid)
            if not sids:
                del connected_clients[tn]
            break

# Broadcast updates
def broadcast_update(tracking_number):
    conn = mysql.connector.connect(**DB_CONFIG, connect_timeout=5)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT status, checkpoints, delivery_location FROM shipments WHERE tracking_number = %s", (tracking_number,))
    result = cursor.fetchone()
    conn.close()

    if result:
        status, checkpoints_str, delivery_location = result['status'], result['checkpoints'], result['delivery_location']
        checkpoints = checkpoints_str.split(';') if checkpoints_str else []
        coords = geocode_locations(checkpoints)
        coords_list = [{'lat': lat, 'lon': lon, 'desc': desc} for lat, lon, desc in coords]
        update_data = json.dumps({'tracking_number': tracking_number, 'status': status, 'checkpoints': checkpoints, 'delivery_location': delivery_location, 'coords': coords_list, 'found': True}).encode('utf-8')
        for sid in connected_clients.get(tracking_number, set()):
            socketio.emit('tracking_update', update_data, room=sid, namespace='/', binary=True)
    else:
        for sid in connected_clients.get(tracking_number, set()):
            socketio.emit('tracking_update', {'tracking_number': tracking_number, 'found': False, 'error': 'Tracking number not found.'}, room=sid)

@app.route('/broadcast/<tracking_number>')
def trigger_broadcast(tracking_number):
    eventlet.spawn(broadcast_update, tracking_number)
    return '', 204

# Health check
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    init_db()
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
