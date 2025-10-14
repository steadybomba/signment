import os
from dotenv import load_dotenv

load_dotenv()

# Flask core settings
SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'default-secret-key')
FLASK_ENV = os.getenv('FLASK_ENV', 'production')

# Flask-SQLAlchemy settings
SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL')
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Flask-SocketIO settings
SOCKETIO_MESSAGE_QUEUE = os.getenv('REDIS_URL', None)

# Flask-Limiter settings
RATELIMIT_STORAGE_URI = os.getenv('REDIS_URL', 'memory://')
RATELIMIT_DEFAULTS = ['200 per day', '50 per hour']

# SMTP settings
SMTP_HOST = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SMTP_USER = os.getenv('SMTP_USERNAME')
SMTP_PASS = os.getenv('SMTP_PASSWORD')
SMTP_FROM = os.getenv('SMTP_USERNAME', 'no-reply@signment.com')

# reCAPTCHA settings
RECAPTCHA_SECRET_KEY = os.getenv('RECAPTCHA_SECRET_KEY', 'your-recaptcha-secret-key')
RECAPTCHA_VERIFY_URL = 'https://www.google.com/recaptcha/api/siteverify'
RECAPTCHA_SITE_KEY = os.getenv('RECAPTCHA_SITE_KEY', 'your-recaptcha-site-key')

# Webhook and Telegram settings
WEBSOCKET_SERVER = os.getenv('WEBSOCKET_SERVER', 'https://signment.onrender.com')
GLOBAL_WEBHOOK_URL = os.getenv('GLOBAL_WEBHOOK_URL', '')
ALLOWED_ADMINS = os.getenv('ALLOWED_ADMINS', '').split(',')

# Geocoding settings
GEOCODING_API_KEY = os.getenv('GEOCODING_API_KEY', 'signment_app')

# Tawk.to settings
TAWK_PROPERTY_ID = os.getenv('TAWK_PROPERTY_ID', 'your-tawk-property-id')
TAWK_WIDGET_ID = os.getenv('TAWK_WIDGET_ID', 'your-tawk-widget-id')

# Status transitions for simulation
STATUS_TRANSITIONS = {
    'Pending': {
        'next': ['In_Transit'],
        'delay': (60, 300),
        'probabilities': [1.0],
        'events': {'Shipment created', 'Awaiting pickup'}
    },
    'In_Transit': {
        'next': ['Out_for_Delivery', 'Delayed'],
        'delay': (300, 1800),
        'probabilities': [0.8, 0.2],
        'events': {'In transit to hub', 'Arrived at sorting facility'}
    },
    'Out_for_Delivery': {
        'next': ['Delivered', 'Returned'],
        'delay': (300, 1800),
        'probabilities': [0.9, 0.1],
        'events': {'Out for delivery', 'Attempted delivery'}
    },
    'Delayed': {
        'next': ['Out_for_Delivery', 'Returned'],
        'delay': (600, 3600),
        'probabilities': [0.7, 0.3],
        'events': {'Delayed due to weather', 'Delayed at customs'}
    }
}

VALID_STATUSES = {'Pending', 'In_Transit', 'Out_for_Delivery', 'Delivered', 'Delayed', 'Returned'}
