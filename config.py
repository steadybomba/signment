import os
from dotenv import load_dotenv

load_dotenv()

class Config(object):
    # Flask core
    SECRET_KEY = os.getenv('SECRET_KEY', 'fallback-secret-key')  # Required
    FLASK_ENV = os.getenv('FLASK_ENV', 'production')  # 'development' or 'production'

    # Database
    SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI', 'sqlite:////app/instance/app.db')  # Required
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Redis (optional)
    REDIS_URL = os.getenv('REDIS_URL', None)
    SOCKETIO_MESSAGE_QUEUE = os.getenv('REDIS_URL', None)
    RATELIMIT_STORAGE_URI = os.getenv('REDIS_URL', 'memory://')
    RATELIMIT_DEFAULTS = ['200 per day', '50 per hour']

    # SMTP/Email
    SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.gmail.com')
    SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
    SMTP_USER = os.getenv('SMTP_USER', '')  # Required
    SMTP_PASS = os.getenv('SMTP_PASS', '')  # Required
    SMTP_FROM = os.getenv('SMTP_FROM', 'noreply@example.com')

    # reCAPTCHA
    RECAPTCHA_SITE_KEY = os.getenv('RECAPTCHA_SITE_KEY', 'your-site-key')
    RECAPTCHA_SECRET_KEY = os.getenv('RECAPTCHA_SECRET_KEY', 'your-secret-key')
    RECAPTCHA_VERIFY_URL = 'https://www.google.com/recaptcha/api/siteverify'

    # Tawk (chat widget)
    TAWK_PROPERTY_ID = os.getenv('TAWK_PROPERTY_ID', 'your-tawk-property-id')
    TAWK_WIDGET_ID = os.getenv('TAWK_WIDGET_ID', 'your-tawk-widget-id')

    # Telegram/Bot
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')  # Required
    ALLOWED_ADMINS = os.getenv('ALLOWED_ADMINS', '').split(',')

    # Simulation defaults
    WEBSOCKET_SERVER = os.getenv('WEBSOCKET_SERVER', 'https://signment.onrender.com')
    GLOBAL_WEBHOOK_URL = os.getenv('GLOBAL_WEBHOOK_URL', '')
    GEOCODING_API_KEY = os.getenv('GEOCODING_API_KEY', 'signment_app')
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
        },
        'Delivered': {
            'next': [],
            'delay': (0, 0),
            'events': {}
        },
        'Returned': {
            'next': [],
            'delay': (0, 0),
            'events': {}
        }
    }
    VALID_STATUSES = {'Pending', 'In_Transit', 'Out_for_Delivery', 'Delivered', 'Delayed', 'Returned'}

    # Validate required environment variables
    required_vars = ['SECRET_KEY', 'SQLALCHEMY_DATABASE_URI', 'SMTP_USER', 'SMTP_PASS', 'TELEGRAM_BOT_TOKEN']
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"Missing required environment variable: {var}")
