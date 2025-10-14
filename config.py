import os

class Config(object):
    # Flask core
    SECRET_KEY = os.getenv('SECRET_KEY', 'fallback-secret-key')  # Required
    FLASK_ENV = os.getenv('FLASK_ENV', 'production')  # 'development' or 'production'

    # Database
    SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI', 'sqlite:////app/instance/app.db')  # Required
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Redis (optional)
    REDIS_URL = os.getenv('REDIS_URL', None)

    # SMTP/Email
    SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.example.com')
    SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
    SMTP_USER = os.getenv('SMTP_USER', '')  # Required
    SMTP_PASS = os.getenv('SMTP_PASS', '')  # Required
    SMTP_FROM = os.getenv('SMTP_FROM', 'noreply@example.com')

    # reCAPTCHA
    RECAPTCHA_SITE_KEY = os.getenv('RECAPTCHA_SITE_KEY', 'your-site-key')
    RECAPTCHA_SECRET_KEY = os.getenv('RECAPTCHA_SECRET_KEY', 'your-secret-key')
    RECAPTCHA_VERIFY_URL = ' to 'https://www.google.com/recaptcha/api/siteverify'

    # Telegram/Bot
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')  # Required

    # Tawk (chat widget)
    TAWK_PROPERTY_ID = os.getenv('TAWK_PROPERTY_ID', 'your-tawk-property-id')
    TAWK_WIDGET_ID = os.getenv('TAWK_WIDGET_ID', 'your-tawk-widget-id')

    # Simulation defaults
    GLOBAL_WEBHOOK_URL = os.getenv('GLOBAL_WEBHOOK_URL', None)
    STATUS_TRANSITIONS = {
        'Pending': {'next': ['In Transit'], 'delay': (60, 300), 'probabilities': [1.0], 'events': {}},
        'In Transit': {'next': ['Out for Delivery', 'Delayed'], 'delay': (300, 3600), 'probabilities': [0.8, 0.2], 'events': {'Delayed': 'Customs hold'}},
        'Out for Delivery': {'next': ['Delivered', 'Returned'], 'delay': (60, 600), 'probabilities': [0.9, 0.1], 'events': {}},
        'Delayed': {'next': ['In Transit'], 'delay': (600, 1800), 'probabilities': [1.0], 'events': {}},
        'Delivered': {'next': [], 'delay': (0, 0)},
        'Returned': {'next': [], 'delay': (0, 0)}
    }

    # Add any other custom keys as needed
