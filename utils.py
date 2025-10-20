import os
import json
import re
import logging
from upstash_redis import Redis
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional, Tuple
from rich.console import Console
from telebot import TeleBot
from threading import Lock
import eventlet
from time import time, sleep

# Logging setup
logger = logging.getLogger('app')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
console = Console()

# Configuration dataclass
@dataclass
class BotConfig:
    telegram_bot_token: str
    redis_url: str
    redis_token: str
    webhook_url: str
    websocket_server: str
    allowed_admins: List[int]
    valid_statuses: List[str]
    route_templates: dict
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_pass: str
    smtp_from: str

    def __post_init__(self):
        if not self.telegram_bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required")
        if not self.redis_url:
            raise ValueError("REDIS_URL is required")
        if not self.webhook_url:
            raise ValueError("WEBHOOK_URL is required")
        if not self.allowed_admins:
            raise ValueError("ALLOWED_ADMINS is required")
        if not self.valid_statuses:
            raise ValueError("VALID_STATUSES is required")

# Global configuration cache
_config = None
def get_config() -> BotConfig:
    """Retrieve or initialize the global BotConfig."""
    global _config
    if _config is None:
        _config = BotConfig(
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            redis_url=os.getenv("REDIS_URL", "https://equal-sparrow-8815.upstash.io"),
            redis_token=os.getenv("REDIS_TOKEN", "ASJvAAImcDIzMjI1Mjg2YjRkYzA0MGVjYjYyYjkxZDY3Yzk0MzlhMHAyODgxNQ"),
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
    return _config

# SMTP connection manager
class SMTPConnectionPool:
    def __init__(self, host: str, port: int, user: str, password: str, max_connections: int = 5, timeout: int = 10, keep_alive: int = 30):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.max_connections = max_connections
        self.timeout = timeout
        self.keep_alive = keep_alive
        self.pool = Queue(maxsize=max_connections)
        self.lock = Lock()
        self.connection_timestamps = {}

    def get_connection(self) -> smtplib.SMTP:
        """Retrieve or create an SMTP connection."""
        with self.lock:
            try:
                while not self.pool.empty():
                    conn = self.pool.get_nowait()
                    last_used = self.connection_timestamps.get(id(conn), 0)
                    if time() - last_used < self.keep_alive and self._is_connection_alive(conn):
                        return conn
                    conn.quit()
            except smtplib.SMTPException:
                pass
            except Exception as e:
                logger.error(f"Error retrieving connection from pool: {e}")

            if self.pool.qsize() < self.max_connections:
                try:
                    conn = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
                    conn.starttls()
                    conn.login(self.user, self.password)
                    self.connection_timestamps[id(conn)] = time()
                    return conn
                except smtplib.SMTPException as e:
                    logger.error(f"Failed to create SMTP connection: {e}")
                    raise
            else:
                logger.warning("Max SMTP connections reached, waiting for available connection")
                return self.pool.get(block=True, timeout=self.timeout)

    def release_connection(self, conn: smtplib.SMTP):
        """Release an SMTP connection back to the pool."""
        with self.lock:
            if self._is_connection_alive(conn):
                self.connection_timestamps[id(conn)] = time()
                self.pool.put_nowait(conn)
            else:
                try:
                    conn.quit()
                except smtplib.SMTPException:
                    pass

    def _is_connection_alive(self, conn: smtplib.SMTP) -> bool:
        """Check if an SMTP connection is still alive."""
        try:
            status = conn.noop()[0]
            return status == 250
        except smtplib.SMTPException:
            return False

    def close_all(self):
        """Close all connections in the pool."""
        with self.lock:
            while not self.pool.empty():
                try:
                    conn = self.pool.get_nowait()
                    conn.quit()
                except smtplib.SMTPException:
                    pass
            self.connection_timestamps.clear()

# Global SMTP connection pool
_smtp_pool = None
def get_smtp_pool() -> SMTPConnectionPool:
    """Retrieve or initialize the global SMTP connection pool."""
    global _smtp_pool
    if _smtp_pool is None:
        config = get_config()
        _smtp_pool = SMTPConnectionPool(
            host=config.smtp_host,
            port=config.smtp_port,
            user=config.smtp_user,
            password=config.smtp_pass,
            max_connections=5,
            timeout=10,
            keep_alive=30
        )
    return _smtp_pool

# Redis client
redis_client = None
try:
    redis_url = os.getenv("REDIS_URL", "https://equal-sparrow-8815.upstash.io")
    redis_token = os.getenv("REDIS_TOKEN", "ASJvAAImcDIzMjI1Mjg2YjRkYzA0MGVjYjYyYjkxZDY3Yzk0MzlhMHAyODgxNQ")
    redis_client = Redis(url=redis_url, token=redis_token)
    # Test connection
    redis_client.set("test", "ping")
    redis_client.delete("test")
    logger.info("Connected to Upstash Redis")
    console.print("[info]Connected to Upstash Redis[/info]")
except Exception as e:
    logger.error(f"Failed to connect to Upstash Redis: {e}")
    console.print(f"[error]Failed to connect to Upstash Redis: {e}[/error]")
    redis_client = None

# Constants
RATE_LIMIT_WINDOW = 60  # 60 seconds
RATE_LIMIT_MAX = 30     # Max requests per window

# In-memory cache for route templates
route_templates_cache = None

# Utility functions
def safe_redis_operation(func, *args, **kwargs):
    """Safely execute a Redis operation with error handling."""
    if not redis_client:
        logger.warning("Redis client not available")
        return None
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Redis operation failed: {e}")
        return None

def generate_unique_id() -> str:
    """Generate a unique tracking ID."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_id = str(uuid.uuid4()).replace("-", "")[:6].upper()
    return f"TRK{timestamp}{unique_id}"

def sanitize_tracking_number(tracking_number: str) -> Optional[str]:
    """Sanitize and validate a tracking number."""
    if not tracking_number:
        return None
    tracking_number = tracking_number.strip().upper()
    if not tracking_number.startswith("TRK") or len(tracking_number) < 10:
        return None
    return tracking_number

def validate_email(email: Optional[str]) -> bool:
    """Validate an email address."""
    if not email:
        return True
    return bool(re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email))

def validate_location(location: Optional[str]) -> bool:
    """Validate a location string."""
    return bool(location and isinstance(location, str) and len(location) <= 100)

def validate_webhook_url(url: Optional[str]) -> bool:
    """Validate a webhook URL."""
    if not url:
        return True
    return bool(re.match(r'^https?://[^\s/$.?#].[^\s]*$', url))

def get_bot() -> TeleBot:
    """Initialize and return the Telegram bot instance."""
    config = get_config()
    bot = TeleBot(config.telegram_bot_token)
    return bot

def is_admin(user_id: int) -> bool:
    """Check if a user is an admin."""
    config = get_config()
    return user_id in config.allowed_admins

def send_dynamic_menu(chat_id: int, message_id: Optional[int] = None, page: int = 1):
    """Send or update the dynamic admin menu."""
    bot = get_bot()
    markup = InlineKeyboardMarkup(row_width=2)
    buttons = [
        InlineKeyboardButton("List Shipments", callback_data=f"list_{page}"),
        InlineKeyboardButton("Generate ID", callback_data="generate_id"),
        InlineKeyboardButton("Add Shipment", callback_data="add"),
        InlineKeyboardButton("Delete Shipment", callback_data=f"delete_menu_{page}"),
        InlineKeyboardButton("Toggle Email", callback_data=f"toggle_email_menu_{page}"),
        InlineKeyboardButton("Bulk Actions", callback_data="bulk_action"),
        InlineKeyboardButton("Stats", callback_data="stats"),
        InlineKeyboardButton("Settings", callback_data="settings"),
        InlineKeyboardButton("Help", callback_data="help")
    ]
    markup.add(*buttons)
    if page > 1:
        markup.add(InlineKeyboardButton("Previous", callback_data=f"menu_page_{page-1}"))
    markup.add(InlineKeyboardButton("Next", callback_data=f"menu_page_{page+1}"))
    text = f"*Admin Menu* (Page {page})"
    try:
        if message_id:
            bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, parse_mode='Markdown', reply_markup=markup)
        else:
            bot.send_message(chat_id, text, parse_mode='Markdown', reply_markup=markup)
        logger.info(f"Sent dynamic menu to chat {chat_id}, page {page}")
    except Exception as e:
        logger.error(f"Failed to send dynamic menu: {e}")

def get_shipment_details(tracking_number: str) -> Optional[dict]:
    """Retrieve shipment details from Redis cache or database."""
    try:
        cached = safe_redis_operation(redis_client.get, f"shipment:{tracking_number}") if redis_client else None
        if cached:
            logger.info(f"Cache hit for shipment {tracking_number}")
            return json.loads(cached)
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            return None
        data = {
            "tracking_number": shipment.tracking_number,
            "status": shipment.status,
            "checkpoints": shipment.checkpoints or "",
            "delivery_location": shipment.delivery_location,
            "recipient_email": shipment.recipient_email,
            "origin_location": shipment.origin_location,
            "webhook_url": shipment.webhook_url,
            "email_notifications": shipment.email_notifications,
            "last_updated": shipment.last_updated.isoformat(),
            "created_at": shipment.created_at.isoformat()
        }
        if redis_client:
            safe_redis_operation(redis_client.set, f"shipment:{tracking_number}", json.dumps(data), ex=3600)
        logger.info(f"Retrieved shipment {tracking_number} from database")
        return data
    except Exception as e:
        logger.error(f"Error retrieving shipment {tracking_number}: {e}")
        return None

def search_shipments(query: str, page: int = 1, per_page: int = 5) -> Tuple[List[str], int]:
    """Search shipments by tracking number, status, or location."""
    try:
        query = query.lower()
        shipments = Shipment.query.filter(
            (Shipment.tracking_number.ilike(f"%{query}%")) |
            (Shipment.status.ilike(f"%{query}%")) |
            (Shipment.delivery_location.ilike(f"%{query}%")) |
            (Shipment.origin_location.ilike(f"%{query}%"))
        ).order_by(Shipment.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()
        total = Shipment.query.filter(
            (Shipment.tracking_number.ilike(f"%{query}%")) |
            (Shipment.status.ilike(f"%{query}%")) |
            (Shipment.delivery_location.ilike(f"%{query}%")) |
            (Shipment.origin_location.ilike(f"%{query}%"))
        ).count()
        return [s.tracking_number for s in shipments], total
    except Exception as e:
        logger.error(f"Error searching shipments: {e}")
        return [], 0

def enqueue_notification(tracking_number: str, notification_type: str, data: dict) -> bool:
    """Enqueue a notification to Redis."""
    if not redis_client:
        logger.error("Cannot enqueue notification: Redis client not available")
        return False
    try:
        notification_data = {
            "tracking_number": tracking_number,
            "type": notification_type,
            "data": data,
            "timestamp": datetime.utcnow().isoformat()
        }
        safe_redis_operation(redis_client.rpush, "notifications", json.dumps(notification_data))
        logger.info(f"Enqueued {notification_type} notification for {tracking_number}")
        return True
    except Exception as e:
        logger.error(f"Failed to enqueue {notification_type} notification: {e}")
        return False

def send_email_notification(recipient_email: str, subject: str, plain_body: str, html_body: Optional[str] = None) -> bool:
    """Send an email notification using SMTP with plain text and optional HTML content."""
    config = get_config()
    smtp_pool = get_smtp_pool()
    max_retries = 3
    retry_delay = 5

    def send_email() -> bool:
        conn = None
        try:
            conn = smtp_pool.get_connection()
            msg = MIMEMultipart('alternative')
            msg['From'] = config.smtp_from
            msg['To'] = recipient_email
            msg['Subject'] = subject

            # Attach plain text part
            msg.attach(MIMEText(plain_body, 'plain'))

            # Attach HTML part if provided
            if html_body:
                msg.attach(MIMEText(html_body, 'html'))

            conn.send_message(msg)
            smtp_pool.release_connection(conn)
            logger.info(f"Sent email to {recipient_email}")
            return True
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error sending email to {recipient_email}: {e}")
            if conn:
                smtp_pool.release_connection(conn)
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending email to {recipient_email}: {e}")
            if conn:
                smtp_pool.release_connection(conn)
            return False

    # Wrap email sending in eventlet for non-blocking execution
    for attempt in range(max_retries):
        with eventlet.Timeout(10, False):
            if send_email():
                return True
            logger.warning(f"Email attempt {attempt + 1} failed for {recipient_email}")
            if attempt < max_retries - 1:
                sleep(retry_delay * (2 ** attempt))
    logger.error(f"Max retries exceeded for email to {recipient_email}")
    return False

def get_shipment_list(page: int = 1, per_page: int = 5) -> Tuple[List[str], int]:
    """Retrieve a paginated list of shipment tracking numbers."""
    try:
        shipments = Shipment.query.order_by(Shipment.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()
        total = Shipment.query.count()
        return [s.tracking_number for s in shipments], total
    except Exception as e:
        logger.error(f"Error retrieving shipment list: {e}")
        return [], 0

def save_shipment(tracking_number: str, status: str, checkpoints: str, delivery_location: str, 
                 recipient_email: Optional[str] = None, origin_location: Optional[str] = None, 
                 webhook_url: Optional[str] = None) -> bool:
    """Save a new shipment to the database."""
    try:
        shipment = Shipment(
            tracking_number=tracking_number,
            status=status,
            checkpoints=checkpoints,
            delivery_location=delivery_location,
            recipient_email=recipient_email,
            origin_location=origin_location,
            webhook_url=webhook_url,
            last_updated=datetime.utcnow(),
            created_at=datetime.utcnow()
        )
        db.session.add(shipment)
        db.session.commit()
        logger.info(f"Saved shipment {tracking_number}")
        return True
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error saving shipment {tracking_number}: {e}")
        return False

def update_shipment(tracking_number: str, status: Optional[str] = None, 
                   delivery_location: Optional[str] = None, recipient_email: Optional[str] = None,
                   origin_location: Optional[str] = None, webhook_url: Optional[str] = None) -> bool:
    """Update an existing shipment's details."""
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            return False
        config = get_config()
        if status and status in config.valid_statuses:
            shipment.status = status
        if delivery_location:
            shipment.delivery_location = delivery_location
        if recipient_email is not None:
            shipment.recipient_email = recipient_email
        if origin_location is not None:
            shipment.origin_location = origin_location
        if webhook_url is not None:
            shipment.webhook_url = webhook_url
        shipment.last_updated = datetime.utcnow()
        db.session.commit()
        invalidate_cache(tracking_number)
        logger.info(f"Updated shipment {tracking_number}")
        return True
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating shipment {tracking_number}: {e}")
        return False

def invalidate_cache(tracking_number: str):
    """Invalidate Redis cache for a shipment."""
    if redis_client:
        try:
            safe_redis_operation(redis_client.delete, f"shipment:{tracking_number}")
            logger.info(f"Invalidated cache for {tracking_number}")
        except Exception as e:
            logger.error(f"Failed to invalidate cache for {tracking_number}: {e}")

def get_app_modules():
    """Return a dictionary of application modules and their versions."""
    try:
        import pkg_resources
        modules = {
            'flask': pkg_resources.get_distribution('flask').version,
            'flask_sqlalchemy': pkg_resources.get_distribution('flask-sqlalchemy').version,
            'upstash-redis': pkg_resources.get_distribution('upstash-redis').version,
            'python-telegram-bot': pkg_resources.get_distribution('python-telegram-bot').version,
            'requests': pkg_resources.get_distribution('requests').version,
            'smtplib': 'stdlib',
            'eventlet': pkg_resources.get_distribution('eventlet').version,
            'flask_socketio': pkg_resources.get_distribution('flask-socketio').version,
            'flask_limiter': pkg_resources.get_distribution('flask-limiter').version,
            'rich': pkg_resources.get_distribution('rich').version,
            'validators': pkg_resources.get_distribution('validators').version
        }
        logger.info("Retrieved application modules")
        return modules
    except Exception as e:
        logger.error(f"Error retrieving application modules: {e}")
        return {}

def cache_route_templates():
    """Cache route templates in Redis or in-memory."""
    global route_templates_cache
    config = get_config()
    route_templates_cache = config.route_templates
    if redis_client:
        try:
            safe_redis_operation(redis_client.set, "route_templates", json.dumps(config.route_templates), ex=86400)
            logger.info("Cached route templates in Redis")
        except Exception as e:
            logger.error(f"Failed to cache route templates in Redis: {e}")
    logger.info("Cached route templates in memory")
    return route_templates_cache

def get_cached_route_templates() -> dict:
    """Retrieve cached route templates from Redis or in-memory."""
    global route_templates_cache
    if route_templates_cache is not None:
        return route_templates_cache
    if redis_client:
        try:
            cached = safe_redis_operation(redis_client.get, "route_templates")
            if cached:
                route_templates_cache = json.loads(cached)
                logger.info("Retrieved route templates from Redis cache")
                return route_templates_cache
        except Exception as e:
            logger.error(f"Failed to retrieve route templates from Redis: {e}")
    config = get_config()
    route_templates_cache = config.route_templates
    logger.info("Retrieved route templates from config")
    return route_templates_cache

def check_bot_status() -> bool:
    """Check if the Telegram bot is responsive."""
    bot = get_bot()
    try:
        bot.get_me()
        logger.info("Telegram bot status check: OK")
        return True
    except Exception as e:
        logger.error(f"Telegram bot status check failed: {e}")
        return False

# Temporary Flask app for SQLAlchemy (for Shipment model)
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('SQLALCHEMY_DATABASE_URI')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Shipment model
class Shipment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tracking_number = db.Column(db.String(20), unique=True, nullable=False)
    status = db.Column(db.String(50), nullable=False)
    checkpoints = db.Column(db.Text, nullable=True)
    delivery_location = db.Column(db.String(100), nullable=False)
    last_updated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    recipient_email = db.Column(db.String(120), nullable=True)
    origin_location = db.Column(db.String(100), nullable=True)
    webhook_url = db.Column(db.String(200), nullable=True)
    email_notifications = db.Column(db.Boolean, default=True)
