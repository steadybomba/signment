import os
import json
import re
import logging
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from upstash_redis import Redis
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from rich.console import Console
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import validators
from datetime import datetime
import eventlet
from threading import Lock
from time import time
import requests
from flask_socketio import emit
import uuid

# Logging setup
logger = logging.getLogger('utils')
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
    route_templates: Dict[str, List[str]]
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
        try:
            _config = BotConfig(
                telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
                redis_url=os.getenv("REDIS_URL", "https://equal-sparrow-8815.upstash.io"),
                redis_token=os.getenv("REDIS_TOKEN", "ASJvAAImcDIzMjI1Mjg2YjRkYzA0MGVjYjYyYjkxZDY3Yzk0MzlhMHAyODgxNQ"),
                webhook_url=os.getenv("WEBHOOK_URL", "https://signment-9a96.onrender.com/telegram/webhook"),
                websocket_server=os.getenv("WEBSOCKET_SERVER", "https://signment-9a96.onrender.com"),
                allowed_admins=[int(uid) for uid in os.getenv("ALLOWED_ADMINS", "").split(",") if uid],
                valid_statuses=os.getenv("VALID_STATUSES", "Pending,In_Transit,Out_for_Delivery,Delivered,Returned,Delayed").split(","),
                route_templates=json.loads(os.getenv("ROUTE_TEMPLATES", '{"Lagos, NG": ["Lagos, NG", "Abuja, NG", "Port Harcourt, NG"], "New York, NY": ["New York, NY", "New Jersey, NJ", "Boston, MA"], "London, UK": ["London, UK", "Birmingham, UK", "Manchester, UK"]}')),
                smtp_host=os.getenv("SMTP_HOST", "smtp.gmail.com"),
                smtp_port=int(os.getenv("SMTP_PORT", 587)),
                smtp_user=os.getenv("SMTP_USER", ""),
                smtp_pass=os.getenv("SMTP_PASS", ""),
                smtp_from=os.getenv("SMTP_FROM", "no-reply@signment.com")
            )
            logger.info("Configuration loaded successfully")
            console.print("[info]Configuration loaded successfully[/info]")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            console.print(f"[error]Failed to load configuration: {e}[/error]")
            raise
    return _config

# SMTP connection pool
class SMTPConnectionPool:
    def __init__(self, host: str, port: int, user: str, password: str, max_connections: int = 5, timeout: int = 10, keep_alive: int = 30):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.max_connections = max_connections
        self.timeout = timeout
        self.keep_alive = keep_alive
        self.pool = []
        self.lock = Lock()
        self.connection_timestamps = {}

    def get_connection(self) -> smtplib.SMTP:
        """Retrieve or create an SMTP connection."""
        with self.lock:
            try:
                while self.pool:
                    conn = self.pool.pop(0)
                    last_used = self.connection_timestamps.get(id(conn), 0)
                    if time() - last_used < self.keep_alive and self._is_connection_alive(conn):
                        return conn
                    try:
                        conn.quit()
                    except smtplib.SMTPException:
                        pass
            except Exception as e:
                logger.error(f"Error retrieving connection from pool: {e}")
                console.print(f"[error]Error retrieving connection from pool: {e}[/error]")

            if len(self.pool) < self.max_connections:
                try:
                    conn = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
                    conn.starttls()
                    conn.login(self.user, self.password)
                    self.connection_timestamps[id(conn)] = time()
                    return conn
                except smtplib.SMTPException as e:
                    logger.error(f"Failed to create SMTP connection: {e}")
                    console.print(f"[error]Failed to create SMTP connection: {e}[/error]")
                    raise
            else:
                logger.warning("Max SMTP connections reached, waiting for available connection")
                console.print("[warning]Max SMTP connections reached[/warning]")
                eventlet.sleep(1)
                return self.get_connection()

    def release_connection(self, conn: smtplib.SMTP):
        """Release an SMTP connection back to the pool."""
        with self.lock:
            if self._is_connection_alive(conn) and len(self.pool) < self.max_connections:
                self.connection_timestamps[id(conn)] = time()
                self.pool.append(conn)
            else:
                try:
                    conn.quit()
                except smtplib.SMTPException:
                    pass

    def _is_connection_alive(self, conn: smtplib.SMTP) -> bool:
        """Check if an SMTP connection is still alive."""
        try:
            return conn.noop()[0] == 250
        except smtplib.SMTPException:
            return False

    def close_all(self):
        """Close all connections in the pool."""
        with self.lock:
            while self.pool:
                try:
                    conn = self.pool.pop(0)
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
    config = get_config()
    redis_client = Redis(url=config.redis_url, token=config.redis_token)
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

def safe_redis_operation(func, *args, **kwargs):
    """Safely execute a Redis operation with error handling."""
    if not redis_client:
        logger.warning("Redis client not available")
        console.print("[warning]Redis client not available[/warning]")
        return None
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Redis operation failed: {e}")
        console.print(f"[error]Redis operation failed: {e}[/error]")
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
    tracking_number = re.sub(r'[^a-zA-Z0-9]', '', tracking_number).strip().upper()
    if not tracking_number.startswith("TRK") or len(tracking_number) < 10 or len(tracking_number) > 50:
        logger.warning(f"Invalid tracking number: {tracking_number}")
        console.print(f"[warning]Invalid tracking number: {tracking_number}[/warning]")
        return None
    return tracking_number

def validate_email(email: Optional[str]) -> bool:
    """Validate an email address."""
    if not email:
        return True
    return validators.email(email)

def validate_location(location: Optional[str]) -> bool:
    """Validate a location string."""
    return bool(location and isinstance(location, str) and len(location) <= 100)

def validate_webhook_url(url: Optional[str]) -> bool:
    """Validate a webhook URL."""
    if not url:
        return True
    return validators.url(url)

def is_admin(user_id: int) -> bool:
    """Check if a user is an admin."""
    config = get_config()
    return user_id in config.allowed_admins

def get_bot() -> Application:
    """Initialize and return the Telegram bot application."""
    try:
        config = get_config()
        app = Application.builder().token(config.telegram_bot_token).build()
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        logger.info("Telegram bot initialized")
        console.print("[info]Telegram bot initialized[/info]")
        return app
    except Exception as e:
        logger.error(f"Failed to initialize Telegram bot: {e}")
        console.print(f"[error]Failed to initialize Telegram bot: {e}[/error]")
        raise

async def start_command(update, context):
    """Handle /start command for Telegram bot."""
    try:
        await update.message.reply_text("Welcome to Signment Tracking! Send a tracking number to get shipment details.")
        logger.info("Processed /start command")
        console.print("[info]Processed /start command[/info]")
    except Exception as e:
        logger.error(f"Error in start_command: {e}")
        console.print(f"[error]Error in start_command: {e}[/error]")

async def handle_message(update, context):
    """Handle text messages with tracking numbers."""
    from app import Shipment, db, geocode_locations, broadcast_update  # Import here to avoid circular imports
    tracking_number = update.message.text.strip()
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        await update.message.reply_text("Invalid tracking number. Please provide a valid tracking number.")
        logger.warning(f"Invalid tracking number: {tracking_number}")
        console.print(f"[warning]Invalid tracking number: {tracking_number}[/warning]")
        return

    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        if not shipment:
            await update.message.reply_text(f"No shipment found for tracking number {sanitized_tn}.")
            logger.warning(f"Shipment not found: {sanitized_tn}")
            console.print(f"[warning]Shipment not found: {sanitized_tn}[/warning]")
            return

        checkpoints = shipment.checkpoints.split(';') if shipment.checkpoints else []
        coords = geocode_locations(checkpoints)
        coords_list = [{'lat': c['lat'], 'lon': c['lon'], 'desc': c['desc']} for c in coords]
        response = (
            f"Tracking Number: {sanitized_tn}\n"
            f"Status: {shipment.status}\n"
            f"Delivery Location: {shipment.delivery_location}\n"
            f"Checkpoints:\n" + "\n".join([f"- {cp}" for cp in checkpoints]) + "\n"
            f"Coordinates: {coords_list}"
        )
        await update.message.reply_text(response)
        logger.info(f"Sent shipment details for {sanitized_tn}")
        console.print(f"[info]Sent shipment details for {sanitized_tn}[/info]")
        broadcast_update(sanitized_tn)
    except Exception as e:
        logger.error(f"Error handling message for {sanitized_tn}: {e}")
        console.print(f"[error]Error handling message for {sanitized_tn}: {e}[/error]")
        await update.message.reply_text("An error occurred while fetching shipment details.")

def send_dynamic_menu(chat_id: int, message_id: Optional[int] = None, page: int = 1):
    """Send or update the dynamic admin menu."""
    from telegram import InlineKeyboardMarkup, InlineKeyboardButton
    bot = get_bot().bot
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
        console.print(f"[info]Sent dynamic menu to chat {chat_id}, page {page}[/info]")
    except Exception as e:
        logger.error(f"Failed to send dynamic menu: {e}")
        console.print(f"[error]Failed to send dynamic menu: {e}[/error]")

def check_bot_status() -> bool:
    """Check if the Telegram bot is responsive."""
    try:
        bot = get_bot().bot
        bot.get_me()
        logger.info("Telegram bot is responsive")
        console.print("[info]Telegram bot is responsive[/info]")
        return True
    except Exception as e:
        logger.error(f"Telegram bot check failed: {e}")
        console.print(f"[error]Telegram bot check failed: {e}[/error]")
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
            msg.attach(MIMEText(plain_body, 'plain'))
            if html_body:
                msg.attach(MIMEText(html_body, 'html'))
            conn.send_message(msg)
            smtp_pool.release_connection(conn)
            logger.info(f"Sent email to {recipient_email}")
            console.print(f"[info]Sent email to {recipient_email}[/info]")
            return True
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error sending email to {recipient_email}: {e}")
            console.print(f"[error]SMTP error sending email to {recipient_email}: {e}[/error]")
            if conn:
                smtp_pool.release_connection(conn)
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending email to {recipient_email}: {e}")
            console.print(f"[error]Unexpected error sending email to {recipient_email}: {e}[/error]")
            if conn:
                smtp_pool.release_connection(conn)
            return False

    for attempt in range(max_retries):
        with eventlet.Timeout(10, False):
            if send_email():
                return True
            logger.warning(f"Email attempt {attempt + 1} failed for {recipient_email}")
            console.print(f"[warning]Email attempt {attempt + 1} failed for {recipient_email}[/warning]")
            if attempt < max_retries - 1:
                eventlet.sleep(retry_delay * (2 ** attempt))
    logger.error(f"Max retries exceeded for email to {recipient_email}")
    console.print(f"[error]Max retries exceeded for email to {recipient_email}[/error]")
    return False

def send_webhook_notification(webhook_url: str, tracking_number: str, data: Dict[str, Any]) -> bool:
    """Send a webhook notification with shipment data."""
    max_retries = 3
    retry_delay = 5

    def send_webhook() -> bool:
        try:
            headers = {"Content-Type": "application/json"}
            response = requests.post(webhook_url, json=data, headers=headers, timeout=10)
            response.raise_for_status()
            logger.info(f"Sent webhook notification to {webhook_url} for {tracking_number}")
            console.print(f"[info]Sent webhook notification to {webhook_url} for {tracking_number}[/info]")
            return True
        except requests.RequestException as e:
            logger.error(f"Webhook request failed for {tracking_number} to {webhook_url}: {e}")
            console.print(f"[error]Webhook request failed for {tracking_number} to {webhook_url}: {e}[/error]")
            return False

    for attempt in range(max_retries):
        with eventlet.Timeout(10, False):
            if send_webhook():
                return True
            logger.warning(f"Webhook attempt {attempt + 1} failed for {tracking_number} to {webhook_url}")
            console.print(f"[warning]Webhook attempt {attempt + 1} failed for {tracking_number} to {webhook_url}[/warning]")
            if attempt < max_retries - 1:
                eventlet.sleep(retry_delay * (2 ** attempt))
    logger.error(f"Max retries exceeded for webhook to {webhook_url} for {tracking_number}")
    console.print(f"[error]Max retries exceeded for webhook to {webhook_url} for {tracking_number}[/error]")
    return False

def send_websocket_notification(tracking_number: str, data: Dict[str, Any]) -> bool:
    """Send a WebSocket notification with shipment data."""
    config = get_config()
    max_retries = 3
    retry_delay = 5

    def send_websocket() -> bool:
        try:
            # Try direct emission if running in the same process
            try:
                emit('shipment_update', data, namespace='/', broadcast=True)
                logger.info(f"Sent WebSocket notification directly for {tracking_number}")
                console.print(f"[info]Sent WebSocket notification directly for {tracking_number}[/info]")
                return True
            except Exception as e:
                logger.debug(f"Direct WebSocket emission failed: {e}, falling back to HTTP")
                console.print(f"[debug]Direct WebSocket emission failed: {e}, falling back to HTTP[/debug]")

            # Fallback to HTTP POST to WEBSOCKET_SERVER/notify
            notify_url = f"{config.websocket_server}/notify"
            headers = {"Content-Type": "application/json"}
            response = requests.post(notify_url, json=data, headers=headers, timeout=10)
            response.raise_for_status()
            logger.info(f"Sent WebSocket notification to {notify_url} for {tracking_number}")
            console.print(f"[info]Sent WebSocket notification to {notify_url} for {tracking_number}[/info]")
            return True
        except requests.RequestException as e:
            logger.error(f"WebSocket notification request failed for {tracking_number} to {notify_url}: {e}")
            console.print(f"[error]WebSocket notification request failed for {tracking_number} to {notify_url}: {e}[/error]")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending WebSocket notification for {tracking_number}: {e}")
            console.print(f"[error]Unexpected error sending WebSocket notification for {tracking_number}: {e}[/error]")
            return False

    for attempt in range(max_retries):
        with eventlet.Timeout(10, False):
            if send_websocket():
                return True
            logger.warning(f"WebSocket notification attempt {attempt + 1} failed for {tracking_number}")
            console.print(f"[warning]WebSocket notification attempt {attempt + 1} failed for {tracking_number}[/warning]")
            if attempt < max_retries - 1:
                eventlet.sleep(retry_delay * (2 ** attempt))
    logger.error(f"Max retries exceeded for WebSocket notification for {tracking_number}")
    console.print(f"[error]Max retries exceeded for WebSocket notification for {tracking_number}[/error]")
    return False

def enqueue_notification(tracking_number: str, notification_type: str, data: Dict[str, Any]) -> bool:
    """Enqueue a notification in Redis."""
    if not redis_client:
        logger.error("Redis client not available for enqueuing notification")
        console.print("[error]Redis client not available for notification[/error]")
        return False
    try:
        notification = {
            "tracking_number": tracking_number,
            "type": notification_type,
            "data": data,
            "timestamp": datetime.utcnow().isoformat()
        }
        safe_redis_operation(redis_client.lpush, "notifications", json.dumps(notification))
        logger.info(f"Enqueued {notification_type} notification for {tracking_number}")
        console.print(f"[info]Enqueued {notification_type} notification for {tracking_number}[/info]")
        return True
    except Exception as e:
        logger.error(f"Failed to enqueue {notification_type} notification for {tracking_number}: {e}")
        console.print(f"[error]Failed to enqueue {notification_type} notification for {tracking_number}: {e}[/error]")
        return False

def get_shipment_list(page: int = 1, per_page: int = 5) -> Tuple[List[str], int]:
    """Retrieve a paginated list of shipment tracking numbers."""
    from app import Shipment, db  # Import here to avoid circular imports
    try:
        shipments = Shipment.query.order_by(Shipment.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()
        total = Shipment.query.count()
        return [s.tracking_number for s in shipments], total
    except Exception as e:
        logger.error(f"Error retrieving shipment list: {e}")
        console.print(f"[error]Error retrieving shipment list: {e}[/error]")
        return [], 0

def search_shipments(query: str, page: int = 1, per_page: int = 5) -> Tuple[List[str], int]:
    """Search shipments by tracking number, status, or location."""
    from app import Shipment  # Import here to avoid circular imports
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
        console.print(f"[error]Error searching shipments: {e}[/error]")
        return [], 0

def get_shipment_details(tracking_number: str) -> Optional[Dict[str, Any]]:
    """Retrieve details for a specific shipment."""
    from app import Shipment  # Import here to avoid circular imports
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        logger.warning(f"Invalid tracking number for details: {tracking_number}")
        console.print(f"[warning]Invalid tracking number for details: {tracking_number}[/warning]")
        return None
    try:
        cached = safe_redis_operation(redis_client.get, f"shipment:{sanitized_tn}") if redis_client else None
        if cached:
            logger.info(f"Cache hit for shipment {sanitized_tn}")
            console.print(f"[info]Cache hit for shipment {sanitized_tn}[/info]")
            return json.loads(cached)
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        if not shipment:
            logger.warning(f"Shipment not found: {sanitized_tn}")
            console.print(f"[warning]Shipment not found: {sanitized_tn}[/warning]")
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
            safe_redis_operation(redis_client.set, f"shipment:{sanitized_tn}", json.dumps(data), ex=3600)
        logger.info(f"Retrieved shipment {sanitized_tn} from database")
        console.print(f"[info]Retrieved shipment {sanitized_tn} from database[/info]")
        return data
    except Exception as e:
        logger.error(f"Failed to retrieve shipment details for {sanitized_tn}: {e}")
        console.print(f"[error]Failed to retrieve shipment details for {sanitized_tn}: {e}[/error]")
        return None

def save_shipment(
    tracking_number: str,
    status: str,
    checkpoints: str,
    delivery_location: str,
    recipient_email: Optional[str] = None,
    origin_location: Optional[str] = None,
    webhook_url: Optional[str] = None
) -> bool:
    """Save or update a shipment in the database."""
    from app import Shipment, db  # Import here to avoid circular imports
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        logger.warning(f"Invalid tracking number: {tracking_number}")
        console.print(f"[warning]Invalid tracking number: {tracking_number}[/warning]")
        raise ValueError("Invalid tracking number")
    if not validate_location(delivery_location):
        logger.warning(f"Invalid delivery location: {delivery_location}")
        console.print(f"[warning]Invalid delivery location: {delivery_location}[/warning]")
        raise ValueError("Invalid delivery location")
    if recipient_email and not validate_email(recipient_email):
        logger.warning(f"Invalid recipient email: {recipient_email}")
        console.print(f"[warning]Invalid recipient email: {recipient_email}[/warning]")
        raise ValueError("Invalid recipient email")
    if webhook_url and not validate_webhook_url(webhook_url):
        logger.warning(f"Invalid webhook URL: {webhook_url}")
        console.print(f"[warning]Invalid webhook URL: {webhook_url}[/warning]")
        raise ValueError("Invalid webhook URL")
    if origin_location and not validate_location(origin_location):
        logger.warning(f"Invalid origin location: {origin_location}")
        console.print(f"[warning]Invalid origin location: {origin_location}[/warning]")
        raise ValueError("Invalid origin location")
    config = get_config()
    if status not in config.valid_statuses:
        logger.warning(f"Invalid status: {status}")
        console.print(f"[warning]Invalid status: {status}[/warning]")
        raise ValueError("Invalid status")

    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        current_time = datetime.utcnow()
        action = "updated" if shipment else "created"
        if shipment:
            shipment.status = status
            shipment.checkpoints = checkpoints
            shipment.delivery_location = delivery_location
            shipment.recipient_email = recipient_email
            shipment.origin_location = origin_location
            shipment.webhook_url = webhook_url
            shipment.last_updated = current_time
        else:
            shipment = Shipment(
                tracking_number=sanitized_tn,
                status=status,
                checkpoints=checkpoints,
                delivery_location=delivery_location,
                recipient_email=recipient_email,
                origin_location=origin_location,
                webhook_url=webhook_url,
                last_updated=current_time,
                created_at=current_time
            )
            db.session.add(shipment)
        db.session.commit()
        invalidate_cache(sanitized_tn)
        logger.info(f"Saved shipment {sanitized_tn} ({action})")
        console.print(f"[info]Saved shipment {sanitized_tn} ({action})[/info]")

        # Send webhook notification if webhook_url is provided
        if webhook_url:
            webhook_data = {
                "tracking_number": sanitized_tn,
                "status": status,
                "checkpoints": checkpoints,
                "delivery_location": delivery_location,
                "recipient_email": recipient_email,
                "origin_location": origin_location,
                "webhook_url": webhook_url,
                "action": action,
                "timestamp": current_time.isoformat()
            }
            send_webhook_notification(webhook_url, sanitized_tn, webhook_data)

        # Send WebSocket notification
        websocket_data = {
            "tracking_number": sanitized_tn,
            "status": status,
            "checkpoints": checkpoints,
            "delivery_location": delivery_location,
            "recipient_email": recipient_email,
            "origin_location": origin_location,
            "webhook_url": webhook_url,
            "action": action,
            "timestamp": current_time.isoformat()
        }
        send_websocket_notification(sanitized_tn, websocket_data)

        return True
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to save shipment {sanitized_tn}: {e}")
        console.print(f"[error]Failed to save shipment {sanitized_tn}: {e}[/error]")
        return False

def update_shipment(
    tracking_number: str,
    status: Optional[str] = None,
    delivery_location: Optional[str] = None,
    recipient_email: Optional[str] = None,
    origin_location: Optional[str] = None,
    webhook_url: Optional[str] = None
) -> bool:
    """Update an existing shipment's details."""
    from app import Shipment, db  # Import here to avoid circular imports
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        logger.warning(f"Invalid tracking number: {tracking_number}")
        console.print(f"[warning]Invalid tracking number: {tracking_number}[/warning]")
        return False
    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        if not shipment:
            logger.warning(f"Shipment not found: {sanitized_tn}")
            console.print(f"[warning]Shipment not found: {sanitized_tn}[/warning]")
            return False
        config = get_config()
        if status and status in config.valid_statuses:
            shipment.status = status
        if delivery_location and validate_location(delivery_location):
            shipment.delivery_location = delivery_location
        if recipient_email is not None and (not recipient_email or validate_email(recipient_email)):
            shipment.recipient_email = recipient_email
        if origin_location is not None and (not origin_location or validate_location(origin_location)):
            shipment.origin_location = origin_location
        if webhook_url is not None and (not webhook_url or validate_webhook_url(webhook_url)):
            shipment.webhook_url = webhook_url
        shipment.last_updated = datetime.utcnow()
        db.session.commit()
        invalidate_cache(sanitized_tn)
        logger.info(f"Updated shipment {sanitized_tn}")
        console.print(f"[info]Updated shipment {sanitized_tn}[/info]")

        # Send webhook notification if webhook_url is provided
        if shipment.webhook_url:
            webhook_data = {
                "tracking_number": sanitized_tn,
                "status": shipment.status,
                "checkpoints": shipment.checkpoints or "",
                "delivery_location": shipment.delivery_location,
                "recipient_email": shipment.recipient_email,
                "origin_location": shipment.origin_location,
                "webhook_url": shipment.webhook_url,
                "action": "updated",
                "timestamp": shipment.last_updated.isoformat()
            }
            send_webhook_notification(shipment.webhook_url, sanitized_tn, webhook_data)

        # Send WebSocket notification
        websocket_data = {
            "tracking_number": sanitized_tn,
            "status": shipment.status,
            "checkpoints": shipment.checkpoints or "",
            "delivery_location": shipment.delivery_location,
            "recipient_email": shipment.recipient_email,
            "origin_location": shipment.origin_location,
            "webhook_url": shipment.webhook_url,
            "action": "updated",
            "timestamp": shipment.last_updated.isoformat()
        }
        send_websocket_notification(sanitized_tn, websocket_data)

        return True
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating shipment {sanitized_tn}: {e}")
        console.print(f"[error]Error updating shipment {sanitized_tn}: {e}[/error]")
        return False

def invalidate_cache(tracking_number: str):
    """Invalidate Redis cache for a shipment."""
    if redis_client:
        try:
            safe_redis_operation(redis_client.delete, f"shipment:{tracking_number}")
            logger.info(f"Invalidated cache for {tracking_number}")
            console.print(f"[info]Invalidated cache for {tracking_number}[/info]")
        except Exception as e:
            logger.error(f"Failed to invalidate cache for {tracking_number}: {e}")
            console.print(f"[error]Failed to invalidate cache for {tracking_number}: {e}[/error]")

def get_app_modules() -> Dict[str, str]:
    """Return a dictionary of application modules and their versions."""
    try:
        import pkg_resources
        modules = {
            "flask": pkg_resources.get_distribution("flask").version,
            "flask-sqlalchemy": pkg_resources.get_distribution("flask-sqlalchemy").version,
            "upstash-redis": pkg_resources.get_distribution("upstash-redis").version,
            "python-telegram-bot": pkg_resources.get_distribution("python-telegram-bot").version,
            "requests": pkg_resources.get_distribution("requests").version,
            "smtplib": "stdlib",
            "eventlet": pkg_resources.get_distribution("eventlet").version,
            "flask-socketio": pkg_resources.get_distribution("flask-socketio").version,
            "flask-limiter": pkg_resources.get_distribution("flask-limiter").version,
            "rich": pkg_resources.get_distribution("rich").version,
            "validators": pkg_resources.get_distribution("validators").version,
            "flask-wtf": pkg_resources.get_distribution("flask-wtf").version,
            "wtforms": pkg_resources.get_distribution("wtforms").version
        }
        logger.info("Retrieved application modules")
        console.print("[info]Retrieved application modules[/info]")
        return modules
    except Exception as e:
        logger.error(f"Error retrieving application modules: {e}")
        console.print(f"[error]Error retrieving application modules: {e}[/error]")
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
            console.print("[info]Cached route templates in Redis[/info]")
        except Exception as e:
            logger.error(f"Failed to cache route templates in Redis: {e}")
            console.print(f"[error]Failed to cache route templates in Redis: {e}[/error]")
    logger.info("Cached route templates in memory")
    console.print("[info]Cached route templates in memory[/info]")

def get_cached_route_templates() -> Dict[str, List[str]]:
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
                console.print("[info]Retrieved route templates from Redis cache[/info]")
                return route_templates_cache
        except Exception as e:
            logger.error(f"Failed to retrieve route templates from Redis: {e}")
            console.print(f"[error]Failed to retrieve route templates from Redis: {e}[/error]")
    config = get_config()
    route_templates_cache = config.route_templates
    logger.info("Retrieved route templates from config")
    console.print(f"[info]Retrieved route templates from config[/info]")
    return route_templates_cache
