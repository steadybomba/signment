import os
import re
import json
from datetime import datetime
try:
    from upstash_redis import Redis
except ImportError:
    Redis = None
import uuid
import logging
import smtplib
import requests
from email.mime.text import MIMEText
from typing import Optional, Tuple, List
from urllib.parse import urlparse
from rich.console import Console
from rich.panel import Panel
from dataclasses import dataclass
from telebot import TeleBot, apihelper

# Logging setup
logger = logging.getLogger('app')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
console = Console()

# Configuration
@dataclass
class BotConfig:
    telegram_bot_token: str
    redis_url: str
    redis_token: str
    webhook_url: str
    websocket_server: str
    allowed_admins: list
    valid_statuses: list
    route_templates: dict
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_pass: str
    smtp_from: str

# Redis client
redis_client = None
if Redis:
    try:
        redis_client = Redis(
            url=os.getenv("REDIS_URL"),
            token=os.getenv("REDIS_TOKEN", "")
        )
        redis_client.ping()
        logger.info("Connected to Upstash Redis")
        console.print("[info]Connected to Upstash Redis[/info]")
    except Exception as e:
        logger.error(f"Upstash Redis connection failed: {e}")
        console.print(Panel(f"[error]Upstash Redis connection failed: {e}[/error]", title="Redis Error", border_style="red"))
        redis_client = None
else:
    logger.warning("Upstash Redis module not installed; Redis operations will be disabled")
    console.print(Panel("[warning]Upstash Redis module not installed; Redis operations will be disabled[/warning]", title="Redis Warning", border_style="yellow"))

# Constants
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", 60))
RATE_LIMIT_MAX = int(os.getenv("RATE_LIMIT_MAX", 100))

# In-memory cache for route templates
route_templates_cache = {}

def safe_redis_operation(func, *args, **kwargs):
    """Safely execute a Redis operation."""
    if redis_client is None:
        logger.warning(f"Redis operation {func.__name__} skipped: Redis unavailable")
        return None
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Redis operation failed: {e}")
        console.print(Panel(f"[error]Redis operation failed: {e}[/error]", title="Redis Error", border_style="red"))
        return None

def cache_route_templates():
    """Cache route templates in Redis or in-memory."""
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
    global route_templates_cache
    route_templates_cache = config.route_templates
    if redis_client:
        try:
            safe_redis_operation(redis_client.setex, "route_templates", 86400, json.dumps(config.route_templates))
            logger.info("Cached route templates in Redis")
            console.print("[info]Cached route templates in Redis[/info]")
        except Exception as e:
            logger.error(f"Failed to cache route templates: {e}")
            console.print(Panel(f"[error]Failed to cache route templates: {e}[/error]", title="Cache Error", border_style="red"))
    logger.info("Cached route templates in memory")
    console.print("[info]Cached route templates in memory[/info]")

def get_cached_route_templates():
    """Retrieve cached route templates."""
    if route_templates_cache:
        return route_templates_cache
    if redis_client:
        try:
            cached = safe_redis_operation(redis_client.get, "route_templates")
            if cached:
                return json.loads(cached)
        except Exception as e:
            logger.error(f"Failed to retrieve cached route templates: {e}")
            console.print(Panel(f"[error]Failed to retrieve cached route templates: {e}[/error]", title="Cache Error", border_style="red"))
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
    return config.route_templates

def enqueue_notification(tracking_number: str, notification_type: str, data: dict):
    """Enqueue a notification to be processed (e.g., email or webhook)."""
    if redis_client is None:
        logger.warning("Cannot enqueue notification: Redis unavailable")
        return False
    try:
        notification = {
            'tracking_number': tracking_number,
            'type': notification_type,  # e.g., 'email', 'webhook'
            'data': data,
            'created_at': datetime.utcnow().isoformat()
        }
        queue_key = "notifications_queue"
        safe_redis_operation(redis_client.lpush, queue_key, json.dumps(notification))
        logger.info(f"Enqueued {notification_type} notification for {tracking_number}")
        console.print(f"[info]Enqueued {notification_type} notification for {tracking_number}[/info]")
        return True
    except Exception as e:
        logger.error(f"Failed to enqueue notification for {tracking_number}: {e}")
        console.print(Panel(f"[error]Failed to enqueue notification for {tracking_number}: {e}[/error]", title="Queue Error", border_style="red"))
        return False

def send_email_notification(tracking_number: str, status: str, checkpoints: str, delivery_location: str, recipient_email: str):
    """Send an email notification for a shipment update."""
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
    try:
        msg = MIMEText(f"Shipment Update: {tracking_number} is now {status} at {delivery_location}\n\nCheckpoints:\n{checkpoints or 'None'}")
        msg['Subject'] = f"Shipment Update: {tracking_number}"
        msg['From'] = config.smtp_from
        msg['To'] = recipient_email
        with smtplib.SMTP(config.smtp_host, config.smtp_port) as server:
            server.starttls()
            server.login(config.smtp_user, config.smtp_pass)
            server.send_message(msg)
        logger.info(f"Sent email notification for {tracking_number} to {recipient_email}")
        console.print(f"[info]Sent email notification for {tracking_number} to {recipient_email}[/info]")
        return True
    except Exception as e:
        logger.error(f"Failed to send email notification for {tracking_number}: {e}")
        console.print(Panel(f"[error]Failed to send email notification for {tracking_number}: {e}[/error]", title="Email Error", border_style="red"))
        return False

def check_bot_status():
    """Check if the Telegram bot is responsive."""
    try:
        bot = get_bot()
        bot.get_me()
        logger.info("Telegram bot status: OK")
        console.print("[info]Telegram bot status: OK[/info]")
        return True
    except Exception as e:
        logger.error(f"Telegram bot status check failed: {e}")
        console.print(Panel(f"[error]Telegram bot status check failed: {e}[/error]", title="Bot Error", border_style="red"))
        return False

def get_bot():
    """Initialize and return the Telegram bot instance."""
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
    bot = TeleBot(config.telegram_bot_token)
    return bot

def get_app_modules():
    """Return Flask app modules."""
    from app import db, Shipment
    def validate_email(email):
        return re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email) if email else True
    def validate_location(location):
        return bool(location and isinstance(location, str) and len(location) <= 100)
    def validate_webhook_url(url):
        return re.match(r'^https?://[^\s/$.?#].[^\s]*$', url) if url else True
    return db, Shipment, sanitize_tracking_number, validate_email, validate_location, validate_webhook_url, get_shipment_list

def is_admin(user_id: int) -> bool:
    """Check if the user is an admin."""
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
    return user_id in config.allowed_admins

def sanitize_input(text: str) -> str:
    """Sanitize input to prevent injection attacks."""
    if not text:
        return ""
    text = re.sub(r'[^\w\s,.-@:/]', '', text)
    return text.strip()

def sanitize_tracking_number(tracking_number: str) -> Optional[str]:
    """Sanitize and validate tracking number."""
    tracking_number = sanitize_input(tracking_number)
    if not re.match(r'^[A-Z0-9]{10,20}$', tracking_number):
        return None
    return tracking_number

def validate_email(email: str) -> bool:
    """Validate email address format."""
    if not email:
        return True
    return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email))

def validate_location(location: str) -> bool:
    """Validate location format."""
    if not location:
        return True
    return bool(re.match(r'^[a-zA-Z\s,]+$', location))

def validate_webhook_url(url: str) -> bool:
    """Validate webhook URL."""
    if not url:
        return True
    try:
        result = urlparse(url)
        return all([result.scheme in ['http', 'https'], result.netloc])
    except Exception:
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
            console.print(Panel(f"[error]Failed to invalidate cache for {tracking_number}: {e}[/error]", title="Cache Error", border_style="red"))

def get_shipment_list(page: int = 1) -> Tuple[List[str], int]:
    """Get a paginated list of shipment tracking numbers."""
    from app import db, Shipment
    per_page = 5
    try:
        shipments = Shipment.query.order_by(Shipment.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()
        total = Shipment.query.count()
        return [s.tracking_number for s in shipments], total
    except Exception as e:
        logger.error(f"Database error fetching shipment list: {e}")
        console.print(Panel(f"[error]Database error fetching shipment list: {e}[/error]", title="Database Error", border_style="red"))
        return [], 0

def get_shipment_details(tracking_number: str) -> Optional[dict]:
    """Get shipment details with caching."""
    from app import db, Shipment
    tracking_number = sanitize_tracking_number(tracking_number)
    if not tracking_number:
        return None
    cache_key = f"shipment:{tracking_number}"
    cached = safe_redis_operation(redis_client.get, cache_key) if redis_client else None
    if cached:
        return json.loads(cached)
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            return None
        data = {
            'tracking_number': shipment.tracking_number,
            'status': shipment.status,
            'checkpoints': shipment.checkpoints or 'None',
            'delivery_location': shipment.delivery_location,
            'recipient_email': shipment.recipient_email,
            'email_notifications': shipment.email_notifications,
            'webhook_url': shipment.webhook_url,
            'paused': safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true" if redis_client else False,
            'speed_multiplier': float(safe_redis_operation(redis_client.hget, "sim_speed_multipliers", tracking_number) or 1.0) if redis_client else 1.0
        }
        if redis_client:
            safe_redis_operation(redis_client.setex, cache_key, 3600, json.dumps(data))
        return data
    except Exception as e:
        logger.error(f"Database error fetching shipment {tracking_number}: {e}")
        console.print(Panel(f"[error]Database error fetching shipment {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
        return None

def save_shipment(tracking_number: str, status: str, checkpoints: str, delivery_location: str,
                 recipient_email: str = '', origin_location: str = None, webhook_url: str = None) -> bool:
    """Save a shipment to the database."""
    from app import db, Shipment
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
    tracking_number = sanitize_tracking_number(tracking_number)
    if not tracking_number:
        raise ValueError("Invalid tracking number")
    if status not in config.valid_statuses:
        raise ValueError(f"Invalid status. Must be one of {', '.join(config.valid_statuses)}")
    if not validate_location(delivery_location):
        raise ValueError("Invalid delivery location")
    if not validate_email(recipient_email):
        raise ValueError("Invalid recipient email")
    if origin_location and not validate_location(origin_location):
        raise ValueError("Invalid origin location")
    if webhook_url and not validate_webhook_url(webhook_url):
        raise ValueError("Invalid webhook URL")
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if shipment:
            shipment.status = status
            shipment.checkpoints = checkpoints
            shipment.delivery_location = delivery_location
            shipment.recipient_email = recipient_email
            shipment.origin_location = origin_location
            shipment.webhook_url = webhook_url
            shipment.last_updated = datetime.utcnow()
        else:
            shipment = Shipment(
                tracking_number=tracking_number,
                status=status,
                checkpoints=checkpoints,
                delivery_location=delivery_location,
                recipient_email=recipient_email,
                origin_location=origin_location,
                webhook_url=webhook_url,
                created_at=datetime.utcnow(),
                last_updated=datetime.utcnow(),
                email_notifications=True
            )
            db.session.add(shipment)
        db.session.commit()
        invalidate_cache(tracking_number)
        logger.info(f"Saved shipment {tracking_number}")
        console.print(f"[info]Saved shipment {tracking_number}[/info]")
        return True
    except Exception as e:
        db.session.rollback()
        logger.error(f"Database error saving shipment {tracking_number}: {e}")
        console.print(Panel(f"[error]Database error saving shipment {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
        return False

def search_shipments(query: str, page: int = 1) -> Tuple[List[str], int]:
    """Search shipments by tracking number or location."""
    from app import db, Shipment
    query = sanitize_input(query)
    per_page = 5
    try:
        shipments = Shipment.query.filter(
            (Shipment.tracking_number.ilike(f"%{query}%")) |
            (Shipment.delivery_location.ilike(f"%{query}%")) |
            (Shipment.origin_location.ilike(f"%{query}%"))
        ).order_by(Shipment.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()
        total = Shipment.query.filter(
            (Shipment.tracking_number.ilike(f"%{query}%")) |
            (Shipment.delivery_location.ilike(f"%{query}%")) |
            (Shipment.origin_location.ilike(f"%{query}%"))
        ).count()
        return [s.tracking_number for s in shipments], total
    except Exception as e:
        logger.error(f"Database error searching shipments: {e}")
        console.print(Panel(f"[error]Database error searching shipments: {e}[/error]", title="Database Error", border_style="red"))
        return [], 0

def send_dynamic_menu(chat_id: int, message_id: Optional[int] = None, page: int = 1):
    """Send a dynamic menu to the admin."""
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
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
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("Add Shipment", callback_data="add"),
        InlineKeyboardButton("Generate ID", callback_data="generate_id"),
        InlineKeyboardButton("View Shipments", callback_data="list_1"),
        InlineKeyboardButton("Delete Shipments", callback_data="delete_menu_1"),
        InlineKeyboardButton("Toggle Email", callback_data="toggle_email_menu_1"),
        InlineKeyboardButton("Broadcast", callback_data="broadcast_menu_1"),
        InlineKeyboardButton("Set Speed", callback_data="setspeed_menu_1"),
        InlineKeyboardButton("Get Speed", callback_data="getspeed_menu_1"),
        InlineKeyboardButton("Settings", callback_data="settings"),
        InlineKeyboardButton("Help", callback_data="help")
    )
    text = f"*Admin Menu* (Page {page})"
    bot = get_bot()
    if message_id:
        bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, parse_mode='Markdown', reply_markup=markup)
    else:
        bot.send_message(chat_id, text, parse_mode='Markdown', reply_markup=markup)
    logger.info(f"Sent dynamic menu to chat {chat_id}")
    console.print(f"[info]Sent dynamic menu to chat {chat_id}[/info]")
