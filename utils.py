import os
import json
import re
import logging
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from upstash_redis import Redis
from telebot import TeleBot
from telebot.types import Update  # Critical for webhook
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
        try:
            return conn.noop()[0] == 250
        except smtplib.SMTPException:
            return False

    def close_all(self):
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
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 30
route_templates_cache = None

def safe_redis_operation(func, *args, **kwargs):
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

def cleanup_resources():
    global redis_client, _smtp_pool
    try:
        if redis_client:
            redis_client.close()
            logger.info("Closed Redis connection")
            console.print("[info]Closed Redis connection[/info]")
            redis_client = None
        if _smtp_pool:
            _smtp_pool.close_all()
            logger.info("Closed all SMTP connections")
            console.print("[info]Closed all SMTP connections[/info]")
            _smtp_pool = None
    except Exception as e:
        logger.error(f"Error during resource cleanup: {e}")
        console.print(f"[error]Error during resource cleanup: {e}[/error]")

def generate_unique_id() -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    unique_id = str(uuid.uuid4()).replace("-", "")[:6].upper()
    return f"TRK{timestamp}{unique_id}"

def sanitize_tracking_number(tracking_number: str) -> Optional[str]:
    if not tracking_number:
        return None
    tracking_number = re.sub(r'[^a-zA-Z0-9]', '', tracking_number).strip().upper()
    if not tracking_number.startswith("TRK") or len(tracking_number) < 10 or len(tracking_number) > 50:
        logger.warning(f"Invalid tracking number format: {tracking_number}")
        console.print(f"[warning]Invalid tracking number format: {tracking_number}[/warning]")
        return None
    return tracking_number

def check_rate_limit(user_id: str) -> bool:
    if not redis_client:
        return True
    key = f"rate_limit:{user_id}"
    try:
        count = safe_redis_operation(redis_client.get, key)
        count = int(count) if count else 0
        if count >= RATE_LIMIT_MAX:
            logger.warning(f"Rate limit exceeded for user {user_id}")
            console.print(f"[warning]Rate limit exceeded for user {user_id}[/warning]")
            return False
        safe_redis_operation(redis_client.incr, key)
        safe_redis_operation(redis_client.expire, key, RATE_LIMIT_WINDOW)
        return True
    except Exception as e:
        logger.error(f"Rate limit check failed for {user_id}: {e}")
        console.print(f"[error]Rate limit check failed for {user_id}: {e}[/error]")
        return True

def validate_email(email: Optional[str]) -> bool:
    if not email:
        return True
    return validators.email(email)

def validate_location(location: Optional[str]) -> bool:
    return bool(location and isinstance(location, str) and len(location) <= 100)

def validate_webhook_url(url: Optional[str]) -> bool:
    if not url:
        return True
    return validators.url(url)

def is_admin(user_id: int) -> bool:
    config = get_config()
    return user_id in config.allowed_admins

def get_bot() -> TeleBot:
    try:
        config = get_config()
        bot = TeleBot(config.telegram_bot_token)
        logger.info("Telegram bot initialized with handlers")
        console.print("[info]Telegram bot initialized with handlers[/info]")
        return bot
    except Exception as e:
        logger.error(f"Failed to initialize Telegram bot: {e}")
        console.print(f"[error]Failed to initialize Telegram bot: {e}[/error]")
        raise

def send_manual_email(tracking_number: str) -> Tuple[bool, str]:
    from app import Shipment
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            return False, f"Shipment `{tracking_number}` not found."
        if not shipment.get('recipient_email') or not shipment.get('email_notifications'):
            return False, f"Email notifications are disabled or no recipient email for `{tracking_number}`."
        subject = f"Shipment Update: {tracking_number}"
        plain_body = (
            f"Tracking Number: {tracking_number}\n"
            f"Status: {shipment['status']}\n"
            f"Delivery Location: {shipment['delivery_location']}\n"
            f"Checkpoints: {shipment.get('checkpoints', 'None')}\n"
            f"Last Updated: {shipment['last_updated']}"
        )
        html_body = (
            f"<h3>Shipment Update: {tracking_number}</h3>"
            f"<p><strong>Status:</strong> {shipment['status']}</p>"
            f"<p><strong>Delivery Location:</strong> {shipment['delivery_location']}</p>"
            f"<p><strong>Checkpoints:</strong> {shipment.get('checkpoints', 'None')}</p>"
            f"<p><strong>Last Updated:</strong> {shipment['last_updated']}</p>"
        )
        success = send_email_notification(shipment['recipient_email'], subject, plain_body, html_body)
        if success:
            logger.info(f"Enqueued manual email notification for {tracking_number}")
            console.print(f"[info]Enqueued manual email notification for {tracking_number}[/info]")
            return True, f"Email notification enqueued for `{tracking_number}`."
        else:
            logger.error(f"Failed to enqueue manual email notification for {tracking_number}")
            console.print(f"[error]Failed to enqueue manual email notification for {tracking_number}[/error]")
            return False, f"Failed to enqueue email notification for `{tracking_number}`."
    except Exception as e:
        logger.error(f"Error sending manual email for {tracking_number}: {e}")
        console.print(f"[error]Error sending manual email for {tracking_number}: {e}[/error]")
        return False, f"Error: {e}"

def send_email_notification(recipient_email: str, subject: str, plain_body: str, html_body: Optional[str] = None) -> bool:
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
        except Exception as e:
            logger.error(f"Error sending email to {recipient_email}: {e}")
            console.print(f"[error]Error sending email to {recipient_email}: {e}[/error]")
            if conn:
                smtp_pool.release_connection(conn)
            return False

    for attempt in range(max_retries):
        with eventlet.Timeout(10, False):
            if send_email():
                return True
            if attempt < max_retries - 1:
                eventlet.sleep(retry_delay * (2 ** attempt))
    return False

def send_webhook_notification(webhook_url: str, tracking_number: str, data: Dict[str, Any]) -> bool:
    max_retries = 3
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            response = requests.post(webhook_url, json=data, headers={"Content-Type": "application/json"}, timeout=10)
            response.raise_for_status()
            logger.info(f"Webhook sent to {webhook_url}")
            console.print(f"[info]Webhook sent to {webhook_url}[/info]")
            return True
        except Exception as e:
            logger.error(f"Webhook failed: {e}")
            console.print(f"[error]Webhook failed: {e}[/error]")
            if attempt < max_retries - 1:
                eventlet.sleep(retry_delay * (2 ** attempt))
    return False

def send_websocket_notification(tracking_number: str, data: Dict[str, Any]) -> bool:
    config = get_config()
    try:
        from flask_socketio import emit
        emit('shipment_update', data, namespace='/', broadcast=True)
        logger.info(f"WebSocket broadcast for {tracking_number}")
        console.print(f"[info]WebSocket broadcast for {tracking_number}[/info]")
        return True
    except Exception as e:
        logger.debug(f"Direct emit failed, falling back to HTTP: {e}")
        try:
            notify_url = f"{config.websocket_server}/notify"
            response = requests.post(notify_url, json=data, timeout=10)
            response.raise_for_status()
            return True
        except Exception as e2:
            logger.error(f"WebSocket fallback failed: {e2}")
            console.print(f"[error]WebSocket fallback failed: {e2}[/error]")
            return False

def enqueue_notification(notification_data: Dict[str, Any]) -> bool:
    if not redis_client:
        return False
    try:
        notification = {
            "tracking_number": notification_data["tracking_number"],
            "type": notification_data["type"],
            "data": notification_data["data"],
            "timestamp": datetime.utcnow().isoformat()
        }
        safe_redis_operation(redis_client.lpush, "notifications", json.dumps(notification))
        logger.info(f"Enqueued {notification_data['type']} notification")
        return True
    except Exception as e:
        logger.error(f"Failed to enqueue notification: {e}")
        return False

def get_shipment_list(page: int = 1, per_page: int = 8) -> Tuple[List[str], int]:
    """Retrieve a paginated list of shipment tracking numbers."""
    from app import Shipment
    try:
        shipments = Shipment.query.order_by(Shipment.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()
        total = Shipment.query.count()
        return [s.tracking_number for s in shipments], total
    except Exception as e:
        logger.error(f"Error retrieving shipment list: {e}")
        console.print(f"[error]Error retrieving shipment list: {e}[/error]")
        return [], 0

# ──────────────────────────────────────────────────────────────
#  TELEGRAM ADMIN COMMAND HANDLERS
# ──────────────────────────────────────────────────────────────

def register_bot_handlers(bot: TeleBot):
    """Register all admin-only Telegram bot commands."""
    from app import Shipment, db, broadcast_update, invalidate_cache, simulate_tracking
    from telegram import InlineKeyboardMarkup, InlineKeyboardButton
    import re

    # ------------------------------------------------------------------
    #  /start /help
    # ------------------------------------------------------------------
    @bot.message_handler(commands=['start', 'help'])
    def cmd_help(message):
        if not is_admin(message.from_user.id):
            bot.reply_to(message, "Unauthorized.")
            return
        txt = (
            "*Shipment Admin Bot*\n\n"
            "/list `[page]` – List shipments\n"
            "/status `<TRK…>` – View details\n"
            "/pause `<TRK…>` – Pause simulation\n"
            "/resume `<TRK…>` – Resume simulation\n"
            "/speed `<TRK…>` `<0.1-10.0>` – Set speed\n"
            "/delete `<TRK…>` – Delete shipment\n"
            "/stats – System stats\n"
            "/menu – Interactive admin menu\n"
            "/help – Show this message"
        )
        bot.reply_to(message, txt, parse_mode='Markdown')
        logger.info(f"/help from user {message.from_user.id}")

    # ------------------------------------------------------------------
    #  /list [page]
    # ------------------------------------------------------------------
    @bot.message_handler(commands=['list'])
    def cmd_list(message):
        if not is_admin(message.from_user.id):
            bot.reply_to(message, "Unauthorized.")
            return
        page = 1
        args = message.text.split()
        if len(args) > 1 and args[1].isdigit():
            page = max(1, int(args[1]))
        tracking_numbers, total = get_shipment_list(page=page, per_page=8)
        if not tracking_numbers:
            bot.reply_to(message, "No shipments found.")
            return
        lines = [f"*Page {page} / {((total - 1) // 8) + 1}*"]
        for tn in tracking_numbers:
            sh = get_shipment_details(tn)
            paused = " (paused)" if redis_client and redis_client.hget("paused_simulations", tn) == "true" else ""
            lines.append(f"• `{tn}` – {sh['status']}{paused}")
        if page * 8 < total:
            lines.append(f"\nUse `/list {page + 1}` for next page.")
        bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')
        logger.info(f"/list page {page} by {message.from_user.id}")

    # ------------------------------------------------------------------
    #  /status <TRK…>
    # ------------------------------------------------------------------
    @bot.message_handler(commands=['status'])
    def cmd_status(message):
        if not is_admin(message.from_user.id):
            bot.reply_to(message, "Unauthorized.")
            return
        try:
            tn = re.split(r'\s+', message.text, 1)[1].strip()
        except IndexError:
            bot.reply_to(message, "Usage: /status `<TRK…>`")
            return
        sanitized = sanitize_tracking_number(tn)
        if not sanitized:
            bot.reply_to(message, "Invalid tracking number.")
            return
        details = get_shipment_details(sanitized)
        if not details:
            bot.reply_to(message, f"`{sanitized}` not found.")
            return
        paused = " (paused)" if redis_client and redis_client.hget("paused_simulations", sanitized) == "true" else ""
        speed = float(redis_client.hget("sim_speed_multipliers", sanitized) or 1.0) if redis_client else 1.0
        chk = (details.get('checkpoints') or '').split(';')
        last5 = "\n".join([f"• {c}" for c in chk[-5:]]) if chk else "_none_"
        txt = (
            f"*Shipment {sanitized}*{paused}\n"
            f"Status: `{details['status']}`\n"
            f"Speed: `{speed:.1f}x`\n"
            f"Delivery: `{details['delivery_location']}`\n"
            f"Email: `{details.get('recipient_email') or '-'}`\n"
            f"Last 5 checkpoints:\n{last5}"
        )
        bot.reply_to(message, txt, parse_mode='Markdown')
        logger.info(f"/status {sanitized} by {message.from_user.id}")

    # ------------------------------------------------------------------
    #  /pause <TRK…>
    # ------------------------------------------------------------------
    @bot.message_handler(commands=['pause'])
    def cmd_pause(message):
        if not is_admin(message.from_user.id):
            bot.reply_to(message, "Unauthorized.")
           毕
            return
        try:
            tn = re.split(r'\s+', message.text, 1)[1].strip()
        except IndexError:
            bot.reply_to(message, "Usage: /pause `<TRK…>`")
            return
        sanitized = sanitize_tracking_number(tn)
        if not sanitized or not redis_client:
            bot.reply_to(message, "Invalid tracking number or Redis unavailable.")
            return
        redis_client.hset("paused_simulations", sanitized, "true")
        invalidate_cache(sanitized)
        broadcast_update(sanitized)
        bot.reply_to(message, f"`{sanitized}` paused.")
        logger.info(f"/pause {sanitized} by {message.from_user.id}")

    # ------------------------------------------------------------------
    #  /resume <TRK…>
    # ------------------------------------------------------------------
    @bot.message_handler(commands=['resume'])
    def cmd_resume(message):
        if not is_admin(message.from_user.id):
            bot.reply_to(message, "Unauthorized.")
            return
        try:
            tn = re.split(r'\s+', message.text, 1)[1].strip()
        except IndexError:
            bot.reply_to(message, "Usage: /resume `<TRK…>`")
            return
        sanitized = sanitize_tracking_number(tn)
        if not sanitized or not redis_client:
            bot.reply_to(message, "Invalid tracking number or Redis unavailable.")
            return
        redis_client.hdel("paused_simulations", sanitized)
        invalidate_cache(sanitized)
        eventlet.spawn(simulate_tracking, sanitized)
        bot.reply_to(message, f"`{sanitized}` resumed.")
        logger.info(f"/resume {sanitized} by {message.from_user.id}")

    # ------------------------------------------------------------------
    #  /speed <TRK…> <multiplier>
    # ------------------------------------------------------------------
    @bot.message_handler(commands=['speed'])
    def cmd_speed(message):
        if not is_admin(message.from_user.id):
            bot.reply_to(message, "Unauthorized.")
            return
        parts = message.text.split()
        if len(parts) < 3:
            bot.reply_to(message, "Usage: /speed `<TRK…>` `<0.1-10.0>`")
            return
        tn, mult = parts[1], parts[2]
        sanitized = sanitize_tracking_number(tn)
        if not sanitized or not redis_client:
            bot.reply_to(message, "Invalid tracking number or Redis unavailable.")
            return
        try:
            speed = float(mult)
            if not 0.1 <= speed <= 10.0:
                raise ValueError
        except ValueError:
            bot.reply_to(message, "Speed must be between 0.1 and 10.0")
            return
        redis_client.hset("sim_speed_multipliers", sanitized, str(speed))
        invalidate_cache(sanitized)
        broadcast_update(sanitized)
        bot.reply_to(message, f"`{sanitized}` speed → `{speed:.1f}x`")
        logger.info(f"/speed {sanitized} → {speed} by {message.from_user.id}")

    # ------------------------------------------------------------------
    #  /delete <TRK…>
    # ------------------------------------------------------------------
    @bot.message_handler(commands=['delete'])
    def cmd_delete(message):
        if not is_admin(message.from_user.id):
            bot.reply_to(message, "Unauthorized.")
            return
        try:
            tn = re.split(r'\s+', message.text, 1)[1].strip()
        except IndexError:
            bot.reply_to(message, "Usage: /delete `<TRK…>`")
            return
        sanitized = sanitize_tracking_number(tn)
        if not sanitized:
            bot.reply_to(message, "Invalid tracking number.")
            return
        shipment = Shipment.query.filter_by(tracking_number=sanitized).first()
        if not shipment:
            bot.reply_to(message, f"`{sanitized}` not found.")
            return
        db.session.delete(shipment)
        db.session.commit()
        invalidate_cache(sanitized)
        if redis_client:
            redis_client.hdel("paused_simulations", sanitized)
            redis_client.hdel("sim_speed_multipliers", sanitized)
        bot.reply_to(message, f"`{sanitized}` deleted.")
        logger.info(f"/delete {sanitized} by {message.from_user.id}")

    # ------------------------------------------------------------------
    #  /stats
    # ------------------------------------------------------------------
    @bot.message_handler(commands=['stats'])
    def cmd_stats(message):
        if not is_admin(message.from_user.id):
            bot.reply_to(message, "Unauthorized.")
            return
        total = Shipment.query.count()
        status_counts = db.session.query(Shipment.status, db.func.count(Shipment.id)) \
            .group_by(Shipment.status).all()
        status_txt = "\n".join([f"• {s}: {c}" for s, c in status_counts]) if status_counts else "_none_"
        queue_len = redis_client.llen("notifications") if redis_client else 0
        txt = (
            "*System Stats*\n"
            f"Total shipments: `{total}`\n"
            f"Notification queue: `{queue_len}`\n\n"
            f"*By status*\n{status_txt}"
        )
        bot.reply_to(message, txt, parse_mode='Markdown')
        logger.info(f"/stats by {message.from_user.id}")

    # ------------------------------------------------------------------
    #  /menu – Interactive inline menu
    # ------------------------------------------------------------------
    @bot.message_handler(commands=['menu'])
    def cmd_menu(message):
        if not is_admin(message.from_user.id):
            bot.reply_to(message, "Unauthorized.")
            return
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("List", callback_data="menu_list_1"),
            InlineKeyboardButton("Stats", callback_data="menu_stats"),
            InlineKeyboardButton("Gen ID", callback_data="menu_genid"),
            InlineKeyboardButton("Help", callback_data="menu_help")
        )
        bot.send_message(
            message.chat.id,
            "*Admin Menu* – Choose an action:",
            reply_markup=markup,
            parse_mode='Markdown'
        )
        logger.info(f"/menu sent to {message.from_user.id}")

    # ------------------------------------------------------------------
    #  Inline Callback Handler
    # ------------------------------------------------------------------
    @bot.callback_query_handler(func=lambda call: True)
    def callback_handler(call):
        user_id = call.from_user.id
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "Unauthorized.")
            return

        data = call.data

        if data.startswith("menu_list_"):
            page = int(data.split("_")[-1])
            tns, total = get_shipment_list(page=page, per_page=6)
            lines = [f"*Page {page} / {((total - 1) // 6) + 1}*"]
            for tn in tns:
                sh = get_shipment_details(tn)
                paused = " (paused)" if redis_client and redis_client.hget("paused_simulations", tn) == "true" else ""
                lines.append(f"• `{tn}` – {sh['status']}{paused}")
            markup = InlineKeyboardMarkup()
            if page > 1:
                markup.add(InlineKeyboardButton("Back", callback_data=f"menu_list_{page-1}"))
            if page * 6 < total:
                markup.add(InlineKeyboardButton("Next", callback_data=f"menu_list_{page+1}"))
            bot.edit_message_text(
                "\n".join(lines),
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            bot.answer_callback_query(call.id)

        elif data == "menu_stats":
            cmd_stats(call.message)
            bot.answer_callback_query(call.id)

        elif data == "menu_genid":
            new_id = generate_unique_id()
            bot.edit_message_text(
                f"New Tracking ID:\n`{new_id}`",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='Markdown'
            )
            bot.answer_callback_query(call.id)

        elif data == "menu_help":
            cmd_help(call.message)
            bot.answer_callback_query(call.id)
