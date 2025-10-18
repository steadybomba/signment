import os
import json
import random
import string
import re
import requests
import logging
import signal
from datetime import datetime
from typing import List, Dict, Optional
from pydantic import BaseModel, HttpUrl, ValidationError
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from sqlalchemy.exc import SQLAlchemyError  # Updated import
from rich.console import Console
from rich.panel import Panel
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import eventlet
from upstash_redis import Redis
from tenacity import retry, stop_after_attempt, wait_exponential
from telebot import TeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# Patch eventlet for compatibility
eventlet.monkey_patch()

# Global console
console = Console()

# HTML Email Template
HTML_EMAIL_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Shipment Update</title>
</head>
<body style="font-family: Arial, sans-serif; margin: 0; padding: 0; background-color: #f4f4f4;">
    <table width="100%" cellpadding="0" cellspacing="0" style="max-width: 600px; margin: 20px auto; background-color: #ffffff; border: 1px solid #e0e0e0; border-radius: 8px;">
        <tr>
            <td style="background-color: #007bff; padding: 20px; text-align: center; border-radius: 8px 8px 0 0;">
                <h1 style="color: #ffffff; margin: 0; font-size: 24px;">Shipment Update</h1>
            </td>
        </tr>
        <tr>
            <td style="padding: 20px;">
                <h2 style="color: #333333; font-size: 20px; margin-top: 0;">Tracking Number: {tracking_number}</h2>
                <p style="color: #555555; font-size: 16px; line-height: 1.5;">
                    Dear Customer,<br>
                    Your shipment has been updated. Below are the latest details:
                </p>
                <table width="100%" cellpadding="10" cellspacing="0" style="border-collapse: collapse; margin: 20px 0;">
                    <tr>
                        <td style="font-weight: bold; color: #333333; border-bottom: 1px solid #e0e0e0;">Status</td>
                        <td style="color: #007bff; border-bottom: 1px solid #e0e0e0;">{status}</td>
                    </tr>
                    <tr>
                        <td style="font-weight: bold; color: #333333; border-bottom: 1px solid #e0e0e0;">Delivery Location</td>
                        <td style="color: #555555; border-bottom: 1px solid #e0e0e0;">{delivery_location}</td>
                    </tr>
                </table>
                <h3 style="color: #333333; font-size: 18px; margin-top: 20px;">Checkpoints</h3>
                {checkpoints_html}
                <p style="color: #555555; font-size: 16px; line-height: 1.5;">
                    Track your shipment in real-time at: <a href="{tracking_url}" style="color: #007bff; text-decoration: none;">Track Now</a>
                </p>
            </td>
        </tr>
        <tr>
            <td style="background-color: #f8f9fa; padding: 15px; text-align: center; border-radius: 0 0 8px 8px; font-size: 14px; color: #555555;">
                <p style="margin: 0;">For support, contact us at <a href="mailto:support@example.com" style="color: #007bff; text-decoration: none;">support@example.com</a></p>
                <p style="margin: 5px 0;">Signment | 123 Logistics Lane, Lagos, NG</p>
                <p style="margin: 0;"><a href="{unsubscribe_url}" style="color: #007bff; text-decoration: none;">Unsubscribe</a></p>
            </td>
        </tr>
    </table>
</body>
</html>
"""

# Plain Text Email Template (Fallback)
PLAIN_TEXT_TEMPLATE = """
Shipment Update for {tracking_number}

Dear Customer,

Your shipment has been updated. Below are the latest details:

Tracking Number: {tracking_number}
Status: {status}
Delivery Location: {delivery_location}
Checkpoints:
{checkpoints_text}

Track your shipment: {tracking_url}

For support, contact us at support@example.com
Signment | 123 Logistics Lane, Lagos, NG
Unsubscribe: {unsubscribe_url}
"""

# Configuration model using Pydantic
class BotConfig(BaseModel):
    telegram_bot_token: str
    redis_url: str
    webhook_url: HttpUrl
    websocket_server: HttpUrl
    allowed_admins: List[int]
    valid_statuses: List[str]
    route_templates: Dict[str, List[str]]
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_pass: str
    smtp_from: str

# Load environment variables
load_dotenv()
try:
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        webhook_url=os.getenv("WEBHOOK_URL", "https://signment.onrender.com/telegram/webhook"),
        websocket_server=os.getenv("WEBSOCKET_SERVER", "https://signment.onrender.com"),
        allowed_admins=[int(uid) for uid in os.getenv("ALLOWED_ADMINS", "").split(",") if uid],
        valid_statuses=os.getenv("VALID_STATUSES", "Pending,In_Transit,Out_for_Delivery,Delivered,Returned,Delayed").split(","),
        route_templates=json.loads(os.getenv("ROUTE_TEMPLATES", '''
            {
                "Lagos, NG": ["Lagos, NG", "Abuja, NG", "Port Harcourt, NG", "Kano, NG"],
                "New York, NY": ["New York, NY", "Chicago, IL", "Los Angeles, CA", "Miami, FL"],
                "London, UK": ["London, UK", "Manchester, UK", "Birmingham, UK", "Edinburgh, UK"]
            }
        ''')),
        smtp_host=os.getenv("SMTP_HOST", "smtp.gmail.com"),
        smtp_port=int(os.getenv("SMTP_PORT", 587)),
        smtp_user=os.getenv("SMTP_USER", ""),
        smtp_pass=os.getenv("SMTP_PASS", ""),
        smtp_from=os.getenv("SMTP_FROM", "no-reply@example.com")
    )
except ValidationError as e:
    console.print(Panel(f"[error]Configuration validation failed: {e}[/error]", title="Config Error", border_style="red"))
    raise

# Redis client with connection pooling
redis_client = None
try:
    redis_client = Redis.from_url(config.redis_url, decode_responses=True, max_connections=10)
    redis_client.ping()
    console.print("[info]Redis connection successful[/info]")
except Exception as e:
    console.print(Panel(f"[error]Redis connection failed: {e}[/error]", title="Redis Error", border_style="red"))
    redis_client = None

# Flask app for webhook and health check
flask_app = Flask(__name__)

# Lazy import functions to avoid circular imports
def get_app_modules():
    try:
        from app import (
            db, Shipment, sanitize_tracking_number, validate_email, validate_location,
            validate_webhook_url, console as app_console
        )
        global console
        console = app_console
        return db, Shipment, sanitize_tracking_number, validate_email, validate_location, validate_webhook_url
    except ImportError as e:
        console.print(Panel(f"[error]Failed to import app modules: {e}[/error]", title="Import Error", border_style="red"))
        raise

# Bot instance (lazy init)
_bot_instance = None
def get_bot():
    global _bot_instance
    if _bot_instance is None:
        if not config.telegram_bot_token:
            console.print(Panel("[critical]Missing TELEGRAM_BOT_TOKEN in config[/critical]", title="Config Error", border_style="red"))
            raise ValueError("Missing TELEGRAM_BOT_TOKEN in config")
        try:
            _bot_instance = TeleBot(config.telegram_bot_token)
            console.print("[info]Bot initialized successfully[/info]")
        except Exception as e:
            console.print(Panel(f"[critical]Failed to initialize bot: {e}[/critical]", title="Bot Init Error", border_style="red"))
            raise
    return _bot_instance

# Rate limiting constants
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 10  # commands per window

# Input sanitization
def sanitize_input(text: str) -> str:
    return re.sub(r'[^\w\s-]', '', text.strip())

# Retry decorator for Redis operations
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def safe_redis_operation(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        console.print(Panel(f"[error]Redis operation failed: {e}[/error]", title="Redis Error", border_style="red"))
        raise

def check_bot_status():
    """Check if the bot is running and can communicate with Telegram API."""
    try:
        bot = get_bot()
        bot.get_me()
        return True
    except Exception as e:
        console.print(Panel(f"[error]Bot status check failed: {e}[/error]", title="Telegram Error", border_style="red"))
        return False

def set_webhook():
    """Set the Telegram webhook for receiving updates."""
    try:
        bot = get_bot()
        bot.remove_webhook()
        bot.set_webhook(url=config.webhook_url)
        console.print(f"[info]Webhook set to {config.webhook_url}[/info]")
    except Exception as e:
        console.print(Panel(f"[error]Failed to set webhook: {e}[/error]", title="Telegram Error", border_style="red"))
        raise

# Database indexes
def create_db_indexes():
    try:
        db, _, _, _, _, _ = get_app_modules()
        with db.engine.connect() as conn:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tracking_number ON shipments (tracking_number)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON shipments (status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_delivery_location ON shipments (delivery_location)")
        console.print("[info]Database indexes created successfully[/info]")
    except SQLAlchemyError as e:
        console.print(Panel(f"[error]Failed to create indexes: {e}[/error]", title="DB Error", border_style="red"))

# Utility Functions
def cache_route_templates():
    """Cache predefined route templates in Redis."""
    try:
        with redis_client.pipeline() as pipe:
            safe_redis_operation(pipe.setex, "route_templates", 86400, json.dumps(config.route_templates))
            pipe.execute()
        console.print("[info]Cached route templates in Redis[/info]")
    except Exception as e:
        console.print(Panel(f"[error]Failed to cache route templates: {e}[/error]", title="Redis Error", border_style="red"))

def get_cached_route_templates():
    """Retrieve cached route templates from Redis."""
    try:
        cached = safe_redis_operation(redis_client.get, "route_templates")
        if cached:
            return json.loads(cached)
        console.print("[warning]Route templates not found in Redis, returning default[/warning]")
        return {'Lagos, NG': ['Lagos, NG']}
    except Exception as e:
        console.print(Panel(f"[error]Failed to retrieve route templates: {e}[/error]", title="Redis Error", border_style="red"))
        return {'Lagos, NG': ['Lagos, NG']}

def is_admin(user_id):
    """Check if the user is an admin based on ALLOWED_ADMINS."""
    return user_id in config.allowed_admins

def generate_unique_id():
    """Generate a unique tracking number using timestamp and random string."""
    db, Shipment, _, _, _, _ = get_app_modules()
    attempts = 0
    while attempts < 10:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        random_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        new_id = f"TRK{timestamp}{random_str}"
        if not Shipment.query.filter_by(tracking_number=new_id).first():
            console.print(f"[info]Generated tracking ID: {new_id}[/info]")
            return new_id
        attempts += 1
    raise ValueError("Failed to generate unique ID after 10 attempts")

def get_shipment_list(page=1, per_page=5, status_filter=None, paused_filter=None):
    """Fetch a paginated list of shipment tracking numbers."""
    db, Shipment, _, _, _, _ = get_app_modules()
    try:
        offset = (page - 1) * per_page
        query = Shipment.query.with_entities(Shipment.tracking_number).order_by(Shipment.tracking_number)
        if status_filter:
            query = query.filter(Shipment.status.in_(status_filter))
        if paused_filter:
            query = query.filter(redis_client.hget("paused_simulations", Shipment.tracking_number) == "true")
        shipments = query.offset(offset).limit(per_page).all()
        total = query.count()
        return [s.tracking_number for s in shipments], total
    except SQLAlchemyError as e:
        console.print(Panel(f"[error]Database error fetching shipment list: {e}[/error]", title="Database Error", border_style="red"))
        return [], 0

def search_shipments(query, page=1, per_page=5):
    """Search shipments by tracking number, status, or location."""
    db, Shipment, _, _, _, _ = get_app_modules()
    query = sanitize_input(query).lower()
    try:
        offset = (page - 1) * per_page
        shipments = Shipment.query.filter(
            db.or_(
                Shipment.tracking_number.ilike(f'%{query}%'),
                Shipment.status.ilike(f'%{query}%'),
                Shipment.delivery_location.ilike(f'%{query}%')
            )
        ).with_entities(Shipment.tracking_number).order_by(Shipment.tracking_number).offset(offset).limit(per_page).all()
        total = Shipment.query.filter(
            db.or_(
                Shipment.tracking_number.ilike(f'%{query}%'),
                Shipment.status.ilike(f'%{query}%'),
                Shipment.delivery_location.ilike(f'%{query}%')
            )
        ).count()
        return [s.tracking_number for s in shipments], total
    except SQLAlchemyError as e:
        console.print(Panel(f"[error]Database error searching shipments: {e}[/error]", title="Database Error", border_style="red"))
        return [], 0

def get_shipment_details(tracking_number):
    """Fetch shipment details, using Redis cache."""
    db, Shipment, sanitize_tracking_number, _, _, _ = get_app_modules()
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        return None
    cached = safe_redis_operation(redis_client.get, f"shipment:{sanitized_tn}") if redis_client else None
    if cached:
        return json.loads(cached)
    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        details = shipment.to_dict() if shipment else None
        if details:
            details['paused'] = safe_redis_operation(redis_client.hget, "paused_simulations", sanitized_tn) == "true" if redis_client else False
            details['speed_multiplier'] = float(safe_redis_operation(redis_client.hget, "sim_speed_multipliers", sanitized_tn) or 1.0) if redis_client else 1.0
            if redis_client:
                with redis_client.pipeline() as pipe:
                    safe_redis_operation(pipe.setex, f"shipment:{sanitized_tn}", 3600, json.dumps(details))
                    safe_redis_operation(pipe.hset, "paused_simulations", sanitized_tn, str(details['paused']))
                    safe_redis_operation(pipe.hset, "sim_speed_multipliers", sanitized_tn, str(details['speed_multiplier']))
                    pipe.execute()
        return details
    except SQLAlchemyError as e:
        console.print(Panel(f"[error]Database error fetching details for {sanitized_tn}: {e}[/error]", title="Database Error", border_style="red"))
        return None

def invalidate_cache(tracking_number):
    """Invalidate Redis cache for a tracking number."""
    db, _, sanitize_tracking_number, _, _, _ = get_app_modules()
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if redis_client and sanitized_tn:
        with redis_client.pipeline() as pipe:
            safe_redis_operation(pipe.delete, f"shipment:{sanitized_tn}")
            safe_redis_operation(pipe.hdel, "paused_simulations", sanitized_tn)
            safe_redis_operation(pipe.hdel, "sim_speed_multipliers", sanitized_tn)
            pipe.execute()

# Notification System
def enqueue_notification(tracking_number: str, notification_type: str, data: Dict) -> None:
    try:
        safe_redis_operation(redis_client.lpush, "notification_queue", json.dumps({
            "tracking_number": tracking_number,
            "type": notification_type,
            "data": data
        }))
        console.print(f"[info]Enqueued {notification_type} notification for {tracking_number}[/info]")
    except Exception as e:
        console.print(Panel(f"[error]Failed to enqueue notification: {e}[/error]", title="Notification Error", border_style="red"))

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def send_webhook_notification(tracking_number, status, checkpoints, delivery_location, webhook_url):
    """Send a webhook notification for a shipment."""
    try:
        payload = {
            'tracking_number': tracking_number,
            'status': status,
            'checkpoints': checkpoints,
            'delivery_location': delivery_location,
            'timestamp': datetime.now().isoformat()
        }
        response = requests.post(webhook_url, json=payload, timeout=5)
        if response.status_code == 200:
            console.print(f"[info]Webhook notification sent for {tracking_number} to {webhook_url}[/info]")
        else:
            console.print(Panel(f"[warning]Webhook failed for {tracking_number}: HTTP {response.status_code}[/warning]", title="Webhook Warning", border_style="yellow"))
    except requests.RequestException as e:
        console.print(Panel(f"[error]Webhook error for {tracking_number}: {e}[/error]", title="Webhook Error", border_style="red"))
        raise

def send_email_notification(recipient: str, tracking_number: str, status: str, delivery_location: str, checkpoints: str):
    """Send an email notification using HTML and plain text templates."""
    max_retries = 3
    retry_delay = 5
    tracking_url = f"{config.websocket_server}/track?tracking_number={tracking_number}"
    unsubscribe_url = f"{config.websocket_server}/unsubscribe?email={recipient}"
    checkpoints_list = checkpoints.split(';') if checkpoints else []
    checkpoints_html = "".join([f"<li style='color: #555555; font-size: 14px; line-height: 1.5;'>{cp}</li>" for cp in checkpoints_list])
    checkpoints_text = "\n".join([f"- {cp}" for cp in checkpoints_list])

    # Format templates
    html_content = HTML_EMAIL_TEMPLATE.format(
        tracking_number=tracking_number,
        status=status,
        delivery_location=delivery_location,
        checkpoints_html=f"<ul style='padding-left: 20px;'>{checkpoints_html}</ul>" if checkpoints_html else "<p>No checkpoints available.</p>",
        tracking_url=tracking_url,
        unsubscribe_url=unsubscribe_url
    )
    text_content = PLAIN_TEXT_TEMPLATE.format(
        tracking_number=tracking_number,
        status=status,
        delivery_location=delivery_location,
        checkpoints_text=checkpoints_text if checkpoints_text else "No checkpoints available.",
        tracking_url=tracking_url,
        unsubscribe_url=unsubscribe_url
    )

    for attempt in range(max_retries):
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = config.smtp_from
            msg['To'] = recipient
            msg['Subject'] = f"Shipment Update for {tracking_number}"
            
            # Attach plain text and HTML parts
            msg.attach(MIMEText(text_content, 'plain'))
            msg.attach(MIMEText(html_content, 'html'))
            
            with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=5) as server:
                server.starttls()
                server.login(config.smtp_user, config.smtp_pass)
                server.send_message(msg)
            console.print(f"[info]Email sent to {recipient} for {tracking_number}[/info]")
            return True
        except smtplib.SMTPException as e:
            console.print(Panel(f"[error]Failed to send email to {recipient} for {tracking_number} (attempt {attempt + 1}): {e}[/error]", title="Email Error", border_style="red"))
            if attempt < max_retries - 1:
                eventlet.sleep(retry_delay * (2 ** attempt))
            continue
        except Exception as e:
            console.print(Panel(f"[error]Unexpected error sending email to {recipient} for {tracking_number}: {e}[/error]", title="Email Error", border_style="red"))
            break
    return False

def process_notification_queue():
    """Process notifications from Redis queue."""
    while True:
        try:
            notification = safe_redis_operation(redis_client.rpop, "notification_queue")
            if not notification:
                eventlet.sleep(1)
                continue
            data = json.loads(notification)
            tracking_number = data["tracking_number"]
            notification_type = data["type"]
            notification_data = data["data"]
            if notification_type == "webhook":
                send_webhook_notification(
                    tracking_number,
                    notification_data["status"],
                    notification_data["checkpoints"],
                    notification_data["delivery_location"],
                    notification_data["webhook_url"]
                )
            elif notification_type == "email":
                if not send_email_notification(
                    recipient=notification_data["recipient_email"],
                    tracking_number=tracking_number,
                    status=notification_data["status"],
                    delivery_location=notification_data["delivery_location"],
                    checkpoints=notification_data["checkpoints"]
                ):
                    # Re-queue on failure
                    safe_redis_operation(redis_client.lpush, "notification_queue", json.dumps(data))
                    console.print(f"[warning]Re-queued failed email notification for {tracking_number}[/warning]")
        except Exception as e:
            console.print(Panel(f"[error]Notification processing failed: {e}[/error]", title="Notification Error", border_style="red"))
            eventlet.sleep(5)

# Start notification queue on module import
def start_notification_queue():
    """Start the notification queue processor."""
    try:
        eventlet.spawn(process_notification_queue)
        console.print("[info]Notification queue processor started[/info]")
    except Exception as e:
        console.print(Panel(f"[error]Failed to start notification queue processor: {e}[/error]", title="Queue Error", border_style="red"))

# Shipment Management
def save_shipment(tracking_number, status, checkpoints, delivery_location, recipient_email='', origin_location=None, webhook_url=None, email_notifications=True):
    """Save or update a shipment in the database and update Redis cache."""
    db, Shipment, sanitize_tracking_number, validate_email, validate_location, validate_webhook_url = get_app_modules()
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        raise ValueError("Invalid tracking number")
    if status not in config.valid_statuses:
        raise ValueError(f"Invalid status. Must be one of: {', '.join(config.valid_statuses)}")
    if not validate_location(delivery_location):
        raise ValueError(f"Invalid delivery location. Must be one of: {', '.join(get_cached_route_templates().keys())}")
    if origin_location and not validate_location(origin_location):
        raise ValueError(f"Invalid origin location. Must be one of: {', '.join(get_cached_route_templates().keys())}")
    if recipient_email and not validate_email(recipient_email):
        raise ValueError("Invalid recipient email")
    if webhook_url and not validate_webhook_url(webhook_url):
        raise ValueError("Invalid webhook URL")
    
    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        last_updated = datetime.now()
        origin_location = origin_location or delivery_location
        webhook_url = webhook_url or None
        checkpoints = checkpoints or ''
        if shipment:
            shipment.status = status
            shipment.checkpoints = checkpoints
            shipment.delivery_location = delivery_location
            shipment.last_updated = last_updated
            shipment.recipient_email = recipient_email
            shipment.origin_location = origin_location
            shipment.webhook_url = webhook_url
            shipment.email_notifications = email_notifications
        else:
            shipment = Shipment(
                tracking_number=sanitized_tn,
                status=status,
                checkpoints=checkpoints,
                delivery_location=delivery_location,
                last_updated=last_updated,
                recipient_email=recipient_email,
                created_at=last_updated,
                origin_location=origin_location,
                webhook_url=webhook_url,
                email_notifications=email_notifications
            )
            db.session.add(shipment)
        db.session.commit()
        details = shipment.to_dict()
        details['paused'] = safe_redis_operation(redis_client.hget, "paused_simulations", sanitized_tn) == "true" if redis_client else False
        details['speed_multiplier'] = float(safe_redis_operation(redis_client.hget, "sim_speed_multipliers", sanitized_tn) or 1.0) if redis_client else 1.0
        if redis_client:
            with redis_client.pipeline() as pipe:
                safe_redis_operation(pipe.setex, f"shipment:{sanitized_tn}", 3600, json.dumps(details))
                safe_redis_operation(pipe.hset, "paused_simulations", sanitized_tn, str(details['paused']))
                safe_redis_operation(pipe.hset, "sim_speed_multipliers", sanitized_tn, str(details['speed_multiplier']))
                pipe.execute()
        console.print(f"[info]Saved shipment {sanitized_tn}: {status}[/info]")
        if recipient_email and email_notifications:
            enqueue_notification(sanitized_tn, "email", {
                "status": status,
                "checkpoints": checkpoints,
                "delivery_location": delivery_location,
                "recipient_email": recipient_email
            })
        if webhook_url:
            enqueue_notification(sanitized_tn, "webhook", {
                "status": status,
                "checkpoints": checkpoints,
                "delivery_location": delivery_location,
                "webhook_url": webhook_url
            })
    except SQLAlchemyError as e:
        db.session.rollback()
        console.print(Panel(f"[error]Database error saving {sanitized_tn}: {e}[/error]", title="Database Error", border_style="red"))
        raise

def send_dynamic_menu(chat_id, message_id=None, page=1, per_page=5):
    """Send a dynamic menu with admin actions for shipments."""
    bot = get_bot()
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("Generate ID", callback_data="generate_id"),
        InlineKeyboardButton("Add Shipment", callback_data="add")
    )
    shipments, total = get_shipment_list(page, per_page)
    if shipments:
        action_buttons = [
            ("View Shipment", f"view_menu_{page}"),
            ("Update Shipment", f"update_menu_{page}"),
            ("Delete Shipment", f"delete_menu_{page}"),
            ("Batch Delete", f"batch_delete_menu_{page}"),
            ("Trigger Broadcast", f"broadcast_menu_{page}"),
            ("Toggle Email", f"toggle_email_menu_{page}"),
            ("Pause Simulation", f"pause_menu_{page}"),
            ("Resume Simulation", f"resume_menu_{page}"),
            ("Set Sim Speed", f"setspeed_menu_{page}"),
            ("View Sim Speed", f"getspeed_menu_{page}"),
            ("Bulk Actions", f"bulk_action_menu_{page}"),
            ("Set Webhook", f"set_webhook_menu_{page}"),
            ("Test Webhook", f"test_webhook_menu_{page}")
        ]
        for text, data in action_buttons:
            markup.add(InlineKeyboardButton(text, callback_data=data))
        if total > per_page:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("Previous", callback_data=f"menu_page_{page-1}"))
            if page * per_page < total:
                nav_buttons.append(InlineKeyboardButton("Next", callback_data=f"menu_page_{page+1}"))
            markup.add(*nav_buttons)
        markup.add(InlineKeyboardButton("List Shipments", callback_data=f"list_{page}"))
    markup.add(
        InlineKeyboardButton("Settings", callback_data="settings"),
        InlineKeyboardButton("Help", callback_data="help"),
        InlineKeyboardButton("Home", callback_data="menu_page_1")
    )
    try:
        message_text = f"*Choose an action (Page {page})*\nAvailable shipments: {total}"
        if message_id:
            bot.edit_message_text(message_text, chat_id=chat_id, message_id=message_id, reply_markup=markup, parse_mode='Markdown')
        else:
            bot.send_message(chat_id, message_text, reply_markup=markup, parse_mode='Markdown')
        console.print(f"[info]Sent dynamic menu to chat {chat_id}, page {page}[/info]")
    except Exception as e:
        console.print(Panel(f"[error]Telegram API error sending menu to {chat_id}: {e}[/error]", title="Telegram Error", border_style="red"))

def show_shipment_details(call, tracking_number):
    """Display shipment details with interactive controls."""
    bot = get_bot()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        response = (
            f"*Shipment*: `{tracking_number}`\n"
            f"*Status*: `{shipment['status']}`\n"
            f"*Paused*: `{shipment.get('paused', False)}`\n"
            f"*Speed Multiplier*: `{shipment.get('speed_multiplier', 1.0)}x`\n"
            f"*Delivery Location*: `{shipment['delivery_location']}`\n"
            f"*Origin Location*: `{shipment.get('origin_location', 'None')}`\n"
            f"*Recipient Email*: `{shipment.get('recipient_email', 'None')}`\n"
            f"*Checkpoints*: `{shipment.get('checkpoints', 'None')}`\n"
            f"*Webhook URL*: `{shipment.get('webhook_url', 'Default')}`\n"
            f"*Email Notifications*: `{'Enabled' if shipment.get('email_notifications', False) else 'Disabled'}`\n"
            f"*Last Updated*: `{shipment.get('last_updated', 'N/A')}`"
        )
        markup = InlineKeyboardMarkup(row_width=2)
        if shipment['status'] not in ['Delivered', 'Returned']:
            is_paused = shipment.get('paused', False)
            markup.add(
                InlineKeyboardButton("Pause" if not is_paused else "Resume", callback_data=f"{'pause' if not is_paused else 'resume'}_{tracking_number}"),
                InlineKeyboardButton("Set Speed", callback_data=f"setspeed_{tracking_number}")
            )
        markup.add(
            InlineKeyboardButton("Broadcast", callback_data=f"broadcast_{tracking_number}"),
            InlineKeyboardButton("Notify", callback_data=f"notify_{tracking_number}"),
            InlineKeyboardButton("Set Webhook", callback_data=f"set_webhook_{tracking_number}"),
            InlineKeyboardButton("Test Webhook", callback_data=f"test_webhook_{tracking_number}"),
            InlineKeyboardButton("Home", callback_data="menu_page_1")
        )
        bot.edit_message_text(response, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
        console.print(f"[info]Sent shipment details for {tracking_number} to admin {call.from_user.id}[/info]")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        console.print(Panel(f"[error]Error in show shipment details for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def delete_shipment(call, tracking_number, page):
    """Delete a shipment and invalidate its cache."""
    bot = get_bot()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        bot.answer_callback_query(call.id, f"Confirm deletion of `{tracking_number}`?", show_alert=True)
        bot.register_next_step_handler(call.message, lambda msg: confirm_delete_shipment(msg, tracking_number, page))
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        console.print(Panel(f"[error]Error in delete shipment for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def confirm_delete_shipment(message, tracking_number, page):
    """Confirm and execute shipment deletion."""
    db, Shipment, _, _, _, _ = get_app_modules()
    bot = get_bot()
    if message.text.lower() != "confirm":
        bot.reply_to(message, "Deletion cancelled.")
        send_dynamic_menu(message.chat.id, page=page)
        return
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.")
            return
        db.session.delete(shipment)
        db.session.commit()
        if redis_client:
            invalidate_cache(tracking_number)
        bot.reply_to(message, f"Shipment `{tracking_number}` deleted.")
        console.print(f"[info]Deleted shipment {tracking_number} by admin {message.from_user.id}[/info]")
        send_dynamic_menu(message.chat.id, page=page)
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.reply_to(message, f"Database error: {e}")
        console.print(Panel(f"[error]Database error deleting {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))

def toggle_batch_selection(call, tracking_number):
    """Toggle a shipment's selection for batch deletion."""
    bot = get_bot()
    chat_id = call.message.chat.id
    batch_key = f"batch_delete:{chat_id}"
    selected = safe_redis_operation(redis_client.smembers, batch_key) if redis_client else set()
    selected = set(selected)
    if tracking_number in selected:
        safe_redis_operation(redis_client.srem, batch_key, tracking_number)
        bot.answer_callback_query(call.id, f"Deselected `{tracking_number}`.", show_alert=True)
    else:
        safe_redis_operation(redis_client.sadd, batch_key, tracking_number)
        bot.answer_callback_query(call.id, f"Selected `{tracking_number}`.", show_alert=True)
    console.print(f"[info]Toggled batch selection for {tracking_number} by admin {call.from_user.id}[/info]")
    show_shipment_menu(call, page=1, prefix="batch_select", prompt="Select shipments to delete", extra_buttons=[
        InlineKeyboardButton("Confirm Delete", callback_data=f"batch_delete_confirm_1"),
        InlineKeyboardButton("Home", callback_data="menu_page_1")
    ])

def batch_delete_shipments(call, page):
    """Delete selected shipments in a batch."""
    bot = get_bot()
    chat_id = call.message.chat.id
    batch_key = f"batch_delete:{chat_id}"
    try:
        selected = safe_redis_operation(redis_client.smembers, batch_key) if redis_client else set()
        if not selected:
            bot.answer_callback_query(call.id, "No shipments selected for deletion.", show_alert=True)
            return
        bot.answer_callback_query(call.id, f"Confirm deletion of {len(selected)} shipments?", show_alert=True)
        bot.register_next_step_handler(call.message, lambda msg: confirm_batch_delete(msg, selected, page))
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        console.print(Panel(f"[error]Error in batch delete for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def confirm_batch_delete(message, selected, page):
    """Confirm and execute batch deletion."""
    db, Shipment, _, _, _, _ = get_app_modules()
    bot = get_bot()
    if message.text.lower() != "confirm":
        bot.reply_to(message, "Batch deletion cancelled.")
        send_dynamic_menu(message.chat.id, page=page)
        return
    try:
        deleted_count = 0
        for tracking_number in selected:
            shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
            if shipment:
                db.session.delete(shipment)
                if redis_client:
                    invalidate_cache(tracking_number)
                deleted_count += 1
        db.session.commit()
        if redis_client:
            safe_redis_operation(redis_client.delete, f"batch_delete:{message.chat.id}")
        bot.reply_to(message, f"Deleted {deleted_count} shipments.")
        console.print(f"[info]Batch deleted {deleted_count} shipments by admin {message.from_user.id}[/info]")
        send_dynamic_menu(message.chat.id, page=page)
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.reply_to(message, f"Database error: {e}")
        console.print(Panel(f"[error]Database error in batch delete: {e}[/error]", title="Database Error", border_style="red"))

def trigger_broadcast(call, tracking_number):
    """Trigger a broadcast for a shipment."""
    bot = get_bot()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        response = requests.get(f'{config.websocket_server}/broadcast/{tracking_number}', timeout=5)
        if response.status_code == 204:
            bot.answer_callback_query(call.id, f"Broadcast triggered for `{tracking_number}`.", show_alert=True)
            console.print(f"[info]Broadcast triggered for {tracking_number} by admin {call.from_user.id}[/info]")
        else:
            bot.answer_callback_query(call.id, f"Broadcast failed: HTTP {response.status_code}", show_alert=True)
    except requests.RequestException as e:
        bot.answer_callback_query(call.id, f"Broadcast error: {e}", show_alert=True)
        console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))

def toggle_email_notifications(call, tracking_number, page):
    """Toggle email notifications for a shipment."""
    bot = get_bot()
    db, Shipment, _, _, _, _ = get_app_modules()
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        shipment.email_notifications = not shipment.email_notifications
        db.session.commit()
        invalidate_cache(tracking_number)
        status = "enabled" if shipment.email_notifications else "disabled"
        bot.answer_callback_query(call.id, f"Email notifications {status} for `{tracking_number}`.", show_alert=True)
        console.print(f"[info]Email notifications {status} for {tracking_number} by admin {call.from_user.id}[/info]")
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.answer_callback_query(call.id, f"Database error: {e}", show_alert=True)
        console.print(Panel(f"[error]Database error toggling email for {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))

def pause_simulation_callback(call, tracking_number, page):
    """Pause a shipment's simulation via callback."""
    bot = get_bot()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        if shipment['status'] in ['Delivered', 'Returned']:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` is already completed (`{shipment['status']}`).", show_alert=True)
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true":
            bot.answer_callback_query(call.id, f"Simulation for `{tracking_number}` is already paused.", show_alert=True)
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
        invalidate_cache(tracking_number)
        console.print(Panel(f"[info]Paused simulation for {tracking_number} by admin {call.from_user.id}[/info]", title="Simulation Paused", border_style="green"))
        bot.answer_callback_query(call.id, f"Simulation paused for `{tracking_number}`.", show_alert=True)
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        console.print(Panel(f"[error]Error pausing simulation for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def resume_simulation_callback(call, tracking_number, page):
    """Resume a shipment's simulation via callback."""
    bot = get_bot()
    try:
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) != "true":
            bot.answer_callback_query(call.id, f"Simulation for `{tracking_number}` is not paused.", show_alert=True)
            return
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        if shipment['status'] in ['Delivered', 'Returned']:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` is already completed (`{shipment['status']}`).", show_alert=True)
            return
        if redis_client:
            safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
        invalidate_cache(tracking_number)
        console.print(Panel(f"[info]Resumed simulation for {tracking_number} by admin {call.from_user.id}[/info]", title="Simulation Resumed", border_style="green"))
        bot.answer_callback_query(call.id, f"Simulation resumed for `{tracking_number}`.", show_alert=True)
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        console.print(Panel(f"[error]Error resuming simulation for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def show_simulation_speed(call, tracking_number):
    """Show the simulation speed for a shipment."""
    bot = get_bot()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        speed = float(safe_redis_operation(redis_client.hget, "sim_speed_multipliers", tracking_number) or 1.0) if redis_client else 1.0
        bot.answer_callback_query(call.id, f"Simulation speed for `{tracking_number}` is `{speed}x`.", show_alert=True)
        console.print(Panel(f"[info]Retrieved simulation speed for {tracking_number}: {speed}x by admin {call.from_user.id}[/info]", title="Speed Retrieved", border_style="green"))
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        console.print(Panel(f"[error]Error retrieving simulation speed for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def bulk_pause_shipments(call, page):
    """Pause simulations for selected shipments."""
    bot = get_bot()
    chat_id = call.message.chat.id
    batch_key = f"bulk_pause:{chat_id}"
    try:
        selected = safe_redis_operation(redis_client.smembers, batch_key) if redis_client else set()
        if not selected:
            bot.answer_callback_query(call.id, "No shipments selected for pausing.", show_alert=True)
            return
        paused_count = 0
        for tracking_number in selected:
            shipment = get_shipment_details(tracking_number)
            if shipment and shipment['status'] not in ['Delivered', 'Returned'] and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) != "true":
                safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
                invalidate_cache(tracking_number)
                paused_count += 1
        safe_redis_operation(redis_client.delete, batch_key)
        bot.answer_callback_query(call.id, f"Paused {paused_count} simulations.", show_alert=True)
        console.print(f"[info]Paused {paused_count} simulations by admin {call.from_user.id}[/info]")
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        console.print(Panel(f"[error]Error in bulk pause for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def bulk_resume_shipments(call, page):
    """Resume simulations for selected shipments."""
    bot = get_bot()
    chat_id = call.message.chat.id
    batch_key = f"bulk_resume:{chat_id}"
    try:
        selected = safe_redis_operation(redis_client.smembers, batch_key) if redis_client else set()
        if not selected:
            bot.answer_callback_query(call.id, "No shipments selected for resuming.", show_alert=True)
            return
        resumed_count = 0
        for tracking_number in selected:
            shipment = get_shipment_details(tracking_number)
            if shipment and shipment['status'] not in ['Delivered', 'Returned'] and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true":
                safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
                invalidate_cache(tracking_number)
                resumed_count += 1
        safe_redis_operation(redis_client.delete, batch_key)
        bot.answer_callback_query(call.id, f"Resumed {resumed_count} simulations.", show_alert=True)
        console.print(f"[info]Resumed {resumed_count} simulations by admin {call.from_user.id}[/info]")
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        console.print(Panel(f"[error]Error in bulk resume for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def send_manual_email(call, tracking_number):
    """Send a manual email notification for a shipment."""
    bot = get_bot()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        if not shipment.get('recipient_email') or not shipment.get('email_notifications'):
            bot.answer_callback_query(call.id, f"Email notifications disabled or no recipient email for `{tracking_number}`.", show_alert=True)
            return
        enqueue_notification(tracking_number, "email", {
            "status": shipment['status'],
            "checkpoints": shipment['checkpoints'],
            "delivery_location": shipment['delivery_location'],
            "recipient_email": shipment['recipient_email']
        })
        bot.answer_callback_query(call.id, f"Email notification queued for `{tracking_number}`.", show_alert=True)
        console.print(f"[info]Manual email queued for {tracking_number} by admin {call.from_user.id}[/info]")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        console.print(Panel(f"[error]Error sending manual email for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def send_manual_webhook(call, tracking_number):
    """Send a manual webhook notification for a shipment."""
    bot = get_bot()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        webhook_url = shipment.get('webhook_url') or config.websocket_server
        enqueue_notification(tracking_number, "webhook", {
            "status": shipment['status'],
            "checkpoints": shipment['checkpoints'],
            "delivery_location": shipment['delivery_location'],
            "webhook_url": webhook_url
        })
        bot.answer_callback_query(call.id, f"Webhook notification queued for `{tracking_number}`.", show_alert=True)
        console.print(f"[info]Manual webhook queued for {tracking_number} by admin {call.from_user.id}[/info]")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        console.print(Panel(f"[error]Error sending manual webhook for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

# Start notification queue on module import
start_notification_queue()
