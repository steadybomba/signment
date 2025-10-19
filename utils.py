import os
import re
import json
try:
    from upstash_redis import Redis
except ImportError:
    Redis = None
import uuid
import logging
import smtplib
import requests
from email.mime.text import MIMEText
from dataclasses import dataclass
from typing import Optional, Tuple, List
from urllib.parse import urlparse
from rich.console import Console
from rich.panel import Panel
from sqlalchemy.exc import SQLAlchemyError
from telebot import TeleBot
from flask_sqlalchemy import SQLAlchemy
from flask import current_app

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
            token=os.getenv("UPSTASH_REDIS_TOKEN", "")
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

def get_bot():
    """Initialize and return the Telegram bot instance."""
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        redis_url=os.getenv("REDIS_URL"),
        redis_token=os.getenv("UPSTASH_REDIS_TOKEN", ""),
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
    return db, Shipment, sanitize_tracking_number, validate_email, validate_location, validate_webhook_url, get_shipment_list

def is_admin(user_id: int) -> bool:
    """Check if the user is an admin."""
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        redis_url=os.getenv("REDIS_URL"),
        redis_token=os.getenv("UPSTASH_REDIS_TOKEN", ""),
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

def get_shipment_list(page: int = 1) -> Tuple[List[str], int]:
    """Get a paginated list of shipment tracking numbers."""
    db, Shipment, _, _, _, _, _ = get_app_modules()
    per_page = 5
    try:
        shipments = Shipment.query.order_by(Shipment.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()
        total = Shipment.query.count()
        return [s.tracking_number for s in shipments], total
    except SQLAlchemyError as e:
        logger.error(f"Database error fetching shipment list: {e}")
        console.print(Panel(f"[error]Database error fetching shipment list: {e}[/error]", title="Database Error", border_style="red"))
        return [], 0

def get_shipment_details(tracking_number: str) -> Optional[dict]:
    """Get shipment details with caching."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
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
    except SQLAlchemyError as e:
        logger.error(f"Database error fetching shipment {tracking_number}: {e}")
        console.print(Panel(f"[error]Database error fetching shipment {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
        return None

def save_shipment(tracking_number: str, status: str, checkpoints: str, delivery_location: str,
                 recipient_email: str = '', origin_location: str = None, webhook_url: str = None) -> bool:
    """Save a shipment to the database."""
    db, Shipment, sanitize_tracking_number, validate_email, validate_location, validate_webhook_url, _ = get_app_modules()
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        redis_url=os.getenv("REDIS_URL"),
        redis_token=os.getenv("UPSTASH_REDIS_TOKEN", ""),
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
        else:
            shipment = Shipment(
                tracking_number=tracking_number,
                status=status,
                checkpoints=checkpoints,
                delivery_location=delivery_location,
                recipient_email=recipient_email,
                origin_location=origin_location,
                webhook_url=webhook_url
            )
            db.session.add(shipment)
        db.session.commit()
        return True
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error saving shipment {tracking_number}: {e}")
        console.print(Panel(f"[error]Database error saving shipment {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
        return False

def delete_shipment(call, tracking_number: str, page: int):
    """Delete a shipment and update the menu."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    tracking_number = sanitize_tracking_number(tracking_number)
    if not tracking_number:
        bot.answer_callback_query(call.id, "Invalid tracking number.", show_alert=True)
        return
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.", show_alert=True)
            return
        db.session.delete(shipment)
        db.session.commit()
        if redis_client:
            safe_redis_operation(redis_client.delete, f"shipment:{tracking_number}")
            safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
            safe_redis_operation(redis_client.hdel, "sim_speed_multipliers", tracking_number)
        bot.edit_message_text(f"Shipment {tracking_number} deleted.", chat_id=call.message.chat.id,
                             message_id=call.message.message_id)
        logger.info(f"Deleted shipment {tracking_number}")
        console.print(f"[info]Deleted shipment {tracking_number}[/info]")
        show_shipment_menu(call, page, prefix="delete", prompt="Select shipment to delete")
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.answer_callback_query(call.id, f"Database error: {e}", show_alert=True)
        logger.error(f"Database error deleting shipment {tracking_number}: {e}")
        console.print(Panel(f"[error]Database error deleting shipment {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))

def confirm_delete_shipment(call, tracking_number: str, page: int):
    """Confirm deletion of a shipment."""
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    tracking_number = sanitize_tracking_number(tracking_number)
    if not tracking_number:
        bot.answer_callback_query(call.id, "Invalid tracking number.", show_alert=True)
        return
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("Confirm", callback_data=f"delete_{tracking_number}_{page}"),
        InlineKeyboardButton("Cancel", callback_data=f"delete_menu_{page}")
    )
    bot.edit_message_text(f"Confirm deletion of shipment {tracking_number}?", chat_id=call.message.chat.id,
                         message_id=call.message.message_id, reply_markup=markup)

def toggle_batch_selection(call, tracking_number: str):
    """Toggle shipment selection for batch operations."""
    tracking_number = sanitize_tracking_number(tracking_number)
    if not tracking_number:
        bot.answer_callback_query(call.id, "Invalid tracking number.", show_alert=True)
        return
    selected = safe_redis_operation(redis_client.smembers, "batch_selected") or set()
    selected = set(selected) if selected else set()
    if tracking_number in selected:
        safe_redis_operation(redis_client.srem, "batch_selected", tracking_number)
        bot.answer_callback_query(call.id, f"Deselected {tracking_number}")
    else:
        safe_redis_operation(redis_client.sadd, "batch_selected", tracking_number)
        bot.answer_callback_query(call.id, f"Selected {tracking_number}")
    logger.info(f"Toggled batch selection for {tracking_number}")
    console.print(f"[info]Toggled batch selection for {tracking_number}[/info]")

def batch_delete_shipments(call, page: int):
    """Delete selected shipments in batch."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    selected = safe_redis_operation(redis_client.smembers, "batch_selected") or set()
    selected = [sanitize_tracking_number(tn) for tn in selected if sanitize_tracking_number(tn)]
    if not selected:
        bot.edit_message_text("No shipments selected for deletion.", chat_id=call.message.chat.id,
                             message_id=call.message.message_id)
        return
    try:
        for tn in selected:
            shipment = Shipment.query.filter_by(tracking_number=tn).first()
            if shipment:
                db.session.delete(shipment)
                if redis_client:
                    safe_redis_operation(redis_client.delete, f"shipment:{tn}")
                    safe_redis_operation(redis_client.hdel, "paused_simulations", tn)
                    safe_redis_operation(redis_client.hdel, "sim_speed_multipliers", tn)
        db.session.commit()
        safe_redis_operation(redis_client.delete, "batch_selected")
        bot.edit_message_text(f"Deleted {len(selected)} shipments.", chat_id=call.message.chat.id,
                             message_id=call.message.message_id)
        logger.info(f"Batch deleted {len(selected)} shipments")
        console.print(f"[info]Batch deleted {len(selected)} shipments[/info]")
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        show_shipment_menu(call, page, prefix="batch_select", prompt="Select shipments to delete",
                          extra_buttons=[InlineKeyboardButton("Confirm Delete", callback_data=f"batch_delete_confirm_{page}"),
                                        InlineKeyboardButton("Home", callback_data="menu_page_1")])
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.answer_callback_query(call.id, f"Database error: {e}", show_alert=True)
        logger.error(f"Database error during batch delete: {e}")
        console.print(Panel(f"[error]Database error during batch delete: {e}[/error]", title="Database Error", border_style="red"))

def confirm_batch_delete(call, page: int):
    """Confirm batch deletion of selected shipments."""
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    selected = safe_redis_operation(redis_client.smembers, "batch_selected") or set()
    if not selected:
        bot.edit_message_text("No shipments selected.", chat_id=call.message.chat.id,
                             message_id=call.message.message_id)
        return
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("Confirm", callback_data=f"batch_delete_confirm_{page}"),
        InlineKeyboardButton("Cancel", callback_data=f"batch_delete_menu_{page}")
    )
    bot.edit_message_text(f"Confirm deletion of {len(selected)} shipments?", chat_id=call.message.chat.id,
                         message_id=call.message.message_id, reply_markup=markup)

def trigger_broadcast(call, tracking_number: str):
    """Trigger a broadcast for a shipment."""
    tracking_number = sanitize_tracking_number(tracking_number)
    if not tracking_number:
        bot.answer_callback_query(call.id, "Invalid tracking number.", show_alert=True)
        return
    shipment = get_shipment_details(tracking_number)
    if not shipment:
        bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.", show_alert=True)
        return
    try:
        message = f"Shipment Update: {tracking_number} is now {shipment['status']} at {shipment['delivery_location']}"
        for admin_id in current_app.config['ALLOWED_ADMINS']:
            bot.send_message(admin_id, message, parse_mode='Markdown')
        bot.answer_callback_query(call.id, f"Broadcast sent for {tracking_number}")
        logger.info(f"Broadcast triggered for {tracking_number}")
        console.print(f"[info]Broadcast triggered for {tracking_number}[/info]")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error broadcasting: {e}", show_alert=True)
        logger.error(f"Error broadcasting for {tracking_number}: {e}")
        console.print(Panel(f"[error]Error broadcasting for {tracking_number}: {e}[/error]", title="Broadcast Error", border_style="red"))

def toggle_email_notifications(call, tracking_number: str, page: int):
    """Toggle email notifications for a shipment."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    tracking_number = sanitize_tracking_number(tracking_number)
    if not tracking_number:
        bot.answer_callback_query(call.id, "Invalid tracking number.", show_alert=True)
        return
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.", show_alert=True)
            return
        shipment.email_notifications = not shipment.email_notifications
        db.session.commit()
        status = "enabled" if shipment.email_notifications else "disabled"
        bot.answer_callback_query(call.id, f"Email notifications {status} for {tracking_number}")
        logger.info(f"Toggled email notifications to {status} for {tracking_number}")
        console.print(f"[info]Toggled email notifications to {status} for {tracking_number}[/info]")
        show_shipment_menu(call, page, prefix="toggle_email", prompt="Select shipment to toggle email notifications")
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.answer_callback_query(call.id, f"Database error: {e}", show_alert=True)
        logger.error(f"Database error toggling email notifications for {tracking_number}: {e}")
        console.print(Panel(f"[error]Database error toggling email notifications for {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))

def pause_simulation_callback(call, tracking_number: str, page: int):
    """Pause a shipment's simulation via callback."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    tracking_number = sanitize_tracking_number(tracking_number)
    if not tracking_number:
        bot.answer_callback_query(call.id, "Invalid tracking number.", show_alert=True)
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.", show_alert=True)
            return
        if shipment['status'] in ['Delivered', 'Returned']:
            bot.answer_callback_query(call.id, f"Shipment {tracking_number} is already completed.", show_alert=True)
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true":
            bot.answer_callback_query(call.id, f"Simulation for {tracking_number} is already paused.", show_alert=True)
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
        bot.answer_callback_query(call.id, f"Simulation paused for {tracking_number}")
        logger.info(f"Paused simulation for {tracking_number} via callback")
        console.print(f"[info]Paused simulation for {tracking_number} via callback[/info]")
        show_shipment_menu(call, page, prefix="pause", prompt="Select shipment to pause")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        logger.error(f"Error pausing simulation for {tracking_number}: {e}")
        console.print(Panel(f"[error]Error pausing simulation for {tracking_number}: {e}[/error]", title="Pause Error", border_style="red"))

def resume_simulation_callback(call, tracking_number: str, page: int):
    """Resume a shipment's simulation via callback."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    tracking_number = sanitize_tracking_number(tracking_number)
    if not tracking_number:
        bot.answer_callback_query(call.id, "Invalid tracking number.", show_alert=True)
        return
    try:
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) != "true":
            bot.answer_callback_query(call.id, f"Simulation for {tracking_number} is not paused.", show_alert=True)
            return
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.", show_alert=True)
            return
        if shipment['status'] in ['Delivered', 'Returned']:
            bot.answer_callback_query(call.id, f"Shipment {tracking_number} is already completed.", show_alert=True)
            return
        if redis_client:
            safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
        bot.answer_callback_query(call.id, f"Simulation resumed for {tracking_number}")
        logger.info(f"Resumed simulation for {tracking_number} via callback")
        console.print(f"[info]Resumed simulation for {tracking_number} via callback[/info]")
        show_shipment_menu(call, page, prefix="resume", prompt="Select shipment to resume")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        logger.error(f"Error resuming simulation for {tracking_number}: {e}")
        console.print(Panel(f"[error]Error resuming simulation for {tracking_number}: {e}[/error]", title="Resume Error", border_style="red"))

def show_simulation_speed(call, tracking_number: str):
    """Show simulation speed for a shipment."""
    tracking_number = sanitize_tracking_number(tracking_number)
    if not tracking_number:
        bot.answer_callback_query(call.id, "Invalid tracking number.", show_alert=True)
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.", show_alert=True)
            return
        speed = shipment.get('speed_multiplier', 1.0)
        bot.edit_message_text(f"Simulation speed for {tracking_number}: {speed}x", chat_id=call.message.chat.id,
                             message_id=call.message.message_id)
        logger.info(f"Displayed simulation speed for {tracking_number}: {speed}x")
        console.print(f"[info]Displayed simulation speed for {tracking_number}: {speed}x[/info]")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        logger.error(f"Error displaying simulation speed for {tracking_number}: {e}")
        console.print(Panel(f"[error]Error displaying simulation speed for {tracking_number}: {e}[/error]", title="Speed Error", border_style="red"))

def send_manual_email(call, tracking_number: str):
    """Send a manual email notification."""
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        redis_url=os.getenv("REDIS_URL"),
        redis_token=os.getenv("UPSTASH_REDIS_TOKEN", ""),
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
        bot.answer_callback_query(call.id, "Invalid tracking number.", show_alert=True)
        return
    shipment = get_shipment_details(tracking_number)
    if not shipment:
        bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.", show_alert=True)
        return
    if not shipment.get('recipient_email') or not shipment.get('email_notifications'):
        bot.answer_callback_query(call.id, "Email notifications are disabled or no recipient email set.", show_alert=True)
        return
    try:
        msg = MIMEText(f"Shipment Update: {tracking_number} is now {shipment['status']} at {shipment['delivery_location']}")
        msg['Subject'] = f"Shipment Update: {tracking_number}"
        msg['From'] = config.smtp_from
        msg['To'] = shipment['recipient_email']
        with smtplib.SMTP(config.smtp_host, config.smtp_port) as server:
            server.starttls()
            server.login(config.smtp_user, config.smtp_pass)
            server.send_message(msg)
        bot.answer_callback_query(call.id, f"Email sent for {tracking_number}")
        logger.info(f"Sent manual email for {tracking_number}")
        console.print(f"[info]Sent manual email for {tracking_number}[/info]")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error sending email: {e}", show_alert=True)
        logger.error(f"Error sending manual email for {tracking_number}: {e}")
        console.print(Panel(f"[error]Error sending manual email for {tracking_number}: {e}[/error]", title="Email Error", border_style="red"))

def send_manual_webhook(call, tracking_number: str):
    """Send a manual webhook notification."""
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        redis_url=os.getenv("REDIS_URL"),
        redis_token=os.getenv("UPSTASH_REDIS_TOKEN", ""),
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
        bot.answer_callback_query(call.id, "Invalid tracking number.", show_alert=True)
        return
    shipment = get_shipment_details(tracking_number)
    if not shipment:
        bot.answer_callback_query(call.id, f"Shipment {tracking_number} not found.", show_alert=True)
        return
    webhook_url = shipment.get('webhook_url') or config.websocket_server
    if not webhook_url:
        bot.answer_callback_query(call.id, "No webhook URL set.", show_alert=True)
        return
    try:
        payload = {
            'tracking_number': tracking_number,
            'status': shipment['status'],
            'delivery_location': shipment['delivery_location'],
            'checkpoints': shipment['checkpoints']
        }
        response = requests.post(webhook_url, json=payload, timeout=5)
        response.raise_for_status()
        bot.answer_callback_query(call.id, f"Webhook sent for {tracking_number}")
        logger.info(f"Sent manual webhook for {tracking_number}")
        console.print(f"[info]Sent manual webhook for {tracking_number}[/info]")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error sending webhook: {e}", show_alert=True)
        logger.error(f"Error sending manual webhook for {tracking_number}: {e}")
        console.print(Panel(f"[error]Error sending manual webhook for {tracking_number}: {e}[/error]", title="Webhook Error", border_style="red"))

def bulk_pause_shipments(call, page: int):
    """Pause multiple shipments."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    selected = safe_redis_operation(redis_client.smembers, "batch_selected") or set()
    selected = [sanitize_tracking_number(tn) for tn in selected if sanitize_tracking_number(tn)]
    if not selected:
        bot.edit_message_text("No shipments selected for pausing.", chat_id=call.message.chat.id,
                             message_id=call.message.message_id)
        return
    try:
        count = 0
        for tn in selected:
            shipment = Shipment.query.filter_by(tracking_number=tn).first()
            if shipment and shipment.status not in ['Delivered', 'Returned']:
                if redis_client:
                    safe_redis_operation(redis_client.hset, "paused_simulations", tn, "true")
                count += 1
        safe_redis_operation(redis_client.delete, "batch_selected")
        bot.edit_message_text(f"Paused {count} shipments.", chat_id=call.message.chat.id,
                             message_id=call.message.message_id)
        logger.info(f"Paused {count} shipments in bulk")
        console.print(f"[info]Paused {count} shipments in bulk[/info]")
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        show_shipment_menu(call, page, prefix="bulk_pause", prompt="Select shipments to pause",
                          extra_buttons=[InlineKeyboardButton("Confirm Pause", callback_data=f"bulk_pause_confirm_{page}"),
                                        InlineKeyboardButton("Home", callback_data="menu_page_1")])
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.answer_callback_query(call.id, f"Database error: {e}", show_alert=True)
        logger.error(f"Database error during bulk pause: {e}")
        console.print(Panel(f"[error]Database error during bulk pause: {e}[/error]", title="Database Error", border_style="red"))

def bulk_resume_shipments(call, page: int):
    """Resume multiple shipments."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    selected = safe_redis_operation(redis_client.smembers, "batch_selected") or set()
    selected = [sanitize_tracking_number(tn) for tn in selected if sanitize_tracking_number(tn)]
    if not selected:
        bot.edit_message_text("No shipments selected for resuming.", chat_id=call.message.chat.id,
                             message_id=call.message.message_id)
        return
    try:
        count = 0
        for tn in selected:
            shipment = Shipment.query.filter_by(tracking_number=tn).first()
            if shipment and shipment.status not in ['Delivered', 'Returned']:
                if redis_client:
                    safe_redis_operation(redis_client.hdel, "paused_simulations", tn)
                count += 1
        safe_redis_operation(redis_client.delete, "batch_selected")
        bot.edit_message_text(f"Resumed {count} shipments.", chat_id=call.message.chat.id,
                             message_id=call.message.message_id)
        logger.info(f"Resumed {count} shipments in bulk")
        console.print(f"[info]Resumed {count} shipments in bulk[/info]")
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        show_shipment_menu(call, page, prefix="bulk_resume", prompt="Select shipments to resume",
                          extra_buttons=[InlineKeyboardButton("Confirm Resume", callback_data=f"bulk_resume_confirm_{page}"),
                                        InlineKeyboardButton("Home", callback_data="menu_page_1")])
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.answer_callback_query(call.id, f"Database error: {e}", show_alert=True)
        logger.error(f"Database error during bulk resume: {e}")
        console.print(Panel(f"[error]Database error during bulk resume: {e}[/error]", title="Database Error", border_style="red"))

def generate_unique_id() -> str:
    """Generate a unique tracking number."""
    return f"TRK{uuid.uuid4().hex[:16].upper()}"

def search_shipments(query: str, page: int = 1) -> Tuple[List[str], int]:
    """Search shipments by tracking number or location."""
    db, Shipment, _, _, _, _, _ = get_app_modules()
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
    except SQLAlchemyError as e:
        logger.error(f"Database error searching shipments: {e}")
        console.print(Panel(f"[error]Database error searching shipments: {e}[/error]", title="Database Error", border_style="red"))
        return [], 0

def send_dynamic_menu(chat_id: int, message_id: Optional[int] = None, page: int = 1):
    """Send a dynamic menu to the admin."""
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        redis_url=os.getenv("REDIS_URL"),
        redis_token=os.getenv("UPSTASH_REDIS_TOKEN", ""),
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
    if message_id:
        bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, parse_mode='Markdown', reply_markup=markup)
    else:
        bot.send_message(chat_id, text, parse_mode='Markdown', reply_markup=markup)
    logger.info(f"Sent dynamic menu to chat {chat_id}")
    console.print(f"[info]Sent dynamic menu to chat {chat_id}[/info]")
