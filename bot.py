# Part 1: Initialization and Setup
# This section initializes the Telegram bot, sets up logging, establishes the Redis connection,
# and defines functions to lazily import app modules and configuration values.

import os
import re
import json
import random
import string
import requests
import logging
from datetime import datetime
from telebot import TeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ForceReply
from flask_sqlalchemy import SQLAlchemyError
from rich.console import Console
from rich.panel import Panel
import eventlet
import shlex
from upstash_redis import Redis

# Patch eventlet for compatibility with gunicorn
eventlet.monkey_patch()

# Logging setup
bot_logger = logging.getLogger('telegram_bot')
bot_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
bot_logger.addHandler(handler)

# Redis client (shared with app.py, using upstash_redis)
redis_client = None
try:
    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    redis_client.ping()
    bot_logger.info("Redis connection successful")
    console = Console()
    console.print("[info]Redis connection successful[/info]")
except Exception as e:
    bot_logger.error(f"Redis connection failed: {e}")
    console = Console()
    console.print(Panel(f"[error]Redis connection failed: {e}[/error]", title="Redis Error", border_style="red"))
    redis_client = None

# Global console (will be synced from app)
console = Console()

# Lazy import functions to avoid circular imports
def get_app_modules():
    try:
        from app import (
            db, Shipment, sanitize_tracking_number, validate_email, validate_location,
            validate_webhook_url, send_email_notification, console as app_console
        )
        global console
        console = app_console  # Sync with app's console
        return db, Shipment, sanitize_tracking_number, validate_email, validate_location, validate_webhook_url, send_email_notification
    except ImportError as e:
        bot_logger.error(f"Failed to import app modules: {e}")
        console.print(Panel(f"[error]Failed to import app modules: {e}[/error]", title="Import Error", border_style="red"))
        raise

def get_config_values():
    try:
        from app import app
        from config import WEBSOCKET_SERVER, VALID_STATUSES, ALLOWED_ADMINS
        token = app.config.get('TELEGRAM_BOT_TOKEN')
        websocket_server = WEBSOCKET_SERVER or 'https://signment.onrender.com'
        valid_statuses = VALID_STATUSES or ['Pending', 'In_Transit', 'Out_for_Delivery', 'Delivered', 'Returned', 'Delayed']
        allowed_admins = [int(uid) for uid in ALLOWED_ADMINS] if ALLOWED_ADMINS else []
        return token, websocket_server, valid_statuses, allowed_admins
    except ImportError as e:
        bot_logger.error(f"Failed to import config: {e}")
        console.print(Panel(f"[error]Failed to import config: {e}[/error]", title="Config Error", border_style="red"))
        raise

# Bot instance (lazy init)
_bot_instance = None
def get_bot():
    global _bot_instance
    if _bot_instance is None:
        token, _, _, _ = get_config_values()
        if not token:
            bot_logger.critical("Missing TELEGRAM_BOT_TOKEN in config", extra={'tracking_number': ''})
            console.print(Panel("[critical]Missing TELEGRAM_BOT_TOKEN in config[/critical]", title="Config Error", border_style="red"))
            raise ValueError("Missing TELEGRAM_BOT_TOKEN in config")
        try:
            _bot_instance = TeleBot(token)
            bot_logger.info("Bot initialized successfully", extra={'tracking_number': ''})
            console.print("[info]Bot initialized successfully[/info]")
        except Exception as e:
            bot_logger.critical(f"Failed to initialize bot: {e}", extra={'tracking_number': ''})
            console.print(Panel(f"[critical]Failed to initialize bot: {e}[/critical]", title="Bot Init Error", border_style="red"))
            raise
    return _bot_instance

bot = get_bot()

def check_bot_status():
    """Check if the bot is running and can communicate with Telegram API."""
    try:
        bot.get_me()
        return True
    except Exception as e:
        bot_logger.error(f"Bot status check failed: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Bot status check failed: {e}[/error]", title="Telegram Error", border_style="red"))
        return False

# Part 2: Utility Functions
# This section includes utility functions for generating unique tracking numbers,
# retrieving shipment lists, caching route templates, and validating shipment details.

def cache_route_templates():
    """Cache predefined route templates in Redis."""
    route_templates = {
        'Lagos, NG': ['Lagos, NG', 'Abuja, NG', 'Port Harcourt, NG', 'Kano, NG'],
        'New York, NY': ['New York, NY', 'Chicago, IL', 'Los Angeles, CA', 'Miami, FL'],
        'London, UK': ['London, UK', 'Manchester, UK', 'Birmingham, UK', 'Edinburgh, UK'],
    }
    try:
        redis_client.setex("route_templates", 86400, json.dumps(route_templates))
        bot_logger.debug("Cached route templates in Redis", extra={'tracking_number': ''})
        console.print("[info]Cached route templates in Redis[/info]")
    except Exception as e:
        bot_logger.error(f"Failed to cache route templates: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Failed to cache route templates: {e}[/error]", title="Redis Error", border_style="red"))

def get_cached_route_templates():
    """Retrieve cached route templates from Redis."""
    try:
        cached = redis_client.get("route_templates")
        if cached:
            return json.loads(cached)
        bot_logger.warning("Route templates not found in Redis, returning default", extra={'tracking_number': ''})
        return {'Lagos, NG': ['Lagos, NG']}
    except Exception as e:
        bot_logger.error(f"Failed to retrieve route templates: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Failed to retrieve route templates: {e}[/error]", title="Redis Error", border_style="red"))
        return {'Lagos, NG': ['Lagos, NG']}

def is_admin(user_id):
    """Check if the user is an admin based on ALLOWED_ADMINS."""
    _, _, _, allowed_admins = get_config_values()
    is_admin_user = user_id in allowed_admins
    bot_logger.debug(f"Checked admin status for user {user_id}: {is_admin_user}", extra={'tracking_number': ''})
    return is_admin_user

def generate_unique_id():
    """Generate a unique tracking number using timestamp and random string."""
    db, Shipment, _, _, _, _, _ = get_app_modules()
    attempts = 0
    while attempts < 10:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        random_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        new_id = f"TRK{timestamp}{random_str}"
        if not Shipment.query.filter_by(tracking_number=new_id).first():
            bot_logger.debug(f"Generated ID: {new_id}", extra={'tracking_number': new_id})
            console.print(f"[info]Generated tracking ID: {new_id}[/info]")
            return new_id
        attempts += 1
    raise ValueError("Failed to generate unique ID after 10 attempts")

def get_shipment_list(page=1, per_page=5):
    """Fetch a paginated list of shipment tracking numbers."""
    db, Shipment, _, _, _, _, _ = get_app_modules()
    try:
        offset = (page - 1) * per_page
        shipments = Shipment.query.with_entities(Shipment.tracking_number).order_by(Shipment.tracking_number).offset(offset).limit(per_page).all()
        total = Shipment.query.count()
        return [s.tracking_number for s in shipments], total
    except SQLAlchemyError as e:
        bot_logger.error(f"Database error fetching shipment list: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Database error fetching shipment list: {e}[/error]", title="Database Error", border_style="red"))
        return [], 0

def search_shipments(query, page=1, per_page=5):
    """Search shipments by tracking number, status, or location."""
    db, Shipment, _, _, _, _, _ = get_app_modules()
    try:
        query = query.lower()
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
        bot_logger.error(f"Database error searching shipments: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Database error searching shipments: {e}[/error]", title="Database Error", border_style="red"))
        return [], 0

def get_shipment_details(tracking_number):
    """Fetch shipment details, using Redis cache."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        return None
    cached = redis_client.get(f"shipment:{sanitized_tn}") if redis_client else None
    if cached:
        bot_logger.debug(f"Retrieved {sanitized_tn} from Redis", extra={'tracking_number': sanitized_tn})
        return json.loads(cached)
    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        details = shipment.to_dict() if shipment else None
        if details:
            details['paused'] = redis_client.hget("paused_simulations", sanitized_tn) == "true" if redis_client else False
            details['speed_multiplier'] = float(redis_client.hget("sim_speed_multipliers", sanitized_tn) or 1.0) if redis_client else 1.0
            if redis_client:
                redis_client.setex(f"shipment:{sanitized_tn}", 3600, json.dumps(details))
        bot_logger.debug(f"Fetched details for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
        return details
    except SQLAlchemyError as e:
        bot_logger.error(f"Database error fetching details: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error fetching details for {sanitized_tn}: {e}[/error]", title="Database Error", border_style="red"))
        return None

def invalidate_cache(tracking_number):
    """Invalidate Redis cache for a tracking number."""
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if redis_client and sanitized_tn:
        redis_client.delete(f"shipment:{sanitized_tn}")

# Part 3: Shipment Management
# This section includes functions for saving, deleting, and managing shipment details,
# including webhook and email notifications, as well as batch operations and simulation controls.

def save_shipment(tracking_number, status, checkpoints, delivery_location, recipient_email='', origin_location=None, webhook_url=None, email_notifications=True):
    """Save or update a shipment in the database and update Redis cache."""
    db, Shipment, sanitize_tracking_number, validate_email, validate_location, validate_webhook_url, send_email_notification = get_app_modules()
    _, websocket_server, valid_statuses, _ = get_config_values()
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        raise ValueError("Invalid tracking number")
    if status not in valid_statuses:
        raise ValueError(f"Invalid status. Must be one of: {', '.join(valid_statuses)}")
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
        details['paused'] = redis_client.hget("paused_simulations", sanitized_tn) == "true" if redis_client else False
        details['speed_multiplier'] = float(redis_client.hget("sim_speed_multipliers", sanitized_tn) or 1.0) if redis_client else 1.0
        if redis_client:
            redis_client.setex(f"shipment:{sanitized_tn}", 3600, json.dumps(details))
        bot_logger.info(f"Saved shipment: status={status}", extra={'tracking_number': sanitized_tn})
        console.print(f"[info]Saved shipment {sanitized_tn}: {status}[/info]")
        if recipient_email and email_notifications:
            eventlet.spawn(send_email_notification, sanitized_tn, status, checkpoints, delivery_location, recipient_email)
        if webhook_url:
            eventlet.spawn(send_webhook_notification, sanitized_tn, status, checkpoints, delivery_location, webhook_url)
        try:
            response = requests.get(f'{websocket_server}/broadcast/{sanitized_tn}', timeout=5)
            if response.status_code != 204:
                bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': sanitized_tn})
        except requests.RequestException as e:
            bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': sanitized_tn})
            console.print(Panel(f"[warning]Broadcast error for {sanitized_tn}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
    except SQLAlchemyError as e:
        db.session.rollback()
        bot_logger.error(f"Database error saving shipment: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error saving {sanitized_tn}: {e}[/error]", title="Database Error", border_style="red"))
        raise
    except Exception as e:
        db.session.rollback()
        raise

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
            bot_logger.info(f"Webhook notification sent for {tracking_number}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Webhook notification sent for {tracking_number} to {webhook_url}[/info]")
        else:
            bot_logger.warning(f"Webhook failed for {tracking_number}: HTTP {response.status_code}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[warning]Webhook failed for {tracking_number}: HTTP {response.status_code}[/warning]", title="Webhook Warning", border_style="yellow"))
    except requests.RequestException as e:
        bot_logger.error(f"Webhook error for {tracking_number}: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Webhook error for {tracking_number}: {e}[/error]", title="Webhook Error", border_style="red"))

def show_shipment_details(call, tracking_number):
    """Display shipment details with interactive controls."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    _, websocket_server, _, _ = get_config_values()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
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
            InlineKeyboardButton("Back", callback_data="menu_page_1")
        )
        bot.edit_message_text(response, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent shipment details for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Sent shipment details for {tracking_number} to admin {call.from_user.id}[/info]")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error in show shipment details: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in show shipment details for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def delete_shipment(call, tracking_number, page):
    """Delete a shipment and invalidate its cache."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        db.session.delete(shipment)
        db.session.commit()
        if redis_client:
            redis_client.delete(f"shipment:{tracking_number}")
            redis_client.hdel("paused_simulations", tracking_number)
            redis_client.hdel("sim_speed_multipliers", tracking_number)
        bot_logger.info(f"Deleted shipment {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Deleted shipment {tracking_number} by admin {call.from_user.id}[/info]")
        bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` deleted.", show_alert=True)
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.answer_callback_query(call.id, f"Database error: {e}", show_alert=True)
        bot_logger.error(f"Database error deleting shipment {tracking_number}: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Database error deleting {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error in delete shipment: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in delete shipment for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def toggle_batch_selection(call, tracking_number):
    """Toggle a shipment's selection for batch deletion."""
    chat_id = call.message.chat.id
    batch_key = f"batch_delete:{chat_id}"
    selected = redis_client.smembers(batch_key) if redis_client else set()
    selected = set(selected)
    if tracking_number in selected:
        redis_client.srem(batch_key, tracking_number)
        bot.answer_callback_query(call.id, f"Deselected `{tracking_number}`.", show_alert=True)
    else:
        redis_client.sadd(batch_key, tracking_number)
        bot.answer_callback_query(call.id, f"Selected `{tracking_number}`.", show_alert=True)
    bot_logger.info(f"Toggled batch selection for {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
    console.print(f"[info]Toggled batch selection for {tracking_number} by admin {call.from_user.id}[/info]")
    show_shipment_menu(call, page=1, prefix="batch_select", prompt="Select shipments to delete", extra_buttons=[
        InlineKeyboardButton("Confirm Delete", callback_data=f"batch_delete_confirm_1"),
        InlineKeyboardButton("Back", callback_data="menu_page_1")
    ])

def batch_delete_shipments(call, page):
    """Delete selected shipments in a batch."""
    db, Shipment, _, _, _, _, _ = get_app_modules()
    chat_id = call.message.chat.id
    batch_key = f"batch_delete:{chat_id}"
    try:
        selected = redis_client.smembers(batch_key) if redis_client else set()
        if not selected:
            bot.answer_callback_query(call.id, "No shipments selected for deletion.", show_alert=True)
            bot_logger.warning("No shipments selected for batch deletion", extra={'tracking_number': ''})
            return
        for tracking_number in selected:
            shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
            if shipment:
                db.session.delete(shipment)
                if redis_client:
                    redis_client.delete(f"shipment:{tracking_number}")
                    redis_client.hdel("paused_simulations", tracking_number)
                    redis_client.hdel("sim_speed_multipliers", tracking_number)
        db.session.commit()
        redis_client.delete(batch_key)
        bot.answer_callback_query(call.id, f"Deleted {len(selected)} shipments.", show_alert=True)
        bot_logger.info(f"Batch deleted {len(selected)} shipments by admin {call.from_user.id}", extra={'tracking_number': ''})
        console.print(f"[info]Batch deleted {len(selected)} shipments by admin {call.from_user.id}[/info]")
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.answer_callback_query(call.id, f"Database error: {e}", show_alert=True)
        bot_logger.error(f"Database error in batch delete: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Database error in batch delete: {e}[/error]", title="Database Error", border_style="red"))
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error in batch delete: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Error in batch delete for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def trigger_broadcast(call, tracking_number):
    """Trigger a broadcast for a shipment."""
    _, websocket_server, _, _ = get_config_values()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            bot_logger.warning(f"Shipment not found for broadcast: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        response = requests.get(f'{websocket_server}/broadcast/{tracking_number}', timeout=5)
        if response.status_code == 204:
            bot.answer_callback_query(call.id, f"Broadcast triggered for `{tracking_number}`.", show_alert=True)
            bot_logger.info(f"Broadcast triggered for {tracking_number}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Broadcast triggered for {tracking_number} by admin {call.from_user.id}[/info]")
        else:
            bot.answer_callback_query(call.id, f"Broadcast failed: HTTP {response.status_code}", show_alert=True)
            bot_logger.warning(f"Broadcast failed for {tracking_number}: {response.status_code}", extra={'tracking_number': tracking_number})
    except requests.RequestException as e:
        bot.answer_callback_query(call.id, f"Broadcast error: {e}", show_alert=True)
        bot_logger.error(f"Broadcast error for {tracking_number}: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error in broadcast: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in broadcast for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def toggle_email_notifications(call, tracking_number, page):
    """Toggle email notifications for a shipment."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        shipment.email_notifications = not shipment.email_notifications
        db.session.commit()
        invalidate_cache(tracking_number)
        status = "enabled" if shipment.email_notifications else "disabled"
        bot.answer_callback_query(call.id, f"Email notifications {status} for `{tracking_number}`.", show_alert=True)
        bot_logger.info(f"Email notifications {status} for {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Email notifications {status} for {tracking_number} by admin {call.from_user.id}[/info]")
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.answer_callback_query(call.id, f"Database error: {e}", show_alert=True)
        bot_logger.error(f"Database error toggling email notifications for {tracking_number}: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Database error toggling email for {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error toggling email notifications: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error toggling email for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def pause_simulation_callback(call, tracking_number, page):
    """Pause a shipment's simulation via callback."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    _, websocket_server, _, _ = get_config_values()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if shipment['status'] in ['Delivered', 'Returned']:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` is already completed (`{shipment['status']}`).", show_alert=True)
            bot_logger.warning(f"Cannot pause completed shipment: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if redis_client and redis_client.hget("paused_simulations", tracking_number) == "true":
            bot.answer_callback_query(call.id, f"Simulation for `{tracking_number}` is already paused.", show_alert=True)
            bot_logger.warning(f"Simulation already paused: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if redis_client:
            redis_client.hset("paused_simulations", tracking_number, "true")
        invalidate_cache(tracking_number)
        bot_logger.info(f"Paused simulation for {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Paused simulation for {tracking_number} by admin {call.from_user.id}[/info]", title="Simulation Paused", border_style="green"))
        try:
            response = requests.get(f'{websocket_server}/broadcast/{tracking_number}', timeout=5)
            if response.status_code != 204:
                bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
        except requests.RequestException as e:
            bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
        bot.answer_callback_query(call.id, f"Simulation paused for `{tracking_number}`.", show_alert=True)
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error pausing simulation: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error pausing simulation for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def resume_simulation_callback(call, tracking_number, page):
    """Resume a shipment's simulation via callback."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    _, websocket_server, _, _ = get_config_values()
    try:
        if redis_client and redis_client.hget("paused_simulations", tracking_number) != "true":
            bot.answer_callback_query(call.id, f"Simulation for `{tracking_number}` is not paused.", show_alert=True)
            bot_logger.warning(f"Simulation not paused: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if shipment['status'] in ['Delivered', 'Returned']:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` is already completed (`{shipment['status']}`).", show_alert=True)
            bot_logger.warning(f"Cannot resume completed shipment: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if redis_client:
            redis_client.hdel("paused_simulations", tracking_number)
        invalidate_cache(tracking_number)
        bot_logger.info(f"Resumed simulation for {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Resumed simulation for {tracking_number} by admin {call.from_user.id}[/info]", title="Simulation Resumed", border_style="green"))
        try:
            response = requests.get(f'{websocket_server}/broadcast/{tracking_number}', timeout=5)
            if response.status_code != 204:
                bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
        except requests.RequestException as e:
            bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
        bot.answer_callback_query(call.id, f"Simulation resumed for `{tracking_number}`.", show_alert=True)
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error resuming simulation: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error resuming simulation for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def show_simulation_speed(call, tracking_number):
    """Show the simulation speed for a shipment."""
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        speed = float(redis_client.hget("sim_speed_multipliers", tracking_number) or 1.0) if redis_client else 1.0
        bot.answer_callback_query(call.id, f"Simulation speed for `{tracking_number}` is `{speed}x`.", show_alert=True)
        bot_logger.info(f"Retrieved simulation speed for {tracking_number}: {speed}x", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Retrieved simulation speed for {tracking_number}: {speed}x by admin {call.from_user.id}[/info]", title="Speed Retrieved", border_style="green"))
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error retrieving simulation speed: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error retrieving simulation speed for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def bulk_pause_shipments(call, page):
    """Pause simulations for selected shipments."""
    db, Shipment, _, _, _, _, _ = get_app_modules()
    _, websocket_server, _, _ = get_config_values()
    chat_id = call.message.chat.id
    batch_key = f"bulk_pause:{chat_id}"
    try:
        selected = redis_client.smembers(batch_key) if redis_client else set()
        if not selected:
            bot.answer_callback_query(call.id, "No shipments selected for pausing.", show_alert=True)
            bot_logger.warning("No shipments selected for bulk pause", extra={'tracking_number': ''})
            return
        paused_count = 0
        for tracking_number in selected:
            shipment = get_shipment_details(tracking_number)
            if shipment and shipment['status'] not in ['Delivered', 'Returned'] and redis_client.hget("paused_simulations", tracking_number) != "true":
                redis_client.hset("paused_simulations", tracking_number, "true")
                invalidate_cache(tracking_number)
                try:
                    response = requests.get(f'{websocket_server}/broadcast/{tracking_number}', timeout=5)
                    if response.status_code != 204:
                        bot_logger.warning(f"Broadcast failed for {tracking_number}: {response.status_code}", extra={'tracking_number': tracking_number})
                except requests.RequestException as e:
                    bot_logger.error(f"Broadcast error for {tracking_number}: {e}", extra={'tracking_number': tracking_number})
                    console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
                paused_count += 1
        redis_client.delete(batch_key)
        bot.answer_callback_query(call.id, f"Paused {paused_count} simulations.", show_alert=True)
        bot_logger.info(f"Paused {paused_count} simulations by admin {call.from_user.id}", extra={'tracking_number': ''})
        console.print(f"[info]Paused {paused_count} simulations by admin {call.from_user.id}[/info]")
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error in bulk pause: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Error in bulk pause for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def bulk_resume_shipments(call, page):
    """Resume simulations for selected shipments."""
    db, Shipment, _, _, _, _, _ = get_app_modules()
    _, websocket_server, _, _ = get_config_values()
    chat_id = call.message.chat.id
    batch_key = f"bulk_resume:{chat_id}"
    try:
        selected = redis_client.smembers(batch_key) if redis_client else set()
        if not selected:
            bot.answer_callback_query(call.id, "No shipments selected for resuming.", show_alert=True)
            bot_logger.warning("No shipments selected for bulk resume", extra={'tracking_number': ''})
            return
        resumed_count = 0
        for tracking_number in selected:
            shipment = get_shipment_details(tracking_number)
            if shipment and shipment['status'] not in ['Delivered', 'Returned'] and redis_client.hget("paused_simulations", tracking_number) == "true":
                redis_client.hdel("paused_simulations", tracking_number)
                invalidate_cache(tracking_number)
                try:
                    response = requests.get(f'{websocket_server}/broadcast/{tracking_number}', timeout=5)
                    if response.status_code != 204:
                        bot_logger.warning(f"Broadcast failed for {tracking_number}: {response.status_code}", extra={'tracking_number': tracking_number})
                except requests.RequestException as e:
                    bot_logger.error(f"Broadcast error for {tracking_number}: {e}", extra={'tracking_number': tracking_number})
                    console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
                resumed_count += 1
        redis_client.delete(batch_key)
        bot.answer_callback_query(call.id, f"Resumed {resumed_count} simulations.", show_alert=True)
        bot_logger.info(f"Resumed {resumed_count} simulations by admin {call.from_user.id}", extra={'tracking_number': ''})
        console.print(f"[info]Resumed {resumed_count} simulations by admin {call.from_user.id}[/info]")
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error in bulk resume: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Error in bulk resume for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def send_manual_email(call, tracking_number):
    """Send a manual email notification for a shipment."""
    db, Shipment, _, _, _, _, send_email_notification = get_app_modules()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if not shipment.get('recipient_email') or not shipment.get('email_notifications'):
            bot.answer_callback_query(call.id, f"Email notifications disabled or no recipient email for `{tracking_number}`.", show_alert=True)
            bot_logger.warning(f"Email notifications disabled or no email for {tracking_number}", extra={'tracking_number': tracking_number})
            return
        eventlet.spawn(
            send_email_notification,
            tracking_number,
            shipment['status'],
            shipment['checkpoints'],
            shipment['delivery_location'],
            shipment['recipient_email']
        )
        bot.answer_callback_query(call.id, f"Email notification sent for `{tracking_number}`.", show_alert=True)
        bot_logger.info(f"Manual email sent for {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Manual email sent for {tracking_number} by admin {call.from_user.id}[/info]")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error sending manual email for {tracking_number}: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error sending manual email for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def send_manual_webhook(call, tracking_number):
    """Send a manual webhook notification for a shipment."""
    _, websocket_server, _, _ = get_config_values()
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        webhook_url = shipment.get('webhook_url') or websocket_server
        eventlet.spawn(
            send_webhook_notification,
            tracking_number,
            shipment['status'],
            shipment['checkpoints'],
            shipment['delivery_location'],
            webhook_url
        )
        bot.answer_callback_query(call.id, f"Webhook notification sent for `{tracking_number}`.", show_alert=True)
        bot_logger.info(f"Manual webhook sent for {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Manual webhook sent for {tracking_number} by admin {call.from_user.id}[/info]")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error sending manual webhook for {tracking_number}: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error sending manual webhook for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

# Part 4: Command Handlers
# This section contains message handlers for Telegram commands like /start, /track, /stats,
# /notify, /search, /bulk_action, /stop, /continue, /setspeed, /getspeed, /debug, /setwebhook,
# and /testwebhook, handling user interactions via commands.

def send_dynamic_menu(chat_id, message_id=None, page=1, per_page=5):
    """Send a dynamic menu with admin actions for shipments."""
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
        InlineKeyboardButton("Cancel", callback_data="cancel")
    )
    try:
        message_text = f"*Choose an action (Page {page})*\nAvailable shipments: {total}"
        if message_id:
            bot.edit_message_text(message_text, chat_id=chat_id, message_id=message_id, reply_markup=markup, parse_mode='Markdown')
        else:
            bot.send_message(chat_id, message_text, reply_markup=markup, parse_mode='Markdown')
        bot_logger.debug(f"Sent dynamic menu, page {page}", extra={'tracking_number': ''})
        console.print(f"[info]Sent dynamic menu to chat {chat_id}, page {page}[/info]")
    except Exception as e:
        bot_logger.error(f"Telegram API error sending menu: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Telegram API error sending menu to {chat_id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['myid'])
def get_my_id(message):
    """Handle /myid command to return the user's Telegram ID."""
    bot.reply_to(message, f"Your Telegram user ID: `{message.from_user.id}`", parse_mode='Markdown')
    bot_logger.info(f"User requested their ID", extra={'tracking_number': ''})
    console.print(f"[info]User {message.from_user.id} requested their ID[/info]")

@bot.message_handler(commands=['start', 'menu'])
def send_menu(message):
    """Handle /start and /menu commands to display the admin menu."""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for user {message.from_user.id}", extra={'tracking_number': ''})
        console.print(f"[warning]Access denied for user {message.from_user.id}[/warning]")
        return
    send_dynamic_menu(message.chat.id, page=1)
    bot_logger.info(f"Menu sent to admin {message.from_user.id}", extra={'tracking_number': ''})
    console.print(f"[info]Menu sent to admin {message.from_user.id}[/info]")

@bot.message_handler(commands=['track'])
def track_shipment(message):
    """Handle /track command to view shipment details with interactive controls."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    _, websocket_server, _, _ = get_config_values()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /track by {message.from_user.id}", extra={'tracking_number': ''})
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /track <tracking_number>")
        bot_logger.warning("Invalid /track command format", extra={'tracking_number': ''})
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}", extra={'tracking_number': parts[1]})
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        response = (
            f"*Shipment*: `{tracking_number}`\n"
            f"*Status*: `{shipment['status']}`\n"
            f"*Paused*: `{shipment.get('paused', False)}`\n"
            f"*Speed Multiplier*: `{shipment.get('speed_multiplier', 1.0)}x`\n"
            f"*Delivery Location*: `{shipment['delivery_location']}`\n"
            f"*Checkpoints*: `{shipment.get('checkpoints', 'None')}`"
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
            InlineKeyboardButton("Back", callback_data="menu_page_1")
        )
        bot.reply_to(message, response, parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent tracking details for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Sent tracking details for {tracking_number} to admin {message.from_user.id}[/info]")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in track command: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in track command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['stats'])
def system_stats(message):
    """Handle /stats command to display system statistics."""
    db, Shipment, _, _, _, _, _ = get_app_modules()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /stats by {message.from_user.id}", extra={'tracking_number': ''})
        return
    try:
        total_shipments = Shipment.query.count()
        active_shipments = Shipment.query.filter(~Shipment.status.in_(['Delivered', 'Returned'])).count()
        paused_count = len(redis_client.hgetall("paused_simulations")) if redis_client else 0
        response = (
            f"*System Statistics*\n"
            f"*Total Shipments*: `{total_shipments}`\n"
            f"*Active Shipments*: `{active_shipments}`\n"
            f"*Paused Simulations*: `{paused_count}`"
        )
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("Active Shipments", callback_data="list_active_1"),
            InlineKeyboardButton("Paused Shipments", callback_data="list_paused_1"),
            InlineKeyboardButton("All Shipments", callback_data="list_1"),
            InlineKeyboardButton("Back", callback_data="menu_page_1")
        )
        bot.reply_to(message, response, parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent system stats to admin {message.from_user.id}", extra={'tracking_number': ''})
        console.print(f"[info]Sent system stats to admin {message.from_user.id}[/info]")
    except SQLAlchemyError as e:
        bot.reply_to(message, f"Database error: {e}")
        bot_logger.error(f"Database error in stats: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Database error in stats: {e}[/error]", title="Database Error", border_style="red"))
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in stats command: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Error in stats command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['notify'])
def manual_notification(message):
    """Handle /notify command to send manual email or webhook notification."""
    db, Shipment, sanitize_tracking_number, _, _, _, send_email_notification = get_app_modules()
    _, websocket_server, _, _ = get_config_values()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /notify by {message.from_user.id}", extra={'tracking_number': ''})
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /notify <tracking_number>")
        bot_logger.warning("Invalid /notify command format", extra={'tracking_number': ''})
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}", extra={'tracking_number': parts[1]})
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        markup = InlineKeyboardMarkup(row_width=2)
        if shipment.get('recipient_email') and shipment.get('email_notifications'):
            markup.add(InlineKeyboardButton("Send Email", callback_data=f"send_email_{tracking_number}"))
        if shipment.get('webhook_url') or websocket_server:
            markup.add(InlineKeyboardButton("Send Webhook", callback_data=f"send_webhook_{tracking_number}"))
        markup.add(InlineKeyboardButton("Back", callback_data="menu_page_1"))
        bot.reply_to(message, f"Select notification type for `{tracking_number}`:", parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent notification options for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Sent notification options for {tracking_number} to admin {message.from_user.id}[/info]")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in notify command: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in notify command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['search'])
def search_command(message):
    """Handle /search command to find shipments by query."""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /search by {message.from_user.id}", extra={'tracking_number': ''})
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /search <query>")
        bot_logger.warning("Invalid /search command format", extra={'tracking_number': ''})
        return
    query = parts[1].strip()
    try:
        shipments, total = search_shipments(query, page=1)
        if not shipments:
            bot.reply_to(message, f"No shipments found for query: `{query}`", parse_mode='Markdown')
            bot_logger.debug(f"No shipments found for query: {query}", extra={'tracking_number': ''})
            return
        set_chat_data(message.chat.id, f"search:{query}", shipments)
        markup = InlineKeyboardMarkup(row_width=1)
        for tn in shipments:
            markup.add(InlineKeyboardButton(tn, callback_data=f"view_{tn}"))
        if total > 5:
            markup.add(InlineKeyboardButton("Next", callback_data=f"search_page_{query}_2"))
        markup.add(InlineKeyboardButton("Back", callback_data="menu_page_1"))
        bot.reply_to(message, f"*Search Results for '{query}'* (Page 1, {total} total):", parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent search results for query: {query}", extra={'tracking_number': ''})
        console.print(f"[info]Sent search results for query '{query}' to admin {message.from_user.id}[/info]")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in search command: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Error in search command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['bulk_action'])
def bulk_action_command(message):
    """Handle /bulk_action command to perform bulk operations."""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /bulk_action by {message.from_user.id}", extra={'tracking_number': ''})
        return
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("Bulk Pause", callback_data="bulk_pause_menu_1"),
        InlineKeyboardButton("Bulk Resume", callback_data="bulk_resume_menu_1"),
        InlineKeyboardButton("Bulk Delete", callback_data="batch_delete_menu_1"),
        InlineKeyboardButton("Back", callback_data="menu_page_1")
    )
    bot.reply_to(message, "*Select bulk action*:", parse_mode='Markdown', reply_markup=markup)
    bot_logger.info(f"Sent bulk action menu to admin {message.from_user.id}", extra={'tracking_number': ''})
    console.print(f"[info]Sent bulk action menu to admin {message.from_user.id}[/info]")

@bot.message_handler(commands=['stop'])
def stop_simulation(message):
    """Handle /stop command to pause a shipment's simulation."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    _, websocket_server, _, _ = get_config_values()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /stop by {message.from_user.id}", extra={'tracking_number': ''})
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /stop <tracking_number>")
        bot_logger.warning("Invalid /stop command format", extra={'tracking_number': ''})
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}", extra={'tracking_number': parts[1]})
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if shipment['status'] in ['Delivered', 'Returned']:
            bot.reply_to(message, f"Shipment `{tracking_number}` is already completed (`{shipment['status']}`).", parse_mode='Markdown')
            bot_logger.warning(f"Cannot pause completed shipment: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if redis_client and redis_client.hget("paused_simulations", tracking_number) == "true":
            bot.reply_to(message, f"Simulation for `{tracking_number}` is already paused.", parse_mode='Markdown')
            bot_logger.warning(f"Simulation already paused: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if redis_client:
            redis_client.hset("paused_simulations", tracking_number, "true")
        invalidate_cache(tracking_number)
        bot_logger.info(f"Paused simulation for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Paused simulation for {tracking_number} by admin {message.from_user.id}[/info]", title="Simulation Paused", border_style="green"))
        try:
            response = requests.get(f'{websocket_server}/broadcast/{tracking_number}', timeout=5)
            if response.status_code != 204:
                bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
        except requests.RequestException as e:
            bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
        bot.reply_to(message, f"Simulation paused for `{tracking_number}`.", parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in stop command: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in stop command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['continue'])
def continue_simulation(message):
    """Handle /continue command to resume a paused shipment's simulation."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    _, websocket_server, _, _ = get_config_values()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /continue by {message.from_user.id}", extra={'tracking_number': ''})
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /continue <tracking_number>")
        bot_logger.warning("Invalid /continue command format", extra={'tracking_number': ''})
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}", extra={'tracking_number': parts[1]})
        return
    try:
        if redis_client and redis_client.hget("paused_simulations", tracking_number) != "true":
            bot.reply_to(message, f"Simulation for `{tracking_number}` is not paused.", parse_mode='Markdown')
            bot_logger.warning(f"Simulation not paused: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if shipment['status'] in ['Delivered', 'Returned']:
            bot.reply_to(message, f"Shipment `{tracking_number}` is already completed (`{shipment['status']}`).", parse_mode='Markdown')
            bot_logger.warning(f"Cannot resume completed shipment: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if redis_client:
            redis_client.hdel("paused_simulations", tracking_number)
        invalidate_cache(tracking_number)
        bot_logger.info(f"Resumed simulation for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Resumed simulation for {tracking_number} by admin {message.from_user.id}[/info]", title="Simulation Resumed", border_style="green"))
        try:
            response = requests.get(f'{websocket_server}/broadcast/{tracking_number}', timeout=5)
            if response.status_code != 204:
                bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
        except requests.RequestException as e:
            bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
        bot.reply_to(message, f"Simulation resumed for `{tracking_number}`.", parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in continue command: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in continue command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['setspeed'])
def set_simulation_speed(message):
    """Handle /setspeed command to set simulation speed for a shipment."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    _, websocket_server, _, _ = get_config_values()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /setspeed by {message.from_user.id}", extra={'tracking_number': ''})
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.reply_to(message, "Usage: /setspeed <tracking_number> <speed>")
        bot_logger.warning("Invalid /setspeed command format", extra={'tracking_number': ''})
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}", extra={'tracking_number': parts[1]})
        return
    try:
        speed = float(parts[2].strip())
        if speed < 0.1 or speed > 10:
            bot.reply_to(message, "Speed must be between 0.1 and 10.0.")
            bot_logger.warning(f"Invalid speed value: {speed}", extra={'tracking_number': tracking_number})
            return
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        if redis_client:
            redis_client.hset("sim_speed_multipliers", tracking_number, str(speed))
        invalidate_cache(tracking_number)
        bot_logger.info(f"Set simulation speed for {tracking_number} to {speed}x", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Set simulation speed for {tracking_number} to {speed}x by admin {message.from_user.id}[/info]", title="Speed Updated", border_style="green"))
        try:
            response = requests.get(f'{websocket_server}/broadcast/{tracking_number}', timeout=5)
            if response.status_code != 204:
                bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
        except requests.RequestException as e:
            bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
        bot.reply_to(message, f"Simulation speed for `{tracking_number}` set to `{speed}x`.", parse_mode='Markdown')
    except ValueError:
        bot.reply_to(message, "Speed must be a number between 0.1 and 10.0.")
        bot_logger.warning(f"Invalid speed format: {parts[2]}", extra={'tracking_number': tracking_number})
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in setspeed command: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in setspeed command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['getspeed'])
def get_simulation_speed(message):
    """Handle /getspeed command to retrieve simulation speed for a shipment."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /getspeed by {message.from_user.id}", extra={'tracking_number': ''})
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /getspeed <tracking_number>")
        bot_logger.warning("Invalid /getspeed command format", extra={'tracking_number': ''})
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}", extra={'tracking_number': parts[1]})
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        speed = float(redis_client.hget("sim_speed_multipliers", tracking_number) or 1.0) if redis_client else 1.0
        bot_logger.info(f"Retrieved simulation speed for {tracking_number}: {speed}x", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Retrieved simulation speed for {tracking_number}: {speed}x by admin {message.from_user.id}[/info]", title="Speed Retrieved", border_style="green"))
        bot.reply_to(message, f"Simulation speed for `{tracking_number}` is `{speed}x`.", parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in getspeed command: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in getspeed command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['debug'])
def debug_shipment(message):
    """Handle /debug command to display detailed shipment information."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /debug by {message.from_user.id}", extra={'tracking_number': ''})
        console.print(f"[warning]Access denied for /debug by {message.from_user.id}[/warning]")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /debug <tracking_number>")
        bot_logger.warning("Invalid /debug command format", extra={'tracking_number': ''})
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}", extra={'tracking_number': parts[1]})
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        response = (
            f"*Debug Info for Shipment*: `{tracking_number}`\n"
            f"*Status*: `{shipment['status']}`\n"
            f"*Paused*: `{shipment.get('paused', False)}`\n"
            f"*Speed Multiplier*: `{shipment.get('speed_multiplier', 1.0)}x`\n"
            f"*Delivery Location*: `{shipment['delivery_location']}`\n"
            f"*Origin Location*: `{shipment.get('origin_location', 'None')}`\n"
            f"*Recipient Email*: `{shipment.get('recipient_email', 'None')}`\n"
            f"*Checkpoints*: `{shipment.get('checkpoints', 'None')}`\n"
            f"*Webhook URL*: `{shipment.get('webhook_url', 'Default')}`\n"
            f"*Email Notifications*: `{'Enabled' if shipment.get('email_notifications', False) else 'Disabled'}`\n"
            f"*Created At*: `{shipment.get('created_at', 'N/A')}`\n"
            f"*Last Updated*: `{shipment.get('last_updated', 'N/A')}`"
        )
        bot.reply_to(message, response, parse_mode='Markdown')
        bot_logger.info(f"Sent debug info for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Sent debug info for {tracking_number} to admin {message.from_user.id}[/info]")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in debug command: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in debug command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['setwebhook'])
def set_webhook_command(message):
    """Handle /setwebhook command to set a webhook URL for a shipment."""
    db, Shipment, sanitize_tracking_number, _, _, validate_webhook_url, _ = get_app_modules()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /setwebhook by {message.from_user.id}", extra={'tracking_number': ''})
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.reply_to(message, "Usage: /setwebhook <tracking_number> <webhook_url>")
        bot_logger.warning("Invalid /setwebhook command format", extra={'tracking_number': ''})
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    webhook_url = parts[2].strip()
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}", extra={'tracking_number': parts[1]})
        return
    if not validate_webhook_url(webhook_url):
        bot.reply_to(message, "Invalid webhook URL.")
        bot_logger.warning(f"Invalid webhook URL: {webhook_url}", extra={'tracking_number': tracking_number})
        return
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        shipment.webhook_url = webhook_url
        db.session.commit()
        invalidate_cache(tracking_number)
        bot.reply_to(message, f"Webhook URL set for `{tracking_number}` to `{webhook_url}`.", parse_mode='Markdown')
        bot_logger.info(f"Set webhook URL for {tracking_number} to {webhook_url}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Set webhook URL for {tracking_number} to {webhook_url} by admin {message.from_user.id}[/info]")
    except SQLAlchemyError as e:
        db.session.rollback()
        bot.reply_to(message, f"Database error: {e}")
        bot_logger.error(f"Database error setting webhook for {tracking_number}: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Database error setting webhook for {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in setwebhook command: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in setwebhook command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(commands=['testwebhook'])
def test_webhook_command(message):
    """Handle /testwebhook command to send a test webhook notification."""
    db, Shipment, sanitize_tracking_number, _, _, _, _ = get_app_modules()
    _, websocket_server, _, _ = get_config_values()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /testwebhook by {message.from_user.id}", extra={'tracking_number': ''})
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /testwebhook <tracking_number>")
        bot_logger.warning("Invalid /testwebhook command format", extra={'tracking_number': ''})
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}", extra={'tracking_number': parts[1]})
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        webhook_url = shipment.get('webhook_url') or websocket_server
        eventlet.spawn(
            send_webhook_notification,
            tracking_number,
            shipment['status'],
            shipment['checkpoints'],
            shipment['delivery_location'],
            webhook_url
        )
        bot.reply_to(message, f"Test webhook notification sent for `{tracking_number}`.", parse_mode='Markdown')
        bot_logger.info(f"Test webhook sent for {tracking_number} by admin {message.from_user.id}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Test webhook sent for {tracking_number} by admin {message.from_user.id}[/info]")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in testwebhook command: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in testwebhook command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

# Part 5: Callback and Input Handlers
# This section contains the callback query handler for inline button interactions and
# input processing functions for handling user inputs (e.g., adding/updating shipments,
# setting simulation speeds, or webhooks) prompted by ForceReply messages.

def set_chat_data(chat_id, key, value):
    """Store data in Redis for a chat session."""
    if redis_client:
        redis_client.setex(f"chat:{chat_id}:{key}", 3600, json.dumps(value))
        bot_logger.debug(f"Set chat data for {chat_id}: {key}", extra={'tracking_number': ''})

def get_chat_data(chat_id, key):
    """Retrieve data from Redis for a chat session."""
    if redis_client:
        data = redis_client.get(f"chat:{chat_id}:{key}")
        return json.loads(data) if data else None
    return None

def show_shipment_menu(call, page=1, prefix="view", prompt="Select a shipment", extra_buttons=None):
    """Show a paginated list of shipments for actions like view, update, or delete."""
    shipments, total = get_shipment_list(page=page)
    chat_id = call.message.chat.id
    set_chat_data(chat_id, f"{prefix}_shipments", shipments)
    markup = InlineKeyboardMarkup(row_width=1)
    for tn in shipments:
        button_text = tn
        if prefix == "batch_select":
            selected = redis_client.smembers(f"batch_delete:{chat_id}") if redis_client else set()
            button_text = f"✅ {tn}" if tn in selected else tn
        markup.add(InlineKeyboardButton(button_text, callback_data=f"{prefix}_{tn}"))
    if total > 5:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("Previous", callback_data=f"{prefix}_page_{page-1}"))
        if page * 5 < total:
            nav_buttons.append(InlineKeyboardButton("Next", callback_data=f"{prefix}_page_{page+1}"))
        markup.add(*nav_buttons)
    if extra_buttons:
        markup.add(*extra_buttons)
    else:
        markup.add(InlineKeyboardButton("Back", callback_data="menu_page_1"))
    try:
        bot.edit_message_text(f"*{prompt}* (Page {page}, {total} total):", chat_id=call.message.chat.id,
                             message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
        bot_logger.debug(f"Sent shipment menu for {prefix}, page {page}", extra={'tracking_number': ''})
        console.print(f"[info]Sent shipment menu for {prefix}, page {page} to admin {call.from_user.id}[/info]")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Telegram API error sending shipment menu: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Telegram API error sending shipment menu: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    """Handle all callback queries from inline buttons."""
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.", show_alert=True)
        bot_logger.warning(f"Access denied for callback by {call.from_user.id}", extra={'tracking_number': ''})
        return
    data = call.data
    try:
        if data == "generate_id":
            new_id = generate_unique_id()
            bot.answer_callback_query(call.id, f"Generated ID: `{new_id}`", show_alert=True)
            bot_logger.info(f"Generated ID {new_id} for admin {call.from_user.id}", extra={'tracking_number': new_id})
            console.print(f"[info]Generated ID {new_id} for admin {call.from_user.id}[/info]")
        elif data == "add":
            markup = ForceReply(selective=True)
            bot.edit_message_text("Enter shipment details (tracking_number status delivery_location [recipient_email] [origin_location] [webhook_url]):",
                                 chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
            set_chat_data(call.message.chat.id, "state", "add_shipment")
            bot_logger.info(f"Prompted add shipment for admin {call.from_user.id}", extra={'tracking_number': ''})
            console.print(f"[info]Prompted add shipment for admin {call.from_user.id}[/info]")
        elif data == "settings":
            bot.edit_message_text("Settings not implemented yet.", chat_id=call.message.chat.id, message_id=call.message.message_id)
            bot_logger.info(f"Settings accessed by admin {call.from_user.id}", extra={'tracking_number': ''})
            console.print(f"[info]Settings accessed by admin {call.from_user.id}[/info]")
        elif data == "help":
            help_text = (
                "*Available Commands*:\n"
                "/start, /menu - Show main menu\n"
                "/track <tracking_number> - Track a shipment\n"
                "/stats - View system stats\n"
                "/notify <tracking_number> - Send manual notification\n"
                "/search <query> - Search shipments\n"
                "/bulk_action - Perform bulk actions\n"
                "/stop <tracking_number> - Pause simulation\n"
                "/continue <tracking_number> - Resume simulation\n"
                "/setspeed <tracking_number> <speed> - Set simulation speed\n"
                "/getspeed <tracking_number> - Get simulation speed\n"
                "/debug <tracking_number> - Debug shipment\n"
                "/setwebhook <tracking_number> <url> - Set webhook URL\n"
                "/testwebhook <tracking_number> - Test webhook\n"
                "/myid - Get your Telegram user ID"
            )
            bot.edit_message_text(help_text, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode='Markdown')
            bot_logger.info(f"Help accessed by admin {call.from_user.id}", extra={'tracking_number': ''})
            console.print(f"[info]Help accessed by admin {call.from_user.id}[/info]")
        elif data == "cancel":
            bot.delete_message(call.message.chat.id, call.message.message_id)
            bot_logger.info(f"Cancelled action by admin {call.from_user.id}", extra={'tracking_number': ''})
            console.print(f"[info]Cancelled action by admin {call.from_user.id}[/info]")
        elif data.startswith("menu_page_"):
            page = int(data.split("_")[-1])
            send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
        elif data.startswith("view_"):
            tracking_number = data.split("_", 1)[1]
            show_shipment_details(call, tracking_number)
        elif data.startswith("view_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="view", prompt="Select a shipment to view")
        elif data.startswith("update_"):
            tracking_number = data.split("_", 1)[1]
            markup = ForceReply(selective=True)
            bot.edit_message_text(f"Enter updated details for `{tracking_number}` (status delivery_location [recipient_email] [origin_location] [webhook_url]):",
                                 chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
            set_chat_data(call.message.chat.id, "state", f"update_shipment:{tracking_number}")
            bot_logger.info(f"Prompted update for {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Prompted update for {tracking_number} by admin {call.from_user.id}[/info]")
        elif data.startswith("update_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="update", prompt="Select a shipment to update")
        elif data.startswith("delete_"):
            tracking_number = data.split("_", 1)[1]
            page = int(get_chat_data(call.message.chat.id, "current_page") or 1)
            delete_shipment(call, tracking_number, page)
        elif data.startswith("delete_menu_"):
            page = int(data.split("_")[-1])
            set_chat_data(call.message.chat.id, "current_page", page)
            show_shipment_menu(call, page=page, prefix="delete", prompt="Select a shipment to delete")
        elif data.startswith("batch_select_"):
            tracking_number = data.split("_", 1)[1]
            toggle_batch_selection(call, tracking_number)
        elif data.startswith("batch_delete_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="batch_select", prompt="Select shipments to delete", extra_buttons=[
                InlineKeyboardButton("Confirm Delete", callback_data=f"batch_delete_confirm_{page}"),
                InlineKeyboardButton("Back", callback_data="menu_page_1")
            ])
        elif data.startswith("batch_delete_confirm_"):
            page = int(data.split("_")[-1])
            batch_delete_shipments(call, page)
        elif data.startswith("broadcast_"):
            tracking_number = data.split("_", 1)[1]
            trigger_broadcast(call, tracking_number)
        elif data.startswith("broadcast_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="broadcast", prompt="Select a shipment to broadcast")
        elif data.startswith("toggle_email_"):
            tracking_number = data.split("_", 1)[1]
            page = int(get_chat_data(call.message.chat.id, "current_page") or 1)
            toggle_email_notifications(call, tracking_number, page)
        elif data.startswith("toggle_email_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="toggle_email", prompt="Select a shipment to toggle email notifications")
        elif data.startswith("pause_"):
            tracking_number = data.split("_", 1)[1]
            page = int(get_chat_data(call.message.chat.id, "current_page") or 1)
            pause_simulation_callback(call, tracking_number, page)
        elif data.startswith("pause_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="pause", prompt="Select a shipment to pause")
        elif data.startswith("resume_"):
            tracking_number = data.split("_", 1)[1]
            page = int(get_chat_data(call.message.chat.id, "current_page") or 1)
            resume_simulation_callback(call, tracking_number, page)
        elif data.startswith("resume_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="resume", prompt="Select a shipment to resume")
        elif data.startswith("setspeed_"):
            tracking_number = data.split("_", 1)[1]
            markup = ForceReply(selective=True)
            bot.edit_message_text(f"Enter simulation speed for `{tracking_number}` (0.1 to 10.0):",
                                 chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
            set_chat_data(call.message.chat.id, "state", f"set_speed:{tracking_number}")
            bot_logger.info(f"Prompted set speed for {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Prompted set speed for {tracking_number} by admin {call.from_user.id}[/info]")
        elif data.startswith("setspeed_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="setspeed", prompt="Select a shipment to set simulation speed")
        elif data.startswith("getspeed_"):
            tracking_number = data.split("_", 1)[1]
            show_simulation_speed(call, tracking_number)
        elif data.startswith("getspeed_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="getspeed", prompt="Select a shipment to view simulation speed")
        elif data.startswith("bulk_action_menu_"):
            page = int(data.split("_")[-1])
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("Bulk Pause", callback_data=f"bulk_pause_menu_{page}"),
                InlineKeyboardButton("Bulk Resume", callback_data=f"bulk_resume_menu_{page}"),
                InlineKeyboardButton("Bulk Delete", callback_data=f"batch_delete_menu_{page}"),
                InlineKeyboardButton("Back", callback_data="menu_page_1")
            )
            bot.edit_message_text("*Select bulk action*:", chat_id=call.message.chat.id, message_id=call.message.message_id,
                                 parse_mode='Markdown', reply_markup=markup)
            bot_logger.info(f"Sent bulk action menu to admin {call.from_user.id}", extra={'tracking_number': ''})
            console.print(f"[info]Sent bulk action menu to admin {call.from_user.id}[/info]")
        elif data.startswith("bulk_pause_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="bulk_pause_select", prompt="Select shipments to pause", extra_buttons=[
                InlineKeyboardButton("Confirm Pause", callback_data=f"bulk_pause_confirm_{page}"),
                InlineKeyboardButton("Back", callback_data="menu_page_1")
            ])
        elif data.startswith("bulk_pause_select_"):
            tracking_number = data.split("_", 1)[1]
            chat_id = call.message.chat.id
            batch_key = f"bulk_pause:{chat_id}"
            selected = redis_client.smembers(batch_key) if redis_client else set()
            selected = set(selected)
            if tracking_number in selected:
                redis_client.srem(batch_key, tracking_number)
                bot.answer_callback_query(call.id, f"Deselected `{tracking_number}`.", show_alert=True)
            else:
                redis_client.sadd(batch_key, tracking_number)
                bot.answer_callback_query(call.id, f"Selected `{tracking_number}`.", show_alert=True)
            bot_logger.info(f"Toggled bulk pause selection for {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Toggled bulk pause selection for {tracking_number} by admin {call.from_user.id}[/info]")
            show_shipment_menu(call, page=1, prefix="bulk_pause_select", prompt="Select shipments to pause", extra_buttons=[
                InlineKeyboardButton("Confirm Pause", callback_data=f"bulk_pause_confirm_1"),
                InlineKeyboardButton("Back", callback_data="menu_page_1")
            ])
        elif data.startswith("bulk_pause_confirm_"):
            page = int(data.split("_")[-1])
            bulk_pause_shipments(call, page)
        elif data.startswith("bulk_resume_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="bulk_resume_select", prompt="Select shipments to resume", extra_buttons=[
                InlineKeyboardButton("Confirm Resume", callback_data=f"bulk_resume_confirm_{page}"),
                InlineKeyboardButton("Back", callback_data="menu_page_1")
            ])
        elif data.startswith("bulk_resume_select_"):
            tracking_number = data.split("_", 1)[1]
            chat_id = call.message.chat.id
            batch_key = f"bulk_resume:{chat_id}"
            selected = redis_client.smembers(batch_key) if redis_client else set()
            selected = set(selected)
            if tracking_number in selected:
                redis_client.srem(batch_key, tracking_number)
                bot.answer_callback_query(call.id, f"Deselected `{tracking_number}`.", show_alert=True)
            else:
                redis_client.sadd(batch_key, tracking_number)
                bot.answer_callback_query(call.id, f"Selected `{tracking_number}`.", show_alert=True)
            bot_logger.info(f"Toggled bulk resume selection for {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Toggled bulk resume selection for {tracking_number} by admin {call.from_user.id}[/info]")
            show_shipment_menu(call, page=1, prefix="bulk_resume_select", prompt="Select shipments to resume", extra_buttons=[
                InlineKeyboardButton("Confirm Resume", callback_data=f"bulk_resume_confirm_1"),
                InlineKeyboardButton("Back", callback_data="menu_page_1")
            ])
        elif data.startswith("bulk_resume_confirm_"):
            page = int(data.split("_")[-1])
            bulk_resume_shipments(call, page)
        elif data.startswith("set_webhook_"):
            tracking_number = data.split("_", 1)[1]
            markup = ForceReply(selective=True)
            bot.edit_message_text(f"Enter webhook URL for `{tracking_number}`:", chat_id=call.message.chat.id,
                                 message_id=call.message.message_id, reply_markup=markup)
            set_chat_data(call.message.chat.id, "state", f"set_webhook:{tracking_number}")
            bot_logger.info(f"Prompted set webhook for {tracking_number} by admin {call.from_user.id}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Prompted set webhook for {tracking_number} by admin {call.from_user.id}[/info]")
        elif data.startswith("set_webhook_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="set_webhook", prompt="Select a shipment to set webhook")
        elif data.startswith("test_webhook_"):
            tracking_number = data.split("_", 1)[1]
            send_manual_webhook(call, tracking_number)
        elif data.startswith("test_webhook_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="test_webhook", prompt="Select a shipment to test webhook")
        elif data.startswith("send_email_"):
            tracking_number = data.split("_", 1)[1]
            send_manual_email(call, tracking_number)
        elif data.startswith("notify_"):
            tracking_number = data.split("_", 1)[1]
            shipment = get_shipment_details(tracking_number)
            if not shipment:
                bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
                bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
                return
            markup = InlineKeyboardMarkup(row_width=2)
            if shipment.get('recipient_email') and shipment.get('email_notifications'):
                markup.add(InlineKeyboardButton("Send Email", callback_data=f"send_email_{tracking_number}"))
            if shipment.get('webhook_url'):
                markup.add(InlineKeyboardButton("Send Webhook", callback_data=f"send_webhook_{tracking_number}"))
            markup.add(InlineKeyboardButton("Back", callback_data="menu_page_1"))
            bot.edit_message_text(f"Select notification type for `{tracking_number}`:", chat_id=call.message.chat.id,
                                 message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
            bot_logger.info(f"Sent notification options for {tracking_number}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Sent notification options for {tracking_number} to admin {call.from_user.id}[/info]")
        elif data.startswith("list_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page=page, prefix="view", prompt="Select a shipment to view")
        elif data.startswith("list_active_"):
            page = int(data.split("_")[-1])
            db, Shipment, _, _, _, _, _ = get_app_modules()
            shipments = Shipment.query.filter(~Shipment.status.in_(['Delivered', 'Returned'])).with_entities(Shipment.tracking_number).order_by(Shipment.tracking_number).offset((page-1)*5).limit(5).all()
            total = Shipment.query.filter(~Shipment.status.in_(['Delivered', 'Returned'])).count()
            set_chat_data(call.message.chat.id, f"list_active_shipments", [s.tracking_number for s in shipments])
            markup = InlineKeyboardMarkup(row_width=1)
            for tn in shipments:
                markup.add(InlineKeyboardButton(tn.tracking_number, callback_data=f"view_{tn.tracking_number}"))
            if total > 5:
                nav_buttons = []
                if page > 1:
                    nav_buttons.append(InlineKeyboardButton("Previous", callback_data=f"list_active_{page-1}"))
                if page * 5 < total:
                    nav_buttons.append(InlineKeyboardButton("Next", callback_data=f"list_active_{page+1}"))
                markup.add(*nav_buttons)
            markup.add(InlineKeyboardButton("Back", callback_data="menu_page_1"))
            bot.edit_message_text(f"*Active Shipments* (Page {page}, {total} total):", chat_id=call.message.chat.id,
                                 message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
            bot_logger.info(f"Sent active shipments list, page {page}", extra={'tracking_number': ''})
            console.print(f"[info]Sent active shipments list, page {page} to admin {call.from_user.id}[/info]")
        elif data.startswith("list_paused_"):
            page = int(data.split("_")[-1])
            db, Shipment, _, _, _, _, _ = get_app_modules()
            paused = redis_client.hgetall("paused_simulations") if redis_client else {}
            shipments = Shipment.query.filter(Shipment.tracking_number.in_(paused.keys())).with_entities(Shipment.tracking_number).order_by(Shipment.tracking_number).offset((page-1)*5).limit(5).all()
            total = len(paused)
            set_chat_data(call.message.chat.id, f"list_paused_shipments", [s.tracking_number for s in shipments])
            markup = InlineKeyboardMarkup(row_width=1)
            for tn in shipments:
                markup.add(InlineKeyboardButton(tn.tracking_number, callback_data=f"view_{tn.tracking_number}"))
            if total > 5:
                nav_buttons = []
                if page > 1:
                    nav_buttons.append(InlineKeyboardButton("Previous", callback_data=f"list_paused_{page-1}"))
                if page * 5 < total:
                    nav_buttons.append(InlineKeyboardButton("Next", callback_data=f"list_paused_{page+1}"))
                markup.add(*nav_buttons)
            markup.add(InlineKeyboardButton("Back", callback_data="menu_page_1"))
            bot.edit_message_text(f"*Paused Shipments* (Page {page}, {total} total):", chat_id=call.message.chat.id,
                                 message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
            bot_logger.info(f"Sent paused shipments list, page {page}", extra={'tracking_number': ''})
            console.print(f"[info]Sent paused shipments list, page {page} to admin {call.from_user.id}[/info]")
        elif data.startswith("search_page_"):
            query, page = data.split("_")[2:4]
            page = int(page)
            shipments, total = search_shipments(query, page=page)
            if not shipments:
                bot.edit_message_text(f"No more shipments found for query: `{query}`", chat_id=call.message.chat.id,
                                     message_id=call.message.message_id, parse_mode='Markdown')
                bot_logger.debug(f"No more shipments for query: {query}, page {page}", extra={'tracking_number': ''})
                return
            set_chat_data(call.message.chat.id, f"search:{query}", shipments)
            markup = InlineKeyboardMarkup(row_width=1)
            for tn in shipments:
                markup.add(InlineKeyboardButton(tn, callback_data=f"view_{tn}"))
            if total > 5:
                nav_buttons = []
                if page > 1:
                    nav_buttons.append(InlineKeyboardButton("Previous", callback_data=f"search_page_{query}_{page-1}"))
                if page * 5 < total:
                    nav_buttons.append(InlineKeyboardButton("Next", callback_data=f"search_page_{query}_{page+1}"))
                markup.add(*nav_buttons)
            markup.add(InlineKeyboardButton("Back", callback_data="menu_page_1"))
            bot.edit_message_text(f"*Search Results for '{query}'* (Page {page}, {total} total):", chat_id=call.message.chat.id,
                                 message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
            bot_logger.info(f"Sent search results for query: {query}, page {page}", extra={'tracking_number': ''})
            console.print(f"[info]Sent search results for query '{query}', page {page} to admin {call.from_user.id}[/info]")
        else:
            bot.answer_callback_query(call.id, "Invalid action.", show_alert=True)
            bot_logger.warning(f"Invalid callback data: {data}", extra={'tracking_number': ''})
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error in callback query: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Error in callback query for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(content_types=['text'], func=lambda message: get_chat_data(message.chat.id, "state") is not None)
def handle_input(message):
    """Handle text input for states like add_shipment, update_shipment, set_speed, or set_webhook."""
    state = get_chat_data(message.chat.id, "state")
    if not state:
        return
    try:
        if state == "add_shipment":
            parts = shlex.split(message.text)
            if len(parts) < 3:
                bot.reply_to(message, "Please provide at least: tracking_number status delivery_location")
                bot_logger.warning("Invalid input for add shipment", extra={'tracking_number': ''})
                return
            tracking_number, status, delivery_location = parts[:3]
            recipient_email = parts[3] if len(parts) > 3 else ''
            origin_location = parts[4] if len(parts) > 4 else None
            webhook_url = parts[5] if len(parts) > 5 else None
            try:
                save_shipment(tracking_number, status, '', delivery_location, recipient_email, origin_location, webhook_url)
                bot.reply_to(message, f"Shipment `{tracking_number}` added successfully.", parse_mode='Markdown')
                bot_logger.info(f"Added shipment {tracking_number} by admin {message.from_user.id}", extra={'tracking_number': tracking_number})
                console.print(f"[info]Added shipment {tracking_number} by admin {message.from_user.id}[/info]")
            except ValueError as e:
                bot.reply_to(message, f"Error: {e}")
                bot_logger.error(f"Error adding shipment {tracking_number}: {e}", extra={'tracking_number': tracking_number})
                console.print(Panel(f"[error]Error adding shipment {tracking_number}: {e}[/error]", title="Validation Error", border_style="red"))
        elif state.startswith("update_shipment:"):
            tracking_number = state.split(":", 1)[1]
            parts = shlex.split(message.text)
            if len(parts) < 2:
                bot.reply_to(message, "Please provide at least: status delivery_location")
                bot_logger.warning(f"Invalid input for update shipment {tracking_number}", extra={'tracking_number': tracking_number})
                return
            status, delivery_location = parts[:2]
            recipient_email = parts[2] if len(parts) > 2 else ''
            origin_location = parts[3] if len(parts) > 3 else None
            webhook_url = parts[4] if len(parts) > 4 else None
            try:
                shipment = get_shipment_details(tracking_number)
                if not shipment:
                    bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
                    bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
                    return
                save_shipment(tracking_number, status, shipment['checkpoints'], delivery_location, recipient_email, origin_location, webhook_url)
                bot.reply_to(message, f"Shipment `{tracking_number}` updated successfully.", parse_mode='Markdown')
                bot_logger.info(f"Updated shipment {tracking_number} by admin {message.from_user.id}", extra={'tracking_number': tracking_number})
                console.print(f"[info]Updated shipment {tracking_number} by admin {message.from_user.id}[/info]")
            except ValueError as e:
                bot.reply_to(message, f"Error: {e}")
                bot_logger.error(f"Error updating shipment {tracking_number}: {e}", extra={'tracking_number': tracking_number})
                console.print(Panel(f"[error]Error updating shipment {tracking_number}: {e}[/error]", title="Validation Error", border_style="red"))
        elif state.startswith("set_speed:"):
            tracking_number = state.split(":", 1)[1]
            try:
                speed = float(message.text.strip())
                if speed < 0.1 or speed > 10:
                    bot.reply_to(message, "Speed must be between 0.1 and 10.0.")
                    bot_logger.warning(f"Invalid speed value: {speed}", extra={'tracking_number': tracking_number})
                    return
                shipment = get_shipment_details(tracking_number)
                if not shipment:
                    bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
                    bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
                    return
                if redis_client:
                    redis_client.hset("sim_speed_multipliers", tracking_number, str(speed))
                invalidate_cache(tracking_number)
                bot.reply_to(message, f"Simulation speed for `{tracking_number}` set to `{speed}x`.", parse_mode='Markdown')
                bot_logger.info(f"Set simulation speed for {tracking_number} to {speed}x", extra={'tracking_number': tracking_number})
                console.print(Panel(f"[info]Set simulation speed for {tracking_number} to {speed}x by admin {message.from_user.id}[/info]", title="Speed Updated", border_style="green"))
                try:
                    _, websocket_server, _, _ = get_config_values()
                    response = requests.get(f'{websocket_server}/broadcast/{tracking_number}', timeout=5)
                    if response.status_code != 204:
                        bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
                except requests.RequestException as e:
                    bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
                    console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
            except ValueError:
                bot.reply_to(message, "Speed must be a number between 0.1 and 10.0.")
                bot_logger.warning(f"Invalid speed format: {message.text}", extra={'tracking_number': tracking_number})
        elif state.startswith("set_webhook:"):
            tracking_number = state.split(":", 1)[1]
            webhook_url = message.text.strip()
            db, Shipment, _, _, _, validate_webhook_url, _ = get_app_modules()
            if not validate_webhook_url(webhook_url):
                bot.reply_to(message, "Invalid webhook URL.")
                bot_logger.warning(f"Invalid webhook URL: {webhook_url}", extra={'tracking_number': tracking_number})
                return
            try:
                shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
                if not shipment:
                    bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
                    bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
                    return
                shipment.webhook_url = webhook_url
                db.session.commit()
                invalidate_cache(tracking_number)
                bot.reply_to(message, f"Webhook URL set for `{tracking_number}` to `{webhook_url}`.", parse_mode='Markdown')
                bot_logger.info(f"Set webhook URL for {tracking_number} to {webhook_url}", extra={'tracking_number': tracking_number})
                console.print(f"[info]Set webhook URL for {tracking_number} to {webhook_url} by admin {message.from_user.id}[/info]")
            except SQLAlchemyError as e:
                db.session.rollback()
                bot.reply_to(message, f"Database error: {e}")
                bot_logger.error(f"Database error setting webhook for {tracking_number}: {e}", extra={'tracking_number': tracking_number})
                console.print(Panel(f"[error]Database error setting webhook for {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
        set_chat_data(message.chat.id, "state", None)
        send_dynamic_menu(message.chat.id)
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error handling input for state {state}: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Error handling input for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))
        set_chat_data(message.chat.id, "state", None)
        send_dynamic_menu(message.chat.id)

if __name__ == "__main__":
    try:
        cache_route_templates()
        bot.infinity_polling()
    except Exception as e:
        bot_logger.critical(f"Bot polling failed: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[critical]Bot polling failed: {e}[/critical]", title="Critical Error", border_style="red"))
