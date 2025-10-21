import os
import json
import logging
import signal
from datetime import datetime
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from telebot import TeleBot
from telebot.types import Update, InlineKeyboardMarkup, InlineKeyboardButton
from utils import (
    get_config, get_bot, is_admin, send_dynamic_menu, get_shipment_details,
    generate_unique_id, search_shipments, RATE_LIMIT_WINDOW, RATE_LIMIT_MAX,
    safe_redis_operation, redis_client, sanitize_tracking_number, enqueue_notification,
    validate_email, validate_location, validate_webhook_url, cleanup_resources,
    send_manual_email, get_shipment_list, save_shipment, update_shipment, get_app_modules
)
from rich.console import Console

# Logging setup
logger = logging.getLogger('flask_app')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
console = Console()

# Flask app setup
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

# Signal handler for graceful shutdown
def handle_shutdown(signum, frame):
    logger.info("Received shutdown signal, cleaning up resources")
    console.print("[info]Received shutdown signal, cleaning up resources[/info]")
    cleanup_resources()
    exit(0)

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

# Initialize database
with app.app_context():
    logger.info("Starting database initialization")
    console.print("[info]Starting database initialization[/info]")
    for attempt in range(1, 4):
        try:
            db.create_all()
            logger.info("Database initialized successfully, shipments table verified")
            console.print("[info]Database initialized, shipments table verified[/info]")
            break
        except Exception as e:
            logger.warning(f"Database initialization attempt {attempt} failed: {e}")
            console.print(f"[warning]Database initialization attempt {attempt} failed: {e}[/warning]")
            if attempt == 3:
                logger.error("Max database initialization attempts reached")
                console.print("[error]Max database initialization attempts reached[/error]")
                raise
            import time
            time.sleep(2)

# Rate limit decorator
def rate_limit(func):
    def wrapper(message):
        user_id = str(message.from_user.id)
        key = f"rate_limit:{user_id}"
        count = safe_redis_operation(redis_client.incr, key) if redis_client else 0
        if count == 1:
            safe_redis_operation(redis_client.expire, key, RATE_LIMIT_WINDOW)
        if count > RATE_LIMIT_MAX:
            bot.send_message(message.chat.id, "Rate limit exceeded. Please try again later.")
            logger.warning(f"Rate limit exceeded for user {user_id}")
            console.print(f"[warning]Rate limit exceeded for user {user_id}[/warning]")
            return
        return func(message)
    return wrapper

# Command handlers
@rate_limit
def get_my_id(message):
    """Handle /myid command to return the user's Telegram ID."""
    user_id = message.from_user.id
    bot.send_message(message.chat.id, f"Your Telegram user ID: `{user_id}`", parse_mode='Markdown')
    logger.info(f"User {user_id} requested their ID")
    console.print(f"[info]User {user_id} requested their ID[/info]")

@rate_limit
def send_menu(message):
    """Handle /start and /menu commands to display the admin menu."""
    user_id = message.from_user.id
    if not is_admin(user_id):
        bot.send_message(message.chat.id, "Access denied.")
        logger.warning(f"Access denied for user {user_id}")
        console.print(f"[warning]Access denied for user {user_id}[/warning]")
        return
    send_dynamic_menu(message.chat.id, page=1)
    logger.info(f"Menu sent to admin {user_id}")
    console.print(f"[info]Menu sent to admin {user_id}[/info]")

@rate_limit
def track_shipment(message):
    """Handle /track command to view shipment details with interactive controls."""
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /track <tracking_number>\nExample: /track TRK20231010120000ABC123")
        logger.warning("Invalid /track command format")
        console.print("[warning]Invalid /track command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.send_message(message.chat.id, "Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        paused = safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true" if redis_client else False
        speed = float(safe_redis_operation(redis_client.hget, "sim_speed_multipliers", tracking_number) or 1.0) if redis_client else 1.0
        response = (
            f"*Shipment*: `{tracking_number}`\n"
            f"*Status*: `{shipment['status']}`\n"
            f"*Paused*: `{paused}`\n"
            f"*Speed Multiplier*: `{speed}x`\n"
            f"*Delivery Location*: `{shipment['delivery_location']}`\n"
            f"*Checkpoints*: `{shipment.get('checkpoints', 'None')}`"
        )
        markup = InlineKeyboardMarkup(row_width=2)
        if shipment['status'] not in ['Delivered', 'Returned']:
            markup.add(
                InlineKeyboardButton("Pause" if not paused else "Resume", callback_data=f"{'pause' if not paused else 'resume'}_{tracking_number}_{1}"),
                InlineKeyboardButton("Set Speed", callback_data=f"setspeed_{tracking_number}")
            )
        markup.add(
            InlineKeyboardButton("Broadcast", callback_data=f"broadcast_{tracking_number}"),
            InlineKeyboardButton("Notify", callback_data=f"notify_{tracking_number}"),
            InlineKeyboardButton("Set Webhook", callback_data=f"set_webhook_{tracking_number}"),
            InlineKeyboardButton("Test Webhook", callback_data=f"test_webhook_{tracking_number}"),
            InlineKeyboardButton("Home", callback_data="menu_page_1")
        )
        bot.send_message(message.chat.id, response, parse_mode='Markdown', reply_markup=markup)
        logger.info(f"Sent tracking details for {tracking_number}")
        console.print(f"[info]Sent tracking details for {tracking_number}[/info]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in track command: {e}")
        console.print(f"[error]Error in track command: {e}[/error]")

@rate_limit
def system_stats(message):
    """Handle /stats command to display system statistics."""
    user_id = message.from_user.id
    if not is_admin(user_id):
        bot.send_message(message.chat.id, "Access denied.")
        logger.warning(f"Access denied for /stats by {user_id}")
        console.print(f"[warning]Access denied for /stats by {user_id}[/warning]")
        return
    try:
        total_shipments = Shipment.query.count()
        active_shipments = Shipment.query.filter(~Shipment.status.in_(['Delivered', 'Returned'])).count()
        paused_count = len(safe_redis_operation(redis_client.hgetall, "paused_simulations") or {}) if redis_client else 0
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
            InlineKeyboardButton("Home", callback_data="menu_page_1")
        )
        bot.send_message(message.chat.id, response, parse_mode='Markdown', reply_markup=markup)
        logger.info(f"Sent system stats to admin {user_id}")
        console.print(f"[info]Sent system stats to admin {user_id}[/info]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in stats command: {e}")
        console.print(f"[error]Error in stats command: {e}[/error]")

@rate_limit
def manual_notification(message):
    """Handle /notify command to send manual email or webhook notification."""
    user_id = message.from_user.id
    if not is_admin(user_id):
        bot.send_message(message.chat.id, "Access denied.")
        logger.warning(f"Access denied for /notify by {user_id}")
        console.print(f"[warning]Access denied for /notify by {user_id}[/warning]")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /notify <tracking_number>\nExample: /notify TRK20231010120000ABC123")
        logger.warning("Invalid /notify command format")
        console.print("[warning]Invalid /notify command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.send_message(message.chat.id, "Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        markup = InlineKeyboardMarkup(row_width=2)
        config = get_config()
        if shipment.get('recipient_email') and shipment.get('email_notifications'):
            markup.add(InlineKeyboardButton("Send Email", callback_data=f"send_email_{tracking_number}"))
        if shipment.get('webhook_url') or config.websocket_server:
            markup.add(InlineKeyboardButton("Send Webhook", callback_data=f"send_webhook_{tracking_number}"))
        markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
        bot.send_message(message.chat.id, f"Select notification type for `{tracking_number}`:", parse_mode='Markdown', reply_markup=markup)
        logger.info(f"Sent notification options for {tracking_number}")
        console.print(f"[info]Sent notification options for {tracking_number}[/info]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in notify command: {e}")
        console.print(f"[error]Error in notify command: {e}[/error]")

@rate_limit
def search_command(message):
    """Handle /search command to find shipments by query."""
    user_id = message.from_user.id
    if not is_admin(user_id):
        bot.send_message(message.chat.id, "Access denied.")
        logger.warning(f"Access denied for /search by {user_id}")
        console.print(f"[warning]Access denied for /search by {user_id}[/warning]")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /search <query>\nExample: /search Lagos")
        logger.warning("Invalid /search command format")
        console.print("[warning]Invalid /search command format[/warning]")
        return
    query = parts[1].strip()
    try:
        shipments, total = search_shipments(query, page=1)
        if not shipments:
            bot.send_message(message.chat.id, f"No shipments found for query: `{query}`", parse_mode='Markdown')
            logger.debug(f"No shipments found for query: {query}")
            console.print(f"[debug]No shipments found for query: {query}[/debug]")
            return
        markup = InlineKeyboardMarkup(row_width=1)
        for tn in shipments:
            markup.add(InlineKeyboardButton(tn, callback_data=f"view_{tn}"))
        if total > 5:
            markup.add(InlineKeyboardButton("Next", callback_data=f"search_page_{query}_2"))
        markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
        bot.send_message(message.chat.id, f"*Search Results for '{query}'* (Page 1, {total} total):", parse_mode='Markdown', reply_markup=markup)
        logger.info(f"Sent search results for query: {query}")
        console.print(f"[info]Sent search results for query: {query}[/info]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in search command: {e}")
        console.print(f"[error]Error in search command: {e}[/error]")

@rate_limit
def bulk_action_command(message):
    """Handle /bulk_action command to perform bulk operations."""
    user_id = message.from_user.id
    if not is_admin(user_id):
        bot.send_message(message.chat.id, "Access denied.")
        logger.warning(f"Access denied for /bulk_action by {user_id}")
        console.print(f"[warning]Access denied for /bulk_action by {user_id}[/warning]")
        return
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("Bulk Pause", callback_data="bulk_pause_menu_1"),
        InlineKeyboardButton("Bulk Resume", callback_data="bulk_resume_menu_1"),
        InlineKeyboardButton("Bulk Delete", callback_data="batch_delete_menu_1"),
        InlineKeyboardButton("Home", callback_data="menu_page_1")
    )
    bot.send_message(message.chat.id, "*Select bulk action*:", parse_mode='Markdown', reply_markup=markup)
    logger.info(f"Sent bulk action menu to admin {user_id}")
    console.print(f"[info]Sent bulk action menu to admin {user_id}[/info]")

@rate_limit
def stop_simulation(message):
    """Handle /stop command to pause a shipment's simulation."""
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /stop <tracking_number>\nExample: /stop TRK20231010120000ABC123")
        logger.warning("Invalid /stop command format")
        console.print("[warning]Invalid /stop command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.send_message(message.chat.id, "Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        if shipment.status in ['Delivered', 'Returned']:
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", parse_mode='Markdown')
            logger.warning(f"Cannot pause completed shipment: {tracking_number}")
            console.print(f"[warning]Cannot pause completed shipment: {tracking_number}[/warning]")
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true":
            bot.send_message(message.chat.id, f"Simulation for `{tracking_number}` is already paused.", parse_mode='Markdown')
            logger.warning(f"Simulation already paused: {tracking_number}")
            console.print(f"[warning]Simulation already paused: {tracking_number}[/warning]")
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
        invalidate_cache(tracking_number)
        logger.info(f"Paused simulation for {tracking_number}")
        console.print(f"[info]Paused simulation for {tracking_number}[/info]")
        bot.send_message(message.chat.id, f"Simulation paused for `{tracking_number}`.", parse_mode='Markdown')
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in stop command: {e}")
        console.print(f"[error]Error in stop command: {e}[/error]")

@rate_limit
def continue_simulation(message):
    """Handle /continue command to resume a paused shipment's simulation."""
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /continue <tracking_number>\nExample: /continue TRK20231010120000ABC123")
        logger.warning("Invalid /continue command format")
        console.print("[warning]Invalid /continue command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.send_message(message.chat.id, "Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        if shipment.status in ['Delivered', 'Returned']:
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", parse_mode='Markdown')
            logger.warning(f"Cannot resume completed shipment: {tracking_number}")
            console.print(f"[warning]Cannot resume completed shipment: {tracking_number}[/warning]")
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) != "true":
            bot.send_message(message.chat.id, f"Simulation for `{tracking_number}` is not paused.", parse_mode='Markdown')
            logger.warning(f"Simulation not paused: {tracking_number}")
            console.print(f"[warning]Simulation not paused: {tracking_number}[/warning]")
            return
        if redis_client:
            safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
        invalidate_cache(tracking_number)
        logger.info(f"Resumed simulation for {tracking_number}")
        console.print(f"[info]Resumed simulation for {tracking_number}[/info]")
        bot.send_message(message.chat.id, f"Simulation resumed for `{tracking_number}`.", parse_mode='Markdown')
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in continue command: {e}")
        console.print(f"[error]Error in continue command: {e}[/error]")

@rate_limit
def set_simulation_speed(message):
    """Handle /setspeed command to set simulation speed for a shipment."""
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.send_message(message.chat.id, "Usage: /setspeed <tracking_number> <speed>\nExample: /setspeed TRK20231010120000ABC123 2.0")
        logger.warning("Invalid /setspeed command format")
        console.print("[warning]Invalid /setspeed command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.send_message(message.chat.id, "Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        speed = float(parts[2].strip())
        if speed < 0.1 or speed > 10:
            bot.send_message(message.chat.id, "Speed must be between 0.1 and 10.0.")
            logger.warning(f"Invalid speed value: {speed}")
            console.print(f"[warning]Invalid speed value: {speed}[/warning]")
            return
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "sim_speed_multipliers", tracking_number, str(speed))
        invalidate_cache(tracking_number)
        logger.info(f"Set simulation speed for {tracking_number} to {speed}x")
        console.print(f"[info]Set simulation speed for {tracking_number} to {speed}x[/info]")
        bot.send_message(message.chat.id, f"Simulation speed set to `{speed}x` for `{tracking_number}`.", parse_mode='Markdown')
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in setspeed command: {e}")
        console.print(f"[error]Error in setspeed command: {e}[/error]")

@rate_limit
def handle_generate(message):
    """Handle /generate command to create a tracking ID."""
    tracking_id = generate_unique_id()
    bot.send_message(message.chat.id, f"Generated Tracking ID: `{tracking_id}`", parse_mode='Markdown')
    logger.info(f"Generated tracking ID {tracking_id} for user {message.from_user.id}")
    console.print(f"[info]Generated tracking ID {tracking_id} for user {message.from_user.id}[/info]")

@rate_limit
def list_shipments(message):
    """Handle /list command to display a paginated list of shipments."""
    user_id = message.from_user.id
    if not is_admin(user_id):
        bot.send_message(message.chat.id, "Access denied.")
        logger.warning(f"Access denied for /list by {user_id}")
        console.print(f"[warning]Access denied for /list by {user_id}[/warning]")
        return
    try:
        shipments, total = get_shipment_list(page=1)
        if not shipments:
            bot.send_message(message.chat.id, "No shipments available.", parse_mode='Markdown')
            logger.debug("No shipments available for /list")
            console.print("[debug]No shipments available for /list[/debug]")
            return
        markup = InlineKeyboardMarkup(row_width=1)
        for tn in shipments:
            markup.add(InlineKeyboardButton(tn, callback_data=f"view_{tn}"))
        if total > 5:
            markup.add(InlineKeyboardButton("Next", callback_data="list_2"))
        markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
        bot.send_message(message.chat.id, f"*Shipment List* (Page 1, {total} total):", parse_mode='Markdown', reply_markup=markup)
        logger.info(f"Sent shipment list to admin {user_id}")
        console.print(f"[info]Sent shipment list to admin {user_id}[/info]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in list command: {e}")
        console.print(f"[error]Error in list command: {e}[/error]")

@rate_limit
def add_shipment(message):
    """Handle /add command to create a new shipment."""
    user_id = message.from_user.id
    if not is_admin(user_id):
        bot.send_message(message.chat.id, "Access denied.")
        logger.warning(f"Access denied for /add by {user_id}")
        console.print(f"[warning]Access denied for /add by {user_id}[/warning]")
        return
    parts = message.text.strip().split(maxsplit=3)
    if len(parts) < 4:
        bot.send_message(
            message.chat.id,
            "Usage: /add <tracking_number> <status> <delivery_location> [recipient_email] [origin_location] [webhook_url]\n"
            "Example: /add TRK20231010120000ABC123 Pending 'Lagos, NG' user@example.com 'Abuja, NG' https://example.com"
        )
        logger.warning("Invalid /add command format")
        console.print("[warning]Invalid /add command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    status = parts[2].strip()
    delivery_location = parts[3].strip()
    args = message.text.split(maxsplit=4)[4:] if len(message.text.split()) > 4 else []
    recipient_email = args[0] if len(args) > 0 else None
    origin_location = args[1] if len(args) > 1 else None
    webhook_url = args[2] if len(args) > 2 else None
    try:
        config = get_config()
        if not tracking_number:
            bot.send_message(message.chat.id, "Invalid tracking number.")
            logger.error(f"Invalid tracking number: {parts[1]}")
            console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
            return
        if status not in config.valid_statuses:
            bot.send_message(message.chat.id, f"Invalid status. Must be one of: {', '.join(config.valid_statuses)}")
            logger.warning(f"Invalid status: {status}")
            console.print(f"[warning]Invalid status: {status}[/warning]")
            return
        if not validate_location(delivery_location) or (origin_location and not validate_location(origin_location)):
            bot.send_message(message.chat.id, "Invalid location.")
            logger.warning(f"Invalid location: {delivery_location} or {origin_location}")
            console.print(f"[warning]Invalid location: {delivery_location} or {origin_location}[/warning]")
            return
        if not validate_email(recipient_email):
            bot.send_message(message.chat.id, "Invalid email address.")
            logger.warning(f"Invalid email: {recipient_email}")
            console.print(f"[warning]Invalid email: {recipient_email}[/warning]")
            return
        if not validate_webhook_url(webhook_url):
            bot.send_message(message.chat.id, "Invalid webhook URL.")
            logger.warning(f"Invalid webhook URL: {webhook_url}")
            console.print(f"[warning]Invalid webhook URL: {webhook_url}[/warning]")
            return
        if save_shipment(tracking_number, status, '', delivery_location, recipient_email, origin_location, webhook_url):
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` added.", parse_mode='Markdown')
            logger.info(f"Added shipment {tracking_number} by admin {user_id}")
            console.print(f"[info]Added shipment {tracking_number} by admin {user_id}[/info]")
        else:
            bot.send_message(message.chat.id, "Failed to add shipment.")
            logger.error(f"Failed to add shipment {tracking_number}")
            console.print(f"[error]Failed to add shipment {tracking_number}[/error]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in add command: {e}")
        console.print(f"[error]Error in add command: {e}[/error]")

@rate_limit
def update_shipment_command(message):
    """Handle /update command to update shipment details."""
    user_id = message.from_user.id
    if not is_admin(user_id):
        bot.send_message(message.chat.id, "Access denied.")
        logger.warning(f"Access denied for /update by {user_id}")
        console.print(f"[warning]Access denied for /update by {user_id}[/warning]")
        return
    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        bot.send_message(
            message.chat.id,
            "Usage: /update <tracking_number> <field=value> [field=value ...]\n"
            "Example: /update TRK20231010120000ABC123 status=In_Transit delivery_location='Abuja, NG'"
        )
        logger.warning("Invalid /update command format")
        console.print("[warning]Invalid /update command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.send_message(message.chat.id, "Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        updates = {}
        for pair in parts[2].split():
            if '=' not in pair:
                bot.send_message(message.chat.id, f"Invalid field format: {pair}. Use field=value.")
                logger.warning(f"Invalid field format: {pair}")
                console.print(f"[warning]Invalid field format: {pair}[/warning]")
                return
            field, value = pair.split('=', 1)
            updates[field] = value
        config = get_config()
        if 'status' in updates and updates['status'] not in config.valid_statuses:
            bot.send_message(message.chat.id, f"Invalid status. Must be one of: {', '.join(config.valid_statuses)}")
            logger.warning(f"Invalid status: {updates['status']}")
            console.print(f"[warning]Invalid status: {updates['status']}[/warning]")
            return
        if 'delivery_location' in updates and not validate_location(updates['delivery_location']):
            bot.send_message(message.chat.id, "Invalid delivery location.")
            logger.warning(f"Invalid delivery location: {updates['delivery_location']}")
            console.print(f"[warning]Invalid delivery location: {updates['delivery_location']}[/warning]")
            return
        if 'origin_location' in updates and updates['origin_location'] and not validate_location(updates['origin_location']):
            bot.send_message(message.chat.id, "Invalid origin location.")
            logger.warning(f"Invalid origin location: {updates['origin_location']}")
            console.print(f"[warning]Invalid origin location: {updates['origin_location']}[/warning]")
            return
        if 'recipient_email' in updates and updates['recipient_email'] and not validate_email(updates['recipient_email']):
            bot.send_message(message.chat.id, "Invalid email address.")
            logger.warning(f"Invalid email: {updates['recipient_email']}")
            console.print(f"[warning]Invalid email: {updates['recipient_email']}[/warning]")
            return
        if 'webhook_url' in updates and updates['webhook_url'] and not validate_webhook_url(updates['webhook_url']):
            bot.send_message(message.chat.id, "Invalid webhook URL.")
            logger.warning(f"Invalid webhook URL: {updates['webhook_url']}")
            console.print(f"[warning]Invalid webhook URL: {updates['webhook_url']}[/warning]")
            return
        if update_shipment(
            tracking_number,
            status=updates.get('status'),
            delivery_location=updates.get('delivery_location'),
            recipient_email=updates.get('recipient_email'),
            origin_location=updates.get('origin_location'),
            webhook_url=updates.get('webhook_url')
        ):
            if 'status' in updates or 'delivery_location' in updates:
                shipment = get_shipment_details(tracking_number)
                if shipment.get('recipient_email') and shipment.get('email_notifications'):
                    notification_data = {
                        "tracking_number": tracking_number,
                        "type": "email",
                        "data": {
                            "recipient_email": shipment['recipient_email'],
                            "status": shipment['status'],
                            "checkpoints": shipment.get('checkpoints', ''),
                            "delivery_location": shipment['delivery_location']
                        }
                    }
                    enqueue_notification(notification_data)
                if shipment.get('webhook_url') or config.websocket_server:
                    notification_data = {
                        "tracking_number": tracking_number,
                        "type": "webhook",
                        "data": {
                            "status": shipment['status'],
                            "checkpoints": shipment.get('checkpoints', ''),
                            "delivery_location": shipment['delivery_location'],
                            "webhook_url": shipment.get('webhook_url') or config.websocket_server
                        }
                    }
                    enqueue_notification(notification_data)
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` updated.", parse_mode='Markdown')
            logger.info(f"Updated shipment {tracking_number} by admin {user_id}")
            console.print(f"[info]Updated shipment {tracking_number} by admin {user_id}[/info]")
        else:
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in update command: {e}")
        console.print(f"[error]Error in update command: {e}[/error]")

@rate_limit
def export_shipments_command(message):
    """Handle /export command to export shipment data as JSON."""
    user_id = message.from_user.id
    if not is_admin(user_id):
        bot.send_message(message.chat.id, "Access denied.")
        logger.warning(f"Access denied for /export by {user_id}")
        console.print(f"[warning]Access denied for /export by {user_id}[/warning]")
        return
    try:
        shipments = Shipment.query.all()
        export_data = [
            {
                "tracking_number": s.tracking_number,
                "status": s.status,
                "delivery_location": s.delivery_location,
                "recipient_email": s.recipient_email,
                "origin_location": s.origin_location,
                "webhook_url": s.webhook_url,
                "checkpoints": s.checkpoints,
                "created_at": s.created_at.isoformat(),
                "last_updated": s.last_updated.isoformat(),
                "email_notifications": s.email_notifications
            } for s in shipments
        ]
        export_json = json.dumps(export_data, indent=2)
        if not export_data:
            bot.send_message(message.chat.id, "No shipments to export or error occurred.", parse_mode='Markdown')
            logger.warning("Failed to export shipments")
            console.print("[warning]Failed to export shipments[/warning]")
            return
        max_length = 4096
        if len(export_json) <= max_length:
            bot.send_message(message.chat.id, f"```json\n{export_json}\n```", parse_mode='Markdown')
        else:
            parts = [export_json[i:i+max_length] for i in range(0, len(export_json), max_length)]
            for i, part in enumerate(parts, 1):
                bot.send_message(message.chat.id, f"```json\nPart {i}/{len(parts)}:\n{part}\n```", parse_mode='Markdown')
        logger.info(f"Exported shipments for admin {user_id}")
        console.print(f"[info]Exported shipments for admin {user_id}[/info]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in export command: {e}")
        console.print(f"[error]Error in export command: {e}[/error]")

@rate_limit
def get_logs_command(message):
    """Handle /logs command to retrieve recent bot logs."""
    user_id = message.from_user.id
    if not is_admin(user_id):
        bot.send_message(message.chat.id, "Access denied.")
        logger.warning(f"Access denied for /logs by {user_id}")
        console.print(f"[warning]Access denied for /logs by {user_id}[/warning]")
        return
    try:
        logs = [
            f"{datetime.utcnow().isoformat()} - flask_app - INFO - Sample log entry {i}"
            for i in range(1, 6)
        ]
        response = "*Recent Logs*:\n" + "\n".join([f"`{log}`" for log in logs])
        bot.send_message(message.chat.id, response, parse_mode='Markdown')
        logger.info(f"Sent recent logs to admin {user_id}")
        console.print(f"[info]Sent recent logs to admin {user_id}[/info]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error in logs command: {e}")
        console.print(f"[error]Error in logs command: {e}[/error]")

def delete_shipment_callback(callback_query, tracking_number, page):
    """Delete a shipment and update the menu."""
    logger.warning("delete_shipment_callback not implemented: requires show_shipment_menu")
    console.print("[warning]delete_shipment_callback not implemented: requires show_shipment_menu[/warning]")
    bot.answer_callback_query(callback_query.id, "Function not implemented: show_shipment_menu missing.", show_alert=True)

def toggle_batch_selection(callback_query, tracking_number):
    """Toggle a shipment's selection for batch operations."""
    if redis_client:
        try:
            selected = safe_redis_operation(redis_client.sismember, "batch_selected", tracking_number)
            if selected:
                safe_redis_operation(redis_client.srem, "batch_selected", tracking_number)
                bot.answer_callback_query(callback_query.id, f"Deselected {tracking_number}")
            else:
                safe_redis_operation(redis_client.sadd, "batch_selected", tracking_number)
                bot.answer_callback_query(callback_query.id, f"Selected {tracking_number}")
            logger.info(f"Toggled batch selection for {tracking_number}")
            console.print(f"[info]Toggled batch selection for {tracking_number}[/info]")
        except Exception as e:
            bot.answer_callback_query(callback_query.id, f"Error: {str(e)}", show_alert=True)
            logger.error(f"Error toggling batch selection for {tracking_number}: {e}")
            console.print(f"[error]Error toggling batch selection for {tracking_number}: {e}[/error]")

def batch_delete_shipments(callback_query, page):
    """Delete selected shipments in batch."""
    logger.warning("batch_delete_shipments not implemented: requires show_shipment_menu")
    console.print("[warning]batch_delete_shipments not implemented: requires show_shipment_menu[/warning]")
    bot.answer_callback_query(callback_query.id, "Function not implemented: show_shipment_menu missing.", show_alert=True)

def trigger_broadcast(callback_query, tracking_number):
    """Trigger a broadcast for a shipment (placeholder)."""
    bot.answer_callback_query(callback_query.id, f"Broadcast for {tracking_number} not implemented.", show_alert=True)
    logger.info(f"Attempted broadcast for {tracking_number}")
    console.print(f"[info]Attempted broadcast for {tracking_number}[/info]")

def toggle_email_notifications(callback_query, tracking_number, page):
    """Toggle email notifications for a shipment."""
    logger.warning("toggle_email_notifications not implemented: requires show_shipment_menu")
    console.print("[warning]toggle_email_notifications not implemented: requires show_shipment_menu[/warning]")
    bot.answer_callback_query(callback_query.id, "Function not implemented: show_shipment_menu missing.", show_alert=True)

def pause_simulation_callback(callback_query, tracking_number, page):
    """Pause a shipment's simulation."""
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.answer_callback_query(callback_query.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        if shipment.status in ['Delivered', 'Returned']:
            bot.answer_callback_query(callback_query.id, f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", show_alert=True)
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true":
            bot.answer_callback_query(callback_query.id, f"Simulation for `{tracking_number}` is already paused.", show_alert=True)
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
        invalidate_cache(tracking_number)
        bot.answer_callback_query(callback_query.id, f"Simulation paused for `{tracking_number}`.")
        logger.info(f"Paused simulation for {tracking_number}")
        console.print(f"[info]Paused simulation for {tracking_number}[/info]")
    except Exception as e:
        bot.answer_callback_query(callback_query.id, f"Error: {str(e)}", show_alert=True)
        logger.error(f"Error pausing simulation for {tracking_number}: {e}")
        console.print(f"[error]Error pausing simulation for {tracking_number}: {e}[/error]")

def resume_simulation_callback(callback_query, tracking_number, page):
    """Resume a shipment's simulation."""
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.answer_callback_query(callback_query.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        if shipment.status in ['Delivered', 'Returned']:
            bot.answer_callback_query(callback_query.id, f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", show_alert=True)
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) != "true":
            bot.answer_callback_query(callback_query.id, f"Simulation for `{tracking_number}` is not paused.", show_alert=True)
            return
        if redis_client:
            safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
        invalidate_cache(tracking_number)
        bot.answer_callback_query(callback_query.id, f"Simulation resumed for `{tracking_number}`.")
        logger.info(f"Resumed simulation for {tracking_number}")
        console.print(f"[info]Resumed simulation for {tracking_number}[/info]")
    except Exception as e:
        bot.answer_callback_query(callback_query.id, f"Error: {str(e)}", show_alert=True)
        logger.error(f"Error resuming simulation for {tracking_number}: {e}")
        console.print(f"[error]Error resuming simulation for {tracking_number}: {e}[/error]")

def show_simulation_speed(callback_query, tracking_number):
    """Show the simulation speed for a shipment."""
    try:
        speed = float(safe_redis_operation(redis_client.hget, "sim_speed_multipliers", tracking_number) or 1.0)
        bot.answer_callback_query(callback_query.id, f"Simulation speed for `{tracking_number}`: {speed}x", show_alert=True)
        logger.info(f"Displayed simulation speed for {tracking_number}")
        console.print(f"[info]Displayed simulation speed for {tracking_number}[/info]")
    except Exception as e:
        bot.answer_callback_query(callback_query.id, f"Error: {str(e)}", show_alert=True)
        logger.error(f"Error showing simulation speed for {tracking_number}: {e}")
        console.print(f"[error]Error showing simulation speed for {tracking_number}: {e}[/error]")

def bulk_pause_shipments(callback_query, page):
    """Pause selected shipments in batch."""
    logger.warning("bulk_pause_shipments not implemented: requires show_shipment_menu")
    console.print("[warning]bulk_pause_shipments not implemented: requires show_shipment_menu[/warning]")
    bot.answer_callback_query(callback_query.id, "Function not implemented: show_shipment_menu missing.", show_alert=True)

def bulk_resume_shipments(callback_query, page):
    """Resume selected shipments in batch."""
    logger.warning("bulk_resume_shipments not implemented: requires show_shipment_menu")
    console.print("[warning]bulk_resume_shipments not implemented: requires show_shipment_menu[/warning]")
    bot.answer_callback_query(callback_query.id, "Function not implemented: show_shipment_menu missing.", show_alert=True)

def handle_add_shipment(message):
    """Handle adding a new shipment via callback."""
    parts = message.text.strip().split()
    if len(parts) < 3:
        bot.send_message(
            message.chat.id,
            "Usage: tracking_number status delivery_location [recipient_email] [origin_location] [webhook_url]\n"
            "Example: TRK20231010120000ABC123 Pending 'Lagos, NG' user@example.com 'Abuja, NG' https://example.com"
        )
        logger.warning("Invalid add shipment input")
        console.print("[warning]Invalid add shipment input[/warning]")
        return
    tracking_number, status, delivery_location = parts[:3]
    recipient_email = parts[3] if len(parts) > 3 else None
    origin_location = parts[4] if len(parts) > 4 else None
    webhook_url = parts[5] if len(parts) > 5 else None
    try:
        config = get_config()
        if not validate_email(recipient_email):
            bot.send_message(message.chat.id, "Invalid email address.")
            logger.warning(f"Invalid email: {recipient_email}")
            console.print(f"[warning]Invalid email: {recipient_email}[/warning]")
            return
        if not validate_location(delivery_location) or (origin_location and not validate_location(origin_location)):
            bot.send_message(message.chat.id, "Invalid location.")
            logger.warning(f"Invalid location: {delivery_location} or {origin_location}")
            console.print(f"[warning]Invalid location: {delivery_location} or {origin_location}[/warning]")
            return
        if not validate_webhook_url(webhook_url):
            bot.send_message(message.chat.id, "Invalid webhook URL.")
            logger.warning(f"Invalid webhook URL: {webhook_url}")
            console.print(f"[warning]Invalid webhook URL: {webhook_url}[/warning]")
            return
        if status not in config.valid_statuses:
            bot.send_message(message.chat.id, f"Invalid status. Must be one of: {', '.join(config.valid_statuses)}")
            logger.warning(f"Invalid status: {status}")
            console.print(f"[warning]Invalid status: {status}[/warning]")
            return
        if save_shipment(tracking_number, status, '', delivery_location, recipient_email, origin_location, webhook_url):
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` added.", parse_mode='Markdown')
            logger.info(f"Added shipment {tracking_number} by admin {message.from_user.id}")
            console.print(f"[info]Added shipment {tracking_number} by admin {message.from_user.id}[/info]")
        else:
            bot.send_message(message.chat.id, "Failed to add shipment.")
            logger.error(f"Failed to add shipment {tracking_number}")
            console.print(f"[error]Failed to add shipment {tracking_number}[/error]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error adding shipment: {e}")
        console.print(f"[error]Error adding shipment: {e}[/error]")

def handle_set_speed(message, tracking_number):
    """Handle setting simulation speed for a shipment."""
    try:
        speed = float(message.text.strip())
        if speed < 0.1 or speed > 10:
            bot.send_message(message.chat.id, "Speed must be between 0.1 and 10.0.")
            logger.warning(f"Invalid speed value: {speed}")
            console.print(f"[warning]Invalid speed value: {speed}[/warning]")
            return
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "sim_speed_multipliers", tracking_number, str(speed))
        invalidate_cache(tracking_number)
        bot.send_message(message.chat.id, f"Simulation speed set to `{speed}x` for `{tracking_number}`.", parse_mode='Markdown')
        logger.info(f"Set simulation speed for {tracking_number} to {speed}x")
        console.print(f"[info]Set simulation speed for {tracking_number} to {speed}x[/info]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error setting speed: {e}")
        console.print(f"[error]Error setting speed: {e}[/error]")

def handle_set_webhook(message, tracking_number):
    """Handle setting webhook URL for a shipment."""
    webhook_url = message.text.strip()
    try:
        if not validate_webhook_url(webhook_url):
            bot.send_message(message.chat.id, "Invalid webhook URL.")
            logger.warning(f"Invalid webhook URL: {webhook_url}")
            console.print(f"[warning]Invalid webhook URL: {webhook_url}[/warning]")
            return
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.send_message(message.chat.id, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        shipment.webhook_url = webhook_url
        db.session.commit()
        invalidate_cache(tracking_number)
        bot.send_message(message.chat.id, f"Webhook URL set for `{tracking_number}`.", parse_mode='Markdown')
        logger.info(f"Set webhook URL for {tracking_number}")
        console.print(f"[info]Set webhook URL for {tracking_number}[/info]")
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {str(e)}")
        logger.error(f"Error setting webhook: {e}")
        console.print(f"[error]Error setting webhook: {e}[/error]")

def handle_callback(callback_query):
    """Handle all callback queries from inline buttons."""
    data = callback_query.data
    try:
        if data.startswith("menu_page_"):
            page = int(data.split("_")[-1])
            send_dynamic_menu(callback_query.message.chat.id, callback_query.message.message_id, page)
        elif data.startswith("view_"):
            tracking_number = data.split("_", 1)[1]
            shipment = get_shipment_details(tracking_number)
            if not shipment:
                bot.answer_callback_query(callback_query.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
                return
            paused = safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true" if redis_client else False
            speed = float(safe_redis_operation(redis_client.hget, "sim_speed_multipliers", tracking_number) or 1.0) if redis_client else 1.0
            response = (
                f"*Shipment*: `{tracking_number}`\n"
                f"*Status*: `{shipment['status']}`\n"
                f"*Paused*: `{paused}`\n"
                f"*Speed Multiplier*: `{speed}x`\n"
                f"*Delivery Location*: `{shipment['delivery_location']}`\n"
                f"*Checkpoints*: `{shipment.get('checkpoints', 'None')}`"
            )
            markup = InlineKeyboardMarkup(row_width=2)
            if shipment['status'] not in ['Delivered', 'Returned']:
                markup.add(
                    InlineKeyboardButton("Pause" if not paused else "Resume", callback_data=f"{'pause' if not paused else 'resume'}_{tracking_number}_{1}"),
                    InlineKeyboardButton("Set Speed", callback_data=f"setspeed_{tracking_number}")
                )
            markup.add(
                InlineKeyboardButton("Broadcast", callback_data=f"broadcast_{tracking_number}"),
                InlineKeyboardButton("Notify", callback_data=f"notify_{tracking_number}"),
                InlineKeyboardButton("Set Webhook", callback_data=f"set_webhook_{tracking_number}"),
                InlineKeyboardButton("Test Webhook", callback_data=f"test_webhook_{tracking_number}"),
                InlineKeyboardButton("Home", callback_data="menu_page_1")
            )
            bot.edit_message_text(response, chat_id=callback_query.message.chat.id,
                                 message_id=callback_query.message.message_id,
                                 parse_mode='Markdown', reply_markup=markup)
            logger.info(f"Displayed details for {tracking_number}")
            console.print(f"[info]Displayed details for {tracking_number}[/info]")
        elif data.startswith("delete_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            delete_shipment_callback(callback_query, tracking_number, page)
        elif data.startswith("batch_select_"):
            tracking_number = data.split("_", 2)[2]
            toggle_batch_selection(callback_query, tracking_number)
        elif data.startswith("batch_delete_confirm_"):
            page = int(data.split("_")[-1])
            batch_delete_shipments(callback_query, page)
        elif data.startswith("broadcast_"):
            tracking_number = data.split("_", 1)[1]
            trigger_broadcast(callback_query, tracking_number)
        elif data.startswith("notify_"):
            tracking_number = data.split("_", 1)[1]
            shipment = get_shipment_details(tracking_number)
            if not shipment:
                bot.answer_callback_query(callback_query.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
                return
            markup = InlineKeyboardMarkup(row_width=2)
            config = get_config()
            if shipment.get('recipient_email') and shipment.get('email_notifications'):
                markup.add(InlineKeyboardButton("Send Email", callback_data=f"send_email_{tracking_number}"))
            if shipment.get('webhook_url') or config.websocket_server:
                markup.add(InlineKeyboardButton("Send Webhook", callback_data=f"send_webhook_{tracking_number}"))
            markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
            bot.edit_message_text(f"Select notification type for `{tracking_number}`:", chat_id=callback_query.message.chat.id,
                                 message_id=callback_query.message.message_id, parse_mode='Markdown', reply_markup=markup)
            logger.info(f"Sent notification options for {tracking_number}")
            console.print(f"[info]Sent notification options for {tracking_number}[/info]")
        elif data.startswith("send_email_"):
            tracking_number = data.split("_", 2)[2]
            success, message = send_manual_email(tracking_number)
            bot.answer_callback_query(callback_query.id, message, show_alert=True)
        elif data.startswith("send_webhook_"):
            tracking_number = data.split("_", 2)[2]
            logger.warning("send_manual_webhook not implemented")
            console.print("[warning]send_manual_webhook not implemented[/warning]")
            bot.answer_callback_query(callback_query.id, "Webhook sending not implemented.", show_alert=True)
        elif data.startswith("toggle_email_menu_"):
            page = int(data.split("_")[-1])
            toggle_email_notifications(callback_query, None, page)
        elif data.startswith("toggle_email_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            toggle_email_notifications(callback_query, tracking_number, page)
        elif data.startswith("pause_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            pause_simulation_callback(callback_query, tracking_number, page)
        elif data.startswith("resume_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            resume_simulation_callback(callback_query, tracking_number, page)
        elif data.startswith("setspeed_"):
            tracking_number = data.split("_", 1)[1]
            bot.answer_callback_query(callback_query.id, f"Enter speed for `{tracking_number}` (0.1 to 10.0):", show_alert=True)
            bot.register_next_step_handler(callback_query.message, lambda m: handle_set_speed(m, tracking_number))
        elif data.startswith("getspeed_"):
            tracking_number = data.split("_", 1)[1]
            show_simulation_speed(callback_query, tracking_number)
        elif data.startswith("bulk_pause_menu_"):
            page = int(data.split("_")[-1])
            bulk_pause_shipments(callback_query, page)
        elif data.startswith("bulk_resume_menu_"):
            page = int(data.split("_")[-1])
            bulk_resume_shipments(callback_query, page)
        elif data.startswith("bulk_pause_confirm_"):
            page = int(data.split("_")[-1])
            bulk_pause_shipments(callback_query, page)
        elif data.startswith("bulk_resume_confirm_"):
            page = int(data.split("_")[-1])
            bulk_resume_shipments(callback_query, page)
        elif data == "generate_id":
            new_id = generate_unique_id()
            bot.answer_callback_query(callback_query.id, f"Generated ID: `{new_id}`", show_alert=True)
        elif data == "add":
            bot.answer_callback_query(callback_query.id,
                                     "Enter shipment details (tracking_number status delivery_location [recipient_email] [origin_location] [webhook_url]):",
                                     show_alert=True)
            bot.register_next_step_handler(callback_query.message, handle_add_shipment)
        elif data.startswith("set_webhook_"):
            tracking_number = data.split("_", 2)[2]
            bot.answer_callback_query(callback_query.id, f"Enter webhook URL for `{tracking_number}`:", show_alert=True)
            bot.register_next_step_handler(callback_query.message, lambda m: handle_set_webhook(m, tracking_number))
        elif data.startswith("test_webhook_"):
            tracking_number = data.split("_", 2)[2]
            logger.warning("send_manual_webhook not implemented")
            console.print("[warning]send_manual_webhook not implemented[/warning]")
            bot.answer_callback_query(callback_query.id, "Webhook testing not implemented.", show_alert=True)
        elif data.startswith("list_"):
            page = int(data.split("_")[-1])
            logger.warning("show_shipment_menu not implemented")
            console.print("[warning]show_shipment_menu not implemented[/warning]")
            bot.answer_callback_query(callback_query.id, "Function not implemented: show_shipment_menu missing.", show_alert=True)
        elif data.startswith("delete_menu_"):
            page = int(data.split("_")[-1])
            logger.warning("show_shipment_menu not implemented")
            console.print("[warning]show_shipment_menu not implemented[/warning]")
            bot.answer_callback_query(callback_query.id, "Function not implemented: show_shipment_menu missing.", show_alert=True)
        elif data.startswith("batch_delete_menu_"):
            page = int(data.split("_")[-1])
            logger.warning("show_shipment_menu not implemented")
            console.print("[warning]show_shipment_menu not implemented[/warning]")
            bot.answer_callback_query(callback_query.id, "Function not implemented: show_shipment_menu missing.", show_alert=True)
        elif data.startswith("broadcast_menu_"):
            page = int(data.split("_")[-1])
            logger.warning("show_shipment_menu not implemented")
            console.print("[warning]show_shipment_menu not implemented[/warning]")
            bot.answer_callback_query(callback_query.id, "Function not implemented: show_shipment_menu missing.", show_alert=True)
        elif data.startswith("setspeed_menu_"):
            page = int(data.split("_")[-1])
            logger.warning("show_shipment_menu not implemented")
            console.print("[warning]show_shipment_menu not implemented[/warning]")
            bot.answer_callback_query(callback_query.id, "Function not implemented: show_shipment_menu missing.", show_alert=True)
        elif data.startswith("getspeed_menu_"):
            page = int(data.split("_")[-1])
            logger.warning("show_shipment_menu not implemented")
            console.print("[warning]show_shipment_menu not implemented[/warning]")
            bot.answer_callback_query(callback_query.id, "Function not implemented: show_shipment_menu missing.", show_alert=True)
        elif data == "settings":
            bot.edit_message_text("Settings: Not implemented yet.", chat_id=callback_query.message.chat.id,
                                 message_id=callback_query.message.message_id)
        elif data == "help":
            bot.edit_message_text(
                "*Help Menu*\n"
                "Available commands:\n"
                "/start or /menu - Show main menu\n"
                "/myid - Get your Telegram ID\n"
                "/track <tracking_number> - Track a shipment\n"
                "/stats - View system statistics\n"
                "/notify <tracking_number> - Send manual notification\n"
                "/search <query> - Search shipments\n"
                "/bulk_action - Perform bulk operations\n"
                "/stop <tracking_number> - Pause simulation\n"
                "/continue <tracking_number> - Resume simulation\n"
                "/setspeed <tracking_number> <speed> - Set simulation speed\n"
                "/generate - Generate a tracking ID\n"
                "/list - List all shipments\n"
                "/add <tracking_number> <status> <delivery_location> [recipient_email] [origin_location] [webhook_url] - Add a new shipment\n"
                "/update <tracking_number> <field=value> [field=value ...] - Update shipment details\n"
                "/export - Export all shipments as JSON\n"
                "/logs - View recent bot logs\n"
                "Example: /track TRK20231010120000ABC123\n"
                "Example: /add TRK20231010120000ABC123 Pending 'Lagos, NG' user@example.com\n"
                "Example: /update TRK20231010120000ABC123 status=In_Transit delivery_location='Abuja, NG'",
                chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id,
                parse_mode='Markdown', reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("Home", callback_data="menu_page_1")))
        bot.answer_callback_query(callback_query.id)
        logger.info(f"Processed callback {data} from user {callback_query.from_user.id}")
        console.print(f"[info]Processed callback {data} from user {callback_query.from_user.id}[/info]")
    except Exception as e:
        bot.answer_callback_query(callback_query.id, f"Error: {str(e)}", show_alert=True)
        logger.error(f"Error in callback handler: {e}")
        console.print(f"[error]Error in callback handler: {e}[/error]")

# Flask routes
@app.route('/')
def index():
    """Serve the index page."""
    logger.info("Serving index page")
    console.print("[info]Serving index page[/info]")
    return jsonify({"status": "ok", "message": "Signment Tracking Service is running"})

@app.route('/telegram/webhook', methods=['POST'])
def webhook():
    """Handle incoming Telegram webhook updates."""
    try:
        update = Update.de_json(request.get_json(), bot)
        bot.process_update(update)
        logger.info("Processed webhook update")
        console.print("[info]Processed webhook update[/info]")
        return jsonify({"status": "ok"})
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        console.print(f"[error]Error processing webhook: {e}[/error]")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health')
def health_check():
    """Health check endpoint."""
    try:
        db.session.execute(db.text("SELECT 1"))
        redis_client.ping() if redis_client else None
        logger.info("Health check passed")
        console.print("[info]Health check passed[/info]")
        return jsonify({"status": "healthy", "database": "ok", "redis": "ok"})
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        console.print(f"[error]Health check failed: {e}[/error]")
        return jsonify({"status": "unhealthy", "message": str(e)}), 500

# Bot setup
bot = get_bot()
bot.message_handler(commands=['myid'])(get_my_id)
bot.message_handler(commands=['start', 'menu'])(send_menu)
bot.message_handler(commands=['track'])(track_shipment)
bot.message_handler(commands=['stats'])(system_stats)
bot.message_handler(commands=['notify'])(manual_notification)
bot.message_handler(commands=['search'])(search_command)
bot.message_handler(commands=['bulk_action'])(bulk_action_command)
bot.message_handler(commands=['stop'])(stop_simulation)
bot.message_handler(commands=['continue'])(continue_simulation)
bot.message_handler(commands=['setspeed'])(set_simulation_speed)
bot.message_handler(commands=['generate'])(handle_generate)
bot.message_handler(commands=['list'])(list_shipments)
bot.message_handler(commands=['add'])(add_shipment)
bot.message_handler(commands=['update'])(update_shipment_command)
bot.message_handler(commands=['export'])(export_shipments_command)
bot.message_handler(commands=['logs'])(get_logs_command)
bot.callback_query_handler(func=lambda call: True)(handle_callback)

def set_webhook():
    """Set the Telegram webhook."""
    config = get_config()
    webhook_url = config.webhook_url
    try:
        bot.remove_webhook()
        bot.set_webhook(url=webhook_url)
        logger.info(f"Webhook set to {webhook_url}")
        console.print(f"[info]Webhook set to {webhook_url}[/info]")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")
        console.print(f"[error]Failed to set webhook: {e}[/error]")

if __name__ == "__main__":
    with app.app_context():
        set_webhook()
        logger.info("Bot started with webhook mode")
        console.print("[info]Bot started with webhook mode[/info]")
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
