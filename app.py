import os
import json
import logging
import signal
from datetime import datetime
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from telegram import Update
from utils import (
    get_config, get_bot, is_admin, send_dynamic_menu, get_shipment_details,
    generate_unique_id, search_shipments, RATE_LIMIT_WINDOW, RATE_LIMIT_MAX,
    safe_redis_operation, redis_client, sanitize_tracking_number, enqueue_notification,
    validate_email, validate_location, validate_webhook_url, cleanup_resources,
    send_manual_email, send_manual_webhook, show_shipment_menu, get_shipment_list,
    save_shipment, update_shipment, get_app_modules
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
    async def wrapper(update, context):
        user_id = str(update.effective_user.id)
        key = f"rate_limit:{user_id}"
        count = safe_redis_operation(redis_client.incr, key) if redis_client else 0
        if count == 1:
            safe_redis_operation(redis_client.expire, key, RATE_LIMIT_WINDOW)
        if count > RATE_LIMIT_MAX:
            await update.message.reply_text("Rate limit exceeded. Please try again later.")
            logger.warning(f"Rate limit exceeded for user {user_id}")
            console.print(f"[warning]Rate limit exceeded for user {user_id}[/warning]")
            return
        return await func(update, context)
    return wrapper

# Command handlers
@rate_limit
async def get_my_id(update, context):
    """Handle /myid command to return the user's Telegram ID."""
    user_id = update.effective_user.id
    await update.message.reply_text(f"Your Telegram user ID: `{user_id}`", parse_mode='Markdown')
    logger.info(f"User {user_id} requested their ID")
    console.print(f"[info]User {user_id} requested their ID[/info]")

@rate_limit
async def send_menu(update, context):
    """Handle /start and /menu commands to display the admin menu."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Access denied.")
        logger.warning(f"Access denied for user {user_id}")
        console.print(f"[warning]Access denied for user {user_id}[/warning]")
        return
    await send_dynamic_menu(update.message.chat.id, page=1)
    logger.info(f"Menu sent to admin {user_id}")
    console.print(f"[info]Menu sent to admin {user_id}[/info]")

@rate_limit
async def track_shipment(update, context):
    """Handle /track command to view shipment details with interactive controls."""
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /track <tracking_number>\nExample: /track TRK20231010120000ABC123")
        logger.warning("Invalid /track command format")
        console.print("[warning]Invalid /track command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        await update.message.reply_text("Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            await update.message.reply_text(f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
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
        await update.message.reply_text(response, parse_mode='Markdown', reply_markup=markup)
        logger.info(f"Sent tracking details for {tracking_number}")
        console.print(f"[info]Sent tracking details for {tracking_number}[/info]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in track command: {e}")
        console.print(f"[error]Error in track command: {e}[/error]")

@rate_limit
async def system_stats(update, context):
    """Handle /stats command to display system statistics."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Access denied.")
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
        await update.message.reply_text(response, parse_mode='Markdown', reply_markup=markup)
        logger.info(f"Sent system stats to admin {user_id}")
        console.print(f"[info]Sent system stats to admin {user_id}[/info]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in stats command: {e}")
        console.print(f"[error]Error in stats command: {e}[/error]")

@rate_limit
async def manual_notification(update, context):
    """Handle /notify command to send manual email or webhook notification."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Access denied.")
        logger.warning(f"Access denied for /notify by {user_id}")
        console.print(f"[warning]Access denied for /notify by {user_id}[/warning]")
        return
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /notify <tracking_number>\nExample: /notify TRK20231010120000ABC123")
        logger.warning("Invalid /notify command format")
        console.print("[warning]Invalid /notify command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        await update.message.reply_text("Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            await update.message.reply_text(f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
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
        await update.message.reply_text(f"Select notification type for `{tracking_number}`:", parse_mode='Markdown', reply_markup=markup)
        logger.info(f"Sent notification options for {tracking_number}")
        console.print(f"[info]Sent notification options for {tracking_number}[/info]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in notify command: {e}")
        console.print(f"[error]Error in notify command: {e}[/error]")

@rate_limit
async def search_command(update, context):
    """Handle /search command to find shipments by query."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Access denied.")
        logger.warning(f"Access denied for /search by {user_id}")
        console.print(f"[warning]Access denied for /search by {user_id}[/warning]")
        return
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /search <query>\nExample: /search Lagos")
        logger.warning("Invalid /search command format")
        console.print("[warning]Invalid /search command format[/warning]")
        return
    query = parts[1].strip()
    try:
        shipments, total = search_shipments(query, page=1)
        if not shipments:
            await update.message.reply_text(f"No shipments found for query: `{query}`", parse_mode='Markdown')
            logger.debug(f"No shipments found for query: {query}")
            console.print(f"[debug]No shipments found for query: {query}[/debug]")
            return
        markup = InlineKeyboardMarkup(row_width=1)
        for tn in shipments:
            markup.add(InlineKeyboardButton(tn, callback_data=f"view_{tn}"))
        if total > 5:
            markup.add(InlineKeyboardButton("Next", callback_data=f"search_page_{query}_2"))
        markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
        await update.message.reply_text(f"*Search Results for '{query}'* (Page 1, {total} total):", parse_mode='Markdown', reply_markup=markup)
        logger.info(f"Sent search results for query: {query}")
        console.print(f"[info]Sent search results for query: {query}[/info]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in search command: {e}")
        console.print(f"[error]Error in search command: {e}[/error]")

@rate_limit
async def bulk_action_command(update, context):
    """Handle /bulk_action command to perform bulk operations."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Access denied.")
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
    await update.message.reply_text("*Select bulk action*:", parse_mode='Markdown', reply_markup=markup)
    logger.info(f"Sent bulk action menu to admin {user_id}")
    console.print(f"[info]Sent bulk action menu to admin {user_id}[/info]")

@rate_limit
async def stop_simulation(update, context):
    """Handle /stop command to pause a shipment's simulation."""
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /stop <tracking_number>\nExample: /stop TRK20231010120000ABC123")
        logger.warning("Invalid /stop command format")
        console.print("[warning]Invalid /stop command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        await update.message.reply_text("Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            await update.message.reply_text(f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        if shipment.status in ['Delivered', 'Returned']:
            await update.message.reply_text(f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", parse_mode='Markdown')
            logger.warning(f"Cannot pause completed shipment: {tracking_number}")
            console.print(f"[warning]Cannot pause completed shipment: {tracking_number}[/warning]")
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true":
            await update.message.reply_text(f"Simulation for `{tracking_number}` is already paused.", parse_mode='Markdown')
            logger.warning(f"Simulation already paused: {tracking_number}")
            console.print(f"[warning]Simulation already paused: {tracking_number}[/warning]")
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
        invalidate_cache(tracking_number)
        logger.info(f"Paused simulation for {tracking_number}")
        console.print(f"[info]Paused simulation for {tracking_number}[/info]")
        await update.message.reply_text(f"Simulation paused for `{tracking_number}`.", parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in stop command: {e}")
        console.print(f"[error]Error in stop command: {e}[/error]")

@rate_limit
async def continue_simulation(update, context):
    """Handle /continue command to resume a paused shipment's simulation."""
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /continue <tracking_number>\nExample: /continue TRK20231010120000ABC123")
        logger.warning("Invalid /continue command format")
        console.print("[warning]Invalid /continue command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        await update.message.reply_text("Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            await update.message.reply_text(f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        if shipment.status in ['Delivered', 'Returned']:
            await update.message.reply_text(f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", parse_mode='Markdown')
            logger.warning(f"Cannot resume completed shipment: {tracking_number}")
            console.print(f"[warning]Cannot resume completed shipment: {tracking_number}[/warning]")
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) != "true":
            await update.message.reply_text(f"Simulation for `{tracking_number}` is not paused.", parse_mode='Markdown')
            logger.warning(f"Simulation not paused: {tracking_number}")
            console.print(f"[warning]Simulation not paused: {tracking_number}[/warning]")
            return
        if redis_client:
            safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
        invalidate_cache(tracking_number)
        logger.info(f"Resumed simulation for {tracking_number}")
        console.print(f"[info]Resumed simulation for {tracking_number}[/info]")
        await update.message.reply_text(f"Simulation resumed for `{tracking_number}`.", parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in continue command: {e}")
        console.print(f"[error]Error in continue command: {e}[/error]")

@rate_limit
async def set_simulation_speed(update, context):
    """Handle /setspeed command to set simulation speed for a shipment."""
    parts = update.message.text.split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /setspeed <tracking_number> <speed>\nExample: /setspeed TRK20231010120000ABC123 2.0")
        logger.warning("Invalid /setspeed command format")
        console.print("[warning]Invalid /setspeed command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        await update.message.reply_text("Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        speed = float(parts[2].strip())
        if speed < 0.1 or speed > 10:
            await update.message.reply_text("Speed must be between 0.1 and 10.0.")
            logger.warning(f"Invalid speed value: {speed}")
            console.print(f"[warning]Invalid speed value: {speed}[/warning]")
            return
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            await update.message.reply_text(f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "sim_speed_multipliers", tracking_number, str(speed))
        invalidate_cache(tracking_number)
        logger.info(f"Set simulation speed for {tracking_number} to {speed}x")
        console.print(f"[info]Set simulation speed for {tracking_number} to {speed}x[/info]")
        await update.message.reply_text(f"Simulation speed set to `{speed}x` for `{tracking_number}`.", parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in setspeed command: {e}")
        console.print(f"[error]Error in setspeed command: {e}[/error]")

@rate_limit
async def handle_generate(update, context):
    """Handle /generate command to create a tracking ID."""
    tracking_id = generate_unique_id()
    await update.message.reply_text(f"Generated Tracking ID: `{tracking_id}`", parse_mode='Markdown')
    logger.info(f"Generated tracking ID {tracking_id} for user {update.effective_user.id}")
    console.print(f"[info]Generated tracking ID {tracking_id} for user {update.effective_user.id}[/info]")

@rate_limit
async def list_shipments(update, context):
    """Handle /list command to display a paginated list of shipments."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Access denied.")
        logger.warning(f"Access denied for /list by {user_id}")
        console.print(f"[warning]Access denied for /list by {user_id}[/warning]")
        return
    try:
        shipments, total = get_shipment_list(page=1)
        if not shipments:
            await update.message.reply_text("No shipments available.", parse_mode='Markdown')
            logger.debug("No shipments available for /list")
            console.print("[debug]No shipments available for /list[/debug]")
            return
        markup = InlineKeyboardMarkup(row_width=1)
        for tn in shipments:
            markup.add(InlineKeyboardButton(tn, callback_data=f"view_{tn}"))
        if total > 5:
            markup.add(InlineKeyboardButton("Next", callback_data="list_2"))
        markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
        await update.message.reply_text(f"*Shipment List* (Page 1, {total} total):", parse_mode='Markdown', reply_markup=markup)
        logger.info(f"Sent shipment list to admin {user_id}")
        console.print(f"[info]Sent shipment list to admin {user_id}[/info]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in list command: {e}")
        console.print(f"[error]Error in list command: {e}[/error]")

@rate_limit
async def add_shipment(update, context):
    """Handle /add command to create a new shipment."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Access denied.")
        logger.warning(f"Access denied for /add by {user_id}")
        console.print(f"[warning]Access denied for /add by {user_id}[/warning]")
        return
    parts = update.message.text.strip().split(maxsplit=3)
    if len(parts) < 4:
        await update.message.reply_text(
            "Usage: /add <tracking_number> <status> <delivery_location> [recipient_email] [origin_location] [webhook_url]\n"
            "Example: /add TRK20231010120000ABC123 Pending 'Lagos, NG' user@example.com 'Abuja, NG' https://example.com"
        )
        logger.warning("Invalid /add command format")
        console.print("[warning]Invalid /add command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    status = parts[2].strip()
    delivery_location = parts[3].strip()
    args = update.message.text.split(maxsplit=4)[4:] if len(update.message.text.split()) > 4 else []
    recipient_email = args[0] if len(args) > 0 else None
    origin_location = args[1] if len(args) > 1 else None
    webhook_url = args[2] if len(args) > 2 else None
    try:
        config = get_config()
        if not tracking_number:
            await update.message.reply_text("Invalid tracking number.")
            logger.error(f"Invalid tracking number: {parts[1]}")
            console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
            return
        if status not in config.valid_statuses:
            await update.message.reply_text(f"Invalid status. Must be one of: {', '.join(config.valid_statuses)}")
            logger.warning(f"Invalid status: {status}")
            console.print(f"[warning]Invalid status: {status}[/warning]")
            return
        if not validate_location(delivery_location) or (origin_location and not validate_location(origin_location)):
            await update.message.reply_text("Invalid location.")
            logger.warning(f"Invalid location: {delivery_location} or {origin_location}")
            console.print(f"[warning]Invalid location: {delivery_location} or {origin_location}[/warning]")
            return
        if not validate_email(recipient_email):
            await update.message.reply_text("Invalid email address.")
            logger.warning(f"Invalid email: {recipient_email}")
            console.print(f"[warning]Invalid email: {recipient_email}[/warning]")
            return
        if not validate_webhook_url(webhook_url):
            await update.message.reply_text("Invalid webhook URL.")
            logger.warning(f"Invalid webhook URL: {webhook_url}")
            console.print(f"[warning]Invalid webhook URL: {webhook_url}[/warning]")
            return
        if save_shipment(tracking_number, status, '', delivery_location, recipient_email, origin_location, webhook_url):
            await update.message.reply_text(f"Shipment `{tracking_number}` added.", parse_mode='Markdown')
            logger.info(f"Added shipment {tracking_number} by admin {user_id}")
            console.print(f"[info]Added shipment {tracking_number} by admin {user_id}[/info]")
        else:
            await update.message.reply_text("Failed to add shipment.")
            logger.error(f"Failed to add shipment {tracking_number}")
            console.print(f"[error]Failed to add shipment {tracking_number}[/error]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in add command: {e}")
        console.print(f"[error]Error in add command: {e}[/error]")

@rate_limit
async def update_shipment_command(update, context):
    """Handle /update command to update shipment details."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Access denied.")
        logger.warning(f"Access denied for /update by {user_id}")
        console.print(f"[warning]Access denied for /update by {user_id}[/warning]")
        return
    parts = update.message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text(
            "Usage: /update <tracking_number> <field=value> [field=value ...]\n"
            "Example: /update TRK20231010120000ABC123 status=In_Transit delivery_location='Abuja, NG'"
        )
        logger.warning("Invalid /update command format")
        console.print("[warning]Invalid /update command format[/warning]")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        await update.message.reply_text("Invalid tracking number.")
        logger.error(f"Invalid tracking number: {parts[1]}")
        console.print(f"[error]Invalid tracking number: {parts[1]}[/error]")
        return
    try:
        updates = {}
        for pair in parts[2].split():
            if '=' not in pair:
                await update.message.reply_text(f"Invalid field format: {pair}. Use field=value.")
                logger.warning(f"Invalid field format: {pair}")
                console.print(f"[warning]Invalid field format: {pair}[/warning]")
                return
            field, value = pair.split('=', 1)
            updates[field] = value
        config = get_config()
        if 'status' in updates and updates['status'] not in config.valid_statuses:
            await update.message.reply_text(f"Invalid status. Must be one of: {', '.join(config.valid_statuses)}")
            logger.warning(f"Invalid status: {updates['status']}")
            console.print(f"[warning]Invalid status: {updates['status']}[/warning]")
            return
        if 'delivery_location' in updates and not validate_location(updates['delivery_location']):
            await update.message.reply_text("Invalid delivery location.")
            logger.warning(f"Invalid delivery location: {updates['delivery_location']}")
            console.print(f"[warning]Invalid delivery location: {updates['delivery_location']}[/warning]")
            return
        if 'origin_location' in updates and updates['origin_location'] and not validate_location(updates['origin_location']):
            await update.message.reply_text("Invalid origin location.")
            logger.warning(f"Invalid origin location: {updates['origin_location']}")
            console.print(f"[warning]Invalid origin location: {updates['origin_location']}[/warning]")
            return
        if 'recipient_email' in updates and updates['recipient_email'] and not validate_email(updates['recipient_email']):
            await update.message.reply_text("Invalid email address.")
            logger.warning(f"Invalid email: {updates['recipient_email']}")
            console.print(f"[warning]Invalid email: {updates['recipient_email']}[/warning]")
            return
        if 'webhook_url' in updates and updates['webhook_url'] and not validate_webhook_url(updates['webhook_url']):
            await update.message.reply_text("Invalid webhook URL.")
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
                    enqueue_notification(tracking_number, "email", notification_data['data'])
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
                    enqueue_notification(tracking_number, "webhook", notification_data['data'])
            await update.message.reply_text(f"Shipment `{tracking_number}` updated.", parse_mode='Markdown')
            logger.info(f"Updated shipment {tracking_number} by admin {user_id}")
            console.print(f"[info]Updated shipment {tracking_number} by admin {user_id}[/info]")
        else:
            await update.message.reply_text(f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in update command: {e}")
        console.print(f"[error]Error in update command: {e}[/error]")

@rate_limit
async def export_shipments_command(update, context):
    """Handle /export command to export shipment data as JSON."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Access denied.")
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
            await update.message.reply_text("No shipments to export or error occurred.", parse_mode='Markdown')
            logger.warning("Failed to export shipments")
            console.print("[warning]Failed to export shipments[/warning]")
            return
        max_length = 4096
        if len(export_json) <= max_length:
            await update.message.reply_text(f"```json\n{export_json}\n```", parse_mode='Markdown')
        else:
            parts = [export_json[i:i+max_length] for i in range(0, len(export_json), max_length)]
            for i, part in enumerate(parts, 1):
                await update.message.reply_text(f"```json\nPart {i}/{len(parts)}:\n{part}\n```", parse_mode='Markdown')
        logger.info(f"Exported shipments for admin {user_id}")
        console.print(f"[info]Exported shipments for admin {user_id}[/info]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in export command: {e}")
        console.print(f"[error]Error in export command: {e}[/error]")

@rate_limit
async def get_logs_command(update, context):
    """Handle /logs command to retrieve recent bot logs."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Access denied.")
        logger.warning(f"Access denied for /logs by {user_id}")
        console.print(f"[warning]Access denied for /logs by {user_id}[/warning]")
        return
    try:
        logs = [
            f"{datetime.utcnow().isoformat()} - flask_app - INFO - Sample log entry {i}"
            for i in range(1, 6)
        ]
        response = "*Recent Logs*:\n" + "\n".join([f"`{log}`" for log in logs])
        await update.message.reply_text(response, parse_mode='Markdown')
        logger.info(f"Sent recent logs to admin {user_id}")
        console.print(f"[info]Sent recent logs to admin {user_id}[/info]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error in logs command: {e}")
        console.print(f"[error]Error in logs command: {e}[/error]")

async def delete_shipment_callback(update, context, tracking_number, page):
    """Delete a shipment and update the menu."""
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            await context.bot.answer_callback_query(update.callback_query.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        db.session.delete(shipment)
        db.session.commit()
        invalidate_cache(tracking_number)
        await context.bot.answer_callback_query(update.callback_query.id, f"Shipment `{tracking_number}` deleted.")
        await show_shipment_menu(update, context, page, prefix="delete", prompt="Select shipment to delete")
        logger.info(f"Deleted shipment {tracking_number}")
        console.print(f"[info]Deleted shipment {tracking_number}[/info]")
    except Exception as e:
        await context.bot.answer_callback_query(update.callback_query.id, f"Error: {str(e)}", show_alert=True)
        logger.error(f"Error deleting shipment {tracking_number}: {e}")
        console.print(f"[error]Error deleting shipment {tracking_number}: {e}[/error]")

async def toggle_batch_selection(update, context, tracking_number):
    """Toggle a shipment's selection for batch operations."""
    if redis_client:
        try:
            selected = safe_redis_operation(redis_client.sismember, "batch_selected", tracking_number)
            if selected:
                safe_redis_operation(redis_client.srem, "batch_selected", tracking_number)
                await context.bot.answer_callback_query(update.callback_query.id, f"Deselected {tracking_number}")
            else:
                safe_redis_operation(redis_client.sadd, "batch_selected", tracking_number)
                await context.bot.answer_callback_query(update.callback_query.id, f"Selected {tracking_number}")
            logger.info(f"Toggled batch selection for {tracking_number}")
            console.print(f"[info]Toggled batch selection for {tracking_number}[/info]")
        except Exception as e:
            await context.bot.answer_callback_query(update.callback_query.id, f"Error: {str(e)}", show_alert=True)
            logger.error(f"Error toggling batch selection for {tracking_number}: {e}")
            console.print(f"[error]Error toggling batch selection for {tracking_number}: {e}[/error]")

async def batch_delete_shipments(update, context, page):
    """Delete selected shipments in batch."""
    if redis_client:
        try:
            selected = safe_redis_operation(redis_client.smembers, "batch_selected") or []
            selected = [s.decode() if isinstance(s, bytes) else s for s in selected]
            if not selected:
                await context.bot.answer_callback_query(update.callback_query.id, "No shipments selected.", show_alert=True)
                return
            for tracking_number in selected:
                shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
                if shipment:
                    db.session.delete(shipment)
                    invalidate_cache(tracking_number)
            db.session.commit()
            safe_redis_operation(redis_client.delete, "batch_selected")
            await context.bot.answer_callback_query(update.callback_query.id, f"Deleted {len(selected)} shipments.")
            await show_shipment_menu(update, context, page, prefix="batch_select", prompt="Select shipments to delete",
                                     extra_buttons=[InlineKeyboardButton("Confirm Delete", callback_data=f"batch_delete_confirm_{page}"),
                                                   InlineKeyboardButton("Home", callback_data="menu_page_1")])
            logger.info(f"Batch deleted {len(selected)} shipments")
            console.print(f"[info]Batch deleted {len(selected)} shipments[/info]")
        except Exception as e:
            await context.bot.answer_callback_query(update.callback_query.id, f"Error: {str(e)}", show_alert=True)
            logger.error(f"Error in batch delete: {e}")
            console.print(f"[error]Error in batch delete: {e}[/error]")

async def trigger_broadcast(update, context, tracking_number):
    """Trigger a broadcast for a shipment (placeholder)."""
    await context.bot.answer_callback_query(update.callback_query.id, f"Broadcast for {tracking_number} not implemented.", show_alert=True)
    logger.info(f"Attempted broadcast for {tracking_number}")
    console.print(f"[info]Attempted broadcast for {tracking_number}[/info]")

async def toggle_email_notifications(update, context, tracking_number, page):
    """Toggle email notifications for a shipment."""
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            await context.bot.answer_callback_query(update.callback_query.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        shipment.email_notifications = not shipment.email_notifications
        db.session.commit()
        invalidate_cache(tracking_number)
        status = "enabled" if shipment.email_notifications else "disabled"
        await context.bot.answer_callback_query(update.callback_query.id, f"Email notifications {status} for `{tracking_number}`.")
        await show_shipment_menu(update, context, page, prefix="toggle_email", prompt="Select shipment to toggle email notifications")
        logger.info(f"Toggled email notifications for {tracking_number} to {status}")
        console.print(f"[info]Toggled email notifications for {tracking_number} to {status}[/info]")
    except Exception as e:
        await context.bot.answer_callback_query(update.callback_query.id, f"Error: {str(e)}", show_alert=True)
        logger.error(f"Error toggling email notifications for {tracking_number}: {e}")
        console.print(f"[error]Error toggling email notifications for {tracking_number}: {e}[/error]")

async def pause_simulation_callback(update, context, tracking_number, page):
    """Pause a shipment's simulation."""
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            await context.bot.answer_callback_query(update.callback_query.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        if shipment.status in ['Delivered', 'Returned']:
            await context.bot.answer_callback_query(update.callback_query.id, f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", show_alert=True)
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true":
            await context.bot.answer_callback_query(update.callback_query.id, f"Simulation for `{tracking_number}` is already paused.", show_alert=True)
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
        invalidate_cache(tracking_number)
        await context.bot.answer_callback_query(update.callback_query.id, f"Simulation paused for `{tracking_number}`.")
        logger.info(f"Paused simulation for {tracking_number}")
        console.print(f"[info]Paused simulation for {tracking_number}[/info]")
    except Exception as e:
        await context.bot.answer_callback_query(update.callback_query.id, f"Error: {str(e)}", show_alert=True)
        logger.error(f"Error pausing simulation for {tracking_number}: {e}")
        console.print(f"[error]Error pausing simulation for {tracking_number}: {e}[/error]")

async def resume_simulation_callback(update, context, tracking_number, page):
    """Resume a shipment's simulation."""
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            await context.bot.answer_callback_query(update.callback_query.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        if shipment.status in ['Delivered', 'Returned']:
            await context.bot.answer_callback_query(update.callback_query.id, f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", show_alert=True)
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) != "true":
            await context.bot.answer_callback_query(update.callback_query.id, f"Simulation for `{tracking_number}` is not paused.", show_alert=True)
            return
        if redis_client:
            safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
        invalidate_cache(tracking_number)
        await context.bot.answer_callback_query(update.callback_query.id, f"Simulation resumed for `{tracking_number}`.")
        logger.info(f"Resumed simulation for {tracking_number}")
        console.print(f"[info]Resumed simulation for {tracking_number}[/info]")
    except Exception as e:
        await context.bot.answer_callback_query(update.callback_query.id, f"Error: {str(e)}", show_alert=True)
        logger.error(f"Error resuming simulation for {tracking_number}: {e}")
        console.print(f"[error]Error resuming simulation for {tracking_number}: {e}[/error]")

async def show_simulation_speed(update, context, tracking_number):
    """Show the simulation speed for a shipment."""
    try:
        speed = float(safe_redis_operation(redis_client.hget, "sim_speed_multipliers", tracking_number) or 1.0)
        await context.bot.answer_callback_query(update.callback_query.id, f"Simulation speed for `{tracking_number}`: {speed}x", show_alert=True)
        logger.info(f"Displayed simulation speed for {tracking_number}")
        console.print(f"[info]Displayed simulation speed for {tracking_number}[/info]")
    except Exception as e:
        await context.bot.answer_callback_query(update.callback_query.id, f"Error: {str(e)}", show_alert=True)
        logger.error(f"Error showing simulation speed for {tracking_number}: {e}")
        console.print(f"[error]Error showing simulation speed for {tracking_number}: {e}[/error]")

async def bulk_pause_shipments(update, context, page):
    """Pause selected shipments in batch."""
    if redis_client:
        try:
            selected = safe_redis_operation(redis_client.smembers, "batch_selected") or []
            selected = [s.decode() if isinstance(s, bytes) else s for s in selected]
            if not selected:
                await context.bot.answer_callback_query(update.callback_query.id, "No shipments selected.", show_alert=True)
                return
            for tracking_number in selected:
                shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
                if shipment and shipment.status not in ['Delivered', 'Returned']:
                    safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
                    invalidate_cache(tracking_number)
            await context.bot.answer_callback_query(update.callback_query.id, f"Paused {len(selected)} shipments.")
            await show_shipment_menu(update, context, page, prefix="bulk_pause", prompt="Select shipments to pause",
                                     extra_buttons=[InlineKeyboardButton("Confirm Pause", callback_data=f"bulk_pause_confirm_{page}"),
                                                   InlineKeyboardButton("Home", callback_data="menu_page_1")])
            logger.info(f"Batch paused {len(selected)} shipments")
            console.print(f"[info]Batch paused {len(selected)} shipments[/info]")
        except Exception as e:
            await context.bot.answer_callback_query(update.callback_query.id, f"Error: {str(e)}", show_alert=True)
            logger.error(f"Error in batch pause: {e}")
            console.print(f"[error]Error in batch pause: {e}[/error]")

async def bulk_resume_shipments(update, context, page):
    """Resume selected shipments in batch."""
    if redis_client:
        try:
            selected = safe_redis_operation(redis_client.smembers, "batch_selected") or []
            selected = [s.decode() if isinstance(s, bytes) else s for s in selected]
            if not selected:
                await context.bot.answer_callback_query(update.callback_query.id, "No shipments selected.", show_alert=True)
                return
            for tracking_number in selected:
                shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
                if shipment and shipment.status not in ['Delivered', 'Returned']:
                    safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
                    invalidate_cache(tracking_number)
            await context.bot.answer_callback_query(update.callback_query.id, f"Resumed {len(selected)} shipments.")
            await show_shipment_menu(update, context, page, prefix="bulk_resume", prompt="Select shipments to resume",
                                     extra_buttons=[InlineKeyboardButton("Confirm Resume", callback_data=f"bulk_resume_confirm_{page}"),
                                                   InlineKeyboardButton("Home", callback_data="menu_page_1")])
            logger.info(f"Batch resumed {len(selected)} shipments")
            console.print(f"[info]Batch resumed {len(selected)} shipments[/info]")
        except Exception as e:
            await context.bot.answer_callback_query(update.callback_query.id, f"Error: {str(e)}", show_alert=True)
            logger.error(f"Error in batch resume: {e}")
            console.print(f"[error]Error in batch resume: {e}[/error]")

async def handle_add_shipment(update, context):
    """Handle adding a new shipment via callback."""
    parts = update.message.text.strip().split()
    if len(parts) < 3:
        await update.message.reply_text(
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
            await update.message.reply_text("Invalid email address.")
            logger.warning(f"Invalid email: {recipient_email}")
            console.print(f"[warning]Invalid email: {recipient_email}[/warning]")
            return
        if not validate_location(delivery_location) or (origin_location and not validate_location(origin_location)):
            await update.message.reply_text("Invalid location.")
            logger.warning(f"Invalid location: {delivery_location} or {origin_location}")
            console.print(f"[warning]Invalid location: {delivery_location} or {origin_location}[/warning]")
            return
        if not validate_webhook_url(webhook_url):
            await update.message.reply_text("Invalid webhook URL.")
            logger.warning(f"Invalid webhook URL: {webhook_url}")
            console.print(f"[warning]Invalid webhook URL: {webhook_url}[/warning]")
            return
        if status not in config.valid_statuses:
            await update.message.reply_text(f"Invalid status. Must be one of: {', '.join(config.valid_statuses)}")
            logger.warning(f"Invalid status: {status}")
            console.print(f"[warning]Invalid status: {status}[/warning]")
            return
        if save_shipment(tracking_number, status, '', delivery_location, recipient_email, origin_location, webhook_url):
            await update.message.reply_text(f"Shipment `{tracking_number}` added.", parse_mode='Markdown')
            logger.info(f"Added shipment {tracking_number} by admin {update.effective_user.id}")
            console.print(f"[info]Added shipment {tracking_number} by admin {update.effective_user.id}[/info]")
        else:
            await update.message.reply_text("Failed to add shipment.")
            logger.error(f"Failed to add shipment {tracking_number}")
            console.print(f"[error]Failed to add shipment {tracking_number}[/error]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error adding shipment: {e}")
        console.print(f"[error]Error adding shipment: {e}[/error]")

async def handle_set_speed(update, context, tracking_number):
    """Handle setting simulation speed for a shipment."""
    try:
        speed = float(update.message.text.strip())
        if speed < 0.1 or speed > 10:
            await update.message.reply_text("Speed must be between 0.1 and 10.0.")
            logger.warning(f"Invalid speed value: {speed}")
            console.print(f"[warning]Invalid speed value: {speed}[/warning]")
            return
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            await update.message.reply_text(f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "sim_speed_multipliers", tracking_number, str(speed))
        invalidate_cache(tracking_number)
        await update.message.reply_text(f"Simulation speed set to `{speed}x` for `{tracking_number}`.", parse_mode='Markdown')
        logger.info(f"Set simulation speed for {tracking_number} to {speed}x")
        console.print(f"[info]Set simulation speed for {tracking_number} to {speed}x[/info]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error setting speed: {e}")
        console.print(f"[error]Error setting speed: {e}[/error]")

async def handle_set_webhook(update, context, tracking_number):
    """Handle setting webhook URL for a shipment."""
    webhook_url = update.message.text.strip()
    try:
        if not validate_webhook_url(webhook_url):
            await update.message.reply_text("Invalid webhook URL.")
            logger.warning(f"Invalid webhook URL: {webhook_url}")
            console.print(f"[warning]Invalid webhook URL: {webhook_url}[/warning]")
            return
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            await update.message.reply_text(f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            logger.warning(f"Shipment not found: {tracking_number}")
            console.print(f"[warning]Shipment not found: {tracking_number}[/warning]")
            return
        shipment.webhook_url = webhook_url
        db.session.commit()
        invalidate_cache(tracking_number)
        await update.message.reply_text(f"Webhook URL set for `{tracking_number}`.", parse_mode='Markdown')
        logger.info(f"Set webhook URL for {tracking_number}")
        console.print(f"[info]Set webhook URL for {tracking_number}[/info]")
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        logger.error(f"Error setting webhook: {e}")
        console.print(f"[error]Error setting webhook: {e}[/error]")

async def handle_callback(update, context):
    """Handle all callback queries from inline buttons."""
    data = update.callback_query.data
    try:
        if data.startswith("menu_page_"):
            page = int(data.split("_")[-1])
            await send_dynamic_menu(update.callback_query.message.chat.id, update.callback_query.message.message_id, page)
        elif data.startswith("view_"):
            tracking_number = data.split("_", 1)[1]
            shipment = get_shipment_details(tracking_number)
            if not shipment:
                await context.bot.answer_callback_query(update.callback_query.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
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
            await context.bot.edit_message_text(response, chat_id=update.callback_query.message.chat.id,
                                               message_id=update.callback_query.message.message_id,
                                               parse_mode='Markdown', reply_markup=markup)
            logger.info(f"Displayed details for {tracking_number}")
            console.print(f"[info]Displayed details for {tracking_number}[/info]")
        elif data.startswith("delete_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            await delete_shipment_callback(update, context, tracking_number, page)
        elif data.startswith("batch_select_"):
            tracking_number = data.split("_", 2)[2]
            await toggle_batch_selection(update, context, tracking_number)
        elif data.startswith("batch_delete_confirm_"):
            page = int(data.split("_")[-1])
            await batch_delete_shipments(update, context, page)
        elif data.startswith("broadcast_"):
            tracking_number = data.split("_", 1)[1]
            await trigger_broadcast(update, context, tracking_number)
        elif data.startswith("notify_"):
            tracking_number = data.split("_", 1)[1]
            shipment = get_shipment_details(tracking_number)
            if not shipment:
                await context.bot.answer_callback_query(update.callback_query.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
                return
            markup = InlineKeyboardMarkup(row_width=2)
            config = get_config()
            if shipment.get('recipient_email') and shipment.get('email_notifications'):
                markup.add(InlineKeyboardButton("Send Email", callback_data=f"send_email_{tracking_number}"))
            if shipment.get('webhook_url') or config.websocket_server:
                markup.add(InlineKeyboardButton("Send Webhook", callback_data=f"send_webhook_{tracking_number}"))
            markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
            await context.bot.edit_message_text(f"Select notification type for `{tracking_number}`:", chat_id=update.callback_query.message.chat.id,
                                               message_id=update.callback_query.message.message_id, parse_mode='Markdown', reply_markup=markup)
            logger.info(f"Sent notification options for {tracking_number}")
            console.print(f"[info]Sent notification options for {tracking_number}[/info]")
        elif data.startswith("send_email_"):
            tracking_number = data.split("_", 2)[2]
            await send_manual_email(update.callback_query, context, tracking_number)
        elif data.startswith("send_webhook_"):
            tracking_number = data.split("_", 2)[2]
            await send_manual_webhook(update.callback_query, context, tracking_number)
        elif data.startswith("toggle_email_menu_"):
            page = int(data.split("_")[-1])
            await show_shipment_menu(update, context, page, prefix="toggle_email", prompt="Select shipment to toggle email notifications")
        elif data.startswith("toggle_email_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            await toggle_email_notifications(update, context, tracking_number, page)
        elif data.startswith("pause_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            await pause_simulation_callback(update, context, tracking_number, page)
        elif data.startswith("resume_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            await resume_simulation_callback(update, context, tracking_number, page)
        elif data.startswith("setspeed_"):
            tracking_number = data.split("_", 1)[1]
            await context.bot.answer_callback_query(update.callback_query.id, f"Enter speed for `{tracking_number}` (0.1 to 10.0):", show_alert=True)
            context.user_data['set_speed_tracking_number'] = tracking_number
            context.dispatcher.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, lambda u, c: handle_set_speed(u, c, tracking_number)))
        elif data.startswith("getspeed_"):
            tracking_number = data.split("_", 1)[1]
            await show_simulation_speed(update, context, tracking_number)
        elif data.startswith("bulk_pause_menu_"):
            page = int(data.split("_")[-1])
            await show_shipment_menu(update, context, page, prefix="bulk_pause", prompt="Select shipments to pause",
                                     extra_buttons=[InlineKeyboardButton("Confirm Pause", callback_data=f"bulk_pause_confirm_{page}"),
                                                   InlineKeyboardButton("Home", callback_data="menu_page_1")])
        elif data.startswith("bulk_resume_menu_"):
            page = int(data.split("_")[-1])
            await show_shipment_menu(update, context, page, prefix="bulk_resume", prompt="Select shipments to resume",
                                     extra_buttons=[InlineKeyboardButton("Confirm Resume", callback_data=f"bulk_resume_confirm_{page}"),
                                                   InlineKeyboardButton("Home", callback_data="menu_page_1")])
        elif data.startswith("bulk_pause_confirm_"):
            page = int(data.split("_")[-1])
            await bulk_pause_shipments(update, context, page)
        elif data.startswith("bulk_resume_confirm_"):
            page = int(data.split("_")[-1])
            await bulk_resume_shipments(update, context, page)
        elif data == "generate_id":
            new_id = generate_unique_id()
            await context.bot.answer_callback_query(update.callback_query.id, f"Generated ID: `{new_id}`", show_alert=True)
        elif data == "add":
            await context.bot.answer_callback_query(update.callback_query.id,
                                                   "Enter shipment details (tracking_number status delivery_location [recipient_email] [origin_location] [webhook_url]):",
                                                   show_alert=True)
            context.dispatcher.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_shipment))
        elif data.startswith("set_webhook_"):
            tracking_number = data.split("_", 2)[2]
            await context.bot.answer_callback_query(update.callback_query.id, f"Enter webhook URL for `{tracking_number}`:", show_alert=True)
            context.user_data['set_webhook_tracking_number'] = tracking_number
            context.dispatcher.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, lambda u, c: handle_set_webhook(u, c, tracking_number)))
        elif data.startswith("test_webhook_"):
            tracking_number = data.split("_", 2)[2]
            await send_manual_webhook(update.callback_query, context, tracking_number)
        elif data.startswith("list_"):
            page = int(data.split("_")[-1])
            await show_shipment_menu(update, context, page, prefix="view", prompt="Select shipment to view")
        elif data.startswith("delete_menu_"):
            page = int(data.split("_")[-1])
            await show_shipment_menu(update, context, page, prefix="delete", prompt="Select shipment to delete")
        elif data.startswith("batch_delete_menu_"):
            page = int(data.split("_")[-1])
            await show_shipment_menu(update, context, page, prefix="batch_select", prompt="Select shipments to delete",
                                     extra_buttons=[InlineKeyboardButton("Confirm Delete", callback_data=f"batch_delete_confirm_{page}"),
                                                   InlineKeyboardButton("Home", callback_data="menu_page_1")])
        elif data.startswith("broadcast_menu_"):
            page = int(data.split("_")[-1])
            await show_shipment_menu(update, context, page, prefix="broadcast", prompt="Select shipment to broadcast")
        elif data.startswith("setspeed_menu_"):
            page = int(data.split("_")[-1])
            await show_shipment_menu(update, context, page, prefix="setspeed", prompt="Select shipment to set simulation speed")
        elif data.startswith("getspeed_menu_"):
            page = int(data.split("_")[-1])
            await show_shipment_menu(update, context, page, prefix="getspeed", prompt="Select shipment to view simulation speed")
        elif data == "settings":
            await context.bot.edit_message_text("Settings: Not implemented yet.", chat_id=update.callback_query.message.chat.id,
                                               message_id=update.callback_query.message.message_id)
        elif data == "help":
            await context.bot.edit_message_text(
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
                chat_id=update.callback_query.message.chat.id, message_id=update.callback_query.message.message_id,
                parse_mode='Markdown', reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("Home", callback_data="menu_page_1")))
        await context.bot.answer_callback_query(update.callback_query.id)
        logger.info(f"Processed callback {data} from user {update.effective_user.id}")
        console.print(f"[info]Processed callback {data} from user {update.effective_user.id}[/info]")
    except Exception as e:
        await context.bot.answer_callback_query(update.callback_query.id, f"Error: {str(e)}", show_alert=True)
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
async def webhook():
    """Handle incoming Telegram webhook updates."""
    try:
        update = Update.de_json(request.get_json(), bot)
        await bot.process_update(update)
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
bot.add_handler(CommandHandler("myid", get_my_id))
bot.add_handler(CommandHandler(["start", "menu"], send_menu))
bot.add_handler(CommandHandler("track", track_shipment))
bot.add_handler(CommandHandler("stats", system_stats))
bot.add_handler(CommandHandler("notify", manual_notification))
bot.add_handler(CommandHandler("search", search_command))
bot.add_handler(CommandHandler("bulk_action", bulk_action_command))
bot.add_handler(CommandHandler("stop", stop_simulation))
bot.add_handler(CommandHandler("continue", continue_simulation))
bot.add_handler(CommandHandler("setspeed", set_simulation_speed))
bot.add_handler(CommandHandler("generate", handle_generate))
bot.add_handler(CommandHandler("list", list_shipments))
bot.add_handler(CommandHandler("add", add_shipment))
bot.add_handler(CommandHandler("update", update_shipment_command))
bot.add_handler(CommandHandler("export", export_shipments_command))
bot.add_handler(CommandHandler("logs", get_logs_command))
bot.add_handler(CallbackQueryHandler(handle_callback))

def set_webhook():
    """Set the Telegram webhook."""
    config = get_config()
    webhook_url = config.webhook_url
    try:
        bot.bot.remove_webhook()
        bot.bot.set_webhook(url=webhook_url)
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
