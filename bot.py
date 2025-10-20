import os
import re
import logging
from datetime import datetime
from telebot import TeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from functools import wraps
from rich.console import Console
from utils import (
    BotConfig, get_bot, is_admin, send_dynamic_menu, get_shipment_details,
    generate_unique_id, search_shipments, RATE_LIMIT_WINDOW, RATE_LIMIT_MAX,
    safe_redis_operation, redis_client, sanitize_tracking_number
)
from flask_sqlalchemy import SQLAlchemy
from flask import Flask

# Logging setup
bot_logger = logging.getLogger('telegram_bot')
bot_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
bot_logger.addHandler(handler)
console = Console()

# Bot configuration
config = BotConfig(
    telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
    redis_url=os.getenv("REDIS_URL"),
    redis_token=os.getenv("REDIS_TOKEN"),
    smtp_host=os.getenv("SMTP_HOST"),
    smtp_port=int(os.getenv("SMTP_PORT", 587)),
    smtp_user=os.getenv("SMTP_USER"),
    smtp_pass=os.getenv("SMTP_PASS"),
    smtp_from=os.getenv("SMTP_FROM"),
    websocket_server=os.getenv("WEBSOCKET_SERVER"),
    global_webhook_url=os.getenv("GLOBAL_WEBHOOK_URL"),
    allowed_admins=[int(uid) for uid in os.getenv("ALLOWED_ADMINS", "").split(",") if uid],
    route_templates={}
)

# Temporary Flask app for SQLAlchemy (for get_app_modules)
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('SQLALCHEMY_DATABASE_URI')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Shipment model (aligned with app.py)
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

# Placeholder utility functions
def get_app_modules():
    """Return database and utility functions."""
    def validate_email(email):
        return re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email) if email else True
    def validate_location(location):
        return bool(location and isinstance(location, str) and len(location) <= 100)
    def validate_webhook_url(url):
        return re.match(r'^https?://[^\s/$.?#].[^\s]*$', url) if url else True
    return db, Shipment, sanitize_tracking_number, validate_email, validate_location, validate_webhook_url, None

def invalidate_cache(tracking_number):
    """Invalidate Redis cache for a shipment."""
    if redis_client:
        try:
            safe_redis_operation(redis_client.delete, f"shipment:{tracking_number}")
            bot_logger.info(f"Invalidated cache for {tracking_number}")
        except Exception as e:
            bot_logger.error(f"Failed to invalidate cache for {tracking_number}: {e}")

def get_shipment_list(page=1, per_page=5):
    """Retrieve a paginated list of shipment tracking numbers."""
    try:
        shipments = Shipment.query.order_by(Shipment.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()
        total = Shipment.query.count()
        return [s.tracking_number for s in shipments], total
    except Exception as e:
        bot_logger.error(f"Error retrieving shipment list: {e}")
        return [], 0

def save_shipment(tracking_number, status, checkpoints, delivery_location, recipient_email=None, origin_location=None, webhook_url=None):
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
        bot_logger.info(f"Saved shipment {tracking_number}")
        return True
    except Exception as e:
        db.session.rollback()
        bot_logger.error(f"Error saving shipment {tracking_number}: {e}")
        return False

def delete_shipment_callback(call, tracking_number, page):
    """Delete a shipment and update the menu."""
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        db.session.delete(shipment)
        db.session.commit()
        invalidate_cache(tracking_number)
        bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` deleted.")
        show_shipment_menu(call, page, prefix="delete", prompt="Select shipment to delete")
        bot_logger.info(f"Deleted shipment {tracking_number}")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error deleting shipment {tracking_number}: {e}")

def toggle_batch_selection(call, tracking_number):
    """Toggle a shipment's selection for batch operations."""
    if redis_client:
        try:
            selected = safe_redis_operation(redis_client.sismember, "batch_selected", tracking_number)
            if selected:
                safe_redis_operation(redis_client.srem, "batch_selected", tracking_number)
                bot.answer_callback_query(call.id, f"Deselected {tracking_number}")
            else:
                safe_redis_operation(redis_client.sadd, "batch_selected", tracking_number)
                bot.answer_callback_query(call.id, f"Selected {tracking_number}")
            bot_logger.info(f"Toggled batch selection for {tracking_number}")
        except Exception as e:
            bot_logger.error(f"Error toggling batch selection for {tracking_number}: {e}")

def batch_delete_shipments(call, page):
    """Delete selected shipments in batch."""
    if redis_client:
        try:
            selected = safe_redis_operation(redis_client.smembers, "batch_selected") or []
            selected = [s.decode() if isinstance(s, bytes) else s for s in selected]
            if not selected:
                bot.answer_callback_query(call.id, "No shipments selected.", show_alert=True)
                return
            for tracking_number in selected:
                shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
                if shipment:
                    db.session.delete(shipment)
                    invalidate_cache(tracking_number)
            db.session.commit()
            safe_redis_operation(redis_client.delete, "batch_selected")
            bot.answer_callback_query(call.id, f"Deleted {len(selected)} shipments.")
            show_shipment_menu(call, page, prefix="batch_select", prompt="Select shipments to delete",
                              extra_buttons=[InlineKeyboardButton("Confirm Delete", callback_data=f"batch_delete_confirm_{page}"),
                                            InlineKeyboardButton("Home", callback_data="menu_page_1")])
            bot_logger.info(f"Batch deleted {len(selected)} shipments")
        except Exception as e:
            bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
            bot_logger.error(f"Error in batch delete: {e}")

def trigger_broadcast(call, tracking_number):
    """Trigger a broadcast for a shipment (placeholder)."""
    bot.answer_callback_query(call.id, f"Broadcast for {tracking_number} not implemented.", show_alert=True)
    bot_logger.info(f"Attempted broadcast for {tracking_number}")

def toggle_email_notifications(call, tracking_number, page):
    """Toggle email notifications for a shipment."""
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        shipment.email_notifications = not shipment.email_notifications
        db.session.commit()
        invalidate_cache(tracking_number)
        status = "enabled" if shipment.email_notifications else "disabled"
        bot.answer_callback_query(call.id, f"Email notifications {status} for `{tracking_number}`.")
        show_shipment_menu(call, page, prefix="toggle_email", prompt="Select shipment to toggle email notifications")
        bot_logger.info(f"Toggled email notifications for {tracking_number} to {status}")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error toggling email notifications for {tracking_number}: {e}")

def send_manual_email(call, tracking_number):
    """Send a manual email notification (placeholder)."""
    bot.answer_callback_query(call.id, f"Manual email for {tracking_number} not implemented.", show_alert=True)
    bot_logger.info(f"Attempted manual email for {tracking_number}")

def send_manual_webhook(call, tracking_number):
    """Send a manual webhook notification (placeholder)."""
    bot.answer_callback_query(call.id, f"Manual webhook for {tracking_number} not implemented.", show_alert=True)
    bot_logger.info(f"Attempted manual webhook for {tracking_number}")

def pause_simulation_callback(call, tracking_number, page):
    """Pause a shipment's simulation."""
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        if shipment.status in ['Delivered', 'Returned']:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", show_alert=True)
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true":
            bot.answer_callback_query(call.id, f"Simulation for `{tracking_number}` is already paused.", show_alert=True)
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
        invalidate_cache(tracking_number)
        bot.answer_callback_query(call.id, f"Simulation paused for `{tracking_number}`.")
        bot_logger.info(f"Paused simulation for {tracking_number}")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error pausing simulation for {tracking_number}: {e}")

def resume_simulation_callback(call, tracking_number, page):
    """Resume a shipment's simulation."""
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            return
        if shipment.status in ['Delivered', 'Returned']:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", show_alert=True)
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) != "true":
            bot.answer_callback_query(call.id, f"Simulation for `{tracking_number}` is not paused.", show_alert=True)
            return
        if redis_client:
            safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
        invalidate_cache(tracking_number)
        bot.answer_callback_query(call.id, f"Simulation resumed for `{tracking_number}`.")
        bot_logger.info(f"Resumed simulation for {tracking_number}")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error resuming simulation for {tracking_number}: {e}")

def show_simulation_speed(call, tracking_number):
    """Show the simulation speed for a shipment."""
    try:
        speed = float(safe_redis_operation(redis_client.hget, "sim_speed_multipliers", tracking_number) or 1.0)
        bot.answer_callback_query(call.id, f"Simulation speed for `{tracking_number}`: {speed}x", show_alert=True)
        bot_logger.info(f"Displayed simulation speed for {tracking_number}")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error showing simulation speed for {tracking_number}: {e}")

def bulk_pause_shipments(call, page):
    """Pause selected shipments in batch."""
    if redis_client:
        try:
            selected = safe_redis_operation(redis_client.smembers, "batch_selected") or []
            selected = [s.decode() if isinstance(s, bytes) else s for s in selected]
            if not selected:
                bot.answer_callback_query(call.id, "No shipments selected.", show_alert=True)
                return
            for tracking_number in selected:
                shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
                if shipment and shipment.status not in ['Delivered', 'Returned']:
                    safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
                    invalidate_cache(tracking_number)
            bot.answer_callback_query(call.id, f"Paused {len(selected)} shipments.")
            show_shipment_menu(call, page, prefix="bulk_pause", prompt="Select shipments to pause",
                              extra_buttons=[InlineKeyboardButton("Confirm Pause", callback_data=f"bulk_pause_confirm_{page}"),
                                            InlineKeyboardButton("Home", callback_data="menu_page_1")])
            bot_logger.info(f"Batch paused {len(selected)} shipments")
        except Exception as e:
            bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
            bot_logger.error(f"Error in batch pause: {e}")

def bulk_resume_shipments(call, page):
    """Resume selected shipments in batch."""
    if redis_client:
        try:
            selected = safe_redis_operation(redis_client.smembers, "batch_selected") or []
            selected = [s.decode() if isinstance(s, bytes) else s for s in selected]
            if not selected:
                bot.answer_callback_query(call.id, "No shipments selected.", show_alert=True)
                return
            for tracking_number in selected:
                shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
                if shipment and shipment.status not in ['Delivered', 'Returned']:
                    safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
                    invalidate_cache(tracking_number)
            bot.answer_callback_query(call.id, f"Resumed {len(selected)} shipments.")
            show_shipment_menu(call, page, prefix="bulk_resume", prompt="Select shipments to resume",
                              extra_buttons=[InlineKeyboardButton("Confirm Resume", callback_data=f"bulk_resume_confirm_{page}"),
                                            InlineKeyboardButton("Home", callback_data="menu_page_1")])
            bot_logger.info(f"Batch resumed {len(selected)} shipments")
        except Exception as e:
            bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
            bot_logger.error(f"Error in batch resume: {e}")

# Rate limit decorator
def rate_limit(func):
    @wraps(func)
    def wrapper(message):
        user_id = str(message.from_user.id)
        key = f"rate_limit:{user_id}"
        count = safe_redis_operation(redis_client.incr, key) if redis_client else 0
        if count == 1:
            safe_redis_operation(redis_client.expire, key, RATE_LIMIT_WINDOW)
        if count > RATE_LIMIT_MAX:
            bot.reply_to(message, "Rate limit exceeded. Please try again later.")
            return
        return func(message)
    return wrapper

# Bot instance
bot = get_bot()

# Command Handlers
@bot.message_handler(commands=['myid'])
@rate_limit
def get_my_id(message):
    """Handle /myid command to return the user's Telegram ID."""
    bot.reply_to(message, f"Your Telegram user ID: `{message.from_user.id}`", parse_mode='Markdown')
    bot_logger.info(f"User requested their ID")
    console.print(f"[info]User {message.from_user.id} requested their ID[/info]")

@bot.message_handler(commands=['start', 'menu'])
@rate_limit
def send_menu(message):
    """Handle /start and /menu commands to display the admin menu."""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for user {message.from_user.id}")
        return
    send_dynamic_menu(message.chat.id, page=1)
    bot_logger.info(f"Menu sent to admin {message.from_user.id}")

@bot.message_handler(commands=['track'])
@rate_limit
def track_shipment(message):
    """Handle /track command to view shipment details with interactive controls."""
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /track <tracking_number>\nExample: /track TRK20231010120000ABC123")
        bot_logger.warning("Invalid /track command format")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}")
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}")
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
        bot.reply_to(message, response, parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent tracking details for {tracking_number}")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in track command: {e}")

@bot.message_handler(commands=['stats'])
@rate_limit
def system_stats(message):
    """Handle /stats command to display system statistics."""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /stats by {message.from_user.id}")
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
        bot.reply_to(message, response, parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent system stats to admin {message.from_user.id}")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in stats command: {e}")

@bot.message_handler(commands=['notify'])
@rate_limit
def manual_notification(message):
    """Handle /notify command to send manual email or webhook notification."""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /notify by {message.from_user.id}")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /notify <tracking_number>\nExample: /notify TRK20231010120000ABC123")
        bot_logger.warning("Invalid /notify command format")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}")
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}")
            return
        markup = InlineKeyboardMarkup(row_width=2)
        if shipment.get('recipient_email') and shipment.get('email_notifications'):
            markup.add(InlineKeyboardButton("Send Email", callback_data=f"send_email_{tracking_number}"))
        if shipment.get('webhook_url') or config.websocket_server:
            markup.add(InlineKeyboardButton("Send Webhook", callback_data=f"send_webhook_{tracking_number}"))
        markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
        bot.reply_to(message, f"Select notification type for `{tracking_number}`:", parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent notification options for {tracking_number}")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in notify command: {e}")

@bot.message_handler(commands=['search'])
@rate_limit
def search_command(message):
    """Handle /search command to find shipments by query."""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /search by {message.from_user.id}")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /search <query>\nExample: /search Lagos")
        bot_logger.warning("Invalid /search command format")
        return
    query = parts[1].strip()
    try:
        shipments, total = search_shipments(query, page=1)
        if not shipments:
            bot.reply_to(message, f"No shipments found for query: `{query}`", parse_mode='Markdown')
            bot_logger.debug(f"No shipments found for query: {query}")
            return
        markup = InlineKeyboardMarkup(row_width=1)
        for tn in shipments:
            markup.add(InlineKeyboardButton(tn, callback_data=f"view_{tn}"))
        if total > 5:
            markup.add(InlineKeyboardButton("Next", callback_data=f"search_page_{query}_2"))
        markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
        bot.reply_to(message, f"*Search Results for '{query}'* (Page 1, {total} total):", parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent search results for query: {query}")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in search command: {e}")

@bot.message_handler(commands=['bulk_action'])
@rate_limit
def bulk_action_command(message):
    """Handle /bulk_action command to perform bulk operations."""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /bulk_action by {message.from_user.id}")
        return
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("Bulk Pause", callback_data="bulk_pause_menu_1"),
        InlineKeyboardButton("Bulk Resume", callback_data="bulk_resume_menu_1"),
        InlineKeyboardButton("Bulk Delete", callback_data="batch_delete_menu_1"),
        InlineKeyboardButton("Home", callback_data="menu_page_1")
    )
    bot.reply_to(message, "*Select bulk action*:", parse_mode='Markdown', reply_markup=markup)
    bot_logger.info(f"Sent bulk action menu to admin {message.from_user.id}")

@bot.message_handler(commands=['stop'])
@rate_limit
def stop_simulation(message):
    """Handle /stop command to pause a shipment's simulation."""
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /stop <tracking_number>\nExample: /stop TRK20231010120000ABC123")
        bot_logger.warning("Invalid /stop command format")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}")
        return
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}")
            return
        if shipment.status in ['Delivered', 'Returned']:
            bot.reply_to(message, f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", parse_mode='Markdown')
            bot_logger.warning(f"Cannot pause completed shipment: {tracking_number}")
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true":
            bot.reply_to(message, f"Simulation for `{tracking_number}` is already paused.", parse_mode='Markdown')
            bot_logger.warning(f"Simulation already paused: {tracking_number}")
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
        invalidate_cache(tracking_number)
        bot_logger.info(f"Paused simulation for {tracking_number}")
        bot.reply_to(message, f"Simulation paused for `{tracking_number}`.", parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in stop command: {e}")

@bot.message_handler(commands=['continue'])
@rate_limit
def continue_simulation(message):
    """Handle /continue command to resume a paused shipment's simulation."""
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /continue <tracking_number>\nExample: /continue TRK20231010120000ABC123")
        bot_logger.warning("Invalid /continue command format")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}")
        return
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}")
            return
        if shipment.status in ['Delivered', 'Returned']:
            bot.reply_to(message, f"Shipment `{tracking_number}` is already completed (`{shipment.status}`).", parse_mode='Markdown')
            bot_logger.warning(f"Cannot resume completed shipment: {tracking_number}")
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) != "true":
            bot.reply_to(message, f"Simulation for `{tracking_number}` is not paused.", parse_mode='Markdown')
            bot_logger.warning(f"Simulation not paused: {tracking_number}")
            return
        if redis_client:
            safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
        invalidate_cache(tracking_number)
        bot_logger.info(f"Resumed simulation for {tracking_number}")
        bot.reply_to(message, f"Simulation resumed for `{tracking_number}`.", parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in continue command: {e}")

@bot.message_handler(commands=['setspeed'])
@rate_limit
def set_simulation_speed(message):
    """Handle /setspeed command to set simulation speed for a shipment."""
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.reply_to(message, "Usage: /setspeed <tracking_number> <speed>\nExample: /setspeed TRK20231010120000ABC123 2.0")
        bot_logger.warning("Invalid /setspeed command format")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}")
        return
    try:
        speed = float(parts[2].strip())
        if speed < 0.1 or speed > 10:
            bot.reply_to(message, "Speed must be between 0.1 and 10.0.")
            bot_logger.warning(f"Invalid speed value: {speed}")
            return
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}")
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "sim_speed_multipliers", tracking_number, str(speed))
        invalidate_cache(tracking_number)
        bot_logger.info(f"Set simulation speed for {tracking_number} to {speed}x")
        bot.reply_to(message, f"Simulation speed set to `{speed}x` for `{tracking_number}`.", parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in setspeed command: {e}")

@bot.message_handler(commands=['generate'])
@rate_limit
def handle_generate(message):
    """Handle /generate command to create a tracking ID."""
    tracking_id = generate_unique_id()
    bot.reply_to(message, f"Generated Tracking ID: `{tracking_id}`", parse_mode='Markdown')
    bot_logger.info(f"Generated tracking ID {tracking_id} for user {message.from_user.id}")

# Callback Handlers
@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    """Handle all callback queries from inline buttons."""
    data = call.data
    try:
        if data.startswith("menu_page_"):
            page = int(data.split("_")[-1])
            send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
        elif data.startswith("view_"):
            tracking_number = data.split("_", 1)[1]
            shipment = get_shipment_details(tracking_number)
            if not shipment:
                bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
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
            bot.edit_message_text(response, chat_id=call.message.chat.id, message_id=call.message.message_id,
                                 parse_mode='Markdown', reply_markup=markup)
            bot_logger.info(f"Displayed details for {tracking_number}")
        elif data.startswith("delete_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            delete_shipment_callback(call, tracking_number, page)
        elif data.startswith("batch_select_"):
            tracking_number = data.split("_", 2)[2]
            toggle_batch_selection(call, tracking_number)
        elif data.startswith("batch_delete_confirm_"):
            page = int(data.split("_")[-1])
            batch_delete_shipments(call, page)
        elif data.startswith("broadcast_"):
            tracking_number = data.split("_", 1)[1]
            trigger_broadcast(call, tracking_number)
        elif data.startswith("notify_"):
            tracking_number = data.split("_", 1)[1]
            shipment = get_shipment_details(tracking_number)
            if not shipment:
                bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
                return
            markup = InlineKeyboardMarkup(row_width=2)
            if shipment.get('recipient_email') and shipment.get('email_notifications'):
                markup.add(InlineKeyboardButton("Send Email", callback_data=f"send_email_{tracking_number}"))
            if shipment.get('webhook_url') or config.websocket_server:
                markup.add(InlineKeyboardButton("Send Webhook", callback_data=f"send_webhook_{tracking_number}"))
            markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
            bot.edit_message_text(f"Select notification type for `{tracking_number}`:", chat_id=call.message.chat.id,
                                 message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
        elif data.startswith("send_email_"):
            tracking_number = data.split("_", 2)[2]
            send_manual_email(call, tracking_number)
        elif data.startswith("send_webhook_"):
            tracking_number = data.split("_", 2)[2]
            send_manual_webhook(call, tracking_number)
        elif data.startswith("toggle_email_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page, prefix="toggle_email", prompt="Select shipment to toggle email notifications")
        elif data.startswith("toggle_email_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            toggle_email_notifications(call, tracking_number, page)
        elif data.startswith("pause_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            pause_simulation_callback(call, tracking_number, page)
        elif data.startswith("resume_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            resume_simulation_callback(call, tracking_number, page)
        elif data.startswith("setspeed_"):
            tracking_number = data.split("_", 1)[1]
            bot.answer_callback_query(call.id, f"Enter speed for `{tracking_number}` (0.1 to 10.0):", show_alert=True)
            bot.register_next_step_handler(call.message, lambda msg: handle_set_speed(msg, tracking_number))
        elif data.startswith("getspeed_"):
            tracking_number = data.split("_", 1)[1]
            show_simulation_speed(call, tracking_number)
        elif data.startswith("bulk_pause_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page, prefix="bulk_pause", prompt="Select shipments to pause",
                              extra_buttons=[InlineKeyboardButton("Confirm Pause", callback_data=f"bulk_pause_confirm_{page}"),
                                            InlineKeyboardButton("Home", callback_data="menu_page_1")])
        elif data.startswith("bulk_resume_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page, prefix="bulk_resume", prompt="Select shipments to resume",
                              extra_buttons=[InlineKeyboardButton("Confirm Resume", callback_data=f"bulk_resume_confirm_{page}"),
                                            InlineKeyboardButton("Home", callback_data="menu_page_1")])
        elif data.startswith("bulk_pause_confirm_"):
            page = int(data.split("_")[-1])
            bulk_pause_shipments(call, page)
        elif data.startswith("bulk_resume_confirm_"):
            page = int(data.split("_")[-1])
            bulk_resume_shipments(call, page)
        elif data == "generate_id":
            new_id = generate_unique_id()
            bot.answer_callback_query(call.id, f"Generated ID: `{new_id}`", show_alert=True)
        elif data == "add":
            bot.answer_callback_query(call.id, "Enter shipment details (tracking_number status delivery_location [recipient_email] [origin_location] [webhook_url]):", show_alert=True)
            bot.register_next_step_handler(call.message, handle_add_shipment)
        elif data.startswith("set_webhook_"):
            tracking_number = data.split("_", 2)[2]
            bot.answer_callback_query(call.id, f"Enter webhook URL for `{tracking_number}`:", show_alert=True)
            bot.register_next_step_handler(call.message, lambda msg: handle_set_webhook(msg, tracking_number))
        elif data.startswith("test_webhook_"):
            tracking_number = data.split("_", 2)[2]
            send_manual_webhook(call, tracking_number)
        elif data.startswith("list_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page, prefix="view", prompt="Select shipment to view")
        elif data.startswith("delete_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page, prefix="delete", prompt="Select shipment to delete")
        elif data.startswith("batch_delete_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page, prefix="batch_select", prompt="Select shipments to delete",
                              extra_buttons=[InlineKeyboardButton("Confirm Delete", callback_data=f"batch_delete_confirm_{page}"),
                                            InlineKeyboardButton("Home", callback_data="menu_page_1")])
        elif data.startswith("broadcast_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page, prefix="broadcast", prompt="Select shipment to broadcast")
        elif data.startswith("setspeed_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page, prefix="setspeed", prompt="Select shipment to set simulation speed")
        elif data.startswith("getspeed_menu_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page, prefix="getspeed", prompt="Select shipment to view simulation speed")
        elif data == "settings":
            bot.edit_message_text("Settings: Not implemented yet.", chat_id=call.message.chat.id,
                                 message_id=call.message.message_id)
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
                "Example: /track TRK20231010120000ABC123\n"
                "Example: /setspeed TRK20231010120000ABC123 2.0",
                chat_id=call.message.chat.id, message_id=call.message.message_id,
                parse_mode='Markdown', reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("Home", callback_data="menu_page_1")))
        bot.answer_callback_query(call.id)
        bot_logger.info(f"Processed callback {data} from user {call.from_user.id}")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error in callback handler: {e}")

def show_shipment_menu(call, page, prefix, prompt, extra_buttons=None):
    """Show a menu of shipments for a specific action."""
    shipments, total = get_shipment_list(page=page)
    if not shipments:
        bot.edit_message_text(f"No shipments available.", chat_id=call.message.chat.id,
                             message_id=call.message.message_id)
        return
    markup = InlineKeyboardMarkup(row_width=1)
    for tn in shipments:
        markup.add(InlineKeyboardButton(tn, callback_data=f"{prefix}_{tn}_{page}"))
    if total > 5:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("Previous", callback_data=f"{prefix}_menu_{page-1}"))
        if page * 5 < total:
            nav_buttons.append(InlineKeyboardButton("Next", callback_data=f"{prefix}_menu_{page+1}"))
        markup.add(*nav_buttons)
    if extra_buttons:
        markup.add(*extra_buttons)
    bot.edit_message_text(f"*{prompt}* (Page {page}, {total} total):", chat_id=call.message.chat.id,
                         message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
    bot_logger.info(f"Sent shipment menu for {prefix}, page {page}")

def handle_add_shipment(message):
    """Handle adding a new shipment."""
    parts = message.text.strip().split()
    if len(parts) < 3:
        bot.reply_to(message, "Usage: tracking_number status delivery_location [recipient_email] [origin_location] [webhook_url]\n"
                            "Example: TRK20231010120000ABC123 Pending 'Lagos, NG' user@example.com 'Abuja, NG' https://example.com")
        bot_logger.warning("Invalid add shipment input")
        return
    tracking_number, status, delivery_location = parts[:3]
    recipient_email = parts[3] if len(parts) > 3 else None
    origin_location = parts[4] if len(parts) > 4 else None
    webhook_url = parts[5] if len(parts) > 5 else None
    try:
        db, Shipment, _, validate_email, validate_location, validate_webhook_url, _ = get_app_modules()
        if not validate_email(recipient_email):
            bot.reply_to(message, "Invalid email address.")
            return
        if not validate_location(delivery_location) or (origin_location and not validate_location(origin_location)):
            bot.reply_to(message, "Invalid location.")
            return
        if not validate_webhook_url(webhook_url):
            bot.reply_to(message, "Invalid webhook URL.")
            return
        if save_shipment(tracking_number, status, '', delivery_location, recipient_email, origin_location, webhook_url):
            bot.reply_to(message, f"Shipment `{tracking_number}` added.", parse_mode='Markdown')
            bot_logger.info(f"Added shipment {tracking_number} by admin {message.from_user.id}")
        else:
            bot.reply_to(message, "Failed to add shipment.")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error adding shipment: {e}")

def handle_set_speed(message, tracking_number):
    """Handle setting simulation speed for a shipment."""
    try:
        speed = float(message.text.strip())
        if speed < 0.1 or speed > 10:
            bot.reply_to(message, "Speed must be between 0.1 and 10.0.")
            return
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "sim_speed_multipliers", tracking_number, str(speed))
        invalidate_cache(tracking_number)
        bot.reply_to(message, f"Simulation speed set to `{speed}x` for `{tracking_number}`.", parse_mode='Markdown')
        bot_logger.info(f"Set simulation speed for {tracking_number} to {speed}x")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error setting speed: {e}")

def handle_set_webhook(message, tracking_number):
    """Handle setting webhook URL for a shipment."""
    webhook_url = message.text.strip()
    try:
        db, Shipment, _, _, _, validate_webhook_url, _ = get_app_modules()
        if not validate_webhook_url(webhook_url):
            bot.reply_to(message, "Invalid webhook URL.")
            return
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            return
        shipment.webhook_url = webhook_url
        db.session.commit()
        invalidate_cache(tracking_number)
        bot.reply_to(message, f"Webhook URL set for `{tracking_number}`.", parse_mode='Markdown')
        bot_logger.info(f"Set webhook URL for {tracking_number}")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error setting webhook: {e}")

def set_webhook():
    """Set the Telegram webhook."""
    webhook_url = os.getenv("WEBHOOK_URL", "https://signment-9a96.onrender.com/telegram/webhook")
    try:
        bot.remove_webhook()
        bot.set_webhook(url=webhook_url)
        bot_logger.info(f"Webhook set to {webhook_url}")
        console.print(f"[info]Webhook set to {webhook_url}[/info]")
    except Exception as e:
        bot_logger.error(f"Failed to set webhook: {e}")
        console.print(f"[error]Failed to set webhook: {e}[/error]")

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        set_webhook()
        bot_logger.info("Bot started with webhook mode")
