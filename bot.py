import os
import re
import json
import logging
import socket
import time
import threading
from datetime import datetime
from telebot import TeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from functools import wraps
from rich.console import Console
from urllib.parse import quote_plus, urlparse

# Import app and db with proper context handling
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text

# Import from app.py - do this AFTER eventlet monkey patching
from app import app, db, Shipment, DHLRealisticSimulator, DHL_CONFIG

# Now import redis_client and other utils
from utils import (
    BotConfig, config, get_bot, is_admin, send_dynamic_menu, get_shipment_details,
    generate_unique_id, search_shipments, RATE_LIMIT_WINDOW, RATE_LIMIT_MAX,
    safe_redis_operation, sanitize_tracking_number, enqueue_notification,
    save_shipment, update_shipment, get_shipment_list, export_shipments, get_recent_logs,
    show_shipment_menu, cache_route_templates, keep_alive, estimate_distance, DHL_CONFIG
)

# Import redis_client directly
from utils import redis_client

# Logging setup
bot_logger = logging.getLogger('telegram_bot')
console = Console()

# Bot instance - get after imports
bot = None

def get_bot_instance():
    """Get or create bot instance with proper context."""
    global bot
    if bot is None:
        with app.app_context():
            bot = get_bot()
    return bot

# === RATE LIMIT DECORATOR ===
def rate_limit(func):
    @wraps(func)
    def wrapper(message):
        user_id = str(message.from_user.id)
        key = f"rate_limit:{user_id}"
        count = safe_redis_operation(redis_client.incr, key) if redis_client else 0
        if count == 1:
            safe_redis_operation(redis_client.expire, key, RATE_LIMIT_WINDOW)
        if count > RATE_LIMIT_MAX:
            bot_instance = get_bot_instance()
            bot_instance.reply_to(message, "Rate limit exceeded. Please try again later.")
            return
        return func(message)
    return wrapper

# === DATABASE HELPERS WITH CONTEXT ===
def get_shipment_with_context(tracking_number):
    """Get shipment details with proper app context."""
    with app.app_context():
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            return None
        return shipment.to_dict()

def get_all_shipments_with_context(page=1, per_page=10):
    """Get all shipments with proper app context."""
    with app.app_context():
        tracking_numbers, total = get_shipment_list(page=page, per_page=per_page)
        shipments = []
        for tn in tracking_numbers:
            s = Shipment.query.filter_by(tracking_number=tn).first()
            if s:
                shipments.append(s.to_dict())
        return shipments, total

def update_shipment_with_context(tracking_number, **kwargs):
    """Update shipment with proper app context."""
    with app.app_context():
        return update_shipment(tracking_number, **kwargs)

def save_shipment_with_context(tracking_number, status, checkpoints, delivery_location, 
                               recipient_email=None, origin_location=None, 
                               webhook_url=None, carrier="DHL"):
    """Save shipment with proper app context."""
    with app.app_context():
        return save_shipment(tracking_number, status, checkpoints, delivery_location, 
                            recipient_email, origin_location, webhook_url, carrier)

def search_shipments_with_context(query, page=1):
    """Search shipments with proper app context."""
    with app.app_context():
        return search_shipments(query, page)

def export_shipments_with_context():
    """Export shipments with proper app context."""
    with app.app_context():
        return export_shipments()

def get_recent_logs_with_context(limit=10):
    """Get recent logs with proper app context."""
    with app.app_context():
        return get_recent_logs(limit)

# === COMMAND HANDLERS ===
@bot.message_handler(commands=['myid'])
@rate_limit
def get_my_id(message):
    """Handle /myid command to return the user's Telegram ID."""
    bot_instance = get_bot_instance()
    bot_instance.reply_to(message, f"Your Telegram user ID: `{message.from_user.id}`", parse_mode='Markdown')
    bot_logger.info(f"User requested their ID")
    console.print(f"[info]User {message.from_user.id} requested their ID[/info]")

@bot.message_handler(commands=['start', 'menu'])
@rate_limit
def send_menu(message):
    """Handle /start and /menu commands to display the admin menu."""
    if not is_admin(message.from_user.id):
        bot_instance = get_bot_instance()
        bot_instance.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for user {message.from_user.id}")
        return
    send_dynamic_menu(message.chat.id, page=1)
    bot_logger.info(f"Menu sent to admin {message.from_user.id}")

@bot.message_handler(commands=['track'])
@rate_limit
def track_shipment(message):
    """Handle /track command to view shipment details with interactive controls."""
    bot_instance = get_bot_instance()
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot_instance.reply_to(message, "Usage: /track <tracking_number>\nExample: /track JD1234567890")
        bot_logger.warning("Invalid /track command format")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot_instance.reply_to(message, "Invalid tracking number. Must be DHL format: JDxxxxxxxxxx")
        bot_logger.error(f"Invalid tracking number: {parts[1]}")
        return
    try:
        shipment = get_shipment_with_context(tracking_number)
        if not shipment:
            bot_instance.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}")
            return
        paused = safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true" if redis_client else False
        speed = float(safe_redis_operation(redis_client.hget, "sim_speed_multipliers", tracking_number) or 1.0) if redis_client else 1.0
        distance = estimate_distance(shipment.get('origin_location') or "Lagos, NG", shipment.get('delivery_location', ''))
        
        # Get service level and delivery window from Redis
        service_level = safe_redis_operation(redis_client.hget, "service_level", tracking_number) if redis_client else None
        if service_level and isinstance(service_level, bytes):
            service_level = service_level.decode('utf-8')
        delivery_window = safe_redis_operation(redis_client.hget, "delivery_window", tracking_number) if redis_client else None
        if delivery_window and isinstance(delivery_window, bytes):
            delivery_window = delivery_window.decode('utf-8')
        
        response = (
            f"*Shipment*: `{tracking_number}`\n"
            f"*Carrier*: `{shipment.get('carrier', 'DHL')}`\n"
            f"*Status*: `{shipment['status']}`\n"
            f"*Service*: `{service_level or 'DHL Express'}`\n"
            f"*Delivery Window*: `{delivery_window or 'Calculating...'}`\n"
            f"*Paused*: `{paused}`\n"
            f"*Speed*: `{speed}x`\n"
            f"*Distance*: `{distance} km`\n"
            f"*Origin*: `{shipment.get('origin_location', 'N/A')}`\n"
            f"*Destination*: `{shipment['delivery_location']}`\n"
            f"*Checkpoints*: `{len(shipment.get('checkpoints', []))}`"
        )
        markup = InlineKeyboardMarkup(row_width=2)
        if shipment['status'] not in ['Delivered', 'Returned']:
            markup.add(
                InlineKeyboardButton("⏸ Pause" if not paused else "▶️ Resume", 
                                   callback_data=f"{'pause' if not paused else 'resume'}_{tracking_number}_1"),
                InlineKeyboardButton("⚡ Speed", callback_data=f"setspeed_{tracking_number}")
            )
        markup.add(
            InlineKeyboardButton("📧 Notify", callback_data=f"notify_{tracking_number}"),
            InlineKeyboardButton("🔗 Webhook", callback_data=f"set_webhook_{tracking_number}"),
            InlineKeyboardButton("🧪 Test Webhook", callback_data=f"test_webhook_{tracking_number}"),
            InlineKeyboardButton("🏠 Home", callback_data="menu_page_1")
        )
        bot_instance.reply_to(message, response, parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent tracking details for {tracking_number}")
    except Exception as e:
        bot_instance.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in track command: {e}")

@bot.message_handler(commands=['stats'])
@rate_limit
def system_stats(message):
    """Handle /stats command to display system statistics."""
    bot_instance = get_bot_instance()
    if not is_admin(message.from_user.id):
        bot_instance.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /stats by {message.from_user.id}")
        return
    try:
        with app.app_context():
            shipments, total = get_all_shipments_with_context()
            active_shipments = len([s for s in shipments if s['status'] not in ['Delivered', 'Returned']])
        paused_count = len(safe_redis_operation(redis_client.hgetall, "paused_simulations") or {}) if redis_client else 0
        response = (
            f"*📊 System Statistics*\n\n"
            f"*Total Shipments*: `{total}`\n"
            f"*Active Shipments*: `{active_shipments}`\n"
            f"*Paused Simulations*: `{paused_count}`\n"
            f"*Redis*: `{'✅ Connected' if redis_client else '❌ Disconnected'}`"
        )
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("📋 All Shipments", callback_data="list_1"),
            InlineKeyboardButton("🏠 Home", callback_data="menu_page_1")
        )
        bot_instance.reply_to(message, response, parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent system stats to admin {message.from_user.id}")
    except Exception as e:
        bot_instance.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in stats command: {e}")

@bot.message_handler(commands=['notify'])
@rate_limit
def manual_notification(message):
    """Handle /notify command to send manual email or webhook notification."""
    bot_instance = get_bot_instance()
    if not is_admin(message.from_user.id):
        bot_instance.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /notify by {message.from_user.id}")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot_instance.reply_to(message, "Usage: /notify <tracking_number>\nExample: /notify JD1234567890")
        bot_logger.warning("Invalid /notify command format")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot_instance.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}")
        return
    try:
        shipment = get_shipment_with_context(tracking_number)
        if not shipment:
            bot_instance.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}")
            return
        markup = InlineKeyboardMarkup(row_width=2)
        if shipment.get('recipient_email') and shipment.get('email_notifications'):
            markup.add(InlineKeyboardButton("📧 Send Email", callback_data=f"send_email_{tracking_number}"))
        if shipment.get('webhook_url'):
            markup.add(InlineKeyboardButton("🔗 Send Webhook", callback_data=f"send_webhook_{tracking_number}"))
        markup.add(InlineKeyboardButton("🏠 Home", callback_data="menu_page_1"))
        bot_instance.reply_to(message, f"Select notification type for `{tracking_number}`:", parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent notification options for {tracking_number}")
    except Exception as e:
        bot_instance.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in notify command: {e}")

@bot.message_handler(commands=['search'])
@rate_limit
def search_command(message):
    """Handle /search command to find shipments by query."""
    bot_instance = get_bot_instance()
    if not is_admin(message.from_user.id):
        bot_instance.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /search by {message.from_user.id}")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot_instance.reply_to(message, "Usage: /search <query>\nExample: /search Lagos")
        bot_logger.warning("Invalid /search command format")
        return
    query = parts[1].strip()
    try:
        shipments, total = search_shipments_with_context(query, page=1)
        if not shipments:
            bot_instance.reply_to(message, f"No shipments found for query: `{query}`", parse_mode='Markdown')
            bot_logger.debug(f"No shipments found for query: {query}")
            return
        markup = InlineKeyboardMarkup(row_width=1)
        for tn in shipments:
            markup.add(InlineKeyboardButton(tn, callback_data=f"view_{tn}"))
        if total > 10:
            markup.add(InlineKeyboardButton("Next", callback_data=f"search_page_{query}_2"))
        markup.add(InlineKeyboardButton("🏠 Home", callback_data="menu_page_1"))
        bot_instance.reply_to(message, f"*Search Results for '{query}'* (Page 1, {total} total):", parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent search results for query: {query}")
    except Exception as e:
        bot_instance.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in search command: {e}")

@bot.message_handler(commands=['bulk_action'])
@rate_limit
def bulk_action_command(message):
    """Handle /bulk_action command to perform bulk operations."""
    bot_instance = get_bot_instance()
    if not is_admin(message.from_user.id):
        bot_instance.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /bulk_action by {message.from_user.id}")
        return
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("⏸ Bulk Pause", callback_data="bulk_pause_menu_1"),
        InlineKeyboardButton("▶️ Bulk Resume", callback_data="bulk_resume_menu_1"),
        InlineKeyboardButton("🗑 Bulk Delete", callback_data="batch_delete_menu_1"),
        InlineKeyboardButton("🏠 Home", callback_data="menu_page_1")
    )
    bot_instance.reply_to(message, "*Select bulk action*:", parse_mode='Markdown', reply_markup=markup)
    bot_logger.info(f"Sent bulk action menu to admin {message.from_user.id}")

@bot.message_handler(commands=['stop'])
@rate_limit
def stop_simulation(message):
    """Handle /stop command to pause a shipment's simulation."""
    bot_instance = get_bot_instance()
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot_instance.reply_to(message, "Usage: /stop <tracking_number>\nExample: /stop JD1234567890")
        bot_logger.warning("Invalid /stop command format")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot_instance.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}")
        return
    try:
        shipment = get_shipment_with_context(tracking_number)
        if not shipment:
            bot_instance.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}")
            return
        if shipment['status'] in ['Delivered', 'Returned']:
            bot_instance.reply_to(message, f"Shipment `{tracking_number}` is already completed (`{shipment['status']}`).", parse_mode='Markdown')
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true":
            bot_instance.reply_to(message, f"Simulation for `{tracking_number}` is already paused.", parse_mode='Markdown')
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
        bot_instance.reply_to(message, f"⏸ Simulation paused for `{tracking_number}`.", parse_mode='Markdown')
        bot_logger.info(f"Paused simulation for {tracking_number}")
    except Exception as e:
        bot_instance.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in stop command: {e}")

@bot.message_handler(commands=['continue'])
@rate_limit
def continue_simulation(message):
    """Handle /continue command to resume a paused shipment's simulation."""
    bot_instance = get_bot_instance()
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot_instance.reply_to(message, "Usage: /continue <tracking_number>\nExample: /continue JD1234567890")
        bot_logger.warning("Invalid /continue command format")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot_instance.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}")
        return
    try:
        shipment = get_shipment_with_context(tracking_number)
        if not shipment:
            bot_instance.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}")
            return
        if shipment['status'] in ['Delivered', 'Returned']:
            bot_instance.reply_to(message, f"Shipment `{tracking_number}` is already completed (`{shipment['status']}`).", parse_mode='Markdown')
            return
        if redis_client and safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) != "true":
            bot_instance.reply_to(message, f"Simulation for `{tracking_number}` is not paused.", parse_mode='Markdown')
            return
        if redis_client:
            safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
        bot_instance.reply_to(message, f"▶️ Simulation resumed for `{tracking_number}`.", parse_mode='Markdown')
        bot_logger.info(f"Resumed simulation for {tracking_number}")
    except Exception as e:
        bot_instance.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in continue command: {e}")

@bot.message_handler(commands=['setspeed'])
@rate_limit
def set_simulation_speed(message):
    """Handle /setspeed command to set simulation speed for a shipment."""
    bot_instance = get_bot_instance()
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot_instance.reply_to(message, "Usage: /setspeed <tracking_number> <speed>\nExample: /setspeed JD1234567890 2.0")
        bot_logger.warning("Invalid /setspeed command format")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    if not tracking_number:
        bot_instance.reply_to(message, "Invalid tracking number.")
        bot_logger.error(f"Invalid tracking number: {parts[1]}")
        return
    try:
        speed = float(parts[2].strip())
        if speed < 0.1 or speed > 10:
            bot_instance.reply_to(message, "Speed must be between 0.1 and 10.0.")
            bot_logger.warning(f"Invalid speed value: {speed}")
            return
        shipment = get_shipment_with_context(tracking_number)
        if not shipment:
            bot_instance.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}")
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "sim_speed_multipliers", tracking_number, str(speed))
        bot_instance.reply_to(message, f"⚡ Simulation speed set to `{speed}x` for `{tracking_number}`.", parse_mode='Markdown')
        bot_logger.info(f"Set simulation speed for {tracking_number} to {speed}x")
    except Exception as e:
        bot_instance.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in setspeed command: {e}")

@bot.message_handler(commands=['generate'])
@rate_limit
def handle_generate(message):
    """Handle /generate command to create a tracking ID."""
    bot_instance = get_bot_instance()
    tracking_id = generate_unique_id()
    bot_instance.reply_to(message, f"Generated Tracking ID: `{tracking_id}`", parse_mode='Markdown')
    bot_logger.info(f"Generated tracking ID {tracking_id} for user {message.from_user.id}")

@bot.message_handler(commands=['list'])
@rate_limit
def list_shipments(message):
    """Handle /list command to display a paginated list of shipments."""
    bot_instance = get_bot_instance()
    if not is_admin(message.from_user.id):
        bot_instance.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /list by {message.from_user.id}")
        return
    try:
        shipments, total = get_all_shipments_with_context(page=1)
        if not shipments:
            bot_instance.reply_to(message, "No shipments available.", parse_mode='Markdown')
            bot_logger.debug("No shipments available for /list")
            return
        markup = InlineKeyboardMarkup(row_width=1)
        for s in shipments:
            tn = s['tracking_number']
            label = f"{tn} [{s['status']}]"
            if s.get('carrier') == 'DHL':
                label = f"{tn} [DHL]"
            markup.add(InlineKeyboardButton(label, callback_data=f"view_{tn}"))
        if total > 10:
            markup.add(InlineKeyboardButton("Next", callback_data="list_2"))
        markup.add(InlineKeyboardButton("🏠 Home", callback_data="menu_page_1"))
        bot_instance.reply_to(message, f"*📋 Shipment List* (Page 1, {total} total):", parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent shipment list to admin {message.from_user.id}")
    except Exception as e:
        bot_instance.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in list command: {e}")

@bot.message_handler(commands=['add'])
@rate_limit
def add_shipment(message):
    """Handle /add command to create a new shipment."""
    bot_instance = get_bot_instance()
    if not is_admin(message.from_user.id):
        bot_instance.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /add by {message.from_user.id}")
        return
    parts = message.text.strip().split(maxsplit=3)
    if len(parts) < 4:
        bot_instance.reply_to(message, "Usage: /add <tracking_number> <status> <delivery_location> [origin_location] [recipient_email] [webhook_url]\n"
                            "Example: /add JD1234567890 Pending 'Lagos, NG' 'Abuja, NG' user@example.com https://example.com")
        bot_logger.warning("Invalid /add command format")
        return
    tracking_number = sanitize_tracking_number(parts[1].strip())
    status = parts[2].strip()
    delivery_location = parts[3].strip()
    args = message.text.split(maxsplit=6)[4:] if len(message.text.split()) > 4 else []
    origin_location = args[0] if len(args) > 0 else "Lagos, NG"
    recipient_email = args[1] if len(args) > 1 else None
    webhook_url = args[2] if len(args) > 2 else None
    try:
        if not tracking_number:
            bot_instance.reply_to(message, "Invalid tracking number. Must be JDxxxxxxxxxx")
            return
        if status not in config.valid_statuses:
            bot_instance.reply_to(message, f"Invalid status. Must be one of: {', '.join(config.valid_statuses)}")
            return
        if save_shipment_with_context(tracking_number, status, '', delivery_location, 
                                     recipient_email, origin_location, webhook_url, "DHL"):
            bot_instance.reply_to(message, f"✅ Shipment `{tracking_number}` added.", parse_mode='Markdown')
            bot_logger.info(f"Added shipment {tracking_number} by admin {message.from_user.id}")
        else:
            bot_instance.reply_to(message, "❌ Failed to add shipment.")
    except Exception as e:
        bot_instance.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in add command: {e}")

@bot.message_handler(commands=['export'])
@rate_limit
def export_shipments_command(message):
    """Handle /export command to export shipment data as JSON."""
    bot_instance = get_bot_instance()
    if not is_admin(message.from_user.id):
        bot_instance.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /export by {message.from_user.id}")
        return
    try:
        export_data = export_shipments_with_context()
        if not export_data:
            bot_instance.reply_to(message, "No shipments to export or error occurred.", parse_mode='Markdown')
            return
        max_length = 4096
        if len(export_data) <= max_length:
            bot_instance.reply_to(message, f"```json\n{export_data}\n```", parse_mode='Markdown')
        else:
            parts = [export_data[i:i+max_length] for i in range(0, len(export_data), max_length)]
            for i, part in enumerate(parts, 1):
                bot_instance.reply_to(message, f"```json\nPart {i}/{len(parts)}:\n{part}\n```", parse_mode='Markdown')
        bot_logger.info(f"Exported shipments for admin {message.from_user.id}")
    except Exception as e:
        bot_instance.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in export command: {e}")

@bot.message_handler(commands=['logs'])
@rate_limit
def get_logs_command(message):
    """Handle /logs command to retrieve recent bot logs."""
    bot_instance = get_bot_instance()
    if not is_admin(message.from_user.id):
        bot_instance.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for /logs by {message.from_user.id}")
        return
    try:
        logs = get_recent_logs_with_context(limit=5)
        if not logs:
            bot_instance.reply_to(message, "No logs available.", parse_mode='Markdown')
            return
        response = "*📋 Recent Logs*:\n" + "\n".join([f"`{log}`" for log in logs])
        bot_instance.reply_to(message, response, parse_mode='Markdown')
        bot_logger.info(f"Sent recent logs to admin {message.from_user.id}")
    except Exception as e:
        bot_instance.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in logs command: {e}")

# === CALLBACK HANDLERS ===
@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    """Handle all callback queries from inline buttons."""
    bot_instance = get_bot_instance()
    data = call.data
    try:
        if data.startswith("menu_page_"):
            page = int(data.split("_")[-1])
            send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
        elif data.startswith("view_"):
            tracking_number = data.split("_", 1)[1]
            shipment = get_shipment_with_context(tracking_number)
            if not shipment:
                bot_instance.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
                return
            paused = safe_redis_operation(redis_client.hget, "paused_simulations", tracking_number) == "true" if redis_client else False
            speed = float(safe_redis_operation(redis_client.hget, "sim_speed_multipliers", tracking_number) or 1.0) if redis_client else 1.0
            distance = estimate_distance(shipment.get('origin_location') or "Lagos, NG", shipment.get('delivery_location', ''))
            
            service_level = safe_redis_operation(redis_client.hget, "service_level", tracking_number) if redis_client else None
            if service_level and isinstance(service_level, bytes):
                service_level = service_level.decode('utf-8')
            delivery_window = safe_redis_operation(redis_client.hget, "delivery_window", tracking_number) if redis_client else None
            if delivery_window and isinstance(delivery_window, bytes):
                delivery_window = delivery_window.decode('utf-8')
            
            response = (
                f"*Shipment*: `{tracking_number}`\n"
                f"*Carrier*: `{shipment.get('carrier', 'DHL')}`\n"
                f"*Status*: `{shipment['status']}`\n"
                f"*Service*: `{service_level or 'DHL Express'}`\n"
                f"*Delivery Window*: `{delivery_window or 'Calculating...'}`\n"
                f"*Paused*: `{paused}`\n"
                f"*Speed*: `{speed}x`\n"
                f"*Distance*: `{distance} km`\n"
                f"*Origin*: `{shipment.get('origin_location', 'N/A')}`\n"
                f"*Destination*: `{shipment['delivery_location']}`\n"
                f"*Checkpoints*: `{len(shipment.get('checkpoints', []))}`"
            )
            markup = InlineKeyboardMarkup(row_width=2)
            if shipment['status'] not in ['Delivered', 'Returned']:
                markup.add(
                    InlineKeyboardButton("⏸ Pause" if not paused else "▶️ Resume", 
                                       callback_data=f"{'pause' if not paused else 'resume'}_{tracking_number}_1"),
                    InlineKeyboardButton("⚡ Speed", callback_data=f"setspeed_{tracking_number}")
                )
            markup.add(
                InlineKeyboardButton("📧 Notify", callback_data=f"notify_{tracking_number}"),
                InlineKeyboardButton("🔗 Webhook", callback_data=f"set_webhook_{tracking_number}"),
                InlineKeyboardButton("🧪 Test Webhook", callback_data=f"test_webhook_{tracking_number}"),
                InlineKeyboardButton("🏠 Home", callback_data="menu_page_1")
            )
            bot_instance.edit_message_text(response, chat_id=call.message.chat.id, message_id=call.message.message_id,
                                 parse_mode='Markdown', reply_markup=markup)
        elif data.startswith("pause_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            if redis_client:
                safe_redis_operation(redis_client.hset, "paused_simulations", tracking_number, "true")
            bot_instance.answer_callback_query(call.id, f"⏸ Simulation paused for `{tracking_number}`.")
            show_shipment_menu(call, page, "view", "Select shipment to view")
        elif data.startswith("resume_"):
            tracking_number, page = data.split("_")[1], int(data.split("_")[2])
            if redis_client:
                safe_redis_operation(redis_client.hdel, "paused_simulations", tracking_number)
            bot_instance.answer_callback_query(call.id, f"▶️ Simulation resumed for `{tracking_number}`.")
            show_shipment_menu(call, page, "view", "Select shipment to view")
        elif data.startswith("setspeed_"):
            tracking_number = data.split("_", 1)[1]
            bot_instance.answer_callback_query(call.id, f"Enter speed for `{tracking_number}` (0.1 to 10.0):", show_alert=True)
            bot_instance.register_next_step_handler(call.message, lambda msg: handle_set_speed(msg, tracking_number))
        elif data.startswith("notify_"):
            tracking_number = data.split("_", 1)[1]
            shipment = get_shipment_with_context(tracking_number)
            if not shipment:
                bot_instance.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
                return
            markup = InlineKeyboardMarkup(row_width=2)
            if shipment.get('recipient_email') and shipment.get('email_notifications'):
                markup.add(InlineKeyboardButton("📧 Send Email", callback_data=f"send_email_{tracking_number}"))
            if shipment.get('webhook_url'):
                markup.add(InlineKeyboardButton("🔗 Send Webhook", callback_data=f"send_webhook_{tracking_number}"))
            markup.add(InlineKeyboardButton("🏠 Home", callback_data="menu_page_1"))
            bot_instance.edit_message_text(f"Select notification type for `{tracking_number}`:", chat_id=call.message.chat.id,
                                 message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
        elif data.startswith("send_email_"):
            tracking_number = data.split("_", 2)[2]
            shipment = get_shipment_with_context(tracking_number)
            if not shipment or not shipment.get('recipient_email'):
                bot_instance.answer_callback_query(call.id, "No email configured.", show_alert=True)
                return
            notification_data = {
                "tracking_number": tracking_number,
                "type": "email",
                "data": {
                    "recipient_email": shipment['recipient_email'],
                    "status": shipment['status'],
                    "checkpoints": "; ".join(shipment.get('checkpoints', [])),
                    "delivery_location": shipment['delivery_location']
                }
            }
            if enqueue_notification(notification_data):
                bot_instance.answer_callback_query(call.id, f"📧 Email enqueued for `{tracking_number}`.")
            else:
                bot_instance.answer_callback_query(call.id, "Failed to enqueue email.", show_alert=True)
        elif data.startswith("send_webhook_"):
            tracking_number = data.split("_", 2)[2]
            shipment = get_shipment_with_context(tracking_number)
            if not shipment or not shipment.get('webhook_url'):
                bot_instance.answer_callback_query(call.id, "No webhook URL set.", show_alert=True)
                return
            notification_data = {
                "tracking_number": tracking_number,
                "type": "webhook",
                "data": {
                    "status": shipment['status'],
                    "checkpoints": "; ".join(shipment.get('checkpoints', [])),
                    "delivery_location": shipment['delivery_location'],
                    "webhook_url": shipment['webhook_url']
                }
            }
            if enqueue_notification(notification_data):
                bot_instance.answer_callback_query(call.id, f"🔗 Webhook enqueued for `{tracking_number}`.")
            else:
                bot_instance.answer_callback_query(call.id, "Failed to enqueue webhook.", show_alert=True)
        elif data.startswith("set_webhook_"):
            tracking_number = data.split("_", 2)[2]
            bot_instance.answer_callback_query(call.id, f"Enter webhook URL for `{tracking_number}`:", show_alert=True)
            bot_instance.register_next_step_handler(call.message, lambda msg: handle_set_webhook(msg, tracking_number))
        elif data.startswith("test_webhook_"):
            tracking_number = data.split("_", 2)[2]
            send_manual_webhook(call, tracking_number)
        elif data.startswith("list_"):
            page = int(data.split("_")[-1])
            show_shipment_menu(call, page, "view", "Select shipment to view")
        elif data == "generate_id":
            new_id = generate_unique_id()
            bot_instance.answer_callback_query(call.id, f"Generated ID: `{new_id}`", show_alert=True)
        elif data == "add":
            bot_instance.answer_callback_query(call.id, "Enter: tracking_number status delivery_location [origin] [email] [webhook]", show_alert=True)
            bot_instance.register_next_step_handler(call.message, handle_add_shipment)
        elif data == "help":
            help_text = (
                "*📚 Help Menu*\n\n"
                "/start - Show menu\n"
                "/track JD... - Track shipment\n"
                "/notify JD... - Send notification\n"
                "/stop JD... - Pause simulation\n"
                "/continue JD... - Resume simulation\n"
                "/setspeed JD... 2.0 - Set speed\n"
                "/generate - New ID\n"
                "/add - Create shipment\n"
                "/list - View all shipments\n"
                "/export - Download JSON\n"
                "/logs - View logs\n"
                "/stats - System statistics\n"
                "/search - Search shipments\n"
                "/bulk_action - Bulk operations\n"
                "/myid - Get your user ID"
            )
            bot_instance.edit_message_text(help_text, call.message.chat.id, call.message.message_id,
                                 parse_mode='Markdown', reply_markup=InlineKeyboardMarkup().add(
                                     InlineKeyboardButton("🏠 Home", callback_data="menu_page_1")))
        bot_instance.answer_callback_query(call.id)
        bot_logger.info(f"Processed callback {data}")
    except Exception as e:
        bot_instance.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Callback error: {e}")

# === STEP HANDLERS ===
def handle_set_speed(message, tracking_number):
    bot_instance = get_bot_instance()
    try:
        speed = float(message.text.strip())
        if speed < 0.1 or speed > 10:
            bot_instance.reply_to(message, "Speed must be 0.1 to 10.0")
            return
        if redis_client:
            safe_redis_operation(redis_client.hset, "sim_speed_multipliers", tracking_number, str(speed))
        bot_instance.reply_to(message, f"⚡ Speed set to `{speed}x` for `{tracking_number}`.", parse_mode='Markdown')
    except:
        bot_instance.reply_to(message, "❌ Invalid speed.")

def handle_set_webhook(message, tracking_number):
    bot_instance = get_bot_instance()
    webhook_url = message.text.strip()
    if not re.match(r'^https?://[^\s/$.?#].[^\s]*$', webhook_url):
        bot_instance.reply_to(message, "❌ Invalid URL.")
        return
    if update_shipment_with_context(tracking_number, webhook_url=webhook_url):
        bot_instance.reply_to(message, f"🔗 Webhook set for `{tracking_number}`.", parse_mode='Markdown')
    else:
        bot_instance.reply_to(message, "❌ Failed to set webhook.")

def handle_add_shipment(message):
    bot_instance = get_bot_instance()
    parts = message.text.strip().split()
    if len(parts) < 3:
        bot_instance.reply_to(message, "Usage: tracking_number status delivery_location [origin] [email] [webhook]")
        return
    tn, status, dest = parts[:3]
    origin = parts[3] if len(parts) > 3 else "Lagos, NG"
    email = parts[4] if len(parts) > 4 else None
    webhook = parts[5] if len(parts) > 5 else None
    tn = sanitize_tracking_number(tn)
    if not tn or status not in config.valid_statuses:
        bot_instance.reply_to(message, "Invalid tracking number or status.")
        return
    if save_shipment_with_context(tn, status, '', dest, email, origin, webhook, "DHL"):
        bot_instance.reply_to(message, f"✅ Shipment `{tn}` added.", parse_mode='Markdown')
    else:
        bot_instance.reply_to(message, "❌ Failed to add shipment.")

def get_startup_welcome_text() -> str:
    return (
        "🚀 *DHL Shipment Bot is online!*\n\n"
        "📦 *Commands:*\n"
        "/menu - Open admin panel\n"
        "/track <tracking_number> - View shipment details\n"
        "/notify <tracking_number> - Trigger notifications\n"
        "/stop <tracking_number> - Pause simulation\n"
        "/continue <tracking_number> - Resume simulation\n"
        "/setspeed <tracking_number> <speed> - Set simulation speed\n"
        "/list - List all shipments\n"
        "/search <query> - Search shipments\n"
        "/stats - System statistics\n"
        "/export - Export shipments as JSON\n"
        "/logs - View recent logs\n"
        "/myid - Get your Telegram user ID\n\n"
        "💡 *Tip:* You can also just send me a tracking number directly!"
    )

def notify_admins_on_startup() -> None:
    bot_instance = get_bot_instance()
    if not config.allowed_admins:
        bot_logger.warning("No allowed admins configured for startup notifications.")
        return

    if redis_client:
        already_notified = safe_redis_operation(redis_client.exists, "bot:startup_notified")
        if already_notified:
            bot_logger.info("Startup welcome already sent recently; skipping.")
            return

    welcome_text = get_startup_welcome_text()
    for admin_id in config.allowed_admins:
        try:
            bot_instance.send_message(admin_id, welcome_text, parse_mode='Markdown', disable_web_page_preview=True)
            bot_logger.info(f"Sent startup welcome to admin {admin_id}")
        except Exception as e:
            bot_logger.error(f"Failed to send startup welcome to {admin_id}: {e}")

    if redis_client:
        safe_redis_operation(redis_client.set, "bot:startup_notified", "1", ex=86400)

def is_valid_webhook_url(url: str) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False

def is_hostname_resolvable(hostname: str) -> bool:
    if not hostname:
        return False
    try:
        socket.getaddrinfo(hostname, None)
        return True
    except socket.gaierror as e:
        bot_logger.error(f"Webhook hostname resolution failed: {hostname} -> {e}")
        return False

def set_webhook() -> None:
    bot_instance = get_bot_instance()
    if not config.webhook_url or not is_valid_webhook_url(config.webhook_url):
        bot_logger.error(f"Skipping webhook setup because WEBHOOK_URL is invalid: {config.webhook_url}")
        return

    webhook_host = urlparse(config.webhook_url).hostname
    if not is_hostname_resolvable(webhook_host):
        bot_logger.error(f"Skipping webhook setup because webhook host is not resolvable: {webhook_host}")
        return

    try:
        bot_instance.remove_webhook()
        time.sleep(1)
        bot_instance.set_webhook(url=config.webhook_url)
        bot_logger.info(f"Webhook set: {config.webhook_url}")
    except Exception as e:
        bot_logger.error(f"Webhook failed: {e}")

def start_bot_service() -> None:
    """Initialize bot services with proper app context."""
    with app.app_context():
        cache_route_templates()
    set_webhook()
    notify_admins_on_startup()

def send_manual_webhook(call, tracking_number):
    bot_instance = get_bot_instance()
    shipment = get_shipment_with_context(tracking_number)
    if not shipment or not shipment.get('webhook_url'):
        bot_instance.answer_callback_query(call.id, "No webhook URL.", show_alert=True)
        return
    data = {
        "tracking_number": tracking_number,
        "type": "webhook",
        "data": {
            "status": shipment['status'],
            "checkpoints": "; ".join(shipment.get('checkpoints', [])),
            "delivery_location": shipment['delivery_location'],
            "webhook_url": shipment['webhook_url']
        }
    }
    if enqueue_notification(data):
        bot_instance.answer_callback_query(call.id, "🔗 Webhook enqueued.")
    else:
        bot_instance.answer_callback_query(call.id, "❌ Failed.", show_alert=True)

def keep_alive_loop():
    """Keep the bot alive with periodic health checks."""
    while True:
        try:
            # Check Redis connection
            if redis_client:
                redis_client.ping()
            # Check database connection
            with app.app_context():
                db.session.execute(text('SELECT 1'))
            bot_logger.debug("Health check passed")
        except Exception as e:
            bot_logger.error(f"Health check failed: {e}")
        time.sleep(300)  # 5 minutes

def main():
    """Main entry point for the bot."""
    try:
        # Initialize bot
        global bot
        bot = get_bot()
        
        start_bot_service()
        bot_logger.info("Webhook setup completed successfully")
        console.print("[green]✅ bot.py started — DHL + All Features Live[/green]")
        
        # Start keep-alive thread
        keep_alive_thread = threading.Thread(target=keep_alive_loop, daemon=True)
        keep_alive_thread.start()
        bot_logger.info("Keep-alive loop started")
        
        # Keep the main thread alive
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        console.print("[yellow]Bot shutting down...[/yellow]")
        bot_logger.info("Bot shutting down")
    except Exception as e:
        console.print(f"[red]Fatal error: {e}[/red]")
        bot_logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()
