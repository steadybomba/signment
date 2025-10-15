import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ForceReply
from datetime import datetime
import random
import string
import requests
from flask_sqlalchemy import SQLAlchemyError
from rich.console import Console
from rich.panel import Panel
import logging
import eventlet
import re
import shlex  # For robust input parsing

# Logging setup
bot_logger = logging.getLogger('telegram_bot')

# In-memory stores (use Redis in production for multi-worker setups)
shipment_cache = {}
chat_data_store = {}  # For batch operations, etc.
route_templates = {}

# Global console (will be synced from app)
console = Console()

# Lazy import functions to avoid circular imports
def get_app_modules():
    from app import db, Shipment, sanitize_tracking_number, validate_email, validate_location, validate_webhook_url, send_email_notification, console as app_console, paused_simulations, sim_speed_multipliers
    global console
    console = app_console  # Sync with app's console
    return db, Shipment, sanitize_tracking_number, validate_email, validate_location, validate_webhook_url, send_email_notification, paused_simulations, sim_speed_multipliers

def get_config_values():
    from config import WEBSOCKET_SERVER, VALID_STATUSES, ALLOWED_ADMINS
    from app import app
    token = app.config.get('TELEGRAM_BOT_TOKEN')
    websocket_server = WEBSOCKET_SERVER or 'http://localhost:5000' if WEBSOCKET_SERVER else 'http://localhost:5000'
    valid_statuses = VALID_STATUSES or ['Pending', 'In_Transit', 'Out_for_Delivery', 'Delivered', 'Returned', 'Delayed']
    allowed_admins = [int(uid) for uid in ALLOWED_ADMINS] if ALLOWED_ADMINS else []
    return token, websocket_server, valid_statuses, allowed_admins

# Bot instance (lazy init)
_bot_instance = None
def get_bot():
    global _bot_instance
    if _bot_instance is None:
        token, _, _, _ = get_config_values()
        if not token:
            raise ValueError("Missing TELEGRAM_BOT_TOKEN in config")
        _bot_instance = telebot.TeleBot(token)
    return _bot_instance

bot = get_bot()

def cache_route_templates():
    """Cache predefined route templates for shipment simulations."""
    global route_templates
    route_templates = {
        'Lagos, NG': ['Lagos, NG', 'Abuja, NG', 'Port Harcourt, NG', 'Kano, NG'],
        'New York, NY': ['New York, NY', 'Chicago, IL', 'Los Angeles, CA', 'Miami, FL'],
        'London, UK': ['London, UK', 'Manchester, UK', 'Birmingham, UK', 'Edinburgh, UK'],
        # Add more as needed
    }
    bot_logger.debug("Cached route templates", extra={'tracking_number': ''})
    console.print("[info]Cached route templates[/info]")

def get_cached_route_templates():
    """Retrieve cached route templates."""
    return route_templates

def is_admin(user_id):
    """Check if the user is an admin based on ALLOWED_ADMINS."""
    _, _, _, allowed_admins = get_config_values()
    is_admin_user = user_id in allowed_admins
    bot_logger.debug(f"Checked admin status for user {user_id}: {is_admin_user}", extra={'tracking_number': ''})
    return is_admin_user

def generate_unique_id():
    """Generate a unique tracking number using timestamp and random string."""
    db, Shipment, _, _, _, _, _, _, _, _ = get_app_modules()
    attempts = 0
    while attempts < 10:  # Prevent infinite loop
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
    db, Shipment, _, _, _, _, _, _, _, _ = get_app_modules()
    try:
        offset = (page - 1) * per_page
        shipments = Shipment.query.with_entities(Shipment.tracking_number).order_by(Shipment.tracking_number).offset(offset).limit(per_page).all()
        total = Shipment.query.count()
        return [s.tracking_number for s in shipments], total
    except SQLAlchemyError as e:
        bot_logger.error(f"Database error fetching shipment list: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Database error fetching shipment list: {e}[/error]", title="Database Error", border_style="red"))
        return [], 0

def get_shipment_details(tracking_number):
    """Fetch shipment details, using cache to reduce database queries."""
    db, Shipment, sanitize_tracking_number, _, _, _, _, paused_simulations, sim_speed_multipliers, _ = get_app_modules()
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        return None
    if sanitized_tn in shipment_cache:
        bot_logger.debug(f"Retrieved {sanitized_tn} from cache", extra={'tracking_number': sanitized_tn})
        return shipment_cache[sanitized_tn]
    try:
        shipment = Shipment.query.filter_by(tracking_number=sanitized_tn).first()
        details = shipment.to_dict() if shipment else None
        if details:
            details['paused'] = paused_simulations.get(sanitized_tn, False)
            details['speed_multiplier'] = sim_speed_multipliers.get(sanitized_tn, 1.0)
            shipment_cache[sanitized_tn] = details
        bot_logger.debug(f"Fetched details for {sanitized_tn}", extra={'tracking_number': sanitized_tn})
        return details
    except SQLAlchemyError as e:
        bot_logger.error(f"Database error fetching details: {e}", extra={'tracking_number': sanitized_tn})
        console.print(Panel(f"[error]Database error fetching details for {sanitized_tn}: {e}[/error]", title="Database Error", border_style="red"))
        return None

def invalidate_cache(tracking_number):
    """Invalidate cache entry for a tracking number."""
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if sanitized_tn in shipment_cache:
        del shipment_cache[sanitized_tn]

def save_shipment(tracking_number, status, checkpoints, delivery_location, recipient_email='', origin_location=None, webhook_url=None, email_notifications=True):
    """Save or update a shipment in the database and update cache."""
    db, Shipment, sanitize_tracking_number, validate_email, validate_location, validate_webhook_url, send_email_notification, paused_simulations, sim_speed_multipliers = get_app_modules()
    _, WEBSOCKET_SERVER, VALID_STATUSES, _ = get_config_values()
    sanitized_tn = sanitize_tracking_number(tracking_number)
    if not sanitized_tn:
        raise ValueError("Invalid tracking number")
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid status. Must be one of: {', '.join(VALID_STATUSES)}")
    if not validate_location(delivery_location):
        raise ValueError(f"Invalid delivery location. Must be one of: {', '.join(route_templates.keys())}")
    if origin_location and not validate_location(origin_location):
        raise ValueError(f"Invalid origin location. Must be one of: {', '.join(route_templates.keys())}")
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
        # Update cache
        details = shipment.to_dict()
        details['paused'] = paused_simulations.get(sanitized_tn, False)
        details['speed_multiplier'] = sim_speed_multipliers.get(sanitized_tn, 1.0)
        shipment_cache[sanitized_tn] = details
        bot_logger.info(f"Saved shipment: status={status}", extra={'tracking_number': sanitized_tn})
        console.print(f"[info]Saved shipment {sanitized_tn}: {status}[/info]")
        if recipient_email and email_notifications:
            eventlet.spawn(send_email_notification, sanitized_tn, status, checkpoints, delivery_location, recipient_email)
        try:
            response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{sanitized_tn}', timeout=5)
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
            ("View Sim Speed", f"getspeed_menu_{page}")
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
    except telebot.apihelper.ApiTelegramException as e:
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

@bot.message_handler(commands=['stop'])
def stop_simulation(message):
    """Handle /stop command to pause a shipment's simulation."""
    db, Shipment, sanitize_tracking_number, _, _, _, _, paused_simulations, _, _ = get_app_modules()
    _, WEBSOCKET_SERVER, _, _ = get_config_values()
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
        if paused_simulations.get(tracking_number, False):
            bot.reply_to(message, f"Simulation for `{tracking_number}` is already paused.", parse_mode='Markdown')
            bot_logger.warning(f"Simulation already paused: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        paused_simulations[tracking_number] = True
        invalidate_cache(tracking_number)
        bot_logger.info(f"Paused simulation for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Paused simulation for {tracking_number} by admin {message.from_user.id}[/info]", title="Simulation Paused", border_style="green"))
        try:
            response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
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
    db, Shipment, sanitize_tracking_number, _, _, _, _, paused_simulations, _, _ = get_app_modules()
    _, WEBSOCKET_SERVER, _, _ = get_config_values()
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
        if not paused_simulations.get(tracking_number, False):
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
        paused_simulations[tracking_number] = False
        invalidate_cache(tracking_number)
        bot_logger.info(f"Resumed simulation for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Resumed simulation for {tracking_number} by admin {message.from_user.id}[/info]", title="Simulation Resumed", border_style="green"))
        try:
            response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
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
    db, Shipment, sanitize_tracking_number, _, _, _, _, _, sim_speed_multipliers, _ = get_app_modules()
    _, WEBSOCKET_SERVER, _, _ = get_config_values()
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
        sim_speed_multipliers[tracking_number] = speed
        invalidate_cache(tracking_number)
        bot_logger.info(f"Set simulation speed for {tracking_number} to {speed}x", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Set simulation speed for {tracking_number} to {speed}x by admin {message.from_user.id}[/info]", title="Speed Updated", border_style="green"))
        try:
            response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
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
    db, Shipment, sanitize_tracking_number, _, _, _, _, _, sim_speed_multipliers, _ = get_app_modules()
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
        speed = sim_speed_multipliers.get(tracking_number, 1.0)
        bot_logger.info(f"Retrieved simulation speed for {tracking_number}: {speed}x", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Retrieved simulation speed for {tracking_number}: {speed}x by admin {message.from_user.id}[/info]", title="Speed Retrieved", border_style="green"))
        bot.reply_to(message, f"Simulation speed for `{tracking_number}` is `{speed}x`.", parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in getspeed command: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in getspeed command for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.message_handler(func=lambda message: message.reply_to_message and message.reply_to_message.reply_markup and isinstance(message.reply_to_message.reply_markup, ForceReply))
def handle_reply_input(message):
    """Handle replies to ForceReply messages for add, update, and setspeed actions."""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for reply input by {message.from_user.id}", extra={'tracking_number': ''})
        return
    original_message = message.reply_to_message.text or ''
    tracking_number = None
    action = None
    if "Enter shipment details" in original_message:
        action = "add"
    elif "Enter updates for" in original_message:
        action = "update"
        match = re.search(r"Enter updates for `?([A-Z0-9]+)`?:", original_message)
        tracking_number = sanitize_tracking_number(match.group(1)) if match else None
    elif "Enter simulation speed for" in original_message:
        action = "setspeed"
        match = re.search(r"Enter simulation speed for `?([A-Z0-9]+)`?", original_message)
        tracking_number = sanitize_tracking_number(match.group(1)) if match else None
    elif message.text.lower() == "cancel":
        bot.reply_to(message, "Action cancelled.")
        send_dynamic_menu(message.chat.id, page=1)
        bot_logger.info(f"Action cancelled by admin {message.from_user.id}", extra={'tracking_number': tracking_number or ''})
        console.print(f"[info]Action cancelled by admin {message.from_user.id}[/info]")
        return
    else:
        bot.reply_to(message, "Unknown action.")
        bot_logger.warning(f"Unknown reply action by {message.from_user.id}", extra={'tracking_number': ''})
        return

    if action == "add":
        handle_add_input(message)
    elif action == "update" and tracking_number:
        handle_update_input(message, tracking_number)
    elif action == "setspeed" and tracking_number:
        handle_setspeed_input(message, tracking_number)

def handle_add_input(message):
    """Handle input for adding a new shipment using shlex for parsing."""
    db, Shipment, sanitize_tracking_number, _, _, _, _, _, _, _ = get_app_modules()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for add input by {message.from_user.id}", extra={'tracking_number': ''})
        return
    if message.text.lower() == "cancel":
        bot.reply_to(message, "Add shipment cancelled.")
        send_dynamic_menu(message.chat.id, page=1)
        bot_logger.info(f"Add shipment cancelled by admin {message.from_user.id}", extra={'tracking_number': ''})
        return
    try:
        # Use shlex to handle quoted strings
        args = shlex.split(message.text)
        if len(args) < 4:
            raise ValueError("Invalid format. Expected at least 4 arguments.")
        tracking_number = sanitize_tracking_number(args[0])
        status = args[1]
        checkpoints = args[2]
        delivery_location = args[3]
        recipient_email = args[4] if len(args) > 4 else ''
        origin_location = args[5] if len(args) > 5 else None
        webhook_url = args[6] if len(args) > 6 else None
        save_shipment(
            tracking_number=tracking_number,
            status=status,
            checkpoints=checkpoints,
            delivery_location=delivery_location,
            recipient_email=recipient_email,
            origin_location=origin_location,
            webhook_url=webhook_url
        )
        bot.reply_to(message, f"Shipment `{tracking_number}` added successfully.", parse_mode='Markdown')
        bot_logger.info(f"Added shipment {tracking_number} by admin {message.from_user.id}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Added shipment {tracking_number} by admin {message.from_user.id}[/info]")
        send_dynamic_menu(message.chat.id, page=1)
    except ValueError as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in add input: {e}", extra={'tracking_number': tracking_number or ''})
        console.print(Panel(f"[error]Error in add input for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))
    except Exception as e:
        bot.reply_to(message, f"Unexpected error: {e}")
        bot_logger.error(f"Unexpected error in add input: {e}", extra={'tracking_number': tracking_number or ''})
        console.print(Panel(f"[error]Unexpected error in add input for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def handle_update_input(message, tracking_number):
    """Handle input for updating an existing shipment."""
    db, Shipment, sanitize_tracking_number, _, _, _, _, _, _, _ = get_app_modules()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for update input by {message.from_user.id}", extra={'tracking_number': tracking_number})
        return
    if message.text.lower() == "cancel":
        bot.reply_to(message, "Update shipment cancelled.")
        send_dynamic_menu(message.chat.id, page=1)
        bot_logger.info(f"Update shipment cancelled for {tracking_number} by admin {message.from_user.id}", extra={'tracking_number': tracking_number})
        return
    try:
        shipment = get_shipment_details(tracking_number)
        if not shipment:
            bot.reply_to(message, f"Shipment `{tracking_number}` not found.", parse_mode='Markdown')
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        updates = {}
        # Parse key=value pairs (allow spaces in values by splitting on first =)
        for part in message.text.strip().split():
            if '=' in part:
                key, value = part.split('=', 1)
                updates[key.strip()] = value.strip().strip('"')
        status = updates.get('status', shipment['status'])
        checkpoints = updates.get('checkpoints', shipment['checkpoints'] or '')
        delivery_location = updates.get('delivery_location', shipment['delivery_location'])
        recipient_email = updates.get('recipient_email', shipment['recipient_email'] or '')
        origin_location = updates.get('origin_location', shipment['origin_location'] or delivery_location)
        webhook_url = updates.get('webhook_url', shipment['webhook_url'] or None)
        email_notifications = updates.get('email_notifications', str(shipment['email_notifications'])).lower() == 'true'
        save_shipment(
            tracking_number=tracking_number,
            status=status,
            checkpoints=checkpoints,
            delivery_location=delivery_location,
            recipient_email=recipient_email,
            origin_location=origin_location,
            webhook_url=webhook_url,
            email_notifications=email_notifications
        )
        bot.reply_to(message, f"Shipment `{tracking_number}` updated successfully.", parse_mode='Markdown')
        bot_logger.info(f"Updated shipment {tracking_number} by admin {message.from_user.id}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Updated shipment {tracking_number} by admin {message.from_user.id}[/info]")
        send_dynamic_menu(message.chat.id, page=1)
    except ValueError as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in update input: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in update input for {tracking_number}: {e}[/error]", title="Telegram Error", border_style="red"))
    except Exception as e:
        bot.reply_to(message, f"Unexpected error: {e}")
        bot_logger.error(f"Unexpected error in update input: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Unexpected error in update input for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def handle_setspeed_input(message, tracking_number):
    """Handle input for setting simulation speed."""
    db, Shipment, sanitize_tracking_number, _, _, _, _, _, sim_speed_multipliers, _ = get_app_modules()
    _, WEBSOCKET_SERVER, _, _ = get_config_values()
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        bot_logger.warning(f"Access denied for set speed input by {message.from_user.id}", extra={'tracking_number': tracking_number})
        return
    if message.text.lower() == "cancel":
        bot.reply_to(message, "Set speed cancelled.")
        send_dynamic_menu(message.chat.id, page=1)
        bot_logger.info(f"Set speed cancelled for {tracking_number} by admin {message.from_user.id}", extra={'tracking_number': tracking_number})
        return
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
        sim_speed_multipliers[tracking_number] = speed
        invalidate_cache(tracking_number)
        bot_logger.info(f"Set simulation speed for {tracking_number} to {speed}x", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Set simulation speed for {tracking_number} to {speed}x by admin {message.from_user.id}[/info]", title="Speed Updated", border_style="green"))
        try:
            response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
            if response.status_code != 204:
                bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
        except requests.RequestException as e:
            bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
        bot.reply_to(message, f"Simulation speed for `{tracking_number}` set to `{speed}x`.", parse_mode='Markdown')
        send_dynamic_menu(message.chat.id, page=1)
    except ValueError:
        bot.reply_to(message, "Speed must be a number between 0.1 and 10.0.")
        bot_logger.warning(f"Invalid speed format: {message.text}", extra={'tracking_number': tracking_number})
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in setspeed input: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error in setspeed input for admin {message.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    """Handle callback queries from inline keyboard buttons for admin actions."""
    bot.last_call = call
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        bot_logger.warning(f"Access denied for callback by {call.from_user.id}", extra={'tracking_number': ''})
        return

    try:
        data = call.data
        page = 1
        parts = data.split('_', 1)
        action = parts[0]
        arg = parts[1] if len(parts) > 1 else None
        if arg and arg[0].isdigit():
            # Extract page if present
            match = re.search(r'_(\d+)$', data)
            if match:
                page = int(match.group(1))
                action = data[:match.start()]

        actions = {
            'generate_id': lambda: bot.answer_callback_query(call.id, f"Generated ID: `{generate_unique_id()}`", show_alert=True),
            'add': lambda: bot.send_message(
                call.message.chat.id,
                "Enter shipment details:\nFormat: `[tracking_number] <status> \"<checkpoints>\" <delivery_location> [recipient_email] [origin_location] [webhook_url]`\nExample: `TRK123 In_Transit \"2025-10-14 13:00 - Lagos, NG - Processed\" \"Lagos, NG\" user@example.com \"Abuja, NG\" https://webhook.site`\nType `cancel` to abort.",
                parse_mode='Markdown',
                reply_markup=ForceReply(selective=True)
            ),
            'view_menu': lambda: show_shipment_menu(call, page, "view", "Select shipment to view"),
            'view': lambda tn=arg: show_shipment_details(call, tn),
            'update_menu': lambda: show_shipment_menu(call, page, "update", "Select shipment to update"),
            'update': lambda tn=arg: bot.send_message(
                call.message.chat.id,
                f"Enter updates for `{tn}`:\nFormat: `<field=value>` (e.g., `status=In_Transit delivery_location=\"New York, NY\"`)\nFields: status, checkpoints, delivery_location, recipient_email, origin_location, webhook_url, email_notifications\nType `cancel` to abort.",
                parse_mode='Markdown',
                reply_markup=ForceReply(selective=True)
            ),
            'delete_menu': lambda: show_shipment_menu(call, page, "delete", "Select shipment to delete"),
            'delete': lambda tn=arg: delete_shipment(call, tn, page),
            'batch_delete_menu': lambda: show_shipment_menu(call, page, "batch_select", "Select shipments to delete", extra_buttons=[
                InlineKeyboardButton("Confirm Delete", callback_data=f"batch_delete_confirm_{page}"),
                InlineKeyboardButton("Back", callback_data=f"menu_page_{page}")
            ]),
            'batch_select': lambda tn=arg: toggle_batch_selection(call, tn),
            'batch_delete_confirm': lambda: batch_delete_shipments(call, page),
            'broadcast_menu': lambda: show_shipment_menu(call, page, "broadcast", "Select shipment to broadcast"),
            'broadcast': lambda tn=arg: trigger_broadcast(call, tn),
            'toggle_email_menu': lambda: show_shipment_menu(call, page, "toggle_email", "Select shipment to toggle email notifications"),
            'toggle_email': lambda tn=arg: toggle_email_notifications(call, tn, page),
            'pause_menu': lambda: show_shipment_menu(call, page, "pause", "Select shipment to pause simulation"),
            'pause': lambda tn=arg: pause_simulation_callback(call, tn, page),
            'resume_menu': lambda: show_shipment_menu(call, page, "resume", "Select shipment to resume simulation"),
            'resume': lambda tn=arg: resume_simulation_callback(call, tn, page),
            'setspeed_menu': lambda: show_shipment_menu(call, page, "setspeed", "Select shipment to set simulation speed"),
            'setspeed': lambda tn=arg: bot.send_message(
                call.message.chat.id,
                f"Enter simulation speed for `{tn}` (0.1 to 10.0):\nType `cancel` to abort.",
                parse_mode='Markdown',
                reply_markup=ForceReply(selective=True)
            ),
            'getspeed_menu': lambda: show_shipment_menu(call, page, "getspeed", "Select shipment to view simulation speed"),
            'getspeed': lambda tn=arg: show_simulation_speed(call, tn),
            'menu_page': lambda: send_dynamic_menu(call.message.chat.id, call.message.message_id, page),
            'list': lambda: list_shipments(call, page),
            'settings': lambda: bot.answer_callback_query(call.id, "Settings not implemented yet.", show_alert=True),
            'help': lambda: bot.answer_callback_query(call.id, "Help: Use /menu to access admin controls.\nCommands: /myid, /stop <tracking_number>, /continue <tracking_number>, /setspeed <tracking_number> <speed>, /getspeed <tracking_number>", show_alert=True),
            'cancel': lambda: (bot.answer_callback_query(call.id, "Action cancelled."), send_dynamic_menu(call.message.chat.id, call.message.message_id, page))
        }

        handler = actions.get(action)
        if handler:
            if 'tn' in handler.__code__.co_varnames:
                tn = sanitize_tracking_number(arg.split('_')[0]) if arg else None
                handler(tn)
            else:
                handler()
        else:
            bot.answer_callback_query(call.id, "Unknown action.", show_alert=True)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error in callback query: {e}", extra={'tracking_number': arg or ''})
        console.print(Panel(f"[error]Error in callback for admin {call.from_user.id}: {e}[/error]", title="Telegram Error", border_style="red"))

def show_shipment_menu(call, page, prefix, prompt, extra_buttons=None):
    """Display a menu of shipments for a specific action."""
    shipments, total = get_shipment_list(page)
    if shipments:
        markup = InlineKeyboardMarkup(row_width=1)
        for tn in shipments:
            markup.add(InlineKeyboardButton(tn, callback_data=f"{prefix}_{tn}"))
        if extra_buttons:
            markup.add(*extra_buttons)
        else:
            markup.add(InlineKeyboardButton("Back", callback_data=f"menu_page_{page}"))
        bot.edit_message_text(f"*{prompt} (Page {page})*:\nAvailable shipments: {total}", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup, parse_mode='Markdown')
        bot_logger.debug(f"{prefix} menu sent, page {page}", extra={'tracking_number': ''})
        console.print(f"[info]{prefix} menu sent to chat {call.message.chat.id}, page {page}[/info]")
    else:
        bot.answer_callback_query(call.id, "No shipments available.", show_alert=True)
        bot_logger.debug(f"No shipments for {prefix} menu, page {page}", extra={'tracking_number': ''})
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)

def show_shipment_details(call, tracking_number):
    """Display details for a specific shipment, including simulation state."""
    details = get_shipment_details(tracking_number)
    if details:
        response = (
            f"*Shipment*: `{details['tracking_number']}`\n"
            f"*Status*: `{details['status']}`\n"
            f"*Paused*: `{details.get('paused', False)}`\n"
            f"*Speed Multiplier*: `{details.get('speed_multiplier', 1.0)}x`\n"
            f"*Delivery Location*: `{details['delivery_location']}`\n"
            f"*Origin Location*: `{details.get('origin_location', 'None')}`\n"
            f"*Recipient Email*: `{details.get('recipient_email', 'None')}`\n"
            f"*Checkpoints*: `{details.get('checkpoints', 'None')}`\n"
            f"*Webhook URL*: `{details.get('webhook_url', 'Default')}`\n"
            f"*Email Notifications*: `{'Enabled' if details.get('email_notifications', False) else 'Disabled'}`\n"
            f"*Last Updated*: `{details.get('last_updated', 'N/A')}`"
        )
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, response, parse_mode='Markdown')
        bot_logger.info(f"Sent details for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Sent details for {tracking_number} to admin {call.from_user.id}[/info]")
    else:
        bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
        bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})

def delete_shipment(call, tracking_number, page):
    """Delete a specific shipment from the database and clear related state."""
    db, Shipment, sanitize_tracking_number, _, _, _, _, paused_simulations, sim_speed_multipliers, _ = get_app_modules()
    _, WEBSOCKET_SERVER, _, _ = get_config_values()
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if shipment:
            db.session.delete(shipment)
            db.session.commit()
            if tracking_number in paused_simulations:
                del paused_simulations[tracking_number]
            if tracking_number in sim_speed_multipliers:
                del sim_speed_multipliers[tracking_number]
            invalidate_cache(tracking_number)
            bot_logger.info(f"Deleted shipment {tracking_number}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Deleted shipment {tracking_number} by admin {call.from_user.id}[/info]")
            try:
                response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
                if response.status_code != 204:
                    bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
            except requests.RequestException as e:
                bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
                console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
            bot.answer_callback_query(call.id, f"Deleted `{tracking_number}`", show_alert=True)
            send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
        else:
            bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
            bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
    except SQLAlchemyError as e:
        db.session.rollback()
        bot_logger.error(f"Database error deleting shipment: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Database error deleting {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
        bot.answer_callback_query(call.id, f"Error deleting `{tracking_number}`: {e}", show_alert=True)

def get_chat_data(chat_id, key, default=[]):
    return chat_data_store.get(f"{chat_id}_{key}", default)

def set_chat_data(chat_id, key, value):
    chat_data_store[f"{chat_id}_{key}"] = value

def toggle_batch_selection(call, tracking_number):
    """Toggle selection of a shipment for batch deletion."""
    batch_list = get_chat_data(call.message.chat.id, 'batch_delete', [])
    if tracking_number in batch_list:
        batch_list.remove(tracking_number)
        bot.answer_callback_query(call.id, f"Deselected `{tracking_number}`")
    else:
        batch_list.append(tracking_number)
        bot.answer_callback_query(call.id, f"Selected `{tracking_number}`")
    set_chat_data(call.message.chat.id, 'batch_delete', batch_list)
    bot_logger.debug(f"Updated batch delete list: {batch_list}", extra={'tracking_number': tracking_number})
    console.print(f"[info]Updated batch delete list for {tracking_number} by admin {call.from_user.id}[/info]")

def batch_delete_shipments(call, page):
    """Delete multiple shipments in a single transaction."""
    db, Shipment, sanitize_tracking_number, _, _, _, _, paused_simulations, sim_speed_multipliers, _ = get_app_modules()
    _, WEBSOCKET_SERVER, _, _ = get_config_values()
    batch_list = get_chat_data(call.message.chat.id, 'batch_delete', [])
    if not batch_list:
        bot.answer_callback_query(call.id, "No shipments selected.", show_alert=True)
        bot_logger.debug(f"No shipments selected for batch delete, page {page}", extra={'tracking_number': ''})
        return
    try:
        deleted_count = 0
        for tn in batch_list:
            shipment = Shipment.query.filter_by(tracking_number=tn).first()
            if shipment:
                db.session.delete(shipment)
                if tn in paused_simulations:
                    del paused_simulations[tn]
                if tn in sim_speed_multipliers:
                    del sim_speed_multipliers[tn]
                invalidate_cache(tn)
                deleted_count += 1
                try:
                    response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tn}', timeout=5)
                    if response.status_code != 204:
                        bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tn})
                except requests.RequestException as e:
                    bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tn})
                    console.print(Panel(f"[warning]Broadcast error for {tn}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
        db.session.commit()
        bot.answer_callback_query(call.id, f"Deleted `{deleted_count}` shipments", show_alert=True)
        bot_logger.info(f"Batch deleted {deleted_count} shipments", extra={'tracking_number': ''})
        console.print(f"[info]Batch deleted {deleted_count} shipments by admin {call.from_user.id}[/info]")
        set_chat_data(call.message.chat.id, 'batch_delete', [])
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except SQLAlchemyError as e:
        db.session.rollback()
        bot_logger.error(f"Database error in batch delete: {e}", extra={'tracking_number': ''})
        console.print(Panel(f"[error]Database error in batch delete: {e}[/error]", title="Database Error", border_style="red"))
        bot.answer_callback_query(call.id, f"Error deleting shipments: {e}", show_alert=True)

def trigger_broadcast(call, tracking_number):
    """Trigger a broadcast for a specific shipment to update clients."""
    _, WEBSOCKET_SERVER, _, _ = get_config_values()
    try:
        response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
        if response.status_code == 204:
            bot.answer_callback_query(call.id, f"Broadcast triggered for `{tracking_number}`", show_alert=True)
            bot_logger.info(f"Broadcast triggered for {tracking_number}", extra={'tracking_number': tracking_number})
            console.print(f"[info]Broadcast triggered for {tracking_number} by admin {call.from_user.id}[/info]")
        else:
            bot.answer_callback_query(call.id, f"Broadcast failed: `{response.status_code}`", show_alert=True)
            bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
    except requests.RequestException as e:
        bot.answer_callback_query(call.id, f"Broadcast error: {e}", show_alert=True)
        bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))

def toggle_email_notifications(call, tracking_number, page):
    """Toggle email notifications for a specific shipment."""
    db, Shipment, sanitize_tracking_number, _, _, _, _, _, _, _ = get_app_modules()
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
        bot.answer_callback_query(call.id, f"Email notifications {status} for `{tracking_number}`", show_alert=True)
        bot_logger.info(f"Email notifications {status} for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(f"[info]Email notifications {status} for {tracking_number} by admin {call.from_user.id}[/info]")
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except SQLAlchemyError as e:
        db.session.rollback()
        bot_logger.error(f"Database error toggling email notifications: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Database error toggling email for {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
        bot.answer_callback_query(call.id, f"Error toggling email for `{tracking_number}`: {e}", show_alert=True)

def pause_simulation_callback(call, tracking_number, page):
    """Handle callback to pause a shipment's simulation via inline menu."""
    db, Shipment, sanitize_tracking_number, _, _, _, _, paused_simulations, _, _ = get_app_modules()
    _, WEBSOCKET_SERVER, _, _ = get_config_values()
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
        if paused_simulations.get(tracking_number, False):
            bot.answer_callback_query(call.id, f"Simulation for `{tracking_number}` is already paused.", show_alert=True)
            bot_logger.warning(f"Simulation already paused: {tracking_number}", extra={'tracking_number': tracking_number})
            return
        paused_simulations[tracking_number] = True
        invalidate_cache(tracking_number)
        bot.answer_callback_query(call.id, f"Simulation paused for `{tracking_number}`", show_alert=True)
        bot_logger.info(f"Paused simulation for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Paused simulation for {tracking_number} by admin {call.from_user.id}[/info]", title="Simulation Paused", border_style="green"))
        try:
            response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
            if response.status_code != 204:
                bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
        except requests.RequestException as e:
            bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except Exception as e:
        bot_logger.error(f"Error pausing simulation: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error pausing simulation for {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
        bot.answer_callback_query(call.id, f"Error pausing simulation for `{tracking_number}`: {e}", show_alert=True)

def resume_simulation_callback(call, tracking_number, page):
    """Handle callback to resume a shipment's simulation via inline menu."""
    db, Shipment, sanitize_tracking_number, _, _, _, _, paused_simulations, _, _ = get_app_modules()
    _, WEBSOCKET_SERVER, _, _ = get_config_values()
    try:
        if not paused_simulations.get(tracking_number, False):
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
        paused_simulations[tracking_number] = False
        invalidate_cache(tracking_number)
        bot.answer_callback_query(call.id, f"Simulation resumed for `{tracking_number}`", show_alert=True)
        bot_logger.info(f"Resumed simulation for {tracking_number}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[info]Resumed simulation for {tracking_number} by admin {call.from_user.id}[/info]", title="Simulation Resumed", border_style="green"))
        try:
            response = requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}', timeout=5)
            if response.status_code != 204:
                bot_logger.warning(f"Broadcast failed: {response.status_code}", extra={'tracking_number': tracking_number})
        except requests.RequestException as e:
            bot_logger.error(f"Broadcast error: {e}", extra={'tracking_number': tracking_number})
            console.print(Panel(f"[warning]Broadcast error for {tracking_number}: {e}[/warning]", title="Broadcast Warning", border_style="yellow"))
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)
    except Exception as e:
        bot_logger.error(f"Error resuming simulation: {e}", extra={'tracking_number': tracking_number})
        console.print(Panel(f"[error]Error resuming simulation for {tracking_number}: {e}[/error]", title="Database Error", border_style="red"))
        bot.answer_callback_query(call.id, f"Error resuming simulation for `{tracking_number}`: {e}", show_alert=True)

def show_simulation_speed(call, tracking_number):
    """Display the simulation speed for a specific shipment."""
    db, Shipment, sanitize_tracking_number, _, _, _, _, _, sim_speed_multipliers, _ = get_app_modules()
    shipment = get_shipment_details(tracking_number)
    if not shipment:
        bot.answer_callback_query(call.id, f"Shipment `{tracking_number}` not found.", show_alert=True)
        bot_logger.warning(f"Shipment not found: {tracking_number}", extra={'tracking_number': tracking_number})
        return
    speed = sim_speed_multipliers.get(tracking_number, 1.0)
    bot.answer_callback_query(call.id, f"Simulation speed for `{tracking_number}` is `{speed}x`.", show_alert=True)
    bot_logger.info(f"Retrieved simulation speed for {tracking_number}: {speed}x", extra={'tracking_number': tracking_number})
    console.print(Panel(f"[info]Retrieved simulation speed for {tracking_number}: {speed}x by admin {call.from_user.id}[/info]", title="Speed Retrieved", border_style="green"))

def list_shipments(call, page):
    """List all shipments for the current page."""
    shipments, total = get_shipment_list(page)
    if shipments:
        response = f"*Shipments (Page {page})*:\n" + "\n".join(f"`{tn}`" for tn in shipments) + f"\n\nTotal shipments: {total}"
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, response, parse_mode='Markdown')
        bot_logger.debug(f"Listed shipments, page {page}", extra={'tracking_number': ''})
        console.print(f"[info]Listed shipments, page {page}, for admin {call.from_user.id}[/info]")
    else:
        bot.answer_callback_query(call.id, "No shipments available.", show_alert=True)
        bot_logger.debug(f"No shipments for list, page {page}", extra={'tracking_number': ''})
        send_dynamic_menu(call.message.chat.id, call.message.message_id, page)

def start_bot():
    """Start the Telegram bot polling with retry logic."""
    cache_route_templates()
    retry_delay = 5
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=5, none_stop=True)
            break
        except Exception as e:
            retries += 1
            bot_logger.error(f"Bot polling error (attempt {retries}/{max_retries}): {e}", extra={'tracking_number': ''})
            console.print(Panel(f"[error]Bot polling error (attempt {retries}/{max_retries}): {e}[/error]", title="Telegram Error", border_style="red"))
            if retries < max_retries:
                console.print(f"[info]Retrying in {retry_delay} seconds...[/info]")
                eventlet.sleep(retry_delay)
                retry_delay *= 2
            else:
                console.print(Panel(f"[critical]Max retries exceeded. Bot polling failed.[/critical]", title="Telegram Critical", border_style="red"))
