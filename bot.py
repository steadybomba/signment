import os
import re
import logging
from telebot import TeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from functools import wraps
from rich.console import Console
from utils import (
    BotConfig, get_bot, is_admin, send_dynamic_menu, get_shipment_details,
    generate_unique_id, search_shipments, RATE_LIMIT_WINDOW, RATE_LIMIT_MAX,
    safe_redis_operation, redis_client, sanitize_tracking_number
)

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
        response = (
            f"*Shipment*: `{tracking_number}`\n"
            f"*Status*: `{shipment['status']}`\n"
            f"*Delivery Location*: `{shipment['delivery_location']}`\n"
            f"*Checkpoints*: `{shipment.get('checkpoints', 'None')}`"
        )
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
        bot.reply_to(message, response, parse_mode='Markdown', reply_markup=markup)
        bot_logger.info(f"Sent tracking details for {tracking_number}")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        bot_logger.error(f"Error in track command: {e}")

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
            response = (
                f"*Shipment*: `{tracking_number}`\n"
                f"*Status*: `{shipment['status']}`\n"
                f"*Delivery Location*: `{shipment['delivery_location']}`\n"
                f"*Checkpoints*: `{shipment.get('checkpoints', 'None')}`"
            )
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(InlineKeyboardButton("Home", callback_data="menu_page_1"))
            bot.edit_message_text(response, chat_id=call.message.chat.id, message_id=call.message.message_id,
                                 parse_mode='Markdown', reply_markup=markup)
            bot_logger.info(f"Displayed details for {tracking_number}")
        elif data == "help":
            bot.edit_message_text(
                "*Help Menu*\n"
                "Available commands:\n"
                "/start or /menu - Show main menu\n"
                "/myid - Get your Telegram ID\n"
                "/track <tracking_number> - Track a shipment\n"
                "/search <query> - Search shipments\n"
                "/generate - Generate a tracking ID\n"
                "Example: /track TRK20231010120000ABC123",
                chat_id=call.message.chat.id, message_id=call.message.message_id,
                parse_mode='Markdown', reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("Home", callback_data="menu_page_1")))
        bot.answer_callback_query(call.id)
        bot_logger.info(f"Processed callback {data} from user {call.from_user.id}")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {e}", show_alert=True)
        bot_logger.error(f"Error in callback handler: {e}")

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
    set_webhook()
    bot_logger.info("Bot started with webhook mode")
