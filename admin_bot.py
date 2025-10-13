# admin_bot.py - Telegram bot for admin control

import telebot
import mysql.connector
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
import eventlet
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import os
from dotenv import load_dotenv
import uuid
import string
import random

# Load env vars
load_dotenv()

# Bot and admin config
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', 'YOUR_TELEGRAM_BOT_TOKEN_HERE')
ALLOWED_ADMINS = [int(os.getenv('ADMIN_USER_ID', '123456789'))]

# Email config
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USERNAME = os.getenv('SMTP_USERNAME', 'your_email@gmail.com')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', 'your_app_password')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'your_email@gmail.com')

# WebSocket URL
WEBSOCKET_SERVER = 'http://localhost:5000'

# DB config
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}

bot = telebot.TeleBot(BOT_TOKEN)

# Init DB
def init_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS shipments (
            tracking_number VARCHAR(50) PRIMARY KEY,
            status VARCHAR(50),
            checkpoints TEXT,
            delivery_location TEXT,
            last_updated DATETIME,
            recipient_email VARCHAR(255),
            created_at DATETIME,
            origin_location TEXT,
            webhook_url TEXT
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tracking_number ON shipments (tracking_number)')
    conn.commit()
    conn.close()

init_db()

# Check admin
def is_admin(user_id):
    return user_id in ALLOWED_ADMINS

# Send email
def send_email_notification(tracking_number, status, checkpoints, delivery_location, recipient_email):
    if not recipient_email:
        return
    msg = MIMEMultipart()
    msg['From'] = FROM_EMAIL
    msg['To'] = recipient_email
    msg['Subject'] = f"Shipment Update: {tracking_number}"
    msg.attach(MIMEText(f"Tracking Number: {tracking_number}\nStatus: {status}\nDelivery Location: {delivery_location}\nCheckpoints: {checkpoints}", 'plain'))
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(FROM_EMAIL, recipient_email, msg.as_string())
        server.quit()
    except Exception as e:
        print(f"Email error: {e}")

# Generate unique ID
def generate_unique_id():
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')  # e.g., 202510130950
    random_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"TRK{timestamp}{random_str}"

# Save shipment
def save_shipment(tracking_number, status, checkpoints, delivery_location, recipient_email='', origin_location=None, webhook_url=None):
    conn = mysql.connector.connect(**DB_CONFIG, connect_timeout=5)
    cursor = conn.cursor()
    last_updated = datetime.now()
    created_at = last_updated if not checkpoints else None
    origin_location = origin_location or delivery_location
    webhook_url = webhook_url or None
    cursor.execute('''
        INSERT INTO shipments (tracking_number, status, checkpoints, delivery_location, last_updated, recipient_email, created_at, origin_location, webhook_url)
        VALUES (%s, %s, %s, %s, %s, %s, COALESCE((SELECT created_at FROM shipments WHERE tracking_number = %s), %s), %s, %s)
        ON DUPLICATE KEY UPDATE status=%s, checkpoints=%s, delivery_location=%s, last_updated=%s, recipient_email=%s, origin_location=%s, webhook_url=%s
    ''', (tracking_number, status, checkpoints, delivery_location, last_updated, recipient_email, tracking_number, last_updated, origin_location, webhook_url,
          status, checkpoints, delivery_location, last_updated, recipient_email, origin_location, webhook_url))
    conn.commit()
    conn.close()
    send_email_notification(tracking_number, status, checkpoints, delivery_location, recipient_email)
    requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}')
    if call := getattr(bot, 'last_call', None):
        send_dynamic_menu(call.message.chat.id, call.message.message_id)

# Get shipment list
def get_shipment_list():
    conn = mysql.connector.connect(**DB_CONFIG, connect_timeout=5)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT tracking_number FROM shipments")
    results = [row['tracking_number'] for row in cursor.fetchall()]
    conn.close()
    return results

# Send menu
def send_dynamic_menu(chat_id, message_id=None):
    markup = InlineKeyboardMarkup()
    markup.row_width = 2
    markup.add(InlineKeyboardButton("Generate ID", callback_data="generate_id"))
    markup.add(InlineKeyboardButton("Add Shipment", callback_data="add"))
    shipments = get_shipment_list()
    if shipments:
        markup.add(InlineKeyboardButton("Update Shipment", callback_data="update_menu"))
        markup.add(InlineKeyboardButton("Delete Shipment", callback_data="delete_menu"))
        markup.add(InlineKeyboardButton("List Shipments", callback_data="list"))
    markup.add(InlineKeyboardButton("Help", callback_data="help"))
    if message_id:
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=markup)
    else:
        bot.send_message(chat_id, "Choose an action:", reply_markup=markup)

@bot.message_handler(commands=['myid'])
def get_my_id(message):
    bot.reply_to(message, f"Your Telegram user ID: {message.from_user.id}")

@bot.message_handler(commands=['start', 'menu'])
def send_menu(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        return
    send_dynamic_menu(message.chat.id)

@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    bot.last_call = call
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Access denied.")
        return

    if call.data == "generate_id":
        new_id = generate_unique_id()
        bot.answer_callback_query(call.id, f"Generated ID: {new_id}")
    elif call.data == "add":
        bot.answer_callback_query(call.id, "Use /add with parameters or generate ID.")
    elif call.data == "update_menu":
        shipments = get_shipment_list()
        if shipments:
            markup = InlineKeyboardMarkup()
            markup.row_width = 1
            for tn in shipments:
                markup.add(InlineKeyboardButton(tn, callback_data=f"update_{tn}"))
            markup.add(InlineKeyboardButton("Back", callback_data="menu"))
            bot.edit_message_reply_markup(chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
        else:
            bot.answer_callback_query(call.id, "No shipments.")
    elif call.data.startswith("update_"):
        tracking_number = call.data.replace("update_", "")
        bot.answer_callback_query(call.id, f"Update {tracking_number} with /update.")
    elif call.data == "delete_menu":
        shipments = get_shipment_list()
        if shipments:
            markup = InlineKeyboardMarkup()
            markup.row_width = 1
            for tn in shipments:
                markup.add(InlineKeyboardButton(tn, callback_data=f"delete_{tn}"))
            markup.add(InlineKeyboardButton("Back", callback_data="menu"))
            bot.edit_message_reply_markup(chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
        else:
            bot.answer_callback_query(call.id, "No shipments.")
    elif call.data.startswith("delete_"):
        tracking_number = call.data.replace("delete_", "")
        conn = mysql.connector.connect(**DB_CONFIG, connect_timeout=5)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM shipments WHERE tracking_number = %s", (tracking_number,))
        conn.commit()
        conn.close()
        requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}')
        bot.answer_callback_query(call.id, f"Deleted {tracking_number}")
        send_dynamic_menu(call.message.chat.id, call.message.message_id)
    elif call.data == "list":
        list_shipments(call.message)
    elif call.data == "help":
        show_help(call.message)
    elif call.data == "menu":
        send_dynamic_menu(call.message.chat.id, call.message.message_id)

@bot.message_handler(commands=['generate_id'])
def generate_id_command(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        return
    new_id = generate_unique_id()
    bot.reply_to(message, f"Generated ID: {new_id}")

@bot.message_handler(commands=['add'])
def add_shipment(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        return
    try:
        parts = message.text.split(maxsplit=7)
        if len(parts) < 5:
            bot.reply_to(message, "Usage: /add [tracking_number] <status> \"<checkpoints>\" <delivery_location> <recipient_email> [origin_location] [webhook_url]")
            return
        tracking_number = parts[1].strip() if len(parts) > 5 else generate_unique_id()
        status = parts[2].strip() if len(parts) > 2 else 'Pending'
        checkpoints = parts[3].strip('"') if len(parts) > 3 else ''
        delivery_location = parts[4].strip() if len(parts) > 4 else 'Unknown'
        recipient_email = parts[5].strip() if len(parts) > 5 else ''
        origin_location = parts[6].strip() if len(parts) > 6 else None
        webhook_url = parts[7].strip() if len(parts) > 7 else None
        if status not in ['Pending', 'In_Transit', 'Out_for_Delivery', 'Delivered']:
            bot.reply_to(message, "Invalid status.")
            return
        save_shipment(tracking_number, status, checkpoints, delivery_location, recipient_email, origin_location, webhook_url)
        bot.reply_to(message, f"Added {tracking_number}. Email to {recipient_email}. Webhook: {webhook_url or 'default'}.")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

@bot.message_handler(commands=['update'])
def update_shipment(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        return
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /update <tracking_number> <field=value> ...")
            return
        tracking_number = parts[1].split()[0].strip()
        conn = mysql.connector.connect(**DB_CONFIG, connect_timeout=5)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT status, checkpoints, delivery_location, recipient_email, origin_location, webhook_url FROM shipments WHERE tracking_number = %s", (tracking_number,))
        result = cursor.fetchone()
        if not result:
            bot.reply_to(message, f"Tracking {tracking_number} not found.")
            conn.close()
            return
        current_status, current_checkpoints, current_location, current_email, current_origin, current_webhook = result['status'], result['checkpoints'], result['delivery_location'], result['recipient_email'], result['origin_location'], result['webhook_url']
        updates = ' '.join(parts[1].split()[1:])
        update_dict = {k: v.strip('"') if v.startswith('"') and v.endswith('"') else v for k, v in (pair.split('=', 1) for pair in updates.split()) if k in ['status', 'checkpoints', 'delivery_location', 'recipient_email', 'origin_location', 'webhook_url']}
        new_status = update_dict.get('status', current_status)
        new_checkpoints = update_dict.get('checkpoints', current_checkpoints)
        new_location = update_dict.get('delivery_location', current_location)
        new_email = update_dict.get('recipient_email', current_email)
        new_origin = update_dict.get('origin_location', current_origin)
        new_webhook = update_dict.get('webhook_url', current_webhook)
        if new_status not in ['Pending', 'In_Transit', 'Out_for_Delivery', 'Delivered']:
            bot.reply_to(message, "Invalid status.")
            conn.close()
            return
        save_shipment(tracking_number, new_status, new_checkpoints, new_location, new_email, new_origin, new_webhook)
        bot.reply_to(message, f"Updated {tracking_number}. Email to {new_email}. Webhook: {new_webhook or 'default'}.")
        send_dynamic_menu(message.chat.id)
        conn.close()
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

@bot.message_handler(commands=['delete'])
def delete_shipment(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "Usage: /delete <tracking_number>")
            return
        tracking_number = parts[1].strip()
        conn = mysql.connector.connect(**DB_CONFIG, connect_timeout=5)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM shipments WHERE tracking_number = %s", (tracking_number,))
        conn.commit()
        conn.close()
        requests.get(f'{WEBSOCKET_SERVER}/broadcast/{tracking_number}')
        bot.reply_to(message, f"Deleted {tracking_number}.")
        send_dynamic_menu(message.chat.id)
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

@bot.message_handler(commands=['list'])
def list_shipments(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        return
    conn = mysql.connector.connect(**DB_CONFIG, connect_timeout=5)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT tracking_number, status, last_updated FROM shipments")
    results = cursor.fetchall()
    conn.close()
    response = "Shipments:\n" + "\n".join([f"{r['tracking_number']}: {r['status']} (Updated: {r['last_updated']})" for r in results]) if results else "No shipments."
    bot.reply_to(message, response)

@bot.message_handler(commands=['help'])
def show_help(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Access denied.")
        return
    help_text = """
    Commands:
/generate_id - Generate a tracking ID
/add [tracking_number] <status> \"<checkpoints>\" <delivery_location> <recipient_email> [origin_location] [webhook_url] - Add shipment
/update <tracking_number> <field=value> ... - Update fields
/delete <tracking_number> - Delete shipment
/list - List shipments
/help - Show this

Status: Pending, In_Transit, Out_for_Delivery, Delivered
Example: /add TRK202510130950ABCD Pending \"\" \"New York, NY\" user@example.com
    """
    bot.reply_to(message, help_text)

if __name__ == '__main__':
    bot.polling()
