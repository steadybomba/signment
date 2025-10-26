import os
import re
import json
import logging
import time
from datetime import datetime
from telebot import TeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from functools import wraps
from rich.console import Console
from flask_sqlalchemy import SQLAlchemy
from flask import Flask
from math import radians, sin, cos, sqrt, atan2
from typing import Optional, List, Tuple, Dict, Any

# === LOGGING & CONSOLE ===
bot_logger = logging.getLogger('telegram_bot')
bot_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
bot_logger.addHandler(handler)
console = Console()

# === UPSTASH REDIS CLIENT ===
try:
    from upstash_redis import Redis
    redis_client = Redis(
        url=os.getenv("REDIS_URL", "https://chief-crane-7882.upstash.io"),
        token=os.getenv("REDIS_TOKEN", "AR7KAAImcDJmZTI5ODIxMjA2YTg0NGEzOTcwYWRjYWM0NTg4MTcxM3AyNzg4Mg")
    )
    # Test connection
    redis_client.set("health_check", "ok", ex=10)
    redis_client.delete("health_check")
    console.print("[green]Upstash Redis connected[/green]")
except Exception as e:
    console.print(f"[yellow]Redis unavailable: {e}[/yellow]")
    redis_client = None

# === CONFIGURATION CLASS (EXPORTED) ===
class BotConfig:
    def __init__(self):
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.redis_url = os.getenv("REDIS_URL")
        self.redis_token = os.getenv("REDIS_TOKEN", "")
        self.webhook_url = os.getenv("WEBHOOK_URL", "https://signment-9a96.onrender.com/telegram/webhook")
        self.websocket_server = os.getenv("WEBSOCKET_SERVER", "https://signment-9a96.onrender.com")
        self.allowed_admins = [int(uid) for uid in os.getenv("ALLOWED_ADMINS", "").split(",") if uid.strip()]
        self.valid_statuses = os.getenv("VALID_STATUSES", "Pending,In_Transit,Out_for_Delivery,Delivered,Returned,Delayed").split(",")
        self.route_templates = json.loads(os.getenv("ROUTE_TEMPLATES", '{"Lagos, NG": ["Lagos, NG"]}'))
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", 587))
        self.smtp_user = os.getenv("SMTP_USER", "")
        self.smtp_pass = os.getenv("SMTP_PASS", "")
        self.smtp_from = os.getenv("SMTP_FROM", "no-reply@example.com")

try:
    config = BotConfig()
except Exception as e:
    bot_logger.error(f"Config init failed: {e}")
    raise

# === DHL CONFIG ===
DHL_CONFIG = {
    "name": "DHL Express",
    "primary_color": "#D40511",
    "secondary_color": "#FFCC00",
    "logo_url": "https://www.dhl.com/etc.clientlibs/dhl/clientlibs/clientlib-site/resources/images/dhl-logo.svg",
    "tracking_prefix": "JD",
    "tracking_format": r"^JD\d{10}$",
    "status_flow": {
        "Pending": {"next": ["In_Transit"], "delay": [60, 180]},
        "In_Transit": {"next": ["Out_for_Delivery", "Delayed"], "delay": [120, 600], "probabilities": [0.92, 0.08]},
        "Out_for_Delivery": {"next": ["Delivered"], "delay": [60, 240]},
        "Delayed": {"next": ["Out_for_Delivery"], "delay": [300, 900]},
        "Delivered": {"next": [], "delay": [0, 0]},
        "Returned": {"next": [], "delay": [0, 0]}
    },
    "events": {
        "In_Transit": ["Shipment picked up", "Departed origin facility", "Arrived at sort facility", "Processed at hub"],
        "Out_for_Delivery": ["Out for delivery", "With delivery courier"],
        "Delayed": ["Held at customs", "Weather delay", "Routing delay"]
    }
}

# === FLASK & DB ===
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('SQLALCHEMY_DATABASE_URI', 'sqlite:///shipments.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class Shipment(db.Model):
    __tablename__ = 'shipments'
    id = db.Column(db.Integer, primary_key=True)
    tracking_number = db.Column(db.String(50), unique=True, nullable=False)
    status = db.Column(db.String(50), nullable=False)
    checkpoints = db.Column(db.Text)
    delivery_location = db.Column(db.String(100), nullable=False)
    last_updated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    recipient_email = db.Column(db.String(120), nullable=True)
    origin_location = db.Column(db.String(100), nullable=True)
    webhook_url = db.Column(db.Text, nullable=True)
    email_notifications = db.Column(db.Boolean, default=True)
    carrier = db.Column(db.String(20), default="DHL")

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'tracking_number': self.tracking_number,
            'status': self.status,
            'delivery_location': self.delivery_location,
            'last_updated': self.last_updated.isoformat(),
            'created_at': self.created_at.isoformat(),
            'recipient_email': self.recipient_email,
            'origin_location': self.origin_location,
            'webhook_url': self.webhook_url,
            'email_notifications': self.email_notifications,
            'carrier': self.carrier,
            'checkpoints': (self.checkpoints or "").split(";") if self.checkpoints else []
        }

# === REDIS HELPERS ===
def safe_redis_operation(func, *args, **kwargs):
    if not redis_client:
        return None
    try:
        return func(*args, **kwargs)
    except Exception as e:
        bot_logger.error(f"Redis error: {e}")
        return None

# === UTILS ===
def get_bot() -> TeleBot:
    return TeleBot(config.telegram_bot_token)

def is_admin(user_id: int) -> bool:
    return user_id in config.allowed_admins

def sanitize_tracking_number(tn: str) -> Optional[str]:
    if not tn:
        return None
    tn = re.sub(r'\W+', '', tn.upper())
    return tn if re.match(DHL_CONFIG['tracking_format'], tn) else None

def generate_unique_id() -> str:
    import secrets
    return f"JD{secrets.randbelow(10**10):010d}"

def validate_email(email: str) -> bool:
    return bool(email and re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email))

def validate_location(location: str) -> bool:
    return bool(location and isinstance(location, str) and len(location) <= 100)

def validate_webhook_url(url: str) -> bool:
    return bool(url and re.match(r'^https?://[^\s/$.?#].[^\s]*$', url))

# === DISTANCE CALCULATION (50+ CITIES) ===
def estimate_distance(origin: str, dest: str) -> float:
    city_coords = {
        "Lagos, NG": (6.5244, 3.3792), "Abuja, NG": (9.0579, 7.4951), "Port Harcourt, NG": (4.8156, 7.0498),
        "Kano, NG": (12.0001, 8.5167), "Ibadan, NG": (7.3775, 3.9470), "Enugu, NG": (6.4584, 7.5170),
        "New York, NY": (40.7128, -74.0060), "Los Angeles, CA": (34.0522, -118.2437), "London, UK": (51.5074, -0.1278),
        "Dubai, UAE": (25.2048, 55.2708), "Tokyo, JP": (35.6762, 139.6503), "Sydney, AU": (-33.8688, 151.2093),
        "Paris, FR": (48.8566, 2.3522), "Berlin, DE": (52.5200, 13.4050), "Mumbai, IN": (19.0760, 72.8777),
        "Singapore, SG": (1.3521, 103.8198), "Hong Kong, HK": (22.3193, 114.1694), "São Paulo, BR": (-23.5505, -46.6333),
        "Johannesburg, ZA": (-26.2041, 28.0473), "Cairo, EG": (30.0444, 31.2357), "Moscow, RU": (55.7558, 37.6173),
        "Toronto, CA": (43.6532, -79.3832), "Mexico City, MX": (19.4326, -99.1332), "Seoul, KR": (37.5665, 126.9780),
        "Bangkok, TH": (13.7563, 100.5018), "Jakarta, ID": (-6.2088, 106.8456), "Delhi, IN": (28.7041, 77.1025),
        "Beijing, CN": (39.9042, 116.4074), "Shanghai, CN": (31.2304, 121.4737), "Istanbul, TR": (41.0082, 28.9784),
        "Karachi, PK": (24.8607, 67.0011), "Buenos Aires, AR": (-34.6037, -58.3816), "Rio de Janeiro, BR": (-22.9068, -43.1729),
        "Lima, PE": (-12.0464, -77.0428), "Bogotá, CO": (4.7110, -74.0721), "Santiago, CL": (-33.4489, -70.6693),
        "Cape Town, ZA": (-33.9249, 18.4241), "Nairobi, KE": (-1.2921, 36.8219), "Accra, GH": (5.6037, -0.1870),
        "Addis Ababa, ET": (8.9806, 38.7578), "Kuala Lumpur, MY": (3.1390, 101.6869), "Hanoi, VN": (21.0285, 105.8342),
        "Manila, PH": (14.5995, 120.9842), "Taipei, TW": (25.0330, 121.5654), "Riyadh, SA": (24.7136, 46.6753),
        "Tel Aviv, IL": (32.0853, 34.7818), "Athens, GR": (37.9838, 23.7275), "Lisbon, PT": (38.7223, -9.1393),
        "Stockholm, SE": (59.3293, 18.0686), "Oslo, NO": (59.9139, 10.7522), "Helsinki, FI": (60.1699, 24.9384),
        "Warsaw, PL": (52.2297, 21.0122), "Prague, CZ": (50.0755, 14.4378), "Budapest, HU": (47.4979, 19.0402),
        "Vienna, AT": (48.2082, 16.3738), "Zurich, CH": (47.3769, 8.5417), "Amsterdam, NL": (52.3676, 4.9041),
        "Brussels, BE": (50.8476, 4.3572), "Dublin, IE": (53.3498, -6.2603), "Madrid, ES": (40.4168, -3.7038),
        "Rome, IT": (41.9028, 12.4964), "Milan, IT": (45.4642, 9.1900), "Barcelona, ES": (41.3851, 2.1734)
    }
    origin_key = next((k for k in city_coords if origin.lower() in k.lower() or k.lower().startswith(origin.lower())), None)
    dest_key = next((k for k in city_coords if dest.lower() in k.lower() or k.lower().startswith(dest.lower())), None)
    if not origin_key or not dest_key:
        return 1000.0
    lat1, lon1 = map(radians, city_coords[origin_key])
    lat2, lon2 = map(radians, city_coords[dest_key])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return round(6371 * c, 1)

# === DB OPERATIONS ===
def get_shipment_list(page: int = 1, per_page: int = 10) -> Tuple[List[str], int]:
    try:
        offset = (page - 1) * per_page
        shipments = Shipment.query.order_by(Shipment.created_at.desc()).offset(offset).limit(per_page).all()
        total = Shipment.query.count()
        return [s.tracking_number for s in shipments], total
    except Exception as e:
        bot_logger.error(f"List error: {e}")
        return [], 0

def get_shipment_details(tracking_number: str) -> Optional[Dict[str, Any]]:
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        return shipment.to_dict() if shipment else None
    except Exception as e:
        bot_logger.error(f"Fetch error {tracking_number}: {e}")
        return None

def save_shipment(tracking_number: str, status: str, checkpoints: str = '', delivery_location: Optional[str] = None,
                  recipient_email: Optional[str] = None, origin_location: Optional[str] = None,
                  webhook_url: Optional[str] = None, carrier: str = "DHL") -> bool:
    try:
        shipment = Shipment(
            tracking_number=tracking_number,
            status=status,
            checkpoints=checkpoints,
            delivery_location=delivery_location or "Lagos, NG",
            recipient_email=recipient_email,
            origin_location=origin_location or "Lagos, NG",
            webhook_url=webhook_url,
            email_notifications=True,
            carrier=carrier
        )
        db.session.add(shipment)
        db.session.commit()
        invalidate_cache(tracking_number)
        bot_logger.info(f"Saved {tracking_number}")
        return True
    except Exception as e:
        db.session.rollback()
        bot_logger.error(f"Save failed: {e}")
        return False

def update_shipment(tracking_number: str, status: Optional[str] = None, delivery_location: Optional[str] = None,
                    recipient_email: Optional[str] = None, origin_location: Optional[str] = None,
                    webhook_url: Optional[str] = None, carrier: Optional[str] = None) -> bool:
    try:
        shipment = Shipment.query.filter_by(tracking_number=tracking_number).first()
        if not shipment:
            return False
        if status and status in config.valid_statuses:
            shipment.status = status
        if delivery_location:
            shipment.delivery_location = delivery_location
        if recipient_email is not None:
            shipment.recipient_email = recipient_email
        if origin_location is not None:
            shipment.origin_location = origin_location
        if webhook_url is not None:
            shipment.webhook_url = webhook_url
        if carrier:
            shipment.carrier = carrier
        shipment.last_updated = datetime.utcnow()
        db.session.commit()
        invalidate_cache(tracking_number)
        bot_logger.info(f"Updated {tracking_number}")
        return True
    except Exception as e:
        db.session.rollback()
        bot_logger.error(f"Update failed: {e}")
        return False

def search_shipments(query: str, page: int = 1, per_page: int = 10) -> Tuple[List[str], int]:
    try:
        query = f"%{query}%"
        offset = (page - 1) * per_page
        shipments = Shipment.query.filter(
            db.or_(
                Shipment.tracking_number.ilike(query),
                Shipment.delivery_location.ilike(query),
                Shipment.origin_location.ilike(query),
                Shipment.recipient_email.ilike(query)
            )
        ).order_by(Shipment.created_at.desc()).offset(offset).limit(per_page).all()
        total = Shipment.query.filter(
            db.or_(
                Shipment.tracking_number.ilike(query),
                Shipment.delivery_location.ilike(query),
                Shipment.origin_location.ilike(query),
                Shipment.recipient_email.ilike(query)
            )
        ).count()
        return [s.tracking_number for s in shipments], total
    except Exception as e:
        bot_logger.error(f"Search error: {e}")
        return [], 0

def invalidate_cache(tracking_number: str):
    if redis_client:
        try:
            safe_redis_operation(redis_client.delete, f"shipment:{tracking_number}")
        except:
            pass

def enqueue_notification(data: Dict[str, Any]) -> bool:
    if not redis_client:
        return False
    try:
        redis_client.rpush("notifications", json.dumps(data))
        return True
    except Exception as e:
        bot_logger.error(f"Queue failed: {e}")
        return False

# === MENU & RATE LIMIT ===
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 20

def rate_limit(func):
    @wraps(func)
    def wrapper(message):
        user_id = str(message.from_user.id)
        key = f"rate_limit:{user_id}"
        count = safe_redis_operation(redis_client.incr, key) if redis_client else 0
        if count == 1:
            safe_redis_operation(redis_client.expire, key, RATE_LIMIT_WINDOW)
        if count > RATE_LIMIT_MAX:
            get_bot().reply_to(message, "Rate limit exceeded. Try again later.")
            return
        return func(message)
    return wrapper

def send_dynamic_menu(chat_id: int, message_id: Optional[int] = None, page: int = 1):
    shipments, total = get_shipment_list(page=page)
    markup = InlineKeyboardMarkup(row_width=2)
    for tn in shipments:
        s = get_shipment_details(tn)
        label = f"{tn} [DHL]" if s.get('carrier') == 'DHL' else f"{tn} [{s['status']}]"
        markup.add(InlineKeyboardButton(label, callback_data=f"view_{tn}"))
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("Prev", callback_data=f"menu_page_{page-1}"))
    if page * 10 < total:
        nav.append(InlineKeyboardButton("Next", callback_data=f"menu_page_{page+1}"))
    if nav:
        markup.add(*nav)
    markup.add(
        InlineKeyboardButton("Generate ID", callback_data="generate_id"),
        InlineKeyboardButton("Add Shipment", callback_data="add"),
        InlineKeyboardButton("Search", callback_data="search_menu"),
        InlineKeyboardButton("Bulk Actions", callback_data="bulk_action"),
        InlineKeyboardButton("Stats", callback_data="stats"),
        InlineKeyboardButton("Help", callback_data="help")
    )
    text = f"*Admin Panel* (Page {page})\nTotal: `{total}` shipments"
    bot = get_bot()
    if message_id:
        bot.edit_message_text(text, chat_id, message_id, parse_mode='Markdown', reply_markup=markup)
    else:
        bot.send_message(chat_id, text, parse_mode='Markdown', reply_markup=markup)

def export_shipments() -> Optional[str]:
    try:
        shipments = Shipment.query.all()
        return json.dumps([s.to_dict() for s in shipments], indent=2, default=str)
    except Exception as e:
        bot_logger.error(f"Export error: {e}")
        return None

def get_recent_logs(limit: int = 5) -> List[str]:
    return [f"{datetime.utcnow().isoformat()} - INFO - Sample log {i}" for i in range(1, limit + 1)]

def show_shipment_menu(call, page: int, prefix: str, prompt: str, extra_buttons=None):
    shipments, total = get_shipment_list(page=page)
    if not shipments:
        get_bot().edit_message_text("No shipments.", call.message.chat.id, call.message.message_id)
        return
    markup = InlineKeyboardMarkup(row_width=1)
    for tn in shipments:
        s = get_shipment_details(tn)
        label = f"{tn} [DHL]" if s.get('carrier') == 'DHL' else tn
        markup.add(InlineKeyboardButton(label, callback_data=f"{prefix}_{tn}_{page}"))
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("Prev", callback_data=f"{prefix}_menu_{page-1}"))
    if page * 10 < total:
        nav.append(InlineKeyboardButton("Next", callback_data=f"{prefix}_menu_{page+1}"))
    if nav:
        markup.add(*nav)
    if extra_buttons:
        markup.add(*extra_buttons)
    get_bot().edit_message_text(f"*{prompt}* (Page {page}):", call.message.chat.id, call.message.message_id,
                               parse_mode='Markdown', reply_markup=markup)

# === WEBHOOK & KEEP-ALIVE ===
def set_webhook():
    try:
        bot = get_bot()
        bot.remove_webhook()
        time.sleep(1)
        bot.set_webhook(url=config.webhook_url)
        bot_logger.info(f"Webhook set: {config.webhook_url}")
    except Exception as e:
        bot_logger.error(f"Webhook failed: {e}")

bot = get_bot()

def keep_alive():
    bot_logger.info("Keep-alive loop started")
    console.print("[info]Keep-alive loop started[/info]")
    while True:
        try:
            info = bot.get_webhook_info()
            if info.url != config.webhook_url:
                bot_logger.warning("Webhook mismatch, resetting...")
                set_webhook()
            time.sleep(300)
        except Exception as e:
            bot_logger.error(f"Keep-alive error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    set_webhook()
    console.print("[green]utils.py ready — Upstash Redis + BotConfig exported[/green]")
    keep_alive()
