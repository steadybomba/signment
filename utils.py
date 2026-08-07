import os
import re
import json
import logging
import threading
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from collections import deque

import validators
import requests
from rich.console import Console
from rich.panel import Panel
from telebot import TeleBot, types

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# ============================================================
# Console and Logging
# ============================================================
console = Console()
logger = logging.getLogger('utils')

# ============================================================
# Rate Limit Defaults
# ============================================================
RATE_LIMIT_WINDOW = int(os.getenv('RATE_LIMIT_WINDOW', '60'))
RATE_LIMIT_MAX = int(os.getenv('RATE_LIMIT_MAX', '5'))

# ============================================================
# Event Logging Helpers
# ============================================================
recent_socket_events = deque(maxlen=200)
recent_socket_events_lock = threading.Lock()

def add_socket_event(evt: dict):
    try:
        with recent_socket_events_lock:
            recent_socket_events.appendleft({'ts': datetime.utcnow().isoformat() + 'Z', **evt})
    except Exception:
        pass

recent_client_errors = deque(maxlen=200)
recent_client_errors_lock = threading.Lock()

def add_client_error(evt: dict):
    try:
        with recent_client_errors_lock:
            recent_client_errors.appendleft({'ts': datetime.utcnow().isoformat() + 'Z', **evt})
    except Exception:
        pass

# ============================================================
# Configuration
# ============================================================
@dataclass
class BotConfig:
    telegram_bot_token: str = ""
    redis_url: Optional[str] = None
    redis_token: str = ""
    webhook_url: str = ""
    websocket_server: str = ""
    allowed_admins: List[int] = field(default_factory=list)
    valid_statuses: List[str] = field(default_factory=lambda: [
        "Pending", "On_Hold", "In_Transit", "Out_for_Delivery", 
        "Delivered", "Returned", "Delayed"
    ])
    route_templates: Dict[str, Any] = field(default_factory=lambda: {"Lagos, NG": ["Lagos, NG"]})
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_pass: str = ""
    smtp_from: str = "no-reply@example.com"


config = BotConfig(
    telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_TOKEN", "")),
    redis_url=os.getenv("REDIS_URL"),
    redis_token=os.getenv("REDIS_TOKEN", ""),
    webhook_url=os.getenv("WEBHOOK_URL", ""),
    websocket_server=os.getenv("WEBSOCKET_SERVER", ""),
    allowed_admins=[int(uid) for uid in os.getenv("ALLOWED_ADMINS", "").split(",") if uid],
    valid_statuses=os.getenv("VALID_STATUSES", "Pending,On_Hold,In_Transit,Out_for_Delivery,Delivered,Returned,Delayed").split(","),
    route_templates=json.loads(os.getenv("ROUTE_TEMPLATES", '{"Lagos, NG": ["Lagos, NG"]}')),
    smtp_host=os.getenv("SMTP_HOST", "smtp.gmail.com"),
    smtp_port=int(os.getenv("SMTP_PORT", 587)),
    smtp_user=os.getenv("SMTP_USER", ""),
    smtp_pass=os.getenv("SMTP_PASS", ""),
    smtp_from=os.getenv("SMTP_FROM", "no-reply@example.com")
)

# ============================================================
# Redis Client
# ============================================================
redis_client = None

# Email throttle caches - declared ONCE here
email_throttle_cache = {}
email_digest_cache = {}

def get_redis_client():
    global redis_client
    if redis_client is not None:
        return redis_client
    try:
        import redis as _redis
        url = os.getenv('REDIS_URL') or None
        token = os.getenv('REDIS_TOKEN', '')
        max_conn = int(os.getenv('REDIS_MAX_CONNECTIONS', '50'))
        if not url:
            return None
        pool = _redis.ConnectionPool.from_url(url, password=token or None, max_connections=max_conn)
        redis_client = _redis.Redis(connection_pool=pool)
        return redis_client
    except Exception:
        return None

redis_client = get_redis_client()

def safe_redis_operation(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception:
        return None

# ============================================================
# Safe Redis Wrappers
# ============================================================
def r_ping():
    try:
        if not redis_client:
            return False
        return redis_client.ping()
    except Exception:
        return False

def r_get(key):
    try:
        if not redis_client:
            return None
        v = redis_client.get(key)
        if v is None:
            return None
        return v.decode() if isinstance(v, bytes) else v
    except Exception:
        return None

def r_set(key, value, ex=None):
    try:
        if not redis_client:
            return None
        if ex:
            return redis_client.set(key, value, ex=ex)
        return redis_client.set(key, value)
    except Exception:
        return None

def r_lpop(key):
    global redis_client
    try:
        if not redis_client:
            return None
        v = redis_client.lpop(key)
        if v is None:
            return None
        return v.decode() if isinstance(v, bytes) else v
    except Exception:
        redis_client = None
        return None

def r_lpush(key, value):
    global redis_client
    try:
        if not redis_client:
            return None
        return redis_client.lpush(key, value)
    except Exception:
        redis_client = None
        return None

def r_ltrim(key, start, end):
    try:
        if not redis_client:
            return None
        return redis_client.ltrim(key, start, end)
    except Exception:
        return None

def r_sadd(key, member):
    try:
        if not redis_client:
            return None
        return redis_client.sadd(key, member)
    except Exception:
        return None

def r_srem(key, member):
    try:
        if not redis_client:
            return None
        return redis_client.srem(key, member)
    except Exception:
        return None

def r_smembers(key):
    try:
        if not redis_client:
            return set()
        members = redis_client.smembers(key) or set()
        return {m.decode() if isinstance(m, bytes) else m for m in members}
    except Exception:
        return set()

def r_scan_iter(pattern):
    try:
        if not redis_client:
            return []
        keys = list(redis_client.scan_iter(pattern))
        return [k.decode() if isinstance(k, bytes) else k for k in keys]
    except Exception:
        return []

def r_keys(pattern):
    try:
        if not redis_client:
            return []
        keys = redis_client.keys(pattern) or []
        return [k.decode() if isinstance(k, bytes) else k for k in keys]
    except Exception:
        return []

def r_hget(key, field):
    try:
        if not redis_client:
            return None
        v = redis_client.hget(key, field)
        if v is None:
            return None
        return v.decode() if isinstance(v, bytes) else v
    except Exception:
        return None

def r_hset(key, field, value):
    try:
        if not redis_client:
            return None
        return redis_client.hset(key, field, value)
    except Exception:
        return None

def r_hgetall(key):
    try:
        if not redis_client:
            return {}
        d = redis_client.hgetall(key) or {}
        return {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) 
                for k, v in d.items()}
    except Exception:
        return {}

def r_hlen(key):
    try:
        if not redis_client:
            return 0
        return redis_client.hlen(key)
    except Exception:
        return 0

def r_exists(key):
    try:
        if not redis_client:
            return False
        return bool(redis_client.exists(key))
    except Exception:
        return False

def r_llen(key):
    try:
        if not redis_client:
            return 0
        return redis_client.llen(key)
    except Exception:
        return 0

# ============================================================
# Validation Helpers
# ============================================================
def sanitize_tracking_number(tn: str) -> str:
    if not tn:
        return ''
    return re.sub(r'[^A-Za-z0-9]', '', tn).upper()

def validate_email(email: str) -> bool:
    try:
        return bool(validators.email(email))
    except Exception:
        return False

def validate_location(loc: str) -> bool:
    return isinstance(loc, str) and len(loc) > 0

def validate_webhook_url(url: str) -> bool:
    try:
        return bool(validators.url(url))
    except Exception:
        return False

# ============================================================
# Bot Helpers
# ============================================================
def get_bot():
    token = os.getenv('TELEGRAM_BOT_TOKEN') or os.getenv('TELEGRAM_TOKEN')
    if not token:
        return None
    try:
        return TeleBot(token)
    except Exception:
        return None

def is_admin(user_id) -> bool:
    allowed_admins = config.allowed_admins or []
    return user_id in allowed_admins

# ============================================================
# ID Generation
# ============================================================
def generate_unique_id(prefix: str = 'JD') -> str:
    import random
    import string as _string
    return f"{prefix}{''.join(random.choices(_string.digits, k=10))}"

# ============================================================
# Route Templates Cache
# ============================================================
_route_templates_cache: Dict[str, Any] = {}

def cache_route_templates():
    global _route_templates_cache
    try:
        _route_templates_cache = config.route_templates or {}
    except Exception:
        _route_templates_cache = {}

def get_cached_route_templates() -> Dict[str, Any]:
    return _route_templates_cache or config.route_templates

# ============================================================
# Database Helpers (Lazy imports to avoid circular)
# ============================================================
def get_shipment_list(page=1, per_page=10):
    try:
        from app import app, Shipment
        with app.app_context():
            q = Shipment.query.order_by(Shipment.created_at.desc())
            total = q.count()
            items = q.offset((page - 1) * per_page).limit(per_page).all()
            return [s.to_dict() for s in items], total
    except Exception:
        return [], 0

def get_shipment_details(tracking_number):
    try:
        from app import app, Shipment
        with app.app_context():
            s = Shipment.query.filter_by(tracking_number=tracking_number).first()
            return s.to_dict() if s else None
    except Exception:
        return None

def save_shipment(tracking_number, status, checkpoints, delivery_location, 
                  recipient_email=None, origin_location=None, webhook_url=None, carrier="DHL"):
    try:
        from app import app, db, Shipment
        with app.app_context():
            existing = Shipment.query.filter_by(tracking_number=tracking_number).first()
            if existing:
                existing.status = status
                existing.checkpoints = checkpoints
                existing.delivery_location = delivery_location
                existing.recipient_email = recipient_email or existing.recipient_email
                existing.origin_location = origin_location or existing.origin_location
                existing.webhook_url = webhook_url or existing.webhook_url
                existing.carrier = carrier or existing.carrier
                existing.last_updated = datetime.utcnow()
            else:
                s = Shipment(
                    tracking_number=tracking_number,
                    status=status,
                    checkpoints=checkpoints,
                    delivery_location=delivery_location,
                    last_updated=datetime.utcnow(),
                    recipient_email=recipient_email or '',
                    created_at=datetime.utcnow(),
                    origin_location=origin_location or '',
                    webhook_url=webhook_url or '',
                    email_notifications=bool(recipient_email),
                    carrier=carrier or 'DHL'
                )
                db.session.add(s)
            db.session.commit()
            return True
    except Exception:
        try:
            from app import db
            db.session.rollback()
        except Exception:
            pass
        return False

def update_shipment(tracking_number: str, **fields) -> bool:
    try:
        from app import app, db, Shipment
        with app.app_context():
            s = Shipment.query.filter_by(tracking_number=tracking_number).first()
            if not s:
                return False
            for k, v in fields.items():
                if hasattr(s, k):
                    setattr(s, k, v)
            s.last_updated = datetime.utcnow()
            db.session.commit()
            return True
    except Exception:
        try:
            from app import db
            db.session.rollback()
        except Exception:
            pass
        return False

def invalidate_cache(*args, **kwargs):
    return None

def export_shipments() -> str:
    try:
        from app import app, Shipment
        with app.app_context():
            items = Shipment.query.order_by(Shipment.created_at.desc()).all()
            return json.dumps([s.to_dict() for s in items])
    except Exception:
        return '[]'

def search_shipments(query: str, page: int = 1, per_page: int = 10):
    try:
        from app import app, Shipment
        with app.app_context():
            q = f"%{query}%"
            results = Shipment.query.filter(
                (Shipment.tracking_number.like(q)) | 
                (Shipment.delivery_location.like(q)) | 
                (Shipment.origin_location.like(q))
            ).order_by(Shipment.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()
            total = Shipment.query.filter(
                (Shipment.tracking_number.like(q)) | 
                (Shipment.delivery_location.like(q)) | 
                (Shipment.origin_location.like(q))
            ).count()
            return [s.tracking_number for s in results], total
    except Exception:
        return [], 0

def get_recent_logs(limit: int = 10):
    try:
        log_file = 'flask_app.log'
        if os.path.exists(log_file):
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()[-limit:]
                return [l.strip() for l in lines]
    except Exception:
        pass
    return []

# ============================================================
# Telegram Menu Helpers
# ============================================================
def send_dynamic_menu(chat_id, page: int = 1, message_id: int = None):
    try:
        bot = get_bot()
        if not bot:
            return None
        shipments, total = get_shipment_list(page=page)
        markup = types.InlineKeyboardMarkup(row_width=1)
        for s in shipments:
            tn = s.get('tracking_number')
            label = f"{tn} [{s.get('status', '')}]"
            markup.add(types.InlineKeyboardButton(label, callback_data=f"view_{tn}"))
        if total > page * 10:
            markup.add(types.InlineKeyboardButton('Next', callback_data=f'list_{page+1}'))
        markup.add(types.InlineKeyboardButton('🏠 Home', callback_data='menu_page_1'))
        text = f"*📋 Shipment List* (Page {page}, {total} total):"
        if message_id:
            try:
                bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, 
                                    parse_mode='Markdown', reply_markup=markup)
                return True
            except Exception:
                pass
        bot.send_message(chat_id, text, parse_mode='Markdown', reply_markup=markup)
        return True
    except Exception:
        return None

def show_shipment_menu(call_or_chat, page: int = 1, mode: str = 'view', title: str = 'Select shipment'):
    try:
        if hasattr(call_or_chat, 'message'):
            chat_id = call_or_chat.message.chat.id
            message_id = call_or_chat.message.message_id
            return send_dynamic_menu(chat_id, page=page, message_id=message_id)
        else:
            return send_dynamic_menu(call_or_chat, page=page)
    except Exception:
        return None

# ============================================================
# Simulation Run Guard
# ============================================================
_running_simulations = set()
_running_simulations_lock = threading.Lock()

def spawn_simulation(tn):
    if not tn:
        return False
    with _running_simulations_lock:
        if tn in _running_simulations:
            return False
        _running_simulations.add(tn)

    def _runner():
        try:
            from app import simulate_tracking
            simulate_tracking(tn)
        except Exception as e:
            logger.error(f"Simulation error for {tn}: {e}")
        finally:
            with _running_simulations_lock:
                _running_simulations.discard(tn)

    try:
        import eventlet
        return bool(eventlet.spawn(_runner))
    except Exception:
        t = threading.Thread(target=_runner, daemon=True)
        t.start()
        return True

# ============================================================
# Notification Helper
# ============================================================
def enqueue_notification(payload: dict):
    try:
        client = get_redis_client()
        if client:
            client.lpush('notifications', json.dumps(payload))
            return

        payload_type = payload.get('type')
        data = payload.get('data', {}) or {}
        if payload_type == 'email':
            from app import send_email_notification
            send_email_notification(
                data.get('recipient_email'),
                data.get('subject', 'Shipment Update'),
                html_body=data.get('html_body'),
                plain_body=data.get('plain_body'),
                tracking_number=payload.get('tracking_number')
            )
        elif payload_type == 'webhook':
            url = data.get('webhook_url')
            if url:
                try:
                    requests.post(url, json={**data, 'tracking_number': payload.get('tracking_number')}, timeout=10)
                except Exception as e:
                    console.print(Panel(f"[warning]Webhook send failed: {e}[/warning]"))
        else:
            console.print(Panel(f"[info]Notification queued (fallback): {payload}[/info]"))
    except Exception:
        pass

# ============================================================
# Exports
# ============================================================
__all__ = [
    'console', 'logger',
    'RATE_LIMIT_WINDOW', 'RATE_LIMIT_MAX',
    'BotConfig', 'config',
    'redis_client', 'get_redis_client', 'safe_redis_operation',
    'r_ping', 'r_get', 'r_set', 'r_lpop', 'r_lpush', 'r_ltrim',
    'r_sadd', 'r_srem', 'r_smembers', 'r_scan_iter', 'r_keys',
    'r_hget', 'r_hset', 'r_hgetall', 'r_hlen', 'r_exists', 'r_llen',
    'sanitize_tracking_number', 'validate_email', 'validate_location', 'validate_webhook_url',
    'get_bot', 'is_admin',
    'generate_unique_id',
    'cache_route_templates', 'get_cached_route_templates',
    'get_shipment_list', 'get_shipment_details', 'save_shipment', 'update_shipment',
    'invalidate_cache', 'export_shipments', 'search_shipments', 'get_recent_logs',
    'send_dynamic_menu', 'show_shipment_menu',
    'spawn_simulation',
    'enqueue_notification',
    'add_socket_event', 'recent_socket_events',
    'add_client_error', 'recent_client_errors',
    'email_throttle_cache', 'email_digest_cache'
]
