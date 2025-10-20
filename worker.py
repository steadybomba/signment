import os
import json
import time
import logging
import smtplib
from email.mime.text import MIMEText
from upstash_redis import Redis
import requests
from rich.console import Console
from rich.panel import Panel
from utils import BotConfig, safe_redis_operation

# Logging setup
logger = logging.getLogger('worker')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
console = Console()

# Initialize Redis client
redis_client = None
try:
    redis_client = Redis(
        url=os.getenv("REDIS_URL"),
        token=os.getenv("REDIS_TOKEN", "")
    )
    redis_client.ping()
    logger.info("Connected to Upstash Redis")
    console.print("[info]Connected to Upstash Redis[/info]")
except Exception as e:
    logger.error(f"Upstash Redis connection failed: {e}")
    console.print(Panel(f"[error]Upstash Redis connection failed: {e}[/error]", title="Redis Error", border_style="red"))
    redis_client = None

# Load configuration
try:
    config = BotConfig(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        redis_url=os.getenv("REDIS_URL"),
        redis_token=os.getenv("REDIS_TOKEN", ""),
        webhook_url=os.getenv("WEBHOOK_URL", "https://signment-9a96.onrender.com/telegram/webhook"),
        websocket_server=os.getenv("WEBSOCKET_SERVER", "https://signment-9a96.onrender.com"),
        allowed_admins=[int(uid) for uid in os.getenv("ALLOWED_ADMINS", "").split(",") if uid],
        valid_statuses=os.getenv("VALID_STATUSES", "Pending,In_Transit,Out_for_Delivery,Delivered,Returned,Delayed").split(","),
        route_templates=json.loads(os.getenv("ROUTE_TEMPLATES", '{"Lagos, NG": ["Lagos, NG"]}')),
        smtp_host=os.getenv("SMTP_HOST", "smtp.gmail.com"),
        smtp_port=int(os.getenv("SMTP_PORT", 587)),
        smtp_user=os.getenv("SMTP_USER", ""),
        smtp_pass=os.getenv("SMTP_PASS", ""),
        smtp_from=os.getenv("SMTP_FROM", "no-reply@example.com")
    )
except Exception as e:
    logger.error(f"Configuration validation failed: {e}")
    console.print(Panel(f"[error]Configuration validation failed: {e}[/error]", title="Config Error", border_style="red"))
    raise

def send_email(tracking_number: str, status: str, checkpoints: str, delivery_location: str, recipient_email: str) -> bool:
    """Send an email notification."""
    try:
        msg = MIMEText(f"Shipment Update: {tracking_number} is now {status} at {delivery_location}\n\nCheckpoints:\n{checkpoints or 'None'}")
        msg['Subject'] = f"Shipment Update: {tracking_number}"
        msg['From'] = config.smtp_from
        msg['To'] = recipient_email
        with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=5) as server:
            server.starttls()
            server.login(config.smtp_user, config.smtp_pass)
            server.send_message(msg)
        logger.info(f"Sent email notification for {tracking_number} to {recipient_email}")
        console.print(f"[info]Sent email notification for {tracking_number} to {recipient_email}[/info]")
        return True
    except smtplib.SMTPException as e:
        logger.error(f"Failed to send email notification for {tracking_number}: {e}")
        console.print(Panel(f"[error]Failed to send email notification for {tracking_number}: {e}[/error]", title="Email Error", border_style="red"))
        return False

def send_webhook(tracking_number: str, status: str, checkpoints: list, delivery_location: str, webhook_url: str) -> bool:
    """Send a webhook notification."""
    try:
        payload = {
            "tracking_number": tracking_number,
            "status": status,
            "checkpoints": checkpoints,
            "delivery_location": delivery_location,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        response = requests.post(webhook_url, json=payload, timeout=5)
        response.raise_for_status()
        logger.info(f"Sent webhook notification for {tracking_number} to {webhook_url}")
        console.print(f"[info]Sent webhook notification for {tracking_number} to {webhook_url}[/info]")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send webhook notification for {tracking_number}: {e}")
        console.print(Panel(f"[error]Failed to send webhook notification for {tracking_number}: {e}[/error]", title="Webhook Error", border_style="red"))
        return False

def process_notifications():
    """Process notifications from the Redis queue."""
    if not redis_client:
        logger.error("Redis client unavailable, cannot process notifications")
        console.print(Panel("[error]Redis client unavailable, cannot process notifications[/error]", title="Worker Error", border_style="red"))
        return

    while True:
        try:
            notification = safe_redis_operation(redis_client.blpop, "notifications_queue", timeout=5)
            if not notification:
                logger.debug("No notifications in queue, waiting...")
                continue

            _, notification_str = notification
            notification = json.loads(notification_str)
            tracking_number = notification.get('tracking_number')
            notification_type = notification.get('type')
            data = notification.get('data', {})

            logger.info(f"Processing {notification_type} notification for {tracking_number}")
            console.print(f"[info]Processing {notification_type} notification for {tracking_number}[/info]")

            if notification_type == "email":
                success = send_email(
                    tracking_number=tracking_number,
                    status=data.get('status', 'Unknown'),
                    checkpoints=data.get('checkpoints', ''),
                    delivery_location=data.get('delivery_location', 'Unknown'),
                    recipient_email=data.get('recipient_email', '')
                )
                if not success:
                    logger.warning(f"Requeueing failed email notification for {tracking_number}")
                    safe_redis_operation(redis_client.lpush, "notifications_queue", notification_str)

            elif notification_type == "webhook":
                checkpoints = data.get('checkpoints', [])
                if isinstance(checkpoints, str):
                    checkpoints = checkpoints.split(';') if checkpoints else []
                success = send_webhook(
                    tracking_number=tracking_number,
                    status=data.get('status', 'Unknown'),
                    checkpoints=checkpoints,
                    delivery_location=data.get('delivery_location', 'Unknown'),
                    webhook_url=data.get('webhook_url', config.websocket_server)
                )
                if not success:
                    logger.warning(f"Requeueing failed webhook notification for {tracking_number}")
                    safe_redis_operation(redis_client.lpush, "notifications_queue", notification_str)

        except json.JSONDecodeError as e:
            logger.error(f"Invalid notification format: {e}")
            console.print(Panel(f"[error]Invalid notification format: {e}[/error]", title="Worker Error", border_style="red"))
        except Exception as e:
            logger.error(f"Unexpected error processing notification: {e}")
            console.print(Panel(f"[error]Unexpected error processing notification: {e}[/error]", title="Worker Error", border_style="red"))
            time.sleep(5)  # Prevent tight loop on persistent errors

if __name__ == "__main__":
    logger.info("Starting notification worker")
    console.print("[info]Starting notification worker[/info]")
    try:
        process_notifications()
    except KeyboardInterrupt:
        logger.info("Shutting down notification worker")
        console.print("[info]Shutting down notification worker[/info]")
    except Exception as e:
        logger.critical(f"Worker crashed: {e}")
        console.print(Panel(f"[critical]Worker crashed: {e}[/critical]", title="Worker Error", border_style="red"))
        raise
