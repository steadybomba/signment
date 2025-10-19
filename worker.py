import os
import json
from upstash_redis import Redis
import smtplib
from email.mime.text import MIMEText
import requests
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

redis_client = Redis(
    url=os.getenv("REDIS_URL"),
    token=os.getenv("UPSTASH_REDIS_TOKEN")
)

def process_notification(notification):
    notification_type = notification['type']
    tracking_number = notification['tracking_number']
    data = notification['data']
    if notification_type == 'email':
        msg = MIMEText(f"Shipment Update: {tracking_number} is now {data['status']} at {data['delivery_location']}\nCheckpoints: {data['checkpoints']}")
        msg['Subject'] = f"Shipment Update: {tracking_number}"
        msg['From'] = os.getenv("SMTP_FROM")
        msg['To'] = data['recipient_email']
        with smtplib.SMTP(os.getenv("SMTP_HOST"), os.getenv("SMTP_PORT")) as server:
            server.starttls()
            server.login(os.getenv("SMTP_USER"), os.getenv("SMTP_PASS"))
            server.send_message(msg)
        logger.info(f"Sent email for {tracking_number}")
    elif notification_type == 'webhook':
        response = requests.post(data['webhook_url'], json={
            'tracking_number': tracking_number,
            'status': data['status'],
            'checkpoints': data['checkpoints'],
            'delivery_location': data['delivery_location']
        }, timeout=5)
        response.raise_for_status()
        logger.info(f"Sent webhook for {tracking_number}")

if __name__ == "__main__":
    logger.info("Starting notification worker")
    while True:
        result = redis_client.brpop("notifications_queue", timeout=30)
        if result:
            _, notification_json = result
            notification = json.loads(notification_json)
            try:
                process_notification(notification)
            except Exception as e:
                logger.error(f"Failed to process notification for {notification['tracking_number']}: {e}")
