import logging
from rich.console import Console
from rich.panel import Panel
from telegram_bot import set_webhook, cache_route_templates

# Logging setup
bot_logger = logging.getLogger('telegram_bot')
console = Console()

if __name__ == '__main__':
    try:
        # Cache route templates for consistency with Flask app
        cache_route_templates()
        bot_logger.info("Route templates cached successfully in start_bot")
        console.print("[info]Route templates cached successfully in start_bot[/info]")

        # Set Telegram webhook
        set_webhook()
        bot_logger.info("Webhook setup completed successfully")
        console.print("[info]Webhook setup completed successfully[/info]")
    except Exception as e:
        bot_logger.error(f"Failed to initialize bot: {e}")
        console.print(Panel(f"[error]Failed to initialize bot: {e}[/error]", title="Bot Startup Error", border_style="red"))
        raise
