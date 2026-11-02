"""
Quick Test Script for Discord Webhook Integration
Usage:
    python test_discord.py "https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN"
Or set ALERT_WEBHOOK_URL in environment and run:
    python test_discord.py
"""

import os
import sys

from src.production.consumer.webhook_dispatcher import AlertWebhookDispatcher

def main():
    # 1. Get webhook URL from CLI argument or environment
    if len(sys.argv) > 1 and sys.argv[1].strip():
        webhook_url = sys.argv[1].strip()
    else:
        webhook_url = os.environ.get("ALERT_WEBHOOK_URL", "").strip()

    if not webhook_url:
        print("=" * 65)
        print("[ERROR] Error: No Discord Webhook URL provided.")
        print("=" * 65)
        print("Usage:")
        print('  python test_discord.py "https://discord.com/api/webhooks/..."')
        print("Or set it as an environment variable:")
        print('  $env:ALERT_WEBHOOK_URL = "https://discord.com/api/webhooks/..."')
        print("  python test_discord.py")
        sys.exit(1)

    print("=" * 65)
    print("CryptoPulse - Discord Webhook Delivery Test")
    print("=" * 65)
    dispatcher = AlertWebhookDispatcher(webhook_url=webhook_url, cooldown_seconds=0)
    print(f"Target Platform : {dispatcher.get_platform().upper()}")
    print(f"Masked Endpoint : {dispatcher.get_masked_url()}")
    print()

    # 2. Craft realistic sample anomaly alert (BTC-USD Volatility Spike)
    sample_alert = {
        "event": "MARKET_VOLATILITY_ALERT",
        "symbol": "BTC",
        "observed_price": 68420.50,
        "price_usd": 68420.50,
        "price_return_pct": 2.85,
        "z_score": 3.64,
        "threshold": 3.0,
        "exchange_rate": 88.01,
        "window_size": 30,
        "sample_count": 28,
        "source": "Coinbase Pro (WebSocket)",
    }

    print("Sending simulated Volatility Surge alert...")
    success, msg = dispatcher.dispatch(sample_alert, force=True)

    if success:
        print()
        print("[SUCCESS] SUCCESS! Alert dispatched to Discord.")
        print(f"Status: {msg}")
        print("Check your Discord channel for the Rich Embed notification!")
    else:
        print()
        print(f"[FAILED] FAILED: {msg}")

    print("=" * 65)

if __name__ == "__main__":
    main()
