"""
Multi-Channel Alert Webhook Dispatcher
-------------------------------------
Dispatches financial volatility and market anomaly alerts to:
- Discord Webhooks (Rich Embeds with color coding)
- Telegram Bot API (HTML formatted messages)
- Slack Webhooks (Formatted attachments)
- Generic JSON Webhooks (n8n, Zapier, custom REST endpoints)

Features:
- Auto-detection of target webhook platform from URL.
- Per-symbol cooldown and rate-limiting to prevent channel flooding.
- Detailed delivery diagnostics and error handling.
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone


class AlertWebhookDispatcher:
    def __init__(self, webhook_url: str = "", cooldown_seconds: int = 15):
        self.webhook_url = webhook_url.strip()
        self.cooldown_seconds = cooldown_seconds
        self.last_dispatched: dict[str, float] = {}

    def set_url(self, url: str) -> None:
        self.webhook_url = url.strip()

    def get_masked_url(self) -> str:
        if not self.webhook_url:
            return "Not Configured"
        if len(self.webhook_url) <= 18:
            return "***"
        return self.webhook_url[:15] + "..." + self.webhook_url[-6:]

    def get_platform(self) -> str:
        url = self.webhook_url.lower()
        if not url:
            return "none"
        if "discord.com/api/webhooks" in url or "discordapp.com/api/webhooks" in url:
            return "discord"
        if "api.telegram.org/bot" in url:
            return "telegram"
        if "hooks.slack.com/services" in url:
            return "slack"
        return "generic"

    def can_dispatch(self, symbol: str) -> bool:
        now = time.time()
        last = self.last_dispatched.get(symbol, 0.0)
        return (now - last) >= self.cooldown_seconds

    def record_dispatch(self, symbol: str) -> None:
        self.last_dispatched[symbol] = time.time()

    def dispatch(self, alert_data: dict, force: bool = False) -> tuple[bool, str]:
        """
        Dispatch an alert to the configured webhook URL.
        Returns: (success: bool, status_message: str)
        """
        if not self.webhook_url:
            return False, "No webhook URL configured (simulation only)"

        symbol = alert_data.get("symbol", "UNKNOWN")
        if not force and not self.can_dispatch(symbol):
            remaining = int(self.cooldown_seconds - (time.time() - self.last_dispatched.get(symbol, 0.0)))
            return False, f"Throttled: {symbol} in cooldown ({remaining}s remaining)"

        platform = self.get_platform()
        try:
            if platform == "discord":
                payload, headers, url = self._build_discord_payload(alert_data)
            elif platform == "telegram":
                payload, headers, url = self._build_telegram_payload(alert_data)
            elif platform == "slack":
                payload, headers, url = self._build_slack_payload(alert_data)
            else:
                payload, headers, url = self._build_generic_payload(alert_data)

            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                status_code = resp.status
                self.record_dispatch(symbol)
                return True, f"Delivered to {platform} (HTTP {status_code})"
        except Exception as exc:
            return False, f"Webhook delivery failed ({platform}): {str(exc)}"

    def _build_discord_payload(self, data: dict) -> tuple[dict, dict, str]:
        symbol = data.get("symbol", "CRYPTO")
        price_usd = data.get("price_usd") or data.get("observed_price", 0.0)
        ret_pct = data.get("price_return_pct")
        if ret_pct is None and "price_return" in data:
            ret_pct = round(data["price_return"] * 100, 3)
        ret_pct = ret_pct if ret_pct is not None else 0.0

        z_score = data.get("z_score", 0.0)
        is_spike = ret_pct >= 0
        anomaly_title = "Price Spike (Pump)" if is_spike else "Sharp Drop (Dump)"
        color = 0x00FF88 if is_spike else 0xFF3366  # Green or Red in decimal

        fx_rate = data.get("exchange_rate", 94.96)
        price_inr = data.get("price_inr") or (price_usd * fx_rate)

        price_str = f"${price_usd:,.2f}" if price_usd >= 1 else f"${price_usd:.4f}"
        price_inr_str = f"₹{price_inr:,.2f}"

        fields = [
            {"name": "Asset", "value": f"**{symbol}**", "inline": True},
            {"name": "Anomaly Type", "value": anomaly_title, "inline": True},
            {"name": "Z-Score", "value": f"**{z_score:+.2f}σ**", "inline": True},
            {"name": "Price (USD)", "value": price_str, "inline": True},
            {"name": "Price (INR)", "value": price_inr_str, "inline": True},
            {"name": "Return", "value": f"{ret_pct:+.3f}%", "inline": True},
            {"name": "Threshold", "value": f"≥ {data.get('threshold', 3.0):.1f}σ (Gaussian)", "inline": True},
            {"name": "Timestamp", "value": str(data.get("alert_time") or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")), "inline": True},
            {"name": "Pipeline Source", "value": "Coinbase Pro -> Kafka -> Spark", "inline": True},
        ]

        payload = {
            "username": "Market Volatility Sentinel",
            "embeds": [
                {
                    "title": f"Market Volatility Alert: {symbol}",
                    "description": f"Statistical anomaly detected on real-time stream exceeding distribution bounds.",
                    "color": color,
                    "fields": fields,
                    "footer": {"text": "Dual-Currency Real-Time Financial Streaming Pipeline"},
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            ],
        }
        return payload, {"Content-Type": "application/json", "User-Agent": "CryptoStreamingPipeline/2.0"}, self.webhook_url

    def _build_telegram_payload(self, data: dict) -> tuple[dict, dict, str]:
        symbol = data.get("symbol", "CRYPTO")
        price_usd = data.get("price_usd") or data.get("observed_price", 0.0)
        ret_pct = data.get("price_return_pct", 0.0)
        z_score = data.get("z_score", 0.0)
        is_spike = ret_pct >= 0
        anomaly_title = "Price Spike (Pump)" if is_spike else "Sharp Drop (Dump)"

        fx_rate = data.get("exchange_rate", 94.96)
        price_inr = data.get("price_inr") or (price_usd * fx_rate)

        price_str = f"${price_usd:,.2f}" if price_usd >= 1 else f"${price_usd:.4f}"
        price_inr_str = f"₹{price_inr:,.2f}"

        parsed_url = urllib.parse.urlparse(self.webhook_url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        chat_id = query_params.get("chat_id", [""])[0] or parsed_url.fragment

        text = (
            f"<b>Market Volatility Alert: {symbol}</b>\n\n"
            f"<b>Type:</b> {anomaly_title}\n"
            f"<b>Z-Score Deviation:</b> {z_score:+.2f}σ\n"
            f"<b>Price (USD):</b> {price_str}\n"
            f"<b>Price (INR):</b> {price_inr_str}\n"
            f"<b>Price Return:</b> {ret_pct:+.3f}%\n"
            f"<b>Threshold:</b> ≥ {data.get('threshold', 3.0):.1f}σ\n"
            f"<b>Timestamp:</b> {data.get('alert_time', datetime.now(timezone.utc).strftime('%H:%M:%S UTC'))}\n\n"
            f"<i>Dual-Currency Real-Time Streaming Pipeline</i>"
        )

        clean_url = urllib.parse.urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            "",
            "",
            "",
        ))

        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        return payload, {"Content-Type": "application/json", "User-Agent": "CryptoStreamingPipeline/2.0"}, clean_url

    def _build_slack_payload(self, data: dict) -> tuple[dict, dict, str]:
        symbol = data.get("symbol", "CRYPTO")
        price_usd = data.get("price_usd") or data.get("observed_price", 0.0)
        ret_pct = data.get("price_return_pct", 0.0)
        z_score = data.get("z_score", 0.0)
        is_spike = ret_pct >= 0
        anomaly_title = "Price Spike (Pump)" if is_spike else "Sharp Drop (Dump)"
        color = "#00ff88" if is_spike else "#ff3366"

        payload = {
            "attachments": [
                {
                    "color": color,
                    "title": f"Market Volatility Alert: {symbol}",
                    "text": (
                        f"*{anomaly_title}* | Z={z_score:+.2f}σ | Price: ${price_usd:,.2f} | "
                        f"Return: {ret_pct:+.3f}% | Threshold: ≥ {data.get('threshold', 3.0):.1f}σ"
                    ),
                    "footer": "Dual-Currency Real-Time Financial Streaming Pipeline",
                    "ts": int(time.time()),
                }
            ]
        }
        return payload, {"Content-Type": "application/json", "User-Agent": "CryptoStreamingPipeline/2.0"}, self.webhook_url

    def _build_generic_payload(self, data: dict) -> tuple[dict, dict, str]:
        payload = {
            "event": "MARKET_VOLATILITY_ALERT",
            "symbol": data.get("symbol"),
            "alert_time": data.get("alert_time") or datetime.now(timezone.utc).isoformat(),
            "observed_price_usd": data.get("price_usd") or data.get("observed_price"),
            "price_return_pct": data.get("price_return_pct"),
            "z_score": data.get("z_score"),
            "threshold": data.get("threshold", 3.0),
            "window_size": data.get("window_size", 30),
            "source": data.get("source", "anomaly_detector"),
        }
        return payload, {"Content-Type": "application/json", "User-Agent": "CryptoStreamingPipeline/2.0"}, self.webhook_url
