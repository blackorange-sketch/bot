"""
Bybit Futures Volume Spike Bot
Моніторить USDT-перпетуальні ф'ючерси на Bybit і надсилає алерти в Telegram
при різкому зростанні торгівельного об'єму на 1-хвилинному таймфреймі.
"""

import asyncio
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field

import aiohttp
import websockets

# ---------------------------------------------------------------------------
# Конфігурація (через змінні середовища)
# ---------------------------------------------------------------------------

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
BYBIT_REST_URL = "https://api.bybit.com"

# Скільки попередніх 1хв-свічок використовувати для розрахунку середнього об'єму
ROLLING_WINDOW = int(os.environ.get("ROLLING_WINDOW", "10"))

# У скільки разів об'єм має перевищити середнє, щоб спрацював алерт
VOLUME_MULTIPLIER = float(os.environ.get("VOLUME_MULTIPLIER", "5.0"))

# Мінімальний об'єм у USDT, нижче якого сигнали ігноруються (відсікає "сміттєві" пари)
MIN_VOLUME_USDT = float(os.environ.get("MIN_VOLUME_USDT", "50000"))

# Скільки пар моніторити одночасно (за замовчуванням — топ за оборотом)
TOP_N_PAIRS = int(os.environ.get("TOP_N_PAIRS", "50"))

# Кулдаун між повторними алертами по одній парі (сек), щоб не спамити
ALERT_COOLDOWN_SEC = int(os.environ.get("ALERT_COOLDOWN_SEC", "300"))

# Скільки символів підписувати в одному WebSocket-з'єднанні (ліміт Bybit — 200 топіків)
SYMBOLS_PER_CONNECTION = 150

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("volume-bot")


# ---------------------------------------------------------------------------
# Стан по кожній парі
# ---------------------------------------------------------------------------

@dataclass
class SymbolState:
    volumes: deque = field(default_factory=lambda: deque(maxlen=ROLLING_WINDOW))
    last_alert_ts: float = 0.0
    last_closed_start: int = 0  # timestamp початку останньої обробленої свічки


# ---------------------------------------------------------------------------
# Отримання списку пар для моніторингу
# ---------------------------------------------------------------------------

async def get_top_symbols(session: aiohttp.ClientSession, top_n: int) -> list[str]:
    """Тягне список USDT-перпетуальних пар, відсортованих за абсолютною зміною ціни за 24г."""
    url = f"{BYBIT_REST_URL}/v5/market/tickers"
    params = {"category": "linear"}
    async with session.get(url, params=params) as resp:
        data = await resp.json()

    tickers = data.get("result", {}).get("list", [])
    usdt_perp = [t for t in tickers if t["symbol"].endswith("USDT")]
    # фільтр за мінімальним оборотом — щоб не потрапляли мертві пари з випадковим % на копійках
    usdt_perp = [t for t in usdt_perp if float(t.get("turnover24h", 0)) >= MIN_VOLUME_USDT]
    usdt_perp.sort(key=lambda t: abs(float(t.get("price24hPcnt", 0))), reverse=True)
    symbols = [t["symbol"] for t in usdt_perp[:top_n]]
    log.info(f"Обрано {len(symbols)} пар для моніторингу (топ за зміною ціни за 24г)")
    return symbols


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

async def send_telegram_alert(session: aiohttp.ClientSession, text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram не налаштовано — алерт лише в логах:\n%s", text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                body = await resp.text()
                log.error(f"Помилка Telegram API: {resp.status} {body}")
    except Exception as e:
        log.error(f"Не вдалося надіслати повідомлення в Telegram: {e}")


# ---------------------------------------------------------------------------
# Обробка kline-повідомлень
# ---------------------------------------------------------------------------

async def handle_kline_message(
    msg: dict,
    states: dict[str, SymbolState],
    session: aiohttp.ClientSession,
) -> None:
    topic = msg.get("topic", "")
    if not topic.startswith("kline."):
        return

    symbol = topic.split(".")[-1]
    state = states.setdefault(symbol, SymbolState())

    for candle in msg.get("data", []):
        is_confirmed = candle.get("confirm", False)
        if not is_confirmed:
            continue  # враховуємо тільки закриті свічки — уникаємо хибних спрацювань на незавершеному барі

        start_ts = candle["start"]
        if start_ts == state.last_closed_start:
            continue  # вже обробляли цю свічку
        state.last_closed_start = start_ts

        volume = float(candle["volume"])
        turnover = float(candle["turnover"])  # об'єм у USDT
        close_price = float(candle["close"])
        open_price = float(candle["open"])

        history = list(state.volumes)

        if len(history) >= ROLLING_WINDOW // 2:  # достатньо історії для адекватного середнього
            avg_volume = sum(history) / len(history)
            if avg_volume > 0 and volume > avg_volume * VOLUME_MULTIPLIER and turnover >= MIN_VOLUME_USDT:
                now = time.time()
                if now - state.last_alert_ts >= ALERT_COOLDOWN_SEC:
                    state.last_alert_ts = now
                    pct_change = (close_price - open_price) / open_price * 100
                    direction = "🟢" if pct_change >= 0 else "🔴"
                    ratio = volume / avg_volume
                    text = (
                        f"⚡️ <b>Сплеск об'єму: {symbol}</b>\n"
                        f"Об'єм: {volume:,.0f} (у {ratio:.1f}x вище середнього)\n"
                        f"Оборот: ${turnover:,.0f}\n"
                        f"Ціна: {close_price:.6g} ({direction}{pct_change:+.2f}%)\n"
                        f"Таймфрейм: 1хв"
                    )
                    log.info(text.replace("\n", " | "))
                    await send_telegram_alert(session, text)

        state.volumes.append(volume)


# ---------------------------------------------------------------------------
# WebSocket-з'єднання (одне на пачку символів)
# ---------------------------------------------------------------------------

async def run_ws_connection(
    symbols: list[str],
    states: dict[str, SymbolState],
    session: aiohttp.ClientSession,
) -> None:
    """Тримає одне WebSocket-з'єднання з автоматичним перепідключенням."""
    topics = [f"kline.1.{s}" for s in symbols]

    while True:
        try:
            async with websockets.connect(BYBIT_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                log.info(f"WebSocket підключено ({len(symbols)} пар)")

                # Bybit дозволяє підписатись максимум на ~10 топіків за раз в одному запиті
                for i in range(0, len(topics), 10):
                    sub_msg = {"op": "subscribe", "args": topics[i:i + 10]}
                    await ws.send(json.dumps(sub_msg))
                    await asyncio.sleep(0.1)

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("op") in ("subscribe", "pong", "ping"):
                        continue
                    await handle_kline_message(msg, states, session)

        except (websockets.ConnectionClosed, ConnectionError, asyncio.TimeoutError) as e:
            log.warning(f"З'єднання розірвано ({e}), перепідключення через 5с...")
            await asyncio.sleep(5)
        except Exception as e:
            log.error(f"Неочікувана помилка WS: {e}", exc_info=True)
            await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Точка входу
# ---------------------------------------------------------------------------

async def main() -> None:
    async with aiohttp.ClientSession() as session:
        symbols = await get_top_symbols(session, TOP_N_PAIRS)
        if not symbols:
            log.error("Не вдалося отримати список пар. Завершення.")
            return

        states: dict[str, SymbolState] = {}

        # Розбиваємо символи на групи по SYMBOLS_PER_CONNECTION — кожна група в окремому WS-з'єднанні
        chunks = [
            symbols[i:i + SYMBOLS_PER_CONNECTION]
            for i in range(0, len(symbols), SYMBOLS_PER_CONNECTION)
        ]

        tasks = [run_ws_connection(chunk, states, session) for chunk in chunks]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Зупинено користувачем")
