"""
Bybit Futures Volume Spike Bot
Моніторить USDT-перпетуальні ф'ючерси на Bybit і надсилає алерти в Telegram
при різкому й статистично значущому зростанні об'єму на 1-хвилинному таймфреймі,
з додатковим контекстом для оцінки ймовірного напряму руху.
"""

import asyncio
import json
import logging
import os
import statistics
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

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

# Поріг для z-score (скільки стандартних відхилень від середнього має бути об'єм)
Z_SCORE_THRESHOLD = float(os.environ.get("Z_SCORE_THRESHOLD", "3.0"))

# Мінімальна абсолютна зміна ціни за свічку (%), щоб відсіяти "порожні" сплески об'єму
MIN_PRICE_CHANGE_PCT = float(os.environ.get("MIN_PRICE_CHANGE_PCT", "0.3"))

# Мінімальний об'єм у USDT, нижче якого сигнали ігноруються (відсікає "сміттєві" пари)
MIN_VOLUME_USDT = float(os.environ.get("MIN_VOLUME_USDT", "50000"))

# Скільки пар моніторити одночасно (обираються за зміною ціни за 24г)
TOP_N_PAIRS = int(os.environ.get("TOP_N_PAIRS", "25"))

# Кулдаун між повторними алертами по одній парі (сек), щоб не спамити
ALERT_COOLDOWN_SEC = int(os.environ.get("ALERT_COOLDOWN_SEC", "300"))

# Як часто оновлювати список топ-пар і перепідписуватись (сек)
REFRESH_INTERVAL_SEC = int(os.environ.get("REFRESH_INTERVAL_SEC", "1800"))

# Як часто опитувати Open Interest по кожній парі (сек)
OI_REFRESH_INTERVAL_SEC = int(os.environ.get("OI_REFRESH_INTERVAL_SEC", "180"))

# Скільки символів підписувати в одному WebSocket-з'єднанні.
# Тепер на кожну пару припадає 4 топіки (kline.1, kline.5, publicTrade, orderbook.50),
# тож тримаємо запас під ліміт Bybit на кількість підписок в одному з'єднанні.
SYMBOLS_PER_CONNECTION = 40

# Типи інструментів у полі symbolType, які вважаються TradFi (акції/ETF/товари), а не криптовалютою
TRADFI_SYMBOL_TYPES = {"stock", "etf", "commodity"}

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
    last_closed_start: int = 0  # timestamp початку останньої обробленої 1хв-свічки

    # Taker buy/sell: minute_start_ms -> [buy_volume, sell_volume]
    trade_buckets: dict = field(default_factory=dict)

    # Локальна копія стакану (ціна -> обсяг, як рядки — так приходять від Bybit)
    ob_bids: dict = field(default_factory=dict)
    ob_asks: dict = field(default_factory=dict)
    ob_bid_pct: Optional[float] = None  # частка обсягу на боці бідів, %

    # Напрям останньої підтвердженої 5хв-свічки: "up" / "down"
    tf5_direction: Optional[str] = None

    # Open Interest
    oi_previous: Optional[float] = None
    oi_current: Optional[float] = None

    funding_rate: Optional[float] = None


# ---------------------------------------------------------------------------
# Виключення TradFi-контрактів (акції, ETF, товари)
# ---------------------------------------------------------------------------

async def get_tradfi_symbols(session: aiohttp.ClientSession) -> set[str]:
    """Повертає множину символів TradFi-контрактів на Bybit, щоб виключити їх з моніторингу."""
    url = f"{BYBIT_REST_URL}/v5/market/instruments-info"
    params = {"category": "linear"}
    headers = {"User-Agent": "Mozilla/5.0 (compatible; VolumeSpikeBot/1.0)"}

    try:
        async with session.get(url, params=params, headers=headers) as resp:
            raw = await resp.text()
            if resp.status != 200:
                log.warning(f"instruments-info повернув статус {resp.status} — TradFi-фільтр пропущено")
                return set()
            data = json.loads(raw)
    except Exception as e:
        log.warning(f"Не вдалося отримати instruments-info ({e}) — TradFi-фільтр пропущено")
        return set()

    if data.get("retCode") != 0:
        log.warning(f"instruments-info повернув помилку: {data.get('retMsg')} — TradFi-фільтр пропущено")
        return set()

    instruments = data.get("result", {}).get("list", [])
    tradfi_symbols = set()
    for instrument in instruments:
        symbol_type = str(instrument.get("symbolType", "")).strip().lower()
        if symbol_type in TRADFI_SYMBOL_TYPES:
            tradfi_symbols.add(instrument["symbol"])

    log.info(f"Знайдено {len(tradfi_symbols)} TradFi-контрактів (акції/ETF/товари) — будуть виключені")
    return tradfi_symbols


# ---------------------------------------------------------------------------
# Тікери: список пар для моніторингу + funding rate
# ---------------------------------------------------------------------------

async def fetch_tickers(session: aiohttp.ClientSession) -> list[dict]:
    """Тягне сирий список усіх linear-тікерів з Bybit."""
    url = f"{BYBIT_REST_URL}/v5/market/tickers"
    params = {"category": "linear"}
    headers = {"User-Agent": "Mozilla/5.0 (compatible; VolumeSpikeBot/1.0)"}

    async with session.get(url, params=params, headers=headers) as resp:
        raw = await resp.text()
        if resp.status != 200:
            log.error(f"Bybit API повернув статус {resp.status}. Тіло відповіді: {raw[:500]}")
            return []
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.error(f"Bybit API повернув не-JSON відповідь (перші 500 символів): {raw[:500]}")
            return []

    if data.get("retCode") != 0:
        log.error(f"Bybit API повернув помилку: retCode={data.get('retCode')} retMsg={data.get('retMsg')}")
        return []

    return data.get("result", {}).get("list", [])


def select_top_symbols(tickers: list[dict], top_n: int, excluded_symbols: set[str]) -> list[str]:
    """Фільтрує й сортує тікери: топ-N криптопар за абсолютною зміною ціни за 24г."""
    usdt_perp = [t for t in tickers if t["symbol"].endswith("USDT")]
    usdt_perp = [t for t in usdt_perp if t["symbol"] not in excluded_symbols]
    usdt_perp = [t for t in usdt_perp if float(t.get("turnover24h", 0)) >= MIN_VOLUME_USDT]
    usdt_perp.sort(key=lambda t: abs(float(t.get("price24hPcnt", 0))), reverse=True)
    symbols = [t["symbol"] for t in usdt_perp[:top_n]]
    log.info(f"Обрано {len(symbols)} пар для моніторингу (топ за зміною ціни за 24г)")
    return symbols


def build_funding_map(tickers: list[dict]) -> dict[str, float]:
    """Дістає funding rate для кожного символу з того самого запиту тікерів."""
    funding_map = {}
    for t in tickers:
        try:
            funding_map[t["symbol"]] = float(t.get("fundingRate", 0))
        except (TypeError, ValueError):
            continue
    return funding_map


# ---------------------------------------------------------------------------
# Open Interest
# ---------------------------------------------------------------------------

async def fetch_open_interest(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    """Тягне поточний Open Interest по одному символу."""
    url = f"{BYBIT_REST_URL}/v5/market/open-interest"
    params = {"category": "linear", "symbol": symbol, "intervalTime": "5min", "limit": 1}
    headers = {"User-Agent": "Mozilla/5.0 (compatible; VolumeSpikeBot/1.0)"}
    try:
        async with session.get(url, params=params, headers=headers) as resp:
            if resp.status != 200:
                return None
            raw = await resp.text()
            data = json.loads(raw)
    except Exception:
        return None

    if data.get("retCode") != 0:
        return None

    lst = data.get("result", {}).get("list", [])
    if not lst:
        return None
    try:
        return float(lst[0]["openInterest"])
    except (KeyError, ValueError, TypeError):
        return None


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
# Обробка kline.1 (основна логіка детекції)
# ---------------------------------------------------------------------------

def _describe_taker_ratio(state: SymbolState, start_ts: int) -> str:
    bucket = state.trade_buckets.get(start_ts)
    if not bucket:
        return "н/д"
    buy_vol, sell_vol = bucket
    total = buy_vol + sell_vol
    if total <= 0:
        return "н/д"
    buy_pct = buy_vol / total * 100
    return f"{buy_pct:.0f}% купівель / {100 - buy_pct:.0f}% продажів"


def _describe_orderbook(state: SymbolState) -> str:
    if state.ob_bid_pct is None:
        return "н/д"
    return f"{state.ob_bid_pct:.0f}% бід / {100 - state.ob_bid_pct:.0f}% аск"


def _describe_open_interest(state: SymbolState) -> str:
    if state.oi_previous is None or state.oi_current is None or state.oi_previous <= 0:
        return "н/д (ще збирається)"
    change_pct = (state.oi_current - state.oi_previous) / state.oi_previous * 100
    if change_pct > 0.5:
        return f"росте ({change_pct:+.2f}%) — ймовірно нові позиції"
    if change_pct < -0.5:
        return f"падає ({change_pct:+.2f}%) — ймовірно закриття позицій"
    return f"без істотних змін ({change_pct:+.2f}%)"


def _describe_funding(state: SymbolState) -> str:
    if state.funding_rate is None:
        return "н/д"
    return f"{state.funding_rate * 100:+.4f}%"


def _describe_tf5(state: SymbolState, pct_change: float) -> str:
    if state.tf5_direction is None:
        return "н/д"
    label = "зростання" if state.tf5_direction == "up" else "падіння"
    same_direction = (state.tf5_direction == "up") == (pct_change >= 0)
    if same_direction:
        return f"{label} (сигнал за трендом)"
    return f"{label} (сигнал проти тренду — можливий розворот)"


def _describe_pre_spike_trend(history: list[float]) -> str:
    recent = history[-3:]
    if len(recent) < 2:
        return "н/д"
    if recent[-1] > recent[0]:
        return "зростав"
    if recent[-1] < recent[0]:
        return "спадав"
    return "стабільний"


async def handle_kline_message(
    msg: dict,
    states: dict[str, SymbolState],
    session: aiohttp.ClientSession,
) -> None:
    topic = msg.get("topic", "")
    if not topic.startswith("kline.1."):
        return

    symbol = topic.split(".")[-1]
    state = states.setdefault(symbol, SymbolState())

    for candle in msg.get("data", []):
        if not candle.get("confirm", False):
            continue  # враховуємо тільки закриті свічки

        start_ts = candle["start"]
        if start_ts == state.last_closed_start:
            continue
        state.last_closed_start = start_ts

        volume = float(candle["volume"])
        turnover = float(candle["turnover"])
        close_price = float(candle["close"])
        open_price = float(candle["open"])

        history = list(state.volumes)

        if len(history) >= max(5, ROLLING_WINDOW // 2):
            mean_volume = statistics.mean(history)
            stdev_volume = statistics.pstdev(history)
            z_score = (volume - mean_volume) / stdev_volume if stdev_volume > 0 else 0.0
            pct_change = (close_price - open_price) / open_price * 100 if open_price else 0.0

            if (
                z_score >= Z_SCORE_THRESHOLD
                and abs(pct_change) >= MIN_PRICE_CHANGE_PCT
                and turnover >= MIN_VOLUME_USDT
            ):
                now = time.time()
                if now - state.last_alert_ts >= ALERT_COOLDOWN_SEC:
                    state.last_alert_ts = now
                    direction = "🟢" if pct_change >= 0 else "🔴"
                    ratio = volume / mean_volume if mean_volume > 0 else 0.0

                    text = (
                        f"⚡️ <b>Сплеск об'єму: {symbol}</b>\n"
                        f"Об'єм: {volume:,.0f} (у {ratio:.1f}x вище середнього, z-score {z_score:.1f})\n"
                        f"Оборот: ${turnover:,.0f}\n"
                        f"Ціна: {close_price:.6g} ({direction}{pct_change:+.2f}%)\n"
                        f"Купівлі/продажі (1хв): {_describe_taker_ratio(state, start_ts)}\n"
                        f"Дисбаланс стакану: {_describe_orderbook(state)}\n"
                        f"Open Interest: {_describe_open_interest(state)}\n"
                        f"Funding rate: {_describe_funding(state)}\n"
                        f"5хв тренд: {_describe_tf5(state, pct_change)}\n"
                        f"Об'єм перед сплеском: {_describe_pre_spike_trend(history)}\n"
                        f"Таймфрейм: 1хв"
                    )
                    log.info(text.replace("\n", " | "))
                    await send_telegram_alert(session, text)

        state.volumes.append(volume)


# ---------------------------------------------------------------------------
# Обробка kline.5 (напрям старшого таймфрейму)
# ---------------------------------------------------------------------------

async def handle_kline5_message(msg: dict, states: dict[str, SymbolState]) -> None:
    topic = msg.get("topic", "")
    if not topic.startswith("kline.5."):
        return
    symbol = topic.split(".")[-1]
    state = states.setdefault(symbol, SymbolState())

    for candle in msg.get("data", []):
        if not candle.get("confirm", False):
            continue
        close_price = float(candle["close"])
        open_price = float(candle["open"])
        state.tf5_direction = "up" if close_price >= open_price else "down"


# ---------------------------------------------------------------------------
# Обробка publicTrade (taker buy/sell по хвилинах)
# ---------------------------------------------------------------------------

async def handle_trade_message(msg: dict, states: dict[str, SymbolState]) -> None:
    topic = msg.get("topic", "")
    if not topic.startswith("publicTrade."):
        return
    symbol = topic.split(".")[-1]
    state = states.setdefault(symbol, SymbolState())

    for trade in msg.get("data", []):
        try:
            ts = int(trade["T"])
            size = float(trade["v"])
            side = trade["S"]
        except (KeyError, ValueError, TypeError):
            continue

        minute_start = (ts // 60000) * 60000
        bucket = state.trade_buckets.setdefault(minute_start, [0.0, 0.0])
        if side == "Buy":
            bucket[0] += size
        else:
            bucket[1] += size

    # прибираємо старі бакети (лишаємо останні ~3 хвилини), щоб не росла пам'ять
    if len(state.trade_buckets) > 5:
        cutoff = max(state.trade_buckets.keys()) - 3 * 60000
        for key in [k for k in state.trade_buckets if k < cutoff]:
            del state.trade_buckets[key]


# ---------------------------------------------------------------------------
# Обробка orderbook (дисбаланс стакану)
# ---------------------------------------------------------------------------

async def handle_orderbook_message(msg: dict, states: dict[str, SymbolState]) -> None:
    topic = msg.get("topic", "")
    if not topic.startswith("orderbook."):
        return
    symbol = topic.split(".")[-1]
    state = states.setdefault(symbol, SymbolState())

    msg_type = msg.get("type")
    data = msg.get("data", {})

    if msg_type == "snapshot":
        state.ob_bids = {p: q for p, q in data.get("b", [])}
        state.ob_asks = {p: q for p, q in data.get("a", [])}
    elif msg_type == "delta":
        for p, q in data.get("b", []):
            if float(q) == 0:
                state.ob_bids.pop(p, None)
            else:
                state.ob_bids[p] = q
        for p, q in data.get("a", []):
            if float(q) == 0:
                state.ob_asks.pop(p, None)
            else:
                state.ob_asks[p] = q
    else:
        return

    bid_vol = sum(float(q) for q in state.ob_bids.values())
    ask_vol = sum(float(q) for q in state.ob_asks.values())
    total = bid_vol + ask_vol
    if total > 0:
        state.ob_bid_pct = bid_vol / total * 100


# ---------------------------------------------------------------------------
# WebSocket-з'єднання (одне на пачку символів, 4 топіки на символ)
# ---------------------------------------------------------------------------

async def run_ws_connection(
    symbols: list[str],
    states: dict[str, SymbolState],
    session: aiohttp.ClientSession,
) -> None:
    """Тримає одне WebSocket-з'єднання з автоматичним перепідключенням."""
    topics: list[str] = []
    for s in symbols:
        topics.extend([
            f"kline.1.{s}",
            f"kline.5.{s}",
            f"publicTrade.{s}",
            f"orderbook.50.{s}",
        ])

    while True:
        try:
            async with websockets.connect(BYBIT_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                log.info(f"WebSocket підключено ({len(symbols)} пар, {len(topics)} топіків)")

                for i in range(0, len(topics), 10):
                    sub_msg = {"op": "subscribe", "args": topics[i:i + 10]}
                    await ws.send(json.dumps(sub_msg))
                    await asyncio.sleep(0.1)

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("op") in ("subscribe", "pong", "ping"):
                        continue

                    topic = msg.get("topic", "")
                    if topic.startswith("kline.1."):
                        await handle_kline_message(msg, states, session)
                    elif topic.startswith("kline.5."):
                        await handle_kline5_message(msg, states)
                    elif topic.startswith("publicTrade."):
                        await handle_trade_message(msg, states)
                    elif topic.startswith("orderbook."):
                        await handle_orderbook_message(msg, states)

        except asyncio.CancelledError:
            raise
        except (websockets.ConnectionClosed, ConnectionError, asyncio.TimeoutError) as e:
            log.warning(f"З'єднання розірвано ({e}), перепідключення через 5с...")
            await asyncio.sleep(5)
        except Exception as e:
            log.error(f"Неочікувана помилка WS: {e}", exc_info=True)
            await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Керування підписками з автооновленням списку пар + фоновий Open Interest
# ---------------------------------------------------------------------------

async def run_market_data(session: aiohttp.ClientSession) -> None:
    states: dict[str, SymbolState] = {}
    excluded_tradfi = await get_tradfi_symbols(session)

    current_symbols: list[str] = []
    ws_tasks: list[asyncio.Task] = []

    async def run_oi_updater() -> None:
        while True:
            for sym in list(current_symbols):
                oi = await fetch_open_interest(session, sym)
                if oi is not None:
                    st = states.setdefault(sym, SymbolState())
                    if st.oi_current is not None:
                        st.oi_previous = st.oi_current
                    st.oi_current = oi
                await asyncio.sleep(0.15)  # невеликий інтервал між запитами проти rate limit
            await asyncio.sleep(OI_REFRESH_INTERVAL_SEC)

    oi_task = asyncio.create_task(run_oi_updater())

    try:
        while True:
            tickers = await fetch_tickers(session)
            if not tickers:
                log.error("Не вдалося отримати список пар. Повтор через 15с...")
                await asyncio.sleep(15)
                continue

            new_symbols = select_top_symbols(tickers, TOP_N_PAIRS, excluded_tradfi)
            if not new_symbols:
                log.error("Порожній список пар після фільтрів. Повтор через 15с...")
                await asyncio.sleep(15)
                continue

            funding_map = build_funding_map(tickers)
            for sym in new_symbols:
                st = states.setdefault(sym, SymbolState())
                if sym in funding_map:
                    st.funding_rate = funding_map[sym]

            if set(new_symbols) != set(current_symbols):
                log.info(
                    f"Список пар змінився ({len(current_symbols)} -> {len(new_symbols)}), "
                    f"перепідключення WebSocket..."
                )

                for task in ws_tasks:
                    task.cancel()
                if ws_tasks:
                    await asyncio.gather(*ws_tasks, return_exceptions=True)

                for sym in list(states.keys()):
                    if sym not in new_symbols:
                        del states[sym]

                current_symbols = new_symbols
                chunks = [
                    current_symbols[i:i + SYMBOLS_PER_CONNECTION]
                    for i in range(0, len(current_symbols), SYMBOLS_PER_CONNECTION)
                ]
                ws_tasks = [
                    asyncio.create_task(run_ws_connection(chunk, states, session))
                    for chunk in chunks
                ]
            else:
                log.info("Список топ-пар без змін, з'єднання не чіпаємо")

            await asyncio.sleep(REFRESH_INTERVAL_SEC)
    finally:
        oi_task.cancel()
        for task in ws_tasks:
            task.cancel()


# ---------------------------------------------------------------------------
# Точка входу
# ---------------------------------------------------------------------------

async def main() -> None:
    async with aiohttp.ClientSession() as session:
        await run_market_data(session)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Зупинено користувачем")
