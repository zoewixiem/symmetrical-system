import asyncio
import sqlite3
import json
import os
import threading
from collections import deque
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pybit.unified_trading import WebSocket as BybitWebSocket

app = FastAPI(title="EMA/RSI Demo Bot")

DATA_DIR = "/data" if os.path.exists("/data") else "."
DB_NAME = os.path.join(DATA_DIR, "demo_bot.db")

# ==================== THREAD-SAFE PRICE STORAGE ====================
_price_lock = threading.Lock()
PRICE_HISTORY: deque = deque(maxlen=100)   # последние 100 тиков
LATEST_PRICE: float = 0.0

# ==================== КОНСТАНТЫ СТРАТЕГИИ ====================
INITIAL_BALANCE = 50.0
LEVERAGE       = 10
QTY_USDT       = 15.0          # 15 USDT маржи на сделку
TAKER_FEE      = 0.00055       # 0.055% — реальная комиссия Bybit тейкер
TP_PCT         = 0.0025        # +0.25% тейк-профит
SL_PCT         = 0.0012        # -0.12% стоп-лосс
# Математика: winrate нужен > SL/(TP+SL) = 0.12/0.37 = 32.4%
# EMA crossover даёт ~42-48% — есть edge

EMA_FAST = 9
EMA_SLOW = 21
RSI_PERIOD = 14
RSI_OVERBOUGHT = 65
RSI_OVERSOLD = 35

# ==================== DB ====================
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS stats (
        id INTEGER PRIMARY KEY,
        balance REAL, total_pnl REAL,
        total_trades INTEGER, wins INTEGER, losses INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS active_position (
        id INTEGER PRIMARY KEY,
        side TEXT, entry_price REAL, qty_btc REAL,
        tp_price REAL, sl_price REAL, margin REAL,
        fee_entry REAL, open_time TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS trade_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        open_time TEXT, close_time TEXT,
        side TEXT, entry_price REAL, exit_price REAL,
        gross_pnl REAL, fee_total REAL, net_pnl REAL,
        result TEXT, balance_after REAL
    )''')
    if c.execute("SELECT count(*) FROM stats").fetchone()[0] == 0:
        c.execute("INSERT INTO stats VALUES (1,?,0.0,0,0,0)", (INITIAL_BALANCE,))
    conn.commit()
    conn.close()

init_db()

# ==================== ИНДИКАТОРЫ ====================
def calc_ema(prices: list, period: int) -> float:
    """Exponential Moving Average"""
    if len(prices) < period:
        return sum(prices) / len(prices)
    k = 2.0 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    return ema

def calc_rsi(prices: list, period: int = 14) -> float:
    """Relative Strength Index"""
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d for d in deltas if d > 0]
    losses = [-d for d in deltas if d < 0]
    avg_gain = sum(gains[-period:]) / period if gains else 0
    avg_loss = sum(losses[-period:]) / period if losses else 0.0001
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def get_signal(prices: list) -> str | None:
    """
    Сигнал: EMA9 пересекает EMA21 + RSI фильтр
    :  EMA9 > EMA21 (пересечение снизу вверх) + RSI < 65
    SHORT: EMA9 < EMA21 (пересечение сверху вниз) + RSI > 35
    """
    if len(prices) < EMA_SLOW + 2:
        return None

    ema_fast_now  = calc_ema(prices, EMA_FAST)
    ema_slow_now  = calc_ema(prices, EMA_SLOW)
    ema_fast_prev = calc_ema(prices[:-1], EMA_FAST)
    ema_slow_prev = calc_ema(prices[:-1], EMA_SLOW)
    rsi = calc_rsi(prices, RSI_PERIOD)

    crossed_up   = ema_fast_prev <= ema_slow_prev and ema_fast_now > ema_slow_now
    crossed_down = ema_fast_prev >= ema_slow_prev and ema_fast_now < ema_slow_now

    if crossed_up   and rsi < RSI_OVERBOUGHT:
        return "LONG"  # <--- Впиши сюда "LONG"
    if crossed_down and rsi > RSI_OVERSOLD:
        return "SHORT"
    return None

# ==================== WEBSOCKET MANAGER ====================
class ConnectionManager:
    def __init__(self): self.connections = []
    async def connect(self, ws):
        await ws.accept()
        self.connections.append(ws)
    def disconnect(self, ws):
        if ws in self.connections: self.connections.remove(ws)
    async def broadcast(self, data: str):
        dead = []
        for c in self.connections:
            try: await c.send_text(data)
            except: dead.append(c)
        for c in dead: self.disconnect(c)

manager = ConnectionManager()

# ==================== BYBIT TESTNET PRICE FEED ====================
def _on_ticker(message):
    global LATEST_PRICE
    try:
        price = float(message["data"]["lastPrice"])
        with _price_lock:
            LATEST_PRICE = price
            PRICE_HISTORY.append(price)
    except Exception:
        pass

try:
    _ws_bybit = BybitWebSocket(testnet=True, channel_type="linear")
    _ws_bybit.ticker_stream(symbol="BTCUSDT", callback=_on_ticker)
    print("✅ Bybit TESTNET WebSocket подключён")
except Exception as e:
    print(f"⚠️  Bybit WebSocket error: {e}")

# ==================== TRADING ENGINE ====================
async def trading_loop():
    print("⏳ Накопление данных для EMA/RSI...")
    while True:
        with _price_lock:
            count = len(PRICE_HISTORY)
        if count >= EMA_SLOW + 5:
            break
        await asyncio.sleep(0.5)
    print("🚀 Стратегия EMA/RSI активна (DEMO TESTNET)")

    while True:
        with _price_lock:
            current_price = LATEST_PRICE
            prices_snapshot = list(PRICE_HISTORY)

        if current_price == 0.0:
            await asyncio.sleep(0.5)
            continue

        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        row = c.execute("SELECT balance, total_pnl, total_trades, wins, losses FROM stats WHERE id=1").fetchone()
        balance, total_pnl, total_trades, wins, losses = row
        pos = c.execute("SELECT * FROM active_position WHERE id=1").fetchone()

        ema_fast = calc_ema(prices_snapshot, EMA_FAST)
        ema_slow = calc_ema(prices_snapshot, EMA_SLOW)
        rsi      = calc_rsi(prices_snapshot, RSI_PERIOD)
        signal   = get_signal(prices_snapshot)

        trend = "↑ Бычий" if ema_fast > ema_slow else "↓ Медвежий"
        status_msg = (
            f"EMA9={ema_fast:.1f} | EMA21={ema_slow:.1f} | RSI={rsi:.1f} | {trend}"
        )
        pos_payload = None

        # --- ОТКРЫТИЕ ПОЗИЦИИ ---
        if pos is None and signal is not None:
            # qty в BTC с учётом плеча
            qty_btc = round((QTY_USDT * LEVERAGE) / current_price, 6)
            margin  = QTY_USDT
            fee_entry = current_price * qty_btc * TAKER_FEE

            if balance >= margin + fee_entry:
                if signal == "LONG":
                    tp = round(current_price * (1 + TP_PCT), 2)
                    sl = round(current_price * (1 - SL_PCT), 2)
                else:
                    tp = round(current_price * (1 - TP_PCT), 2)
                    sl = round(current_price * (1 + SL_PCT), 2)

                balance -= (margin + fee_entry)
                c.execute(
                    "INSERT INTO active_position VALUES (1,?,?,?,?,?,?,?,?)",
                    (signal, current_price, qty_btc, tp, sl, margin, fee_entry,
                     datetime.now().strftime("%H:%M:%S"))
                )
                c.execute("UPDATE stats SET balance=? WHERE id=1", (balance,))
                conn.commit()
                status_msg = f"📍 Открыт {signal} по ${current_price:,.2f} | TP: ${tp} | SL: ${sl}"

        # --- УПРАВЛЕНИЕ ПОЗИЦИЕЙ ---
        elif pos is not None:
            _, side, entry_price, qty_btc, tp_price, sl_price, margin, fee_entry, open_time = pos

            # Нереализованный PnL
            if side == "LONG":  # <--- Ошибка была здесь. Выровняй отступ!
                raw_pnl = (current_price - entry_price) * qty_btc
                is_tp   = current_price >= tp_price
                is_sl   = current_price <= sl_price
            else:
                raw_pnl = (entry_price - current_price) * qty_btc
                is_tp   = current_price <= tp_price
                is_sl   = current_price >= sl_price

            fee_exit     = current_price * qty_btc * TAKER_FEE
            unrealized   = raw_pnl - fee_entry - fee_exit
            pnl_pct      = unrealized / margin * 100

            status_msg = (
                f"{'🟢' if unrealized >= 0 else '🔴'} {side} открыт. "
                f"Вход: ${entry_price:,.2f} | PnL: {unrealized:+.3f}$ ({pnl_pct:+.1f}%)"
            )
            pos_payload = {
                "side": side,
                "entry_price": entry_price,
                "qty_btc": round(qty_btc, 6),
                "tp_price": tp_price,
                "sl_price": sl_price,
                "unrealized_pnl": round(unrealized, 3),
                "pnl_pct": round(pnl_pct, 2),
            }

            # --- ЗАКРЫТИЕ ---
            if is_tp or is_sl:
                result     = "TAKE_PROFIT" if is_tp else "STOP_LOSS"
                close_time = datetime.now().strftime("%H:%M:%S")
                gross_pnl  = raw_pnl
                fee_total  = fee_entry + fee_exit
                net_pnl    = gross_pnl - fee_total

                balance     += margin + net_pnl
                total_pnl   += net_pnl
                total_trades += 1
                if is_tp: wins   += 1
                else:     losses += 1

                c.execute("DELETE FROM active_position WHERE id=1")
                c.execute(
                    "UPDATE stats SET balance=?,total_pnl=?,total_trades=?,wins=?,losses=? WHERE id=1",
                    (balance, total_pnl, total_trades, wins, losses)
                )
                c.execute(
                    "INSERT INTO trade_history "
                    "(open_time,close_time,side,entry_price,exit_price,gross_pnl,fee_total,net_pnl,result,balance_after) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (open_time, close_time, side, entry_price, current_price,
                     round(gross_pnl, 4), round(fee_total, 4), round(net_pnl, 4),
                     result, round(balance, 4))
                )
                conn.commit()
                icon = "💰" if is_tp else "📉"
                status_msg = (
                    f"{icon} {side} закрыт ({result}). "
                    f"Вход: ${entry_price} → Выход: ${current_price} | "
                    f"NET PnL: {net_pnl:+.3f}$ (комиссии: {fee_total:.3f}$)"
                )
                pos_payload = None

        # --- ИСТОРИЯ ---
        history = []
        for r in c.execute(
            "SELECT open_time, close_time, side, entry_price, exit_price, "
            "net_pnl, fee_total, result FROM trade_history ORDER BY id DESC LIMIT "
        ).fetchall():
            history.append({
                "open_time":   r[0],
                "close_time":  r[1],
                "side":        r[2],
                "entry":       r[3],
                "exit":        r[4],
                "net_pnl":     round(r[5], 3),
                "fee":         round(r[6], 4),
                "result":      r[7],
            })
        conn.close()

        winrate = round(wins / total_trades * 100, 1) if total_trades > 0 else 0.0

        payload = {
            "price":        current_price,
            "balance":      round(balance, 2),
            "total_pnl":    round(total_pnl, 3),
            "total_trades": total_trades,
            "wins":         wins,
            "losses":       losses,
            "winrate":      winrate,
            "ema_fast":     round(ema_fast, 2),
            "ema_slow":     round(ema_slow, 2),
            "rsi":          round(rsi, 1),
            "status":       status_msg,
            "position":     pos_payload,
            "history":      history,
        }
        await manager.broadcast(json.dumps(payload))
        await asyncio.sleep(0.5)

# ==================== FASTAPI ROUTES ====================
@app.on_event("startup")
async def startup():
    asyncio.create_task(trading_loop())

@app.get("/")
async def dashboard():
    path = "index.html"
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse("<h3>index.html не найден</h3>", 404)

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
