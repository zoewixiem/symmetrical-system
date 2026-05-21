import asyncio
import sqlite3
import json
import os
import time
from collections import deque
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pybit.unified_trading import WebSocket as BybitWebSocket

app = FastAPI(title="EMA/RSI Smart Maker Demo")

DATA_DIR = "/data" if os.path.exists("/data") else "."
DB_NAME = os.path.join(DATA_DIR, "demo_bot.db")

# ==================== PRICE STORAGE ====================
PRICE_HISTORY: deque = deque(maxlen=100)
LATEST_PRICE: float = 0.0

# ==================== КОНСТАНТЫ СТРАТЕГИИ ====================
INITIAL_BALANCE = 50.0
LEVERAGE       = 10
QTY_USDT       = 15.0          # 15 USDT маржи на сделку
FEE_MAKER      = 0.00020       # 0.02% — комиссия лимитного ордера (Вход и TP)
FEE_TAKER      = 0.00055       # 0.055% — комиссия рыночного ордера (SL)
TP_PCT         = 0.0025        # +0.25% тейк-профит
SL_PCT         = 0.0012        # -0.12% стоп-лосс

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
        id INTEGER PRIMARY KEY, balance REAL, total_pnl REAL,
        total_trades INTEGER, wins INTEGER, losses INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS active_position (
        id INTEGER PRIMARY KEY, side TEXT, entry_price REAL, qty_btc REAL,
        tp_price REAL, sl_price REAL, margin REAL, fee_entry REAL, open_time TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS trade_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, open_time TEXT, close_time TEXT,
        side TEXT, entry_price REAL, exit_price REAL, gross_pnl REAL, 
        fee_total REAL, net_pnl REAL, result TEXT, balance_after REAL
    )''')
    if c.execute("SELECT count(*) FROM stats").fetchone()[0] == 0:
        c.execute("INSERT INTO stats VALUES (1,?,0.0,0,0,0)", (INITIAL_BALANCE,))
    conn.commit()
    conn.close()

init_db()

# ==================== ИНДИКАТОРЫ ====================
def calc_ema(prices: list, period: int) -> float:
    if len(prices) < period: return sum(prices) / len(prices)
    k = 2.0 / (period + 1)
    ema = prices[0]
    for p in prices[1:]: ema = p * k + ema * (1 - k)
    return ema

def calc_rsi(prices: list, period: int = 14) -> float:
    if len(prices) < period + 1: return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d for d in deltas if d > 0]
    losses = [-d for d in deltas if d < 0]
    avg_gain = sum(gains[-period:]) / period if gains else 0
    avg_loss = sum(losses[-period:]) / period if losses else 0.0001
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def get_signal(prices: list) -> str | None:
    if len(prices) < EMA_SLOW + 2: return None
    ema_fast_now, ema_slow_now = calc_ema(prices, EMA_FAST), calc_ema(prices, EMA_SLOW)
    ema_fast_prev, ema_slow_prev = calc_ema(prices[:-1], EMA_FAST), calc_ema(prices[:-1], EMA_SLOW)
    rsi = calc_rsi(prices, RSI_PERIOD)

    if ema_fast_prev <= ema_slow_prev and ema_fast_now > ema_slow_now and rsi < RSI_OVERBOUGHT:
        return "LONG"
    if ema_fast_prev >= ema_slow_prev and ema_fast_now < ema_slow_now and rsi > RSI_OVERSOLD:
        return "SHORT"
    return None

# ==================== WEBSOCKET MANAGER ====================
class ConnectionManager:
    def __init__(self): self.connections = []
    async def connect(self, ws): await ws.accept(); self.connections.append(ws)
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
        LATEST_PRICE = price
        PRICE_HISTORY.append(price)
    except Exception: pass

try:
    _ws_bybit = BybitWebSocket(testnet=True, channel_type="linear")
    _ws_bybit.ticker_stream(symbol="BTCUSDT", callback=_on_ticker)
    print("✅ Bybit TESTNET WebSocket подключён")
except Exception as e: print(f"⚠️  Bybit WebSocket error: {e}")

# ==================== TRADING ENGINE ====================
async def trading_loop():
    print("⏳ Накопление данных для EMA/RSI...")
    while len(PRICE_HISTORY) < EMA_SLOW + 5: await asyncio.sleep(0.5)
    print("🚀 Умный Maker активен (DEMO TESTNET)")
    
    pending_order = None # Память для висящей лимитки

    while True:
        current_price = LATEST_PRICE
        prices_snapshot = list(PRICE_HISTORY)
        if current_price == 0.0:
            await asyncio.sleep(0.5)
            continue

        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        balance, total_pnl, total_trades, wins, losses = c.execute("SELECT balance, total_pnl, total_trades, wins, losses FROM stats WHERE id=1").fetchone()
        pos = c.execute("SELECT * FROM active_position WHERE id=1").fetchone()

        ema_fast, ema_slow = calc_ema(prices_snapshot, EMA_FAST), calc_ema(prices_snapshot, EMA_SLOW)
        rsi = calc_rsi(prices_snapshot, RSI_PERIOD)
        signal = get_signal(prices_snapshot)

        trend = "↑ Бычий" if ema_fast > ema_slow else "↓ Медвежий"
        status_msg = f"EMA9={ema_fast:.1f} | EMA21={ema_slow:.1f} | RSI={rsi:.1f} | {trend}"
        pos_payload = None

        # --- ЛОГИКА 1: ИЩЕМ СИГНАЛ И СТАВИМ ЛИМИТКУ ---
        if pos is None and pending_order is None and signal is not None:
            qty_btc = round((QTY_USDT * LEVERAGE) / current_price, 6)
            if balance >= QTY_USDT:
                tp = round(current_price * (1 + TP_PCT), 2) if signal == "LONG" else round(current_price * (1 - TP_PCT), 2)
                sl = round(current_price * (1 - SL_PCT), 2) if signal == "LONG" else round(current_price * (1 + SL_PCT), 2)
                
                # Запоминаем ордер в память
                pending_order = {
                    "side": signal, "price": current_price, "qty_btc": qty_btc,
                    "tp": tp, "sl": sl, "margin": QTY_USDT, "timestamp": time.time()
                }
                status_msg = f"⏳ Выставлена ЛИМИТКА {signal} по ${current_price:,.2f}. Ждем касания..."

        # --- ЛОГИКА 2: ЛИМИТКА ВИСИТ В СТАКАНЕ ---
        elif pending_order is not None and pos is None:
            # Проверка тайм-аута (10 секунд)
            if time.time() - pending_order["timestamp"] > 10.0:
                status_msg = f"⏱ Рынок ушел. Лимитка {pending_order['side']} отменена по тайм-ауту."
                pending_order = None
            else:
                # Проверка срабатывания ордера (касание цены)
                filled = False
                if pending_order["side"] == "LONG" and current_price <= pending_order["price"]: filled = True
                elif pending_order["side"] == "SHORT" and current_price >= pending_order["price"]: filled = True
                
                if filled:
                    fee_entry = pending_order["price"] * pending_order["qty_btc"] * FEE_MAKER
                    balance -= (pending_order["margin"] + fee_entry)
                    c.execute(
                        "INSERT INTO active_position VALUES (1,?,?,?,?,?,?,?,?)",
                        (pending_order["side"], pending_order["price"], pending_order["qty_btc"], 
                         pending_order["tp"], pending_order["sl"], pending_order["margin"], 
                         fee_entry, datetime.now().strftime("%H:%M:%S"))
                    )
                    c.execute("UPDATE stats SET balance=? WHERE id=1", (balance,))
                    conn.commit()
                    pos = c.execute("SELECT * FROM active_position WHERE id=1").fetchone()
                    status_msg = f"⚡ Лимитка сработала! Открыт {pending_order['side']} по ${pending_order['price']:,.2f}"
                    pending_order = None
                else:
                    status_msg = f"⏳ Ожидание... Лимитка {pending_order['side']} висит по ${pending_order['price']:,.2f}"

        # --- ЛОГИКА 3: УПРАВЛЕНИЕ ОТКРЫТОЙ ПОЗИЦИЕЙ ---
        if pos is not None:
            _, side, entry_price, qty_btc, tp_price, sl_price, margin, fee_entry, open_time = pos

            if side == "LONG":
                raw_pnl = (current_price - entry_price) * qty_btc
                is_tp, is_sl = current_price >= tp_price, current_price <= sl_price
            else:
                raw_pnl = (entry_price - current_price) * qty_btc
                is_tp, is_sl = current_price <= tp_price, current_price >= sl_price

            # Для расчета текущего PnL на экране берем худший сценарий (Market закрытие)
            unrealized = raw_pnl - fee_entry - (current_price * qty_btc * FEE_TAKER)
            pnl_pct = unrealized / margin * 100

            if pending_order is None: # Не перекрываем сообщение о сработанной лимитке в первую секунду
                status_msg = f"{'🟢' if unrealized >= 0 else '🔴'} {side} в работе. Вход: ${entry_price:,.2f} | PnL: {unrealized:+.3f}$"
            
            pos_payload = {
                "side": side, "entry_price": entry_price, "qty_btc": round(qty_btc, 6),
                "tp_price": tp_price, "sl_price": sl_price,
                "unrealized_pnl": round(unrealized, 3), "pnl_pct": round(pnl_pct, 2),
            }

            # --- ЗАКРЫТИЕ ПОЗИЦИИ ---
            if is_tp or is_sl:
                result = "TAKE_PROFIT" if is_tp else "STOP_LOSS"
                close_time = datetime.now().strftime("%H:%M:%S")
                
                # Тейк-профит закрывается лимиткой (дешево), Стоп-лосс бьет по рынку (дорого)
                fee_exit = current_price * qty_btc * (FEE_MAKER if is_tp else FEE_TAKER)
                net_pnl = raw_pnl - fee_entry - fee_exit

                balance += margin + net_pnl
                total_pnl += net_pnl
                total_trades += 1
                if is_tp: wins += 1
                else: losses += 1

                c.execute("DELETE FROM active_position WHERE id=1")
                c.execute("UPDATE stats SET balance=?,total_pnl=?,total_trades=?,wins=?,losses=? WHERE id=1", (balance, total_pnl, total_trades, wins, losses))
                c.execute(
                    "INSERT INTO trade_history (open_time,close_time,side,entry_price,exit_price,gross_pnl,fee_total,net_pnl,result,balance_after) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (open_time, close_time, side, entry_price, current_price, round(raw_pnl, 4), round(fee_entry + fee_exit, 4), round(net_pnl, 4), result, round(balance, 4))
                )
                conn.commit()
                status_msg = f"{'💰' if is_tp else '📉'} {side} закрыт ({result}). NET PnL: {net_pnl:+.3f}$"
                pos_payload = None

        # --- ИСТОРИЯ ---
        history = [{"open_time": r[0], "close_time": r[1], "side": r[2], "entry": r[3], "exit": r[4], "net_pnl": round(r[5], 3), "fee": round(r[6], 4), "result": r[7]} 
                   for r in c.execute("SELECT open_time, close_time, side, entry_price, exit_price, net_pnl, fee_total, result FROM trade_history ORDER BY id DESC LIMIT 8").fetchall()]
        conn.close()

        payload = {
            "price": current_price, "balance": round(balance, 2), "total_pnl": round(total_pnl, 3),
            "total_trades": total_trades, "wins": wins, "losses": losses, "winrate": round(wins / total_trades * 100, 1) if total_trades > 0 else 0.0,
            "ema_fast": round(ema_fast, 2), "ema_slow": round(ema_slow, 2), "rsi": round(rsi, 1),
            "status": status_msg, "position": pos_payload, "history": history,
        }
        await manager.broadcast(json.dumps(payload))
        await asyncio.sleep(0.5)

@app.on_event("startup")
async def startup(): asyncio.create_task(trading_loop())

@app.get("/")
async def dashboard():
    path = "index.html"
    return HTMLResponse(open(path, "r", encoding="utf-8").read()) if os.path.exists(path) else HTMLResponse("<h3>index.html не найден</h3>", 404)

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect: manager.disconnect(websocket)
