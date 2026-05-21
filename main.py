import asyncio
import sqlite3
import json
import os
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pybit.unified_trading import WebSocket as BybitWebSocket

app = FastAPI(title="HFT Scalper Bot $300 Edition")

# Для Railway настраиваем путь к постоянному диску (Volume), если он подключен в /data
DATA_DIR = "/data" if os.path.exists("/data") else "."
DB_NAME = os.path.join(DATA_DIR, "trading_bot_web.db")
JS_LOG_FILE = os.path.join(DATA_DIR, "bot_logs.js")

LATEST_REAL_PRICE = 0.0
PRICE_HISTORY = []
JS_LOGS_CACHE = []

# ==================== МОДУЛЬ ЗАПИСИ В JS ФАЙЛ ====================
def log_action_to_js(action_type, message):
    global JS_LOGS_CACHE
    log_entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "action": action_type,
        "message": message
    }
    JS_LOGS_CACHE.append(log_entry)
    if len(JS_LOGS_CACHE) > 50:
        JS_LOGS_CACHE.pop(0)
    try:
        with open(JS_LOG_FILE, "w", encoding="utf-8") as f:
            f.write(f"// Лог HFT скальпера от {datetime.now().strftime('%Y-%m-%d')}\n")
            f.write("const botLogData = ")
            f.write(json.dumps(JS_LOGS_CACHE, indent=2, ensure_ascii=False))
            f.write(";\n\nif (typeof module !== 'undefined') { module.exports = botLogData; }")
    except Exception as e:
        print(f"Ошибка записи в JS: {e}")

# ==================== ИНИЦИАЛИЗАЦИЯ БАЗЫ SQL С ТОЧНЫМ УЧЕТОМ ====================
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stats (
            id INTEGER PRIMARY KEY,
            balance REAL,
            pnl REAL,
            total_trades INTEGER,
            total_wins INTEGER,
            total_losses INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS active_position (
            id INTEGER PRIMARY KEY,
            type TEXT, entry_price REAL, qty REAL, tp_price REAL, sl_price REAL, margin_used REAL    
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, type TEXT, entry_price REAL, exit_price REAL, pnl REAL, result TEXT, balance_after REAL
        )
    ''')

    cursor.execute("SELECT count(*) FROM stats")
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            "INSERT INTO stats (id, balance, pnl, total_trades, total_wins, total_losses) VALUES (1, 300.0, 0.0, 0, 0, 0)"
        )
    # ВНИМАНИЕ: Если вы хотите, чтобы при перезапуске на Railway баланс НЕ сбрасывался в 300, 
    # закомментируйте строку ниже. Иначе при каждом деплое база будет обнуляться.
    else:
        cursor.execute("UPDATE stats SET balance = 300.0, pnl = 0.0, total_trades = 0, total_wins = 0, total_losses = 0 WHERE id = 1")

    cursor.execute("DELETE FROM active_position")
    conn.commit()
    conn.close()

    if os.path.exists(JS_LOG_FILE):
        try: os.remove(JS_LOG_FILE)
        except: pass
    log_action_to_js("SYSTEM", "Робот запущен. Баланс: $300.00. Режим: Сверхвысокая частота (HFT)")

init_db()

# ==================== ВЕБ-СОКЕТЫ МЕНЕДЖЕР ====================
class ConnectionManager:
    def __init__(self):
        self.active_connections = []

    async def connect(self, ws):
        await ws.accept()
        self.active_connections.append(ws)

    def disconnect(self, ws):
        if ws in self.active_connections: 
            self.active_connections.remove(ws)

    async def broadcast(self, msg):
        for c in self.active_connections:
            try: await c.send_text(msg)
            except: pass

manager = ConnectionManager()

# ==================== СТРИМ ЦЕН BYBIT ====================
def handle_bybit_ticker(message):
    global LATEST_REAL_PRICE, PRICE_HISTORY
    if "data" in message and "lastPrice" in message["data"]:
        LATEST_REAL_PRICE = float(message["data"]["lastPrice"])
        PRICE_HISTORY.append(LATEST_REAL_PRICE)
        if len(PRICE_HISTORY) > 15:
            PRICE_HISTORY.pop(0)

try:
    ws_bybit = BybitWebSocket(testnet=False, channel_type="linear")
    ws_bybit.ticker_stream(symbol="BTCUSDT", callback=handle_bybit_ticker)
except Exception as e:
    print(f"Ошибка WebSocket Bybit: {e}")

# ==================== ВЫСОКОЧАСТОТНЫЙ ТОРГОВЫЙ ДВИЖОК ====================
async def trading_bot_loop():
    global LATEST_REAL_PRICE, PRICE_HISTORY
    leverage = 20
    qty = 0.025
    HFT_TP_PCT = 0.0003
    HFT_SL_PCT = 0.0003

    print("⏳ Накопление тиков для HFT-анализа...")
    while LATEST_REAL_PRICE == 0.0 or len(PRICE_HISTORY) < 8:
        await asyncio.sleep(0.1)

    while True:
        current_price = LATEST_REAL_PRICE
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        cursor.execute("SELECT balance, pnl, total_trades, total_wins, total_losses FROM stats WHERE id = 1")
        balance, total_pnl, total_trades, total_wins, total_losses = cursor.fetchone()

        cursor.execute("SELECT id, type, entry_price, qty, tp_price, sl_price, margin_used FROM active_position WHERE id = 1")
        position = cursor.fetchone()

        local_history = PRICE_HISTORY[:-1]
        local_max = max(local_history) if local_history else current_price
        local_min = min(local_history) if local_history else current_price

        status_msg = f"HFT Активен. Max: {local_max} | Min: {local_min} | Сделок: {total_trades}"

        if not position:
            margin_required = (current_price * qty) / leverage
            if balance >= margin_required:
                if current_price > local_max:
                    balance -= margin_required
                    tp_price = round(current_price * (1 + HFT_TP_PCT), 2)
                    sl_price = round(current_price * (1 - HFT_SL_PCT), 2)
                    cursor.execute("INSERT INTO active_position VALUES (1, 'LONG', ?, ?, ?, ?, ?)", (current_price, qty, tp_price, sl_price, margin_required))
                    cursor.execute("UPDATE stats SET balance = ? WHERE id = 1", (balance,))
                    conn.commit()
                    log_action_to_js("HFT_BUY", f"🔥 Импульс Вверх! LONG по ${current_price}. Цель: ${tp_price}")
                elif current_price < local_min:
                    balance -= margin_required
                    tp_price = round(current_price * (1 - HFT_TP_PCT), 2)
                    sl_price = round(current_price * (1 + HFT_SL_PCT), 2)
                    cursor.execute("INSERT INTO active_position VALUES (1, 'SHORT', ?, ?, ?, ?, ?)", (current_price, qty, tp_price, sl_price, margin_required))
                    cursor.execute("UPDATE stats SET balance = ? WHERE id = 1", (balance,))
                    conn.commit()
                    log_action_to_js("HFT_SHORT", f"⚡ Импульс Вниз! SHORT по ${current_price}. Цель: ${tp_price}")
        else:
            pos_id, p_type, entry_price, p_qty, tp_price, sl_price, margin_used = position
            if p_type == 'LONG':
                price_diff_pct = (current_price - entry_price) / entry_price
                is_tp = current_price >= tp_price
                is_sl = current_price <= sl_price
            else:
                price_diff_pct = (entry_price - current_price) / entry_price
                is_tp = current_price <= tp_price
                is_sl = current_price >= sl_price

            unrealized_pnl = price_diff_pct * leverage * margin_used
            status_msg = f"Скальпим {p_type}. Текущий результат: {unrealized_pnl:+.2f}$"

            is_close = is_tp or is_sl
            if is_close:
                close_result = "TAKE_PROFIT" if is_tp else "STOP_LOSS"
                total_trades += 1
                if is_tp: total_wins += 1
                else: total_losses += 1
                
                balance += (margin_used + unrealized_pnl)
                total_pnl += unrealized_pnl

                cursor.execute("DELETE FROM active_position WHERE id = 1")
                cursor.execute("UPDATE stats SET balance = ?, pnl = ?, total_trades = ?, total_wins = ?, total_losses = ? WHERE id = 1", (balance, total_pnl, total_trades, total_wins, total_losses))
                cursor.execute("INSERT INTO trade_history (timestamp, type, entry_price, exit_price, pnl, result, balance_after) VALUES (?, ?, ?, ?, ?, ?, ?)",
                               (datetime.now().strftime("%H:%M:%S"), p_type, entry_price, current_price, unrealized_pnl, close_result, balance))
                conn.commit()
                log_action_to_js(close_result, f"{'💰 ПЛЮС' if is_tp else '📉 МИНУС'}! {p_type} закрыт. PnL: {unrealized_pnl:+.2f}$. Баланс: ${balance:.2f}")

        # Подготовка данных для отправки
        cursor.execute("SELECT type, entry_price, tp_price, sl_price FROM active_position WHERE id = 1")
        active_pos_check = cursor.fetchone()
        pos_payload = {"entry_price": f"{active_pos_check[0]} (Вход: {active_pos_check[1]})", "qty": qty, "tp_price": f"${active_pos_check[2]}", "sl_price": f"${active_pos_check[3]}", "unrealized_pnl": 0} if active_pos_check else None

        payload = {
            "price": current_price, "balance": round(balance, 2), "total_pnl": round(total_pnl, 2), "total_trades": total_trades, "status": status_msg, "position": pos_payload,
            "history": [{"time": r[0], "entry": f"{r[1]} ({r[2]})", "exit": r[3], "pnl": round(r[4], 2), "res": f"{'✅' if r[5] == 'TAKE_PROFIT' else '❌'} {r[5]}"} for r in cursor.execute("SELECT timestamp, type, entry_price, exit_price, pnl, result FROM trade_history ORDER BY id DESC LIMIT 6").fetchall()]
        }
        conn.close()
        await manager.broadcast(json.dumps(payload))
        await asyncio.sleep(0.5)

@app.on_event("startup")
async def startup_event(): 
    asyncio.create_task(trading_bot_loop())

@app.get("/")
async def get_dashboard():
    # Читаем файл динамически на каждый запрос, чтобы избежать проблем с путями
    if os.path.exists("index.html"):
        with open("index.html", "r", encoding="utf-8") as f: 
            return HTMLResponse(content=f.read(), status_code=200)
    return HTMLResponse(content="<h3>Файл index.html не найден</h3>", status_code=404)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)