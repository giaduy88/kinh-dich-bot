import time
import pandas as pd
import requests
import re
import os
import io
from datetime import datetime, timezone, timedelta

# --- THƯ VIỆN ---
try:
    import ccxt
    from lunardate import LunarDate
except ImportError:
    pass

# --- HÀM TẢI DỮ LIỆU ---
def get_historical_data(symbol, days):
    to_ts = int(time.time())
    from_ts = to_ts - (days * 24 * 3600)
    
    # 1. XỬ LÝ CRYPTO
    if "/USDT" in symbol.upper() or "USDT" in symbol.upper():
        try:
            # Chuẩn hóa mã (VD: BTCUSDT -> BTC/USDT)
            sym_map = symbol.upper()
            if "USDT" in sym_map and "/" not in sym_map:
                sym_map = sym_map.replace("USDT", "/USDT")
            elif "/USDT" not in sym_map: # Trường hợp gõ tắt BTC
                sym_map += "/USDT"

            ex = ccxt.kucoin() # Dùng KuCoin để né chặn IP
            ohlcv = ex.fetch_ohlcv(sym_map, '1h', limit=min(days*24, 1000))
            data = []
            for c in ohlcv:
                data.append({
                    "t": datetime.fromtimestamp(c[0]/1000, tz=timezone(timedelta(hours=7))),
                    "p": float(c[4])
                })
            return data, "OK", "CRYPTO"
        except Exception as e:
            return [], f"Lỗi Crypto: {str(e)}", "ERROR"

    # 2. XỬ LÝ CHỨNG KHOÁN
    else:
        try:
            url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/stock?symbol={symbol}&resolution=1H&from={from_ts}&to={to_ts}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).json()
            data = []
            if 't' in res and res['t']:
                for i in range(len(res['t'])):
                    data.append({
                        "t": datetime.fromtimestamp(res['t'][i], tz=timezone(timedelta(hours=7))),
                        "p": float(res['c'][i])
                    })
            return data, "OK", "STOCK"
        except Exception as e:
            return [], f"Lỗi Stock: {str(e)}", "ERROR"

# --- INDICATORS & LOGIC ---
def add_indicators(df):
    if df.empty: return df
    df['SMA20'] = df['p'].rolling(window=20).mean()
    delta = df['p'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    return df.fillna(0)

king_wen_matrix = [[1, 10, 13, 25, 44, 6, 33, 12], [43, 58, 49, 17, 28, 47, 31, 45], [14, 38, 30, 21, 50, 64, 56, 35], [34, 54, 55, 51, 32, 40, 62, 16], [9, 61, 37, 42, 57, 59, 53, 20], [5, 60, 63, 3, 48, 29, 39, 8], [26, 41, 22, 27, 18, 4, 52, 23], [11, 19, 36, 24, 46, 7, 15, 2]]

def calculate_hexagram(dt):
    if dt.hour == 23: dt_l = dt + timedelta(days=1)
    else: dt_l = dt
    lunar = LunarDate.fromSolarDate(dt_l.year, dt_l.month, dt_l.day)
    chi = 1 if dt.hour==23 or dt.hour==0 else ((dt.hour+1)//2 + 1 if dt.hour%2!=0 else dt.hour//2 + 1)
    base = ((lunar.year - 1984)%12 + 1) + lunar.month + lunar.day
    thuong, ha = base%8 or 8, (base+chi)%8 or 8
    hao = (base+chi)%6 or 6
    id_goc = king_wen_matrix[thuong-1][ha-1]
    is_upper, line = hao>3, hao-3 if hao>3 else hao
    target = thuong if is_upper else ha
    trans = {1:{1:5,2:3,3:2},2:{1:6,2:4,3:1},3:{1:7,2:1,3:4},4:{1:8,2:2,3:3},5:{1:1,2:7,3:6},6:{1:2,2:8,3:5},7:{1:3,2:5,3:8},8:{1:4,2:6,3:7}}
    new_trig = trans[target][line]
    new_thuong, new_ha = (new_trig, thuong) if is_upper else (thuong, new_trig)
    return f"G{id_goc}-B{king_wen_matrix[new_thuong-1][new_ha-1]}"

def analyze_smart_action(text):
    if not isinstance(text, str) or not text: return "GIỮ", 0.0
    text = text.lower()
    avoid = ['đứng ngoài', 'quan sát', 'không nên mua', 'rút lui', 'chờ đợi', 'thận trọng']
    if any(w in text for w in avoid): return "GIỮ", 0.0
    strong_buy = ['đại cát', 'lợi lớn', 'bay cao', 'thời cơ vàng', 'mua ngay', 'tất tay', 'all-in']
    if any(w in text for w in strong_buy): return "MUA", 1.0 
    strong_sell = ['nguy hiểm', 'sập', 'tháo chạy', 'bán tháo', 'tuyệt vọng', 'cắt lỗ ngay']
    if any(w in text for w in strong_sell): return "BÁN", 1.0
    normal_buy = ['mua', 'tốt', 'lãi', 'tích lũy', 'hanh thông', 'tăng', 'nên mua']
    if any(w in text for w in normal_buy): return "MUA", 0.5
    normal_sell = ['bán', 'xấu', 'lỗ', 'giảm', 'trở ngại', 'hạ tỷ trọng', 'nên bán']
    if any(w in text for w in normal_sell): return "BÁN", 0.5
    return "GIỮ", 0.0

def run_backtest_core(symbol, days, advice_map):
    raw_data, msg, asset_type = get_historical_data(symbol, days)
    if not raw_data:
        return f"❌ Lỗi tải dữ liệu {symbol}: {msg}"

    df = pd.DataFrame(raw_data)
    df = add_indicators(df)
    data = df.to_dict('records')
    
    if len(data) < 20:
        return f"⚠️ Dữ liệu quá ít ({len(data)} nến) để backtest."

    # [FIX] ĐIỀU CHỈNH VỐN THEO LOẠI TÀI SẢN
    if asset_type == "CRYPTO":
        capital = 5_000 # 5000 USD
        currency = "$"
        min_order = 100 # Min order 100$
    else:
        capital = 100_000_000 # 100 Triệu VND
        currency = "đ"
        min_order = 5_000_000 # Min order 5tr

    cash, stock, avg_price = capital, 0, 0
    trade_count, win_count, loss_count = 0, 0, 0
    stop_loss_pct, take_profit_pct = -0.07, 0.15
    
    for item in data:
        dt, price = item['t'], item['p']
        sma20, rsi = item.get('SMA20', 0), item.get('RSI', 50)
        
        holding_pnl = (price - avg_price) / avg_price if (stock > 0 and avg_price > 0) else 0

        key = calculate_hexagram(dt)
        advice = advice_map.get(key, "")
        action, percent = analyze_smart_action(advice)
        
        risk_action = None
        if stock > 0:
            if holding_pnl <= stop_loss_pct: risk_action = "STOP_LOSS"
            elif holding_pnl >= take_profit_pct: risk_action = "TAKE_PROFIT"

        if action == "MUA":
            if price < sma20 and rsi > 35: action = "GIỮ"
            if rsi > 75: action = "GIỮ"

        final_action, final_percent = action, percent
        if risk_action == "STOP_LOSS": final_action, final_percent = "BÁN", 1.0
        elif risk_action == "TAKE_PROFIT": final_action, final_percent = "BÁN", 0.5

        if final_action == "MUA":
            amt = cash * final_percent
            if amt > min_order:
                qty = amt / price
                if asset_type == "STOCK": qty = int(qty // 100) * 100
                if qty > 0:
                    current_val, new_val = stock * avg_price, qty * price
                    stock += qty
                    avg_price = (current_val + new_val) / stock
                    cash -= qty * price

        elif final_action == "BÁN":
            qty = stock * final_percent
            if asset_type == "STOCK": qty = int(qty // 100) * 100
            if qty > stock: qty = stock
            
            if qty > 0:
                stock -= qty
                cash += qty * price
                trade_pnl = (price - avg_price) * qty
                if trade_pnl > 0: win_count += 1
                elif trade_pnl < 0: loss_count += 1
                trade_count += 1
                if stock == 0: avg_price = 0

    final_equity = cash + (stock * data[-1]['p'])
    roi = (final_equity - capital) / capital
    win_rate = (win_count / trade_count) if trade_count > 0 else 0
    
    # Format số tiền cho đẹp
    def fmt_money(val):
        if asset_type == "CRYPTO": return f"{val:,.2f}"
        return f"{val/1e6:,.1f} tr"

    report = (
        f"📊 <b>KẾT QUẢ BACKTEST: {symbol.upper()}</b>\n"
        f"⏳ Thời gian: {days} ngày qua\n"
        f"🕯 Số nến: {len(data)}\n"
        f"--------------------------\n"
        f"💰 Vốn đầu: {currency}{fmt_money(capital)}\n"
        f"💎 Vốn cuối: {currency}{fmt_money(final_equity)}\n"
        f"🚀 <b>ROI: {roi:+.2%}</b>\n"
        f"--------------------------\n"
        f"🛒 Tổng lệnh: {trade_count}\n"
        f"✅ Thắng: {win_count} | ❌ Thua: {loss_count}\n"
        f"🎯 Win Rate: {win_rate:.1%}"
    )
    return report
