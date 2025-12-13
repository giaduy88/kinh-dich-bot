import time
import pandas as pd
import requests
import re
import os
import io
from datetime import datetime, timezone, timedelta

# --- CẤU HÌNH MẶC ĐỊNH ---
# (Các giá trị này sẽ bị ghi đè khi gọi từ Telegram)
DEFAULT_SYMBOL = "HPG"
DEFAULT_DAYS = 180

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
    
    # Tự động nhận diện Crypto (có chứa USDT hoặc ký tự /)
    is_crypto = "USDT" in symbol.upper() or "/" in symbol
    
    data = []
    if is_crypto:
        try:
            # Dùng CCXT lấy dữ liệu Crypto
            symbol_map = symbol.upper().replace("USDT", "/USDT") if "/" not in symbol else symbol
            ex = ccxt.binance() # Hoặc kucoin
            # Lấy nến 1h. Limit tối đa của API thường là 500-1000 nến
            ohlcv = ex.fetch_ohlcv(symbol_map, '1h', limit=min(days*24, 1000))
            for c in ohlcv:
                data.append({
                    "t": datetime.fromtimestamp(c[0]/1000, tz=timezone(timedelta(hours=7))),
                    "p": float(c[4])
                })
        except Exception as e:
            return [], f"Lỗi Crypto: {str(e)}"
    else:
        # Dùng API DNSE cho Stock
        try:
            url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/stock?symbol={symbol}&resolution=1H&from={from_ts}&to={to_ts}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).json()
            if 't' in res and res['t']:
                for i in range(len(res['t'])):
                    data.append({
                        "t": datetime.fromtimestamp(res['t'][i], tz=timezone(timedelta(hours=7))),
                        "p": float(res['c'][i])
                    })
        except Exception as e:
            return [], f"Lỗi Stock: {str(e)}"
            
    return data, "OK"

# --- CÁC HÀM LOGIC (Dùng chung logic với main.py) ---
def add_indicators(df):
    if df.empty: return df
    df['SMA20'] = df['p'].rolling(window=20).mean()
    delta = df['p'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    return df.fillna(0)

# Ma trận King Wen (Kinh Dịch)
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

# --- CORE BACKTEST FUNCTION (Được gọi từ main.py) ---
def run_backtest_core(symbol, days, advice_map):
    raw_data, msg = get_historical_data(symbol, days)
    if not raw_data:
        return f"❌ Lỗi tải dữ liệu {symbol}: {msg}"

    df = pd.DataFrame(raw_data)
    df = add_indicators(df)
    data = df.to_dict('records')
    
    if len(data) < 20:
        return f"⚠️ Dữ liệu quá ít ({len(data)} nến) để backtest."

    # Init Portfolio
    capital = 100_000_000
    cash, stock, avg_price = capital, 0, 0
    trade_count, win_count, loss_count = 0, 0, 0
    stop_loss_pct, take_profit_pct = -0.07, 0.15
    
    history_log = []

    for item in data:
        dt, price = item['t'], item['p']
        sma20, rsi = item.get('SMA20', 0), item.get('RSI', 50)
        
        # PnL Check
        holding_pnl = (price - avg_price) / avg_price if (stock > 0 and avg_price > 0) else 0

        # Logic Kinh Dịch
        key = calculate_hexagram(dt)
        advice = advice_map.get(key, "")
        action, percent = analyze_smart_action(advice)
        
        # Risk Management
        risk_action = None
        if stock > 0:
            if holding_pnl <= stop_loss_pct: risk_action = "STOP_LOSS"
            elif holding_pnl >= take_profit_pct: risk_action = "TAKE_PROFIT"

        # Technical Filter
        if action == "MUA":
            if price < sma20 and rsi > 35: action = "GIỮ"
            if rsi > 75: action = "GIỮ"

        # Final Decision
        final_action, final_percent = action, percent
        if risk_action == "STOP_LOSS": final_action, final_percent = "BÁN", 1.0
        elif risk_action == "TAKE_PROFIT": final_action, final_percent = "BÁN", 0.5

        # Execution
        executed = False
        pnl_realized = 0
        type_str = ""

        if final_action == "MUA":
            amt = cash * final_percent
            if amt > 50000: # Min order
                qty = amt / price
                # Làm tròn cổ phiếu (lô 100) nếu không phải Crypto
                if "USDT" not in symbol.upper() and "/" not in symbol:
                    qty = int(qty // 100) * 100
                
                if qty > 0:
                    current_val = stock * avg_price
                    new_val = qty * price
                    stock += qty
                    avg_price = (current_val + new_val) / stock
                    cash -= qty * price
                    executed = True
                    type_str = "MUA"

        elif final_action == "BÁN":
            qty = stock * final_percent
            if "USDT" not in symbol.upper() and "/" not in symbol:
                qty = int(qty // 100) * 100
                if qty > stock: qty = stock
            
            if qty > 0:
                stock -= qty
                cash += qty * price
                executed = True
                type_str = risk_action if risk_action else "BÁN"
                
                # Check Win/Loss
                trade_pnl = (price - avg_price) * qty
                if trade_pnl > 0: win_count += 1
                elif trade_pnl < 0: loss_count += 1
                trade_count += 1
                
                if stock == 0: avg_price = 0

    # Summary
    final_equity = cash + (stock * data[-1]['p'])
    roi = (final_equity - capital) / capital
    win_rate = (win_count / trade_count) if trade_count > 0 else 0
    
    report = (
        f"📊 <b>KẾT QUẢ BACKTEST: {symbol}</b>\n"
        f"⏳ Thời gian: {days} ngày qua\n"
        f"🕯 Số nến: {len(data)}\n"
        f"--------------------------\n"
        f"💰 Vốn đầu: {capital/1e6:.0f} tr\n"
        f"💎 Vốn cuối: {final_equity/1e6:.1f} tr\n"
        f"🚀 <b>ROI: {roi:+.2%}</b>\n"
        f"--------------------------\n"
        f"🛒 Tổng lệnh: {trade_count}\n"
        f"✅ Thắng: {win_count} | ❌ Thua: {loss_count}\n"
        f"🎯 Win Rate: {win_rate:.1%}"
    )
    return report

# --- MAIN BLOCK (Để test offline) ---
if __name__ == "__main__":
    # Mock data để test file này chạy độc lập
    print(run_backtest_core("HPG", 180, {}))
