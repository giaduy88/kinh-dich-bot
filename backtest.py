import time
import pandas as pd
import requests
import re
import os
from datetime import datetime, timezone, timedelta

# --- THƯ VIỆN ---
try:
    import ccxt
    from lunardate import LunarDate
except ImportError:
    pass

# --- HÀM TẢI DỮ LIỆU (LOOP FETCHING) ---
def get_historical_data(symbol, days):
    end_ts = int(time.time()) * 1000 
    start_ts = end_ts - (days * 24 * 3600 * 1000)
    
    # 1. XỬ LÝ CRYPTO
    if "/USDT" in symbol.upper() or "USDT" in symbol.upper():
        try:
            sym_map = symbol.upper()
            if "USDT" in sym_map and "/" not in sym_map: sym_map = sym_map.replace("USDT", "/USDT")
            elif "/USDT" not in sym_map: sym_map += "/USDT"

            ex = ccxt.kucoin() 
            all_ohlcv = []
            current_since = start_ts
            
            while current_since < end_ts:
                try:
                    ohlcv = ex.fetch_ohlcv(sym_map, '1h', since=current_since, limit=1000)
                except Exception as e:
                    return [], f"Lỗi sàn Crypto: {str(e)}", "ERROR"

                if not ohlcv: break 
                
                start_candle = ohlcv[0][0]
                last_candle = ohlcv[-1][0]
                
                if len(all_ohlcv) > 0 and start_candle <= all_ohlcv[-1]['ts_raw']: break
                
                for c in ohlcv:
                    if c[0] >= start_ts and c[0] <= end_ts:
                         all_ohlcv.append({
                            "ts_raw": c[0],
                            "t": datetime.fromtimestamp(c[0]/1000, tz=timezone(timedelta(hours=7))),
                            "p": float(c[4])
                        })
                
                current_since = last_candle + (60 * 60 * 1000)
                time.sleep(0.1)

            if not all_ohlcv: return [], "Không tìm thấy dữ liệu Crypto.", "ERROR"
            return all_ohlcv, "OK", "CRYPTO"
            
        except Exception as e:
            return [], f"Lỗi hệ thống Crypto: {str(e)}", "ERROR"

    # 2. XỬ LÝ CHỨNG KHOÁN
    else:
        try:
            to_ts_sec = int(time.time())
            from_ts_sec = to_ts_sec - (days * 24 * 3600)
            
            url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/stock?symbol={symbol}&resolution=1H&from={from_ts_sec}&to={to_ts_sec}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10).json()
            
            data = []
            if 't' in res and res['t']:
                for i in range(len(res['t'])):
                    data.append({
                        "t": datetime.fromtimestamp(res['t'][i], tz=timezone(timedelta(hours=7))),
                        "p": float(res['c'][i])
                    })
            else: return [], "Không tìm thấy dữ liệu Stock.", "ERROR"
            return data, "OK", "STOCK"
        except Exception as e:
            return [], f"Lỗi kết nối Stock: {str(e)}", "ERROR"

# --- LOGIC ---
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
    new_trig = {1:{1:5,2:3,3:2},2:{1:6,2:4,3:1},3:{1:7,2:1,3:4},4:{1:8,2:2,3:3},5:{1:1,2:7,3:6},6:{1:2,2:8,3:5},7:{1:3,2:5,3:8},8:{1:4,2:6,3:7}}[target][line]
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
    try:
        raw_data, msg, asset_type = get_historical_data(symbol, days)
        if not raw_data: return f"❌ <b>Backtest Thất Bại</b>\nLý do: {msg}"

        df = pd.DataFrame(raw_data)
        df = add_indicators(df)
        data = df.to_dict('records')
        
        if len(data) < 20: return f"❌ <b>Dữ liệu quá ít</b>\nChỉ tìm thấy {len(data)} nến."

        if asset_type == "CRYPTO": capital, currency, min_order = 5000, "$", 100
        else: capital, currency, min_order = 100_000_000, "đ", 5_000_000

        cash, stock, avg_price = capital, 0, 0
        trade_count, win_count, loss_count = 0, 0, 0
        
        for item in data:
            dt, price = item['t'], item['p']
            sma20, rsi = item.get('SMA20', 0), item.get('RSI', 50)
            holding_pnl = (price - avg_price) / avg_price if (stock > 0 and avg_price > 0) else 0

            key = calculate_hexagram(dt)
            advice = advice_map.get(key, "")
            action, percent = analyze_smart_action(advice)
            
            risk_action = None
            if stock > 0:
                if holding_pnl <= -0.07: risk_action = "STOP_LOSS"
                elif holding_pnl >= 0.15: risk_action = "TAKE_PROFIT"

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
        net_profit = final_equity - capital
        roi = net_profit / capital
        win_rate = (win_count / trade_count) if trade_count > 0 else 0
        
        def fmt(v): return f"{v:,.2f}" if asset_type == "CRYPTO" else f"{v/1e6:,.1f} tr"

        # [UPDATE] BÁO CÁO CHI TIẾT ĐẦY ĐỦ
        return (
            f"📊 <b>KẾT QUẢ BACKTEST CHI TIẾT</b>\n"
            f"--------------------------\n"
            f"🔠 <b>Mã:</b> {symbol.upper()}\n"
            f"⏳ <b>Thời gian:</b> {days} ngày\n"
            f"🕯 <b>Dữ liệu:</b> {len(data)} nến\n"
            f"--------------------------\n"
            f"💰 <b>Vốn ban đầu:</b> {currency} {fmt(capital)}\n"
            f"💎 <b>Vốn kết thúc:</b> {currency} {fmt(final_equity)}\n"
            f"💵 <b>Lợi nhuận ròng:</b> {currency} {fmt(net_profit)}\n"
            f"🚀 <b>ROI: {roi:+.2%}</b>\n"
            f"--------------------------\n"
            f"🛒 <b>Tổng số lệnh:</b> {trade_count}\n"
            f"✅ <b>Lệnh Thắng:</b> {win_count}\n"
            f"❌ <b>Lệnh Thua:</b> {loss_count}\n"
            f"🎯 <b>Tỷ lệ Thắng (Winrate):</b> {win_rate:.1%}"
        )
    except Exception as e:
        return f"❌ <b>Lỗi Backtest</b>: {str(e)}"
