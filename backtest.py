import time
import pandas as pd
import requests
import math
from datetime import datetime, timezone, timedelta

# --- CẤU HÌNH KIỂM THỬ ---
SYMBOL = "HPG"       # Mã muốn test
CAPITAL = 100000000  # Vốn giả lập (100 triệu)
DAYS_BACK = 180      # Test dữ liệu 6 tháng gần nhất
MARKET_TYPE = "Stock" # "Stock" hoặc "Crypto"

# CẤU HÌNH LOGIC (GIỐNG V1.4)
STOP_LOSS_PCT = -0.07
TAKE_PROFIT_PCT = 0.15

# --- THƯ VIỆN ---
try:
    from lunardate import LunarDate
except ImportError:
    print("Cần cài thư viện lunardate")
    pass

# --- 1. DATA FETCHING ---
def get_historical_data(symbol, days):
    print(f"⏳ Đang tải dữ liệu {symbol} trong {days} ngày qua...")
    to_ts = int(time.time())
    from_ts = to_ts - (days * 24 * 3600)
    
    # API DNSE cho chứng khoán
    if MARKET_TYPE == "Stock":
        url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/stock?symbol={symbol}&resolution=1H&from={from_ts}&to={to_ts}"
        try:
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).json()
            data = []
            if 't' in res and res['t']:
                for i in range(len(res['t'])):
                    data.append({
                        "t": datetime.fromtimestamp(res['t'][i], tz=timezone(timedelta(hours=7))),
                        "p": float(res['c'][i])
                    })
            return data
        except: return []
    
    # API Kucoin cho Crypto
    else:
        # (Demo đơn giản cho Crypto - cần ccxt nếu muốn đầy đủ hơn)
        return []

# --- 2. LOGIC BỔ TRỢ ---
def add_indicators(df):
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

# --- 3. GIẢ LẬP LỜI KHUYÊN (MOCK DATA) ---
# Vì backtest không gọi file CSV thật, ta dùng hàm Hash để giả lập ngẫu nhiên "tính tốt xấu" của quẻ
# giúp test logic đi tiền. (Thực tế cần file CSV đầy đủ)
def mock_smart_action(key):
    # Giả lập: Quẻ có ID chẵn là Tốt, Lẻ là Xấu (Để test cơ chế)
    # Trong thực tế: Bạn cần load file data_loi_khuyen.csv vào đây
    num = int(key.split('-')[0][1:]) 
    if num % 5 == 0: return "MUA", 1.0 # Đại cát
    if num % 3 == 0: return "MUA", 0.5 # Tốt
    if num % 7 == 0: return "BÁN", 1.0 # Xấu
    return "GIỮ", 0.0

# --- 4. ENGINE BACKTEST ---
def run_backtest():
    raw_data = get_historical_data(SYMBOL, DAYS_BACK)
    if not raw_data:
        print("❌ Không lấy được dữ liệu.")
        return

    df = pd.DataFrame(raw_data)
    df = add_indicators(df)
    data = df.to_dict('records')

    print(f"✅ Đã tải {len(data)} nến. Bắt đầu chạy giả lập...")
    
    cash = CAPITAL
    stock = 0
    avg_price = 0
    
    trade_count = 0
    win_count = 0
    loss_count = 0
    
    history = []

    for item in data:
        dt, price = item['t'], item['p']
        sma20 = item['SMA20']
        rsi = item['RSI']
        
        # PnL Check
        holding_pnl = (price - avg_price) / avg_price if (stock > 0 and avg_price > 0) else 0

        # Logic Kinh Dịch
        key = calculate_hexagram(dt)
        action, percent = mock_smart_action(key) # Dùng Mock hoặc Load CSV thật
        
        display_label = "GIỮ"

        # RISK MANAGEMENT
        risk_action = None
        if stock > 0:
            if holding_pnl <= STOP_LOSS_PCT: risk_action = "STOP_LOSS"
            elif holding_pnl >= TAKE_PROFIT_PCT: risk_action = "TAKE_PROFIT"

        # TECHNICAL FILTER
        if action == "MUA":
            if price < sma20 and rsi > 35: action = "GIỮ"
            if rsi > 75: action = "GIỮ"

        # FINAL DECISION
        final_action = action
        final_percent = percent

        if risk_action == "STOP_LOSS":
            final_action = "BÁN"; final_percent = 1.0
        elif risk_action == "TAKE_PROFIT":
            final_action = "BÁN"; final_percent = 0.5

        # EXECUTION SIMULATION
        executed = False
        pnl_realized = 0

        if final_action == "MUA":
            amt = cash * final_percent
            if amt > 10000:
                qty = int(amt / price)
                if qty > 0:
                    current_val = stock * avg_price
                    new_val = qty * price
                    stock += qty
                    avg_price = (current_val + new_val) / stock
                    cash -= qty * price
                    display_label = "MUA"
                    executed = True

        elif final_action == "BÁN":
            qty = int(stock * final_percent)
            if qty > 0:
                stock -= qty
                cash += qty * price
                display_label = "BÁN"
                if risk_action: display_label = risk_action
                
                # Tính lãi lỗ thực hiện
                pnl_realized = (price - avg_price) * qty
                if pnl_realized > 0: win_count += 1
                elif pnl_realized < 0: loss_count += 1
                trade_count += 1
                executed = True
                
                if stock == 0: avg_price = 0

        # Ghi log nếu có giao dịch
        if executed:
            total_equity = cash + (stock * price)
            history.append({
                "Time": dt.strftime('%d/%m %H:%M'),
                "Action": display_label,
                "Price": price,
                "Equity": total_equity,
                "PnL": pnl_realized
            })

    # --- 5. BÁO CÁO KẾT QUẢ ---
    final_equity = cash + (stock * data[-1]['p'])
    roi = (final_equity - CAPITAL) / CAPITAL
    
    print("\n" + "="*40)
    print(f"📊 KẾT QUẢ BACKTEST ({SYMBOL} - {DAYS_BACK} ngày)")
    print("="*40)
    print(f"💰 Vốn ban đầu:   {CAPITAL:,.0f} đ")
    print(f"💎 Vốn cuối cùng: {final_equity:,.0f} đ")
    print(f"🚀 Lợi nhuận:     {final_equity - CAPITAL:,.0f} đ")
    print(f"📈 ROI:           {roi:.2%}")
    print(f"----------------------------------------")
    print(f"🛒 Tổng lệnh bán: {trade_count}")
    print(f"✅ Số lệnh thắng: {win_count}")
    print(f"❌ Số lệnh thua:  {loss_count}")
    print(f"🎯 Win Rate:      {win_count/trade_count:.1%}" if trade_count > 0 else "🎯 Win Rate: 0%")
    print("="*40)
    
    # In 5 giao dịch gần nhất
    print("\n📝 5 Giao dịch gần nhất:")
    for h in history[-5:]:
        print(f"{h['Time']} | {h['Action']:<10} | Giá: {h['Price']:,.0f} | Tài sản: {h['Equity']:,.0f}")

if __name__ == "__main__":
    run_backtest()
