import os
import requests
import pandas as pd
import time
import re
import io
import sys
import math
from datetime import datetime, timezone, timedelta

# Import module Backtest vừa tạo
try:
    from backtest import run_backtest_core
except ImportError:
    pass

# --- 1. CẤU HÌNH ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
CONFIG_DB_ID = os.environ.get("CONFIG_DB_ID")
LOG_DB_ID    = os.environ.get("LOG_DB_ID")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

STOP_LOSS_PCT = -0.07
TAKE_PROFIT_PCT = 0.15

if not NOTION_TOKEN or not CONFIG_DB_ID or not LOG_DB_ID:
    print("❌ LỖI: Thiếu Notion Secrets.")
    sys.exit(1)

def extract_id(text):
    if not text: return ""
    match = re.search(r'([a-f0-9]{32})', text.replace("-", ""))
    return match.group(1) if match else text

CONFIG_DB_ID = extract_id(CONFIG_DB_ID)
LOG_DB_ID = extract_id(LOG_DB_ID)

# --- 2. DỮ LIỆU DỰ PHÒNG ---
BACKUP_CSV = """KEY_ID,Lời Khuyên
G1-B1,Đại cát đại lợi, thời cơ chín muồi. Nên mua tất tay.
G1-B43,Nguy hiểm rình rập, bán tháo ngay lập tức.
G1-B14,Vận khí tốt, có thể mua vào tích lũy.
G23-B4,Mông lung xấu, nên hạ tỷ trọng bán bớt.
"""

# --- 3. THƯ VIỆN ---
try:
    import ccxt
    from lunardate import LunarDate
except ImportError: pass
import ccxt
from lunardate import LunarDate

def send_telegram_message(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except: pass

# --- 4. HÀM CHECK LỆNH TỪ TELEGRAM (NEW) ---
def check_telegram_command(adv_map):
    if not TELEGRAM_TOKEN: return

    print("📩 Đang kiểm tra tin nhắn Telegram...")
    try:
        # Lấy cập nhật mới nhất
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        res = requests.get(url, timeout=10).json()
        
        if not res.get('ok') or not res.get('result'):
            return

        # Lấy tin nhắn cuối cùng
        last_msg = res['result'][-1]
        message = last_msg.get('message', {})
        text = message.get('text', '').strip()
        msg_date = message.get('date', 0)
        
        # Chỉ xử lý tin nhắn trong vòng 10 phút gần nhất (để tránh spam lại lệnh cũ)
        now_ts = int(time.time())
        if now_ts - msg_date > 600: 
            print("   -> Không có lệnh mới (Tin nhắn quá cũ).")
            return

        # Cú pháp lệnh: bp <Mã> <Ngày> (Ví dụ: bp HPG 90 hoặc bp BTC 30)
        # "bp" viết tắt của Backtest Pro
        if text.lower().startswith('bp '):
            parts = text.split()
            if len(parts) >= 2:
                symbol = parts[1].upper()
                days = int(parts[2]) if len(parts) > 2 else 90 # Mặc định 90 ngày
                
                # Sửa lỗi mã Crypto nếu người dùng quên /USDT
                if len(symbol) > 3 and "USDT" not in symbol and "/" not in symbol and "VN" not in symbol:
                     symbol += "USDT"

                print(f"   -> ⚙️ Phát hiện lệnh Backtest: {symbol} trong {days} ngày")
                send_telegram_message(f"⏳ <b>Đang chạy Backtest cho {symbol}...</b>\nVui lòng đợi 10-20 giây.")
                
                # GỌI HÀM TỪ backtest.py
                report = run_backtest_core(symbol, days, adv_map)
                send_telegram_message(report)
            else:
                send_telegram_message("⚠️ Sai cú pháp. Hãy nhập: <code>bp HPG 90</code>")

    except Exception as e:
        print(f"❌ Lỗi check telegram: {e}")

# --- 5. CÁC HÀM CŨ (GIỮ NGUYÊN) ---
def get_stock_data(symbol):
    try:
        to_ts = int(time.time())
        from_ts = to_ts - (30 * 24 * 3600) 
        url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/stock?symbol={symbol}&resolution=1H&from={from_ts}&to={to_ts}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers).json()
        data = []
        if 't' in res and res['t']:
            for i in range(len(res['t'])):
                data.append({
                    "t": datetime.fromtimestamp(res['t'][i], tz=timezone(timedelta(hours=7))),
                    "p": float(res['c'][i])
                })
        return data
    except: return []

def add_technical_indicators(data):
    if not data: return []
    df = pd.DataFrame(data)
    df['SMA20'] = df['p'].rolling(window=20).mean()
    delta = df['p'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    df = df.fillna(0)
    return df.to_dict('records')

def notion_request(endpoint, method="POST", payload=None):
    url = f"https://api.notion.com/v1/{endpoint}"
    headers = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    try:
        if method == "POST": response = requests.post(url, headers=headers, json=payload)
        else: response = requests.get(url, headers=headers)
        return response.json() if response.status_code == 200 else None
    except: return None

def load_advice_data():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, 'data_loi_khuyen.csv')
    if os.path.exists(file_path): return pd.read_csv(file_path)
    return pd.read_csv(io.StringIO(BACKUP_CSV))

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
    avoid = ['đứng ngoài', 'quan sát', 'không nên mua', 'rút lui', 'chờ đợi', 'thận trọng', 'rủi ro', 'lo sợ']
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

def get_existing_signatures(symbol):
    payload = {"filter": {"property": "Mã", "rich_text": {"contains": symbol}}, "sorts": [{"property": "Giờ Giao Dịch", "direction": "descending"}], "page_size": 100}
    try: data = notion_request(f"databases/{LOG_DB_ID}/query", "POST", payload)
    except: 
        payload["sorts"] = [{"property": "Thời Gian", "direction": "descending"}]
        data = notion_request(f"databases/{LOG_DB_ID}/query", "POST", payload)
    s = set()
    if data and 'results' in data:
        for p in data['results']:
            try:
                t = p['properties']['Thời Gian']['title'][0]['plain_text']
                match = re.search(r'(\d{2}:\d{2} \d{2}/\d{2})', t)
                if match: s.add(match.group(1))
            except: pass
    return s

def run_campaign(config):
    try:
        name = config['properties']['Tên Chiến Dịch']['title'][0]['plain_text']
        market = config['properties']['Sàn Giao Dịch']['select']['name']
        symbol = config['properties']['Mã Tài Sản']['rich_text'][0]['plain_text']
        capital = config['properties']['Vốn Ban Đầu']['number']
    except: return

    print(f"\n🚀 Processing: {name} ({symbol})")
    
    data_raw = []
    if "Binance" in market or "Crypto" in market:
        try:
            xc = ccxt.kucoin()
            ohlcv = xc.fetch_ohlcv(symbol, '1h', limit=500)
            for c in ohlcv: data_raw.append({"t": datetime.fromtimestamp(c[0]/1000, tz=timezone(timedelta(hours=7))), "p": c[4]})
        except: pass
    elif "Stock" in market or "VNIndex" in market:
        data_raw = get_stock_data(symbol)

    if not data_raw:
        print("   -> ❌ Không có dữ liệu giá.")
        return

    data_full = add_technical_indicators(data_raw)
    data_to_trade = data_full[-48:] 

    df_adv = load_advice_data()
    adv_map = dict(zip(df_adv['KEY_ID'], df_adv['Lời Khuyên']))
    existing = get_existing_signatures(symbol)
    
    cash, stock, equity, avg_price = capital, 0, capital, 0
    new_logs_count = 0

    for item in data_to_trade:
        dt, price = item['t'], item['p']
        sma20 = item.get('SMA20', 0)
        rsi = item.get('RSI', 0)
        time_sig = dt.strftime('%H:%M %d/%m')
        holding_pnl = (price - avg_price) / avg_price if (stock > 0 and avg_price > 0) else 0

        key = calculate_hexagram(dt)
        advice = adv_map.get(key, "")
        if not advice: advice = f"Chưa có lời khuyên cho quẻ {key}"
        action, percent = analyze_smart_action(advice)
        
        qty, note, display_label = 0, "", "GIỮ"
        risk_reason = ""
        risk_action = None
        if stock > 0:
            if holding_pnl <= STOP_LOSS_PCT: risk_action = "STOP_LOSS"
            elif holding_pnl >= TAKE_PROFIT_PCT: risk_action = "TAKE_PROFIT"

        tech_status = "OK"
        if action == "MUA":
            if price < sma20 and rsi > 35: action, tech_status, risk_reason = "GIỮ", "BAD_TECH", f"(⛔ Giá < SMA20 & RSI={rsi:.0f})"
            if rsi > 75: action, tech_status, risk_reason = "GIỮ", "OVERBOUGHT", f"(⛔ RSI={rsi:.0f} Quá mua)"

        final_action, final_percent = action, percent
        if risk_action == "STOP_LOSS": final_action, final_percent, risk_reason = "BÁN", 1.0, f"⚠️ CẮT LỖ (Lỗ {holding_pnl:.1%})"
        elif risk_action == "TAKE_PROFIT": final_action, final_percent, risk_reason = "BÁN", 0.5, f"💰 CHỐT LỜI (Lãi {holding_pnl:.1%})"
        elif tech_status != "OK" and display_label == "MUA": display_label = "✋ ĐỢI (TECH XẤU)"

        if final_action == "MUA":
            amt = cash * final_percent
            if amt > 1:
                qty = amt / price
                if "Stock" in market or "VNIndex" in market: qty = int(qty // 100) * 100
                if qty > 0:
                    current_val, new_val = stock * avg_price, qty * price
                    stock += qty
                    avg_price = (current_val + new_val) / stock
                    cash -= qty * price
                    note, display_label = f"MUA {int(final_percent*100)}%", "MUA"
            if display_label != "MUA" and stock > 0: display_label = "✊ GIỮ"

        elif final_action == "BÁN":
            qty_sell = stock * final_percent
            if "Stock" in market or "VNIndex" in market: qty_sell = int(qty_sell // 100) * 100
            if qty_sell > stock: qty_sell = stock
            if qty_sell > 0:
                stock -= qty_sell
                cash += qty_sell * price
                note = f"BÁN {int(final_percent*100)}%"
                display_label = risk_action if risk_action else "BÁN"
                if risk_action == "STOP_LOSS": display_label = "✂️ CẮT LỖ"
                elif risk_action == "TAKE_PROFIT": display_label = "💵 CHỐT LỜI"
                if stock == 0: avg_price = 0
            if display_label not in ["BÁN", "✂️ CẮT LỖ", "💵 CHỐT LỜI"] and stock == 0: display_label = "⛔ KHÔNG MUA"
        else:
            display_label = "✊ GIỮ" if stock > 0 else "⛔ KHÔNG MUA"

        current_asset_val = stock * price
        equity = cash + current_asset_val
        roi_total = (equity - capital) / capital
        allocation = current_asset_val / equity if equity > 0 else 0
        holding_pnl_new = (price - avg_price) / avg_price if (stock > 0 and avg_price > 0) else 0

        if time_sig not in existing:
            icon = "⚪"
            if "MUA" in display_label: icon = "🟢"
            if "BÁN" in display_label: icon = "🔴"
            if "CẮT LỖ" in display_label: icon = "⚠️"
            if "CHỐT LỜI" in display_label: icon = "💰"
            if "GIỮ" in display_label: icon = "✊"
            if "KHÔNG MUA" in display_label: icon = "⛔"

            title = f"{icon} {display_label} | {time_sig}"
            payload = {
                "parent": {"database_id": LOG_DB_ID},
                "properties": {
                    "Thời Gian": {"title": [{"text": {"content": title}}]},
                    "Mã": {"rich_text": [{"text": {"content": f"{symbol} ({name})" }}]}, 
                    "Giá": {"number": price},
                    "INPUT MÃ": {"rich_text": [{"text": {"content": key}}]},
                    "Loại Lệnh": {"select": {"name": display_label}},
                    "Số Lượng": {"number": qty if note else 0},
                    "Số Dư": {"number": equity},
                    "ROI": {"number": roi_total},
                    "Giờ Giao Dịch": {"date": {"start": dt.isoformat()}},
                    "Tỷ Trọng": {"number": allocation},
                    "% Lời/Lỗ CP": {"number": holding_pnl_new}
                }
            }
            notion_request("pages", "POST", payload)
            print(f"   ✅ [GHI] {title}")
            existing.add(time_sig)
            new_logs_count += 1
            
            if any(x in display_label for x in ["MUA", "BÁN", "CẮT LỖ", "CHỐT LỜI"]):
                msg_reason = risk_reason if risk_reason else advice
                msg = (
                    f"🔔 <b>TÍN HIỆU: {symbol}</b>\n"
                    f"{icon} <b>Lệnh:</b> {display_label}\n"
                    f"⏰ <b>Time:</b> {time_sig}\n"
                    f"💵 <b>Giá:</b> {price:,.2f}\n"
                    f"📊 <b>RSI:</b> {rsi:.0f} | <b>SMA20:</b> {sma20:,.0f}\n"
                    f"💡 <b>Lý do:</b> {msg_reason}"
                )
                send_telegram_message(msg)

    if new_logs_count == 0:
        print("   -> Dữ liệu đã đồng bộ.")

# --- MAIN ---
print("📡 Đang khởi động...")
df_adv = load_advice_data()
adv_map = dict(zip(df_adv['KEY_ID'], df_adv['Lời Khuyên']))

# [NEW] Kiểm tra lệnh từ Telegram trước
check_telegram_command(adv_map)

# Sau đó mới chạy logic Notion thông thường
query = {"filter": {"property": "Trạng Thái", "status": {"equals": "Đang chạy"}}}
res = notion_request(f"databases/{CONFIG_DB_ID}/query", "POST", query)

if res and 'results' in res:
    print(f"✅ Tìm thấy {len(res['results'])} chiến dịch.")
    for cfg in res['results']: run_campaign(cfg)
else:
    print("❌ Lỗi kết nối Notion.")
