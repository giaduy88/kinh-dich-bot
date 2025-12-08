import os
import requests
import pandas as pd
import time
import re
import json
from datetime import datetime, timezone, timedelta
import ccxt
from lunardate import LunarDate

# --- 1. LẤY CẤU HÌNH TỪ BIẾN MÔI TRƯỜNG (GITHUB SECRETS) ---
# Code sẽ tự tìm trong "Két sắt" của GitHub để lấy khóa
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
CONFIG_DB_ID = os.environ.get("CONFIG_DB_ID")
LOG_DB_ID    = os.environ.get("LOG_DB_ID")

# Kiểm tra xem có lấy được khóa không (để debug)
if not NOTION_TOKEN or not CONFIG_DB_ID or not LOG_DB_ID:
    print("❌ LỖI: Không tìm thấy Token/ID trong biến môi trường.")
    print("👉 Hãy kiểm tra lại mục Settings > Secrets and variables > Actions trên GitHub.")
    exit(1)

def extract_id(text):
    if not text: return ""
    match = re.search(r'([a-f0-9]{32})', text.replace("-", ""))
    return match.group(1) if match else text

CONFIG_DB_ID = extract_id(CONFIG_DB_ID)
LOG_DB_ID = extract_id(LOG_DB_ID)

# --- 2. HÀM GỌI API CHỨNG KHOÁN (DNSE) ---
def get_stock_price(symbol, days=10):
    try:
        to_ts = int(time.time())
        from_ts = to_ts - (days * 24 * 3600)
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

# --- 3. HÀM GỌI NOTION ---
def notion_request(endpoint, method="POST", payload=None):
    url = f"https://api.notion.com/v1/{endpoint}"
    headers = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    try:
        if method == "POST": response = requests.post(url, headers=headers, json=payload)
        else: response = requests.get(url, headers=headers)
        return response.json() if response.status_code == 200 else None
    except: return None

# --- 4. HÀM KIỂM TRA LOG CŨ ---
def get_latest_log_time(symbol):
    payload = {
        "filter": {"property": "Mã", "rich_text": {"contains": symbol}},
        "sorts": [{"property": "Thời Gian", "direction": "descending"}],
        "page_size": 1
    }
    data = notion_request(f"databases/{LOG_DB_ID}/query", "POST", payload)
    
    if data and data['results']:
        try:
            title_text = data['results'][0]['properties']['Thời Gian']['title'][0]['plain_text']
            match = re.search(r'(\d{2}:\d{2} \d{2}/\d{2})', title_text)
            if match:
                current_year = datetime.now().year
                dt = datetime.strptime(f"{match.group(1)}/{current_year}", '%H:%M %d/%m/%Y')
                return dt.replace(tzinfo=timezone(timedelta(hours=7)))
        except: pass
    return datetime(2000, 1, 1, tzinfo=timezone(timedelta(hours=7)))

# --- 5. LOGIC KINH DỊCH ---
king_wen_matrix = [[1, 10, 13, 25, 44, 6, 33, 12], [43, 58, 49, 17, 28, 47, 31, 45], [14, 38, 30, 21, 50, 64, 56, 35], [34, 54, 55, 51, 32, 40, 62, 16], [9, 61, 37, 42, 57, 59, 53, 20], [5, 60, 63, 3, 48, 29, 39, 8], [26, 41, 22, 27, 18, 4, 52, 23], [11, 19, 36, 24, 46, 7, 15, 2]]

def calculate_hexagram(dt_real):
    if dt_real.hour == 23: dt_lunar = dt_real + timedelta(days=1)
    else: dt_lunar = dt_real
    lunar = LunarDate.fromSolarDate(dt_lunar.year, dt_lunar.month, dt_lunar.day)
    def get_chi(h): return 1 if h==23 or h==0 else ((h+1)//2 + 1 if h%2!=0 else h//2 + 1)
    
    so_nam = ((lunar.year - 1984) % 12) + 1
    base = so_nam + lunar.month + lunar.day
    chi = get_chi(dt_real.hour)
    
    thuong, ha = base % 8 or 8, (base + chi) % 8 or 8
    hao = (base + chi) % 6 or 6
    id_goc = king_wen_matrix[thuong-1][ha-1]
    
    is_upper, line = hao > 3, hao - 3 if hao > 3 else hao
    target = thuong if is_upper else ha
    trans = {1:{1:5,2:3,3:2},2:{1:6,2:4,3:1},3:{1:7,2:1,3:4},4:{1:8,2:2,3:3},5:{1:1,2:7,3:6},6:{1:2,2:8,3:5},7:{1:3,2:5,3:8},8:{1:4,2:6,3:7}}
    new_trig = trans[target][line]
    new_thuong, new_ha = (new_trig, thuong) if is_upper else (thuong, new_trig)
    
    id_bien = king_wen_matrix[new_thuong-1][new_ha-1]
    return f"G{id_goc}-B{id_bien}"

def analyze_sentiment(text):
    if not isinstance(text, str): return "GIỮ"
    text = text.lower()
    buys = ['mua vào', 'nên mua', 'lợi lớn', 'tăng lên', 'thắng lợi', 'triển vọng', 'cát', 'hanh thông']
    sells = ['bán ra', 'giảm', 'xuống thấp', 'lỗ', 'mắc kẹt', 'nguy hiểm', 'trở ngại', 'xấu', 'đừng mua']
    b, s = sum(1 for w in buys if w in text), sum(1 for w in sells if w in text)
    return "MUA" if b > s else ("BÁN" if s > b else "GIỮ")

# --- 6. CHẠY CHIẾN DỊCH ---
def run_campaign(config):
    props = config['properties']
    try:
        name = props['Tên Chiến Dịch']['title'][0]['plain_text']
        market = props['Sàn Giao Dịch']['select']['name']
        symbol = props['Mã Tài Sản']['rich_text'][0]['plain_text']
        capital = props['Vốn Ban Đầu']['number']
    except: return

    print(f"🚀 Check: {name} ({symbol})")
    
    data = []
    # Crypto
    if "Binance" in market or "Crypto" in market:
        try:
            exchange = ccxt.kucoin()
            ohlcv = exchange.fetch_ohlcv(symbol, '1h', limit=48)
            for c in ohlcv: data.append({"t": datetime.fromtimestamp(c[0]/1000, tz=timezone(timedelta(hours=7))), "p": c[4]})
        except: pass
    # Stock
    elif "Stock" in market or "VNIndex" in market:
        data = get_stock_price(symbol)

    if not data: return

    cash, stock, equity = capital, 0, capital
    try:
        df_adv = pd.read_csv('data_loi_khuyen.csv')
        adv_map = dict(zip(df_adv['KEY_ID'], df_adv['Lời Khuyên']))
    except: adv_map = {}

    last_log_time = get_latest_log_time(symbol)
    new_logs = 0

    for item in data[-48:]:
        dt, price = item['t'], item['p']
        key = calculate_hexagram(dt)
        signal = analyze_sentiment(adv_map.get(key, ""))
        qty, note = 0, ""
        
        if signal == "MUA" and cash > capital*0.01:
            qty = cash / price
            if "Stock" in market: qty = int(qty // 100) * 100
            if qty > 0: stock += qty; cash -= qty * price; note = "MUA"
        elif signal == "BÁN" and stock > 0:
            cash += stock * price; qty = stock; stock = 0; note = "BÁN"
            
        equity = cash + stock*price
        
        if note and dt > last_log_time:
            roi_val = (equity - capital) / capital
            icon = "🟢" if signal == "MUA" else "🔴"
            title = f"{icon} {signal} | {dt.strftime('%H:%M %d/%m')}"
            
            payload = {
                "parent": {"database_id": LOG_DB_ID},
                "properties": {
                    "Thời Gian": {"title": [{"text": {"content": title}}]},
                    "Mã": {"rich_text": [{"text": {"content": f"{symbol} ({name})" }}]}, 
                    "Giá": {"number": price},
                    "INPUT MÃ": {"rich_text": [{"text": {"content": key}}]},
                    "Loại Lệnh": {"select": {"name": signal}},
                    "Số Lượng": {"number": qty},
                    "Số Dư": {"number": equity},
                    "ROI": {"number": roi_val}
                }
            }
            notion_request("pages", "POST", payload)
            print(f"   [NEW] {title} | ROI: {roi_val:.2%}")
            new_logs += 1
            last_log_time = dt

    if new_logs == 0: print("   -> Không có lệnh mới.")

# --- 7. MAIN ---
query = {"filter": {"property": "Trạng Thái", "status": {"equals": "Đang chạy"}}}
res = notion_request(f"databases/{CONFIG_DB_ID}/query", "POST", query)
if res and 'results' in res:
    for cfg in res['results']: run_campaign(cfg)
else: print("❌ Lỗi kết nối Notion hoặc không có chiến dịch.")
