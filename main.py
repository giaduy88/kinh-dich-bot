import os
import requests
import pandas as pd
import time
import re
import io
import sys
from datetime import datetime, timezone, timedelta

# --- 1. CẤU HÌNH ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
CONFIG_DB_ID = os.environ.get("CONFIG_DB_ID")
LOG_DB_ID    = os.environ.get("LOG_DB_ID")

if not NOTION_TOKEN or not CONFIG_DB_ID or not LOG_DB_ID:
    print("❌ LỖI: Thiếu Secrets.")
    sys.exit(1)

def extract_id(text):
    if not text: return ""
    match = re.search(r'([a-f0-9]{32})', text.replace("-", ""))
    return match.group(1) if match else text

CONFIG_DB_ID = extract_id(CONFIG_DB_ID)
LOG_DB_ID = extract_id(LOG_DB_ID)

# --- 2. CẤU HÌNH THỜI GIAN BẮT ĐẦU (THEO YÊU CẦU CỦA BẠN) ---
# Ngày 18/10 năm Ất Tỵ (2025) lúc 23h
LUNAR_START_CFG = {
    "year": 2025,  # Năm Ất Tỵ
    "month": 10,   # Tháng 10 Âm
    "day": 18,     # Ngày 18 Âm
    "hour": 23     # 23h đêm
}

# --- 3. DỮ LIỆU DỰ PHÒNG ---
BACKUP_CSV = """KEY_ID,Lời Khuyên
G1-B1,Đại cát, nên mua vào.
G1-B43,Quyết liệt, bán ra ngay.
G23-B4,Mông lung xấu, bán cắt lỗ.
G23-B35,Tấn tới tốt đẹp, mua vào.
"""

# --- 4. THƯ VIỆN & HÀM CHUYỂN ĐỔI NGÀY ---
try:
    import ccxt
    from lunardate import LunarDate
except ImportError: pass
import ccxt
from lunardate import LunarDate

def get_start_timestamp_from_lunar():
    """Chuyển đổi ngày Âm Lịch cấu hình sang Timestamp Dương Lịch"""
    try:
        # 1. Đổi sang Dương Lịch
        # Lưu ý: Thư viện lunardate đổi chính xác theo lịch vạn niên
        solar_date = LunarDate(
            LUNAR_START_CFG["year"], 
            LUNAR_START_CFG["month"], 
            LUNAR_START_CFG["day"]
        ).toSolarDate()
        
        # 2. Tạo datetime object (Múi giờ +7)
        tz = timezone(timedelta(hours=7))
        dt_start = datetime(
            solar_date.year, 
            solar_date.month, 
            solar_date.day, 
            LUNAR_START_CFG["hour"], 0, 0, tzinfo=tz
        )
        
        print(f"📅 Cấu hình chạy từ: 23h 18/10 Âm Lịch (Ất Tỵ)")
        print(f"   -> Tương đương Dương Lịch: {dt_start.strftime('%H:%M %d/%m/%Y')}")
        
        return int(dt_start.timestamp())
    except Exception as e:
        print(f"❌ Lỗi tính ngày Âm Lịch: {e}")
        # Fallback về 48h trước nếu lỗi
        return int(time.time()) - (48 * 3600)

# --- 5. HÀM API CHỨNG KHOÁN (DNSE) ---
def get_stock_data(symbol, start_ts):
    try:
        to_ts = int(time.time())
        # API DNSE lấy từ start_ts đến hiện tại
        url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/stock?symbol={symbol}&resolution=1H&from={start_ts}&to={to_ts}"
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
    except Exception as e:
        print(f"❌ Lỗi Stock {symbol}: {e}")
        return []

# --- 6. HÀM NOTION ---
def notion_request(endpoint, method="POST", payload=None):
    url = f"https://api.notion.com/v1/{endpoint}"
    headers = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    try:
        if method == "POST": response = requests.post(url, headers=headers, json=payload)
        else: response = requests.get(url, headers=headers)
        return response.json() if response.status_code == 200 else None
    except: return None

# --- 7. HÀM LOAD FILE ---
def load_advice_data():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, 'data_loi_khuyen.csv')
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    return pd.read_csv(io.StringIO(BACKUP_CSV))

# --- 8. LOGIC KINH DỊCH ---
king_wen_matrix = [[1, 10, 13, 25, 44, 6, 33, 12], [43, 58, 49, 17, 28, 47, 31, 45], [14, 38, 30, 21, 50, 64, 56, 35], [34, 54, 55, 51, 32, 40, 62, 16], [9, 61, 37, 42, 57, 59, 53, 20], [5, 60, 63, 3, 48, 29, 39, 8], [26, 41, 22, 27, 18, 4, 52, 23], [11, 19, 36, 24, 46, 7, 15, 2]]

def calculate_hexagram(dt):
    # Logic: 23h hôm nay tính là ngày hôm sau (Dạ Tý)
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

def analyze_sentiment(text):
    if not isinstance(text, str): return "GIỮ"
    text = text.lower()
    buys = ['mua', 'lợi', 'tốt', 'lãi', 'cát', 'lên', 'tăng', 'hanh thông']
    sells = ['bán', 'xấu', 'lỗ', 'nguy', 'hại', 'xuống', 'giảm', 'trở ngại', 'kẹt']
    b_score = sum(1 for w in buys if w in text)
    s_score = sum(1 for w in sells if w in text)
    if b_score > s_score: return "MUA"
    if s_score > b_score: return "BÁN"
    return "GIỮ"

# --- 9. KIỂM TRA LỊCH SỬ ---
def get_existing_signatures(symbol):
    payload = {
        "filter": {"property": "Mã", "rich_text": {"contains": symbol}},
        "sorts": [{"property": "Giờ Giao Dịch", "direction": "descending"}],
        "page_size": 100 
    }
    try:
        data = notion_request(f"databases/{LOG_DB_ID}/query", "POST", payload)
    except: return set()

    s = set()
    if data and 'results' in data:
        for p in data['results']:
            try:
                # Dùng chữ ký thời gian từ cột Title để check trùng
                t = p['properties']['Thời Gian']['title'][0]['plain_text']
                match = re.search(r'(\d{2}:\d{2} \d{2}/\d{2})', t)
                if match: s.add(match.group(1))
            except: pass
    return s

# --- 10. HÀM CHẠY CHIẾN DỊCH ---
def run_campaign(config, start_ts):
    try:
        name = config['properties']['Tên Chiến Dịch']['title'][0]['plain_text']
        market = config['properties']['Sàn Giao Dịch']['select']['name']
        symbol = config['properties']['Mã Tài Sản']['rich_text'][0]['plain_text']
        capital = config['properties']['Vốn Ban Đầu']['number']
    except: return

    print(f"\n🚀 Checking: {name} ({symbol})")
    
    data = []
    # 1. Lấy dữ liệu từ Mốc Thời Gian đã định
    if "Binance" in market or "Crypto" in market:
        try:
            xc = ccxt.kucoin()
            # ccxt dùng mili-seconds
            ohlcv = xc.fetch_ohlcv(symbol, '1h', since=start_ts*1000)
            for c in ohlcv: data.append({"t": datetime.fromtimestamp(c[0]/1000, tz=timezone(timedelta(hours=7))), "p": c[4]})
        except: pass
    elif "Stock" in market or "VNIndex" in market:
        data = get_stock_data(symbol, start_ts)

    if not data:
        print("   -> ❌ Không có dữ liệu giá.")
        return

    print(f"   -> Đã lấy {len(data)} cây nến từ mốc khởi tạo.")

    df_adv = load_advice_data()
    adv_map = dict(zip(df_adv['KEY_ID'], df_adv['Lời Khuyên']))

    existing = get_existing_signatures(symbol)
    cash, stock, equity = capital, 0, capital
    new_logs = 0

    for item in data:
        dt, price = item['t'], item['p']
        time_sig = dt.strftime('%H:%M %d/%m')
        
        key = calculate_hexagram(dt)
        advice = adv_map.get(key, "")
        signal = analyze_sentiment(advice)
        
        qty, note = 0, ""
        
        # Logic giao dịch
        if signal == "MUA" and cash > capital*0.01:
            qty = cash / price
            if "Stock" in market or "VNIndex" in market: qty = int(qty // 100) * 100
            if qty > 0: stock += qty; cash -= qty * price; note = "MUA"
        elif signal == "BÁN" and stock > 0:
            cash=stock*price; qty=stock; stock=0; note="BÁN"
        
        equity = cash + stock*price
        
        # GHI LOG
        if note and (time_sig not in existing):
            roi = (equity - capital) / capital
            icon = "🟢" if signal == "MUA" else "🔴"
            title = f"{icon} {signal} | {time_sig}"
            
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
                    "ROI": {"number": roi},
                    "Giờ Giao Dịch": {"date": {"start": dt.isoformat()}} 
                }
            }
            notion_request("pages", "POST", payload)
            print(f"   ✅ [GHI MỚI] {title}")
            existing.add(time_sig)
            new_logs += 1

    if new_logs == 0:
        print("   -> Dữ liệu đã đồng bộ.")

# --- MAIN ---
print("📡 Đang kết nối Notion...")
# 1. Tính toán thời gian bắt đầu
START_TS = get_start_timestamp_from_lunar()

# 2. Quét các chiến dịch
query = {"filter": {"property": "Trạng Thái", "status": {"equals": "Đang chạy"}}}
res = notion_request(f"databases/{CONFIG_DB_ID}/query", "POST", query)

if res and 'results' in res:
    print(f"✅ Tìm thấy {len(res['results'])} chiến dịch.")
    for cfg in res['results']: 
        run_campaign(cfg, START_TS) # Truyền mốc thời gian vào
else:
    print("❌ Lỗi kết nối Notion. Check Token/ID.")
