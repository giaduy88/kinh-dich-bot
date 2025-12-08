import os
import requests
import pandas as pd
import time
import re
import io
import sys
from datetime import datetime, timezone, timedelta

# --- 1. CẤU HÌNH (LẤY TỪ GITHUB SECRETS) ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
CONFIG_DB_ID = os.environ.get("CONFIG_DB_ID")
LOG_DB_ID    = os.environ.get("LOG_DB_ID")

# Kiểm tra biến môi trường
if not NOTION_TOKEN or not CONFIG_DB_ID or not LOG_DB_ID:
    print("❌ LỖI: Chưa cấu hình Secrets trong Settings > Secrets and variables > Actions")
    sys.exit(1) # Dừng ngay nếu thiếu key

def extract_id(text):
    if not text: return ""
    match = re.search(r'([a-f0-9]{32})', text.replace("-", ""))
    return match.group(1) if match else text

CONFIG_DB_ID = extract_id(CONFIG_DB_ID)
LOG_DB_ID = extract_id(LOG_DB_ID)

# --- 2. DỮ LIỆU DỰ PHÒNG (TRÁNH LỖI MẤT FILE) ---
BACKUP_CSV = """KEY_ID,Lời Khuyên
G1-B1,Đại cát, nên mua vào.
G1-B43,Quyết liệt, bán ra ngay.
G23-B4,Mông lung xấu, bán cắt lỗ.
G23-B35,Tấn tới tốt đẹp, mua vào.
"""

# --- 3. CÀI ĐẶT THƯ VIỆN ---
try:
    import ccxt
    from lunardate import LunarDate
except ImportError:
    # Trên GitHub Actions, dòng này thường không chạy được vì quyền user
    # Nhưng ta đã cài qua file .yml rồi nên cứ try/except
    pass
import ccxt
from lunardate import LunarDate

# --- 4. HÀM API CHỨNG KHOÁN (DNSE DIRECT - KHÔNG CẦN THƯ VIỆN) ---
def get_stock_data(symbol):
    try:
        to_ts = int(time.time())
        from_ts = to_ts - (5 * 24 * 3600)
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
    except Exception as e:
        print(f"❌ Lỗi lấy data Stock {symbol}: {e}")
        return []

# --- 5. HÀM NOTION ---
def notion_request(endpoint, method="POST", payload=None):
    url = f"https://api.notion.com/v1/{endpoint}"
    headers = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    try:
        if method == "POST": response = requests.post(url, headers=headers, json=payload)
        else: response = requests.get(url, headers=headers)
        return response.json() if response.status_code == 200 else None
    except: return None

# --- 6. HÀM LOAD FILE THÔNG MINH ---
def load_advice_data():
    # Tìm file ở thư mục hiện tại (GitHub Workspace)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, 'data_loi_khuyen.csv')
    
    if os.path.exists(file_path):
        print(f"✅ Đã tìm thấy file CSV tại: {file_path}")
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            print(f"❌ Lỗi đọc file CSV: {e}")
    else:
        print(f"⚠️ KHÔNG TÌM THẤY file 'data_loi_khuyen.csv' tại {current_dir}")
        print("ℹ️ Hãy kiểm tra lại tên file trong Repo (chữ hoa/thường).")

    print("⚡ Kích hoạt dữ liệu dự phòng (Backup Data)...")
    return pd.read_csv(io.StringIO(BACKUP_CSV))

# --- 7. LOGIC KINH DỊCH ---
king_wen_matrix = [[1, 10, 13, 25, 44, 6, 33, 12], [43, 58, 49, 17, 28, 47, 31, 45], [14, 38, 30, 21, 50, 64, 56, 35], [34, 54, 55, 51, 32, 40, 62, 16], [9, 61, 37, 42, 57, 59, 53, 20], [5, 60, 63, 3, 48, 29, 39, 8], [26, 41, 22, 27, 18, 4, 52, 23], [11, 19, 36, 24, 46, 7, 15, 2]]

def calculate_hexagram(dt):
    # Fix múi giờ & Giờ Tý
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
    # Mở rộng từ khóa để bắt dính hơn
    buys = ['mua', 'lợi', 'tốt', 'lãi', 'cát', 'lên', 'tăng', 'hanh thông']
    sells = ['bán', 'xấu', 'lỗ', 'nguy', 'hại', 'xuống', 'giảm', 'trở ngại', 'kẹt']
    
    b_score = sum(1 for w in buys if w in text)
    s_score = sum(1 for w in sells if w in text)
    
    if b_score > s_score: return "MUA"
    if s_score > b_score: return "BÁN"
    return "GIỮ"

# --- 8. KIỂM TRA LỊCH SỬ NOTION ---
def get_existing_signatures(symbol):
    payload = {
        "filter": {"property": "Mã", "rich_text": {"contains": symbol}},
        "sorts": [{"property": "Thời Gian", "direction": "descending"}],
        "page_size": 50 
    }
    data = notion_request(f"databases/{LOG_DB_ID}/query", "POST", payload)
    s = set()
    if data and 'results' in data:
        for p in data['results']:
            try:
                t = p['properties']['Thời Gian']['title'][0]['plain_text']
                # Lấy chữ ký thời gian (VD: 13:00 08/12)
                match = re.search(r'(\d{2}:\d{2} \d{2}/\d{2})', t)
                if match: s.add(match.group(1))
            except: pass
    return s

# --- 9. HÀM CHẠY CHIẾN DỊCH ---
def run_campaign(config):
    try:
        name = config['properties']['Tên Chiến Dịch']['title'][0]['plain_text']
        market = config['properties']['Sàn Giao Dịch']['select']['name']
        symbol = config['properties']['Mã Tài Sản']['rich_text'][0]['plain_text']
        capital = config['properties']['Vốn Ban Đầu']['number']
    except: return

    print(f"\n🚀 Checking: {name} ({symbol})")
    
    # 1. Lấy dữ liệu
    data = []
    if "Binance" in market or "Crypto" in market:
        try:
            xc = ccxt.kucoin()
            ohlcv = xc.fetch_ohlcv(symbol, '1h', limit=48)
            for c in ohlcv: data.append({"t": datetime.fromtimestamp(c[0]/1000, tz=timezone(timedelta(hours=7))), "p": c[4]})
        except: pass
    elif "Stock" in market:
        data = get_stock_data(symbol)

    if not data:
        print("   -> ❌ Không có dữ liệu giá.")
        return

    # 2. Nạp Lời khuyên
    df_adv = load_advice_data()
    adv_map = dict(zip(df_adv['KEY_ID'], df_adv['Lời Khuyên']))

    # 3. Lấy lịch sử đã ghi
    existing = get_existing_signatures(symbol)
    print(f"   -> Đã có {len(existing)} bản ghi cũ trên Notion.")

    cash, stock, equity = capital, 0, capital
    new_logs = 0

    for item in data[-48:]: # Quét 48h
        dt, price = item['t'], item['p']
        time_sig = dt.strftime('%H:%M %d/%m')
        
        key = calculate_hexagram(dt)
        advice = adv_map.get(key, "")
        signal = analyze_sentiment(advice)
        
        qty, note = 0, ""
        
        # Logic giao dịch
        if signal == "MUA" and cash > capital*0.01:
            qty = cash/price; stock=qty; cash=0; note="MUA"
        elif signal == "BÁN" and stock > 0:
            cash=stock*price; qty=stock; stock=0; note="BÁN"
        
        equity = cash + stock*price
        
        # In ra console để debug xem bot tính ra gì
        # print(f"      {time_sig} | {key} | {signal}")

        # Ghi log nếu: Có lệnh VÀ Chưa ghi bao giờ
        if note:
            if time_sig not in existing:
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
                        "ROI": {"number": roi}
                    }
                }
                notion_request("pages", "POST", payload)
                print(f"   ✅ [GHI MỚI] {title}")
                existing.add(time_sig) # Cache lại để không ghi trùng
                new_logs += 1
            else:
                # Đã có rồi thì thôi
                pass

    if new_logs == 0:
        print("   -> Không có lệnh mới cần ghi (Dữ liệu đã đồng bộ).")
    else:
        print(f"   -> Đã ghi thêm {new_logs} dòng.")

# --- MAIN ---
print("📡 Đang kết nối Notion...")
query = {"filter": {"property": "Trạng Thái", "status": {"equals": "Đang chạy"}}}
res = notion_request(f"databases/{CONFIG_DB_ID}/query", "POST", query)

if res and 'results' in res:
    print(f"✅ Tìm thấy {len(res['results'])} chiến dịch.")
    for cfg in res['results']: run_campaign(cfg)
else:
    print("❌ Lỗi kết nối Notion (Hoặc không có chiến dịch 'Đang chạy'). Check Token/ID.")
