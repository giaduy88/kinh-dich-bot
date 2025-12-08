import os
import requests
import pandas as pd
import time
import re
import io
import sys
import math
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

# --- 4. HÀM API CHỨNG KHOÁN (DNSE) ---
def get_stock_data(symbol):
    try:
        to_ts = int(time.time())
        from_ts = to_ts - (5 * 24 * 3600) # Lấy 5 ngày gần nhất
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
        print(f"❌ Lỗi Stock {symbol}: {e}")
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

# --- 6. HÀM LOAD FILE ---
def load_advice_data():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, 'data_loi_khuyen.csv')
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    return pd.read_csv(io.StringIO(BACKUP_CSV))

# --- 7. LOGIC KINH DỊCH ---
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

# --- 8. PHÂN TÍCH CẢM XÚC NÂNG CAO (SMART AI) ---
def analyze_smart_action(text):
    if not isinstance(text, str): return "GIỮ", 0.0
    text = text.lower()
    
    # 1. Tín hiệu MẠNH (100% Vốn/Hàng)
    strong_buy = ['đại cát', 'lợi lớn', 'bay cao', 'thời cơ vàng', 'mua ngay', 'tất tay', 'all-in']
    strong_sell = ['nguy hiểm', 'sập', 'tháo chạy', 'bán tháo', 'tuyệt vọng', 'cắt lỗ ngay']
    
    if any(w in text for w in strong_buy): return "MUA", 1.0  # Mua 100% tiền
    if any(w in text for w in strong_sell): return "BÁN", 1.0 # Bán 100% hàng

    # 2. Tín hiệu TRUNG BÌNH (50% Vốn/Hàng)
    normal_buy = ['mua', 'tốt', 'lãi', 'tích lũy', 'hanh thông', 'tăng']
    normal_sell = ['bán', 'xấu', 'lỗ', 'giảm', 'trở ngại', 'hạ tỷ trọng']

    if any(w in text for w in normal_buy): return "MUA", 0.5  # Mua 50% tiền
    if any(w in text for w in normal_sell): return "BÁN", 0.5 # Bán 50% hàng

    return "GIỮ", 0.0

# --- 9. KIỂM TRA LỊCH SỬ (CHỐNG TRÙNG) ---
def get_existing_signatures(symbol):
    # Lấy 100 bản ghi gần nhất để đối chiếu
    payload = {
        "filter": {"property": "Mã", "rich_text": {"contains": symbol}},
        "sorts": [{"property": "Giờ Giao Dịch", "direction": "descending"}],
        "page_size": 100 
    }
    # Fallback sort nếu chưa có cột date
    try:
        data = notion_request(f"databases/{LOG_DB_ID}/query", "POST", payload)
    except:
        payload["sorts"] = [{"property": "Thời Gian", "direction": "descending"}]
        data = notion_request(f"databases/{LOG_DB_ID}/query", "POST", payload)

    s = set()
    if data and 'results' in data:
        for p in data['results']:
            try:
                # Dùng Time Signature từ Tiêu đề (VD: 13:00 08/12)
                t = p['properties']['Thời Gian']['title'][0]['plain_text']
                match = re.search(r'(\d{2}:\d{2} \d{2}/\d{2})', t)
                if match: s.add(match.group(1))
            except: pass
    return s

# --- 10. HÀM CHẠY CHIẾN DỊCH ---
def run_campaign(config):
    try:
        name = config['properties']['Tên Chiến Dịch']['title'][0]['plain_text']
        market = config['properties']['Sàn Giao Dịch']['select']['name']
        symbol = config['properties']['Mã Tài Sản']['rich_text'][0]['plain_text']
        capital = config['properties']['Vốn Ban Đầu']['number']
    except: return

    print(f"\n🚀 Processing: {name} ({symbol})")
    
    # 1. Lấy Data
    data = []
    if "Binance" in market or "Crypto" in market:
        try:
            xc = ccxt.kucoin()
            ohlcv = xc.fetch_ohlcv(symbol, '1h', limit=48)
            for c in ohlcv: data.append({"t": datetime.fromtimestamp(c[0]/1000, tz=timezone(timedelta(hours=7))), "p": c[4]})
        except: pass
    elif "Stock" in market or "VNIndex" in market:
        data = get_stock_data(symbol)

    if not data:
        print("   -> ❌ Không có dữ liệu giá.")
        return

    # 2. Nạp Lời khuyên
    df_adv = load_advice_data()
    adv_map = dict(zip(df_adv['KEY_ID'], df_adv['Lời Khuyên']))

    # 3. Lấy dữ liệu đã ghi để chống trùng
    existing = get_existing_signatures(symbol)
    
    # Khôi phục trạng thái tài khoản giả định (Reset mỗi lần chạy Action để tính ROI chuẩn cho nến hiện tại)
    # Lưu ý: Để theo dõi Portfolio thực tế lâu dài, bạn cần lưu 'cash/stock' vào Database riêng. 
    # Ở đây ta giả lập dòng tiền chạy từ đầu chuỗi 48h để khớp với biểu đồ.
    cash, stock, equity = capital, 0, capital
    
    new_logs_count = 0

    for item in data:
        dt, price = item['t'], item['p']
        time_sig = dt.strftime('%H:%M %d/%m')
        
        key = calculate_hexagram(dt)
        advice = adv_map.get(key, "")
        
        # --- LOGIC THÔNG MINH (V27) ---
        action, percent = analyze_smart_action(advice)
        
        qty = 0
        note = "" # Ghi chú lệnh thực hiện
        display_label = "GIỮ" # Nhãn hiển thị trên Notion

        # XỬ LÝ MUA
        if action == "MUA":
            # Tính tiền muốn mua (50% hoặc 100% tiền đang có)
            amount_to_spend = cash * percent
            if amount_to_spend > 10000: # Mua tối thiểu 10k VND hoặc 1$
                # Tính số lượng
                qty = amount_to_spend / price
                
                # Làm tròn lô chứng khoán
                if "Stock" in market or "VNIndex" in market:
                    qty = int(qty // 100) * 100
                
                if qty > 0:
                    cost = qty * price
                    stock += qty
                    cash -= cost
                    note = f"MUA {int(percent*100)}%" # VD: MUA 50%
                    display_label = "MUA"

        # XỬ LÝ BÁN
        elif action == "BÁN":
            # Tính lượng hàng muốn bán
            qty_to_sell = stock * percent
            
            # Làm tròn lô chứng khoán
            if "Stock" in market or "VNIndex" in market:
                qty_to_sell = int(qty_to_sell // 100) * 100
                if qty_to_sell > stock: qty_to_sell = stock # Fix lỗi làm tròn
            
            if qty_to_sell > 0:
                stock -= qty_to_sell
                cash += qty_to_sell * price
                note = f"BÁN {int(percent*100)}%"
                display_label = "BÁN"

        # XỬ LÝ TRẠNG THÁI "KHÔNG MUA" vs "GIỮ"
        else: # Action là GIỮ
            if stock > 0:
                display_label = "✊ GIỮ" # Đang gồng lãi/lỗ
            else:
                display_label = "⛔ KHÔNG MUA" # Đang cầm tiền, đứng ngoài quan sát

        equity = cash + (stock * price)
        roi = (equity - capital) / capital

        # --- GHI VÀO NOTION ---
        # Điều kiện: Chưa tồn tại trong lịch sử
        if time_sig not in existing:
            icon = "⚪"
            if "MUA" in display_label: icon = "🟢"
            if "BÁN" in display_label: icon = "🔴"
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
                    "Số Lượng": {"number": qty if note else 0}, # Chỉ ghi số lượng nếu có lệnh
                    "Số Dư": {"number": equity},
                    "ROI": {"number": roi},
                    "Giờ Giao Dịch": {"date": {"start": dt.isoformat()}} 
                }
            }
            notion_request("pages", "POST", payload)
            print(f"   ✅ [GHI] {title} | ROI: {roi:.2%}")
            existing.add(time_sig)
            new_logs_count += 1

    if new_logs_count == 0:
        print("   -> Dữ liệu đã đồng bộ (Không có lệnh mới).")

# --- MAIN ---
print("📡 Đang kết nối Notion...")
query = {"filter": {"property": "Trạng Thái", "status": {"equals": "Đang chạy"}}}
res = notion_request(f"databases/{CONFIG_DB_ID}/query", "POST", query)

if res and 'results' in res:
    print(f"✅ Tìm thấy {len(res['results'])} chiến dịch.")
    for cfg in res['results']: run_campaign(cfg)
else:
    print("❌ Lỗi kết nối Notion. Check Token/ID.")
