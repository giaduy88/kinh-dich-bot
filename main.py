import os
import requests
import pandas as pd
import time
import re
import io
import sys
import math
from datetime import datetime, timezone, timedelta

# Import Module Backtest
try: from backtest import run_backtest_core
except ImportError: pass

# --- 1. CẤU HÌNH ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
CONFIG_DB_ID = os.environ.get("CONFIG_DB_ID")
LOG_DB_ID    = os.environ.get("LOG_DB_ID")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# --- PARAMETERS ---
STOP_LOSS_PCT = -0.07
TAKE_PROFIT_PCT = 0.15
USD_VND_RATE = 25300 
FEE_CRYPTO = 0.001   # 0.1%
FEE_STOCK = 0.0015   # 0.15%

if not NOTION_TOKEN or not CONFIG_DB_ID or not LOG_DB_ID:
    print("❌ [BUILD LOG] LỖI: Thiếu Notion Secrets.")
    sys.exit(1)

def extract_id(text):
    match = re.search(r'([a-f0-9]{32})', (text or "").replace("-", ""))
    return match.group(1) if match else text

CONFIG_DB_ID = extract_id(CONFIG_DB_ID)
LOG_DB_ID = extract_id(LOG_DB_ID)

BACKUP_CSV = """KEY_ID,Lời Khuyên
G1-B1,Đại cát đại lợi, thời cơ chín muồi. Nên mua tất tay.
G1-B43,Nguy hiểm rình rập, bán tháo ngay lập tức.
G1-B14,Vận khí tốt, có thể mua vào tích lũy.
G23-B4,Mông lung xấu, nên hạ tỷ trọng bán bớt."""

try:
    import ccxt
    from lunardate import LunarDate
except ImportError: pass

# --- UTILS ---
def send_telegram_message(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except: pass

def check_telegram_command(adv_map):
    if not TELEGRAM_TOKEN: return
    print("📩 [BUILD LOG] Đang kiểm tra tin nhắn Telegram...")
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        res = requests.get(url, timeout=10).json()
        if not res.get('ok') or not res.get('result'): return
        
        last_msg = res['result'][-1]
        msg_date = last_msg.get('message', {}).get('date', 0)
        text = last_msg.get('message', {}).get('text', '').strip()
        
        if int(time.time()) - msg_date > 600: return

        if text.lower().startswith('bp '):
            try:
                parts = text.split()
                if len(parts) >= 2:
                    symbol = parts[1].upper()
                    days = int(parts[2]) if len(parts) > 2 else 90
                    print(f"   -> ⚙️ Backtest: {symbol} ({days} ngày)")
                    send_telegram_message(f"⏳ <b>Đang chạy Backtest cho {symbol}...</b>")
                    report = run_backtest_core(symbol, days, adv_map)
                    send_telegram_message(report)
            except Exception as e:
                send_telegram_message(f"❌ Lỗi lệnh: {str(e)}")
    except Exception as e: print(f"❌ [BUILD LOG] Lỗi Telegram: {e}")

def get_stock_data(symbol):
    try:
        to_ts = int(time.time())
        from_ts = to_ts - (30 * 24 * 3600) 
        url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/stock?symbol={symbol}&resolution=1H&from={from_ts}&to={to_ts}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).json()
        data = []
        if 't' in res and res['t']:
            for i in range(len(res['t'])):
                data.append({"t": datetime.fromtimestamp(res['t'][i], tz=timezone(timedelta(hours=7))), "p": float(res['c'][i])})
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
    except: return None

    print(f"\n🚀 Processing: {name} ({symbol})")
    
    is_crypto = "Binance" in market or "Crypto" in market
    data_raw = []
    
    if is_crypto:
        try:
            ex = ccxt.kucoin()
            ohlcv = ex.fetch_ohlcv(symbol if "/USDT" in symbol else symbol+"/USDT", '1h', limit=500)
            for c in ohlcv: data_raw.append({"t": datetime.fromtimestamp(c[0]/1000, tz=timezone(timedelta(hours=7))), "p": c[4]})
        except: pass
    elif "Stock" in market or "VNIndex" in market:
        data_raw = get_stock_data(symbol)

    if not data_raw:
        print(f"❌ [BUILD LOG] Không tìm thấy dữ liệu giá cho {symbol}.")
        return None

    data_full = add_technical_indicators(data_raw)
    data_to_trade = data_full[-48:] 
    df_adv = load_advice_data()
    adv_map = dict(zip(df_adv['KEY_ID'], df_adv['Lời Khuyên']))
    existing = get_existing_signatures(symbol)
    
    cash, stock, avg_price = capital, 0, 0
    new_logs_count = 0
    fee_rate = FEE_CRYPTO if is_crypto else FEE_STOCK

    # --- SIMULATION ---
    for i, item in enumerate(data_to_trade):
        dt, price = item['t'], item['p']
        sma20, rsi = item.get('SMA20', 0), item.get('RSI', 50)
        time_sig = dt.strftime('%H:%M %d/%m')
        holding_pnl = (price - avg_price) / avg_price if (stock > 0 and avg_price > 0) else 0
        
        # Biến cờ kiểm tra nến cuối cùng
        is_last_candle = (i == len(data_to_trade) - 1)

        key = calculate_hexagram(dt)
        advice = adv_map.get(key, "")
        if not advice: advice = f"Quẻ {key} (Chưa có lời khuyên)"
        
        action, percent = analyze_smart_action(advice)
        risk_action, risk_reason, tech_reason = None, "", ""
        
        if stock > 0:
            if holding_pnl <= STOP_LOSS_PCT: risk_action, risk_reason = "STOP_LOSS", f"⚠️ CẮT LỖ (Lỗ {holding_pnl:.1%})"
            elif holding_pnl >= TAKE_PROFIT_PCT: risk_action, risk_reason = "TAKE_PROFIT", f"💰 CHỐT LỜI (Lãi {holding_pnl:.1%})"

        tech_status = "OK"
        if action == "MUA":
            if price < sma20 and rsi > 35: action, tech_status, tech_reason = "GIỮ", "BAD_TECH", f"(⛔ Giá < SMA20 & RSI={rsi:.0f})"
            if rsi > 75: action, tech_status, tech_reason = "GIỮ", "OVERBOUGHT", f"(⛔ RSI={rsi:.0f} Quá mua)"

        final_action, final_percent = action, percent
        if risk_action == "STOP_LOSS": final_action, final_percent = "BÁN", 1.0
        elif risk_action == "TAKE_PROFIT": final_action, final_percent = "BÁN", 0.5
        
        display_label, qty, note = "GIỮ", 0, ""

        if final_action == "MUA":
            amt = cash * final_percent
            if amt > 1:
                qty = amt / price
                if not is_crypto: qty = int(qty // 100) * 100
                if qty > 0:
                    buy_val = qty * price
                    fee_val = buy_val * fee_rate
                    current_val = stock * avg_price
                    stock += qty
                    avg_price = (current_val + buy_val) / stock 
                    cash -= (buy_val + fee_val)
                    display_label = "MUA"
            if display_label != "MUA" and stock > 0: display_label = "✊ GIỮ"

        elif final_action == "BÁN":
            qty_sell = stock * final_percent
            if not is_crypto: qty_sell = int(qty_sell // 100) * 100
            if qty_sell > stock: qty_sell = stock
            if qty_sell > 0:
                sell_val = qty_sell * price
                fee_val = sell_val * fee_rate
                stock -= qty_sell
                cash += (sell_val - fee_val)
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

        # PHẦN 1: GHI NOTION (Chỉ ghi khi chưa có)
        if time_sig not in existing:
            icon = "⚪"
            if "MUA" in display_label: icon = "🟢"
            elif "BÁN" in display_label: icon = "🔴"
            elif "CẮT LỖ" in display_label: icon = "⚠️"
            elif "CHỐT LỜI" in display_label: icon = "💰"
            elif "GIỮ" in display_label: icon = "✊"
            elif "KHÔNG MUA" in display_label: icon = "⛔"
            
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
            existing.add(time_sig)
            new_logs_count += 1
            print(f"   ✅ [GHI] {title}")

        # PHẦN 2: GỬI TELEGRAM (Chỉ gửi ở nến cuối cùng của phiên chạy)
        # Tách biệt hoàn toàn với việc Ghi Notion
        if is_last_candle:
            icon = "⚪"
            if "MUA" in display_label: icon = "🟢"
            elif "BÁN" in display_label: icon = "🔴"
            elif "CẮT LỖ" in display_label: icon = "⚠️"
            elif "CHỐT LỜI" in display_label: icon = "💰"
            elif "GIỮ" in display_label: icon = "✊"
            elif "KHÔNG MUA" in display_label: icon = "⛔"

            final_reason = advice
            if risk_reason: final_reason = risk_reason
            elif tech_reason: final_reason = tech_reason
            
            msg = (
                f"🔔 <b>TÍN HIỆU: {symbol}</b>\n"
                f"{icon} <b>Lệnh:</b> {display_label}\n"
                f"⏰ <b>Time:</b> {time_sig}\n"
                f"💵 <b>Giá:</b> {price:,.2f}\n"
                f"📊 <b>Chỉ số:</b> RSI {rsi:.0f} | SMA20 {sma20:,.0f}\n"
                f"💡 <b>Lý do:</b> {final_reason}"
            )
            send_telegram_message(msg)

    if new_logs_count == 0:
        print(f"✅ [BUILD LOG] Dữ liệu đã đồng bộ (Không ghi thêm vào Notion).")

    last_item = data_to_trade[-1]
    equity_final = cash + (stock * last_item['p'])
    pnl_value = (last_item['p'] - avg_price) * stock if stock > 0 else 0
    pnl_percent = (last_item['p'] - avg_price) / avg_price if (stock > 0 and avg_price > 0) else 0

    return {
        "symbol": symbol, "price": last_item['p'], "equity": equity_final, "cash": cash, "stock_amt": stock,
        "roi": (equity_final - capital) / capital, "pnl_percent": pnl_percent, "pnl_value": pnl_value,
        "hold": stock > 0, "rsi": last_item.get('RSI', 50), "type": "CRYPTO" if is_crypto else "STOCK"
    }

print("📡 Đang khởi động...")
df_adv = load_advice_data()
adv_map = dict(zip(df_adv['KEY_ID'], df_adv['Lời Khuyên']))
check_telegram_command(adv_map)
query = {"filter": {"property": "Trạng Thái", "status": {"equals": "Đang chạy"}}}
res = notion_request(f"databases/{CONFIG_DB_ID}/query", "POST", query)
daily_stats = []
if res and 'results' in res:
    print(f"✅ Tìm thấy {len(res['results'])} chiến dịch.")
    for cfg in res['results']: 
        stat = run_campaign(cfg)
        if stat: daily_stats.append(stat)

now_utc = datetime.now(timezone.utc)
now_vn = now_utc + timedelta(hours=7)
if now_vn.hour == 6 and daily_stats:
    total_nav_vnd, total_cash_vnd, total_cash_usd = 0, 0, 0
    list_stock, list_crypto = [], []
    for s in daily_stats:
        if s['type'] == "STOCK":
            total_nav_vnd += s['equity']; total_cash_vnd += s['cash']; list_stock.append(s)
        else:
            total_nav_vnd += s['equity'] * USD_VND_RATE; total_cash_usd += s['cash']; list_crypto.append(s)

    daily_key = calculate_hexagram(now_vn)
    daily_advice = adv_map.get(daily_key, "Vận khí bình ổn.")
    msg = f"☕ <b>MORNING BRIEFING</b> - {now_vn.strftime('%d/%m/%Y')}\n--------------------------\n💰 <b>TỔNG NAV: {total_nav_vnd/1e6:,.1f} tr</b>\n💵 <b>Tiền mặt khả dụng:</b>\n   • VNĐ: {total_cash_vnd:,.0f} đ\n   • USD: {total_cash_usd:,.2f} $\n--------------------------\n\n"
    if list_stock:
        msg += "🇻🇳 <b>CHỨNG KHOÁN:</b>\n"
        for i, s in enumerate(list_stock, 1):
            status = f"✊ Giữ {s['stock_amt']:,.0f} cp" if s['hold'] else "⚪ Full Cash"
            msg += f"{i}. <b>{s['symbol']}</b>: {'🟢' if s['pnl_percent']>=0 else '🔴'} {s['pnl_percent']:+.2%}\n   • Vị thế: {status}\n\n"
    if list_crypto:
        msg += "🌍 <b>CRYPTO:</b>\n"
        for i, s in enumerate(list_crypto, 1):
            status = f"✊ Giữ {s['stock_amt']:.4f}" if s['hold'] else "⚪ Full Cash"
            msg += f"{i}. <b>{s['symbol']}</b>: {'🟢' if s['pnl_percent']>=0 else '🔴'} {s['pnl_percent']:+.2%}\n   • Vị thế: {status}\n\n"
    msg += f"🔮 <b>QUẺ NGÀY ({daily_key}):</b>\n<i>{daily_advice}</i>"
    send_telegram_message(msg)
