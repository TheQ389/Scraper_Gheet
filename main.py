import time
import random
import re
import urllib3
import requests
import urllib.parse
from bs4 import BeautifulSoup
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import json
import socket
import psutil  # pip install psutil — đo RAM thực tế

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
socket.setdefaulttimeout(60)

# ==========================================
# CẤU HÌNH DỰ ÁN
# ==========================================
PROJECT_NAME  = "ScrapMap"
VERSION       = "6.5 - Live Sync Filter (Cập nhật Blacklist tức thì)"
CREDENTIALS_FILE = "credentials.json"
SHEET_ID      = "1fRPPqaQ30wwcnRoZ0_vadEGQonfbYFFMIjFyBOHNsHA"
BATCH_SIZE    = 20
MEMORY_LIMIT_MB      = 1200   # Khởi động lại Chrome khi RAM process vượt ngưỡng này
MAX_ROWS_PER_TAB     = 50000
LOCK_TIMEOUT_SEC     = 600    # 10 phút
N8N_WEBHOOK_URL      = "https://driver.flowhost.vn/webhook/scraper_map_notify_n8n"

# --- CSS SELECTORS ---
LINK_SELECTOR     = "a.hfpxzc"
PHONE_SELECTOR    = 'button[data-item-id^="phone:tel:"]'
WEB_SELECTOR      = 'a[data-item-id="authority"]'
ADDRESS_SELECTOR  = 'button[data-item-id="address"]'
TITLE_SELECTOR    = "h1.DUwDvf"
CATEGORY_SELECTOR = "button.DkEaL"
RATING_SELECTORS  = [
    'div.F7loa span[aria-hidden="true"]',   # Ưu tiên 1
    'span.MW4etd',                          # Ưu tiên 2 (fallback)
]

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def notify_n8n(payload: dict, timeout: int = 8):
    try: requests.post(N8N_WEBHOOK_URL, json=payload, timeout=timeout, verify=False)
    except: pass  

def classify_phone(phone: str):
    if not phone: return "", ""
    p = re.sub(r"\D", "", phone)
    if p.startswith("84") and len(p) >= 11: p = "0" + p[2:]
    p_sheet = f"'{p}"
    if len(p) == 10 and p.startswith(("03","05","07","08","09")): return p_sheet, "Di động"
    if len(p) in (10, 11) and p.startswith("02"): return p_sheet, "Máy bàn"
    if len(p) == 8 and p.startswith("1900"): return p_sheet, "Hotline"
    return p_sheet, "Khác"

def get_process_memory_mb() -> float:
    try: return psutil.Process().memory_info().rss / 1024 / 1024
    except: return 0.0

def safe_get_text(driver, css: str, attr: str = None) -> str:
    try:
        el = driver.find_element(By.CSS_SELECTOR, css)
        return (el.get_attribute(attr) if attr else el.text).strip()
    except: return ""

def safe_find_elements(driver, css: str):
    try: return driver.find_elements(By.CSS_SELECTOR, css)
    except: return []

# ==========================================
# CLASS CHÍNH
# ==========================================

class GSheetScraper:
    def __init__(self):
        self.processed_phones: set = set()
        self.processed_urls:   set = set()
        self.batch_data:    list = []
        self.search_count:  int  = 0
        self.driver         = None
        self.wait           = None
        self.current_sheet_num  = 1
        self.current_row_count  = 0
        self.machine_name = f"{socket.gethostname()}_{random.randint(1000, 9999)}"
        self.start_time         = time.time()
        self.total_saved        = 0

        self._print_banner()
        self._load_dedup_from_sheet() 
        self.setup_browser()
        self.connect_sheet()

    def _print_banner(self):
        print("=" * 60)
        print(f"🚀 {PROJECT_NAME} {VERSION}")
        print(f"💻 Machine ID: {self.machine_name}")
        print("=" * 60)

    def log(self, msg: str):
        ram = get_process_memory_mb()
        elapsed = int(time.time() - self.start_time)
        h, m, s = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60
        print(f"[{datetime.now():%H:%M:%S}] [{self.machine_name.split('_')[0]}] [RAM:{ram:.0f}MB] [{h:02d}:{m:02d}:{s:02d}] {msg}")

    def _api_retry(self, func, *args, **kwargs):
        max_attempts = 5
        for attempt in range(max_attempts):
            try: return func(*args, **kwargs)
            except Exception as e:
                err_msg = str(e).lower()
                if any(x in err_msg for x in ["429", "quota", "too many"]):
                    sleep_time = (2 ** attempt) + random.uniform(1, 3)
                    self.log(f"⚠️ Quá tải API Google. Chờ {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
                else:
                    if attempt == max_attempts - 1: raise e
                    time.sleep(2)
        return None

    def _load_dedup_from_sheet(self):
        self.log("⏳ Đang nạp dữ liệu chống trùng từ Google Sheet...")
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
            client = gspread.authorize(creds)
            sheet  = self._api_retry(client.open_by_key, SHEET_ID)
            for ws in self._api_retry(sheet.worksheets):
                if not ws.title.startswith("KetQua"): continue
                p_list = self._api_retry(ws.col_values, 5)[1:]  
                w_list = self._api_retry(ws.col_values, 7)[1:]
                for p in p_list: 
                    if p: self.processed_phones.add(p.strip())
                for w in w_list:
                    if w: self.processed_urls.add(w.strip().split("?")[0])
            self.log(f"✅ Đã nạp {len(self.processed_phones)} SĐT & {len(self.processed_urls)} URL.")
        except Exception as e: self.log(f"⚠️ Lỗi dedup: {e}")

    def setup_browser(self):
        if self.driver: 
            try: self.driver.quit()
            except: pass
        self.log("🚀 Khởi động Chrome...")
        opts = webdriver.ChromeOptions()
        opts.page_load_strategy = "eager"
        opts.add_argument("--lang=vi")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        svc = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=svc, options=opts)
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"})
        self.driver.set_page_load_timeout(25)
        self.wait = WebDriverWait(self.driver, 15)
        self.search_count = 0

    def _should_restart_browser(self) -> bool:
        return get_process_memory_mb() > MEMORY_LIMIT_MB

    def connect_sheet(self):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        self.client = gspread.authorize(creds)
        self.sheet  = self._api_retry(self.client.open_by_key, SHEET_ID)
        self.ws_kw  = self._api_retry(self.sheet.worksheet, "TuKhoa")
        self.ws_loc = self._api_retry(self.sheet.worksheet, "DiaDiem")
        self._find_or_create_output_sheet()

    def _find_or_create_output_sheet(self):
        max_num = 0; latest_ws = None
        for ws in self._api_retry(self.sheet.worksheets):
            t = ws.title
            n = 1 if t == "KetQua" else (int(t[7:]) if t.startswith("KetQua ") and t[7:].isdigit() else 0)
            if n >= max_num: max_num = n; latest_ws = ws
        if latest_ws is None: latest_ws = self._create_result_sheet(1); max_num = 1
        self.ws_out = latest_ws; self.current_sheet_num = max_num
        try: self.current_row_count = len(self._api_retry(self.ws_out.col_values, 1))
        except: self.current_row_count = 0

    def _create_result_sheet(self, num: int):
        title = "KetQua" if num == 1 else f"KetQua {num}"
        ws = self._api_retry(self.sheet.add_worksheet, title=title, rows=1000, cols=10)
        self._api_retry(ws.append_row, ["Từ Khóa","Địa Điểm","Tên Quán","Ngành Nghề","Số Điện Thoại","Loại SĐT","Website","Email","Đánh Giá","Máy Cào"])
        return ws

    def _check_and_rotate_sheet(self):
        if self.current_row_count >= MAX_ROWS_PER_TAB:
            self.current_sheet_num += 1
            try: self.ws_out = self._create_result_sheet(self.current_sheet_num); self.current_row_count = 1
            except Exception as e: self.log(f"❌ Lỗi tạo tab: {e}")

    def flush_batch(self, force: bool = False):
        if not (len(self.batch_data) >= BATCH_SIZE or (force and self.batch_data)): return
        try:
            self._check_and_rotate_sheet()
            self._api_retry(self.ws_out.append_rows, self.batch_data, value_input_option='USER_ENTERED', table_range='A1')
            self.current_row_count += len(self.batch_data); self.total_saved += len(self.batch_data)
            self.log(f"✅ Đã lưu {len(self.batch_data)} dòng. (Session: {self.total_saved})")
            self.batch_data.clear()
        except Exception as e: self.log(f"❌ Ghi Sheet lỗi: {e}")

    def update_cell(self, worksheet, row, col, value):
        try: self._api_retry(worksheet.update_cell, row, col, value)
        except: pass

    # ─── MAIN RUN ─────────────────────────────────────────────
    def run(self, limit_per_location=120):
        while True:
            try: keywords = self._api_retry(self.ws_kw.get_all_values)[1:] 
            except: time.sleep(5); continue

            has_work = False
            for kw_index, kw_data in enumerate(keywords):
                kw_row = kw_index + 2
                kw = kw_data[0]
                
                # Logic xác định cột Trạng thái & Blacklist động
                col_b = str(kw_data[1]).strip() if len(kw_data) > 1 else ""
                col_c = str(kw_data[2]).strip() if len(kw_data) > 2 else ""
                is_new_form = col_b.lower() not in ["done", "running...", "error", ""]
                kw_status = col_c if is_new_form else col_b
                
                if kw_status.lower() == "done": continue 
                has_work = True
                self.log(f"🔥 Từ khóa: {kw}")

                while True:
                    # Đọc lại địa điểm
                    try: locations = self._api_retry(self.ws_loc.get_all_values)[1:]
                    except: time.sleep(5); continue

                    target_row = -1; target_data = None; other_running = False; now = int(time.time())
                    for loc_row, loc_data in enumerate(locations, start=2):
                        status = str(loc_data[3]).strip() if len(loc_data) > 3 else ""
                        if status in ["Done", "No Result", "Skip", "Error"]: continue
                        if status == "": target_row, target_data = loc_row, loc_data; break
                        elif status.startswith("Running..."):
                            parts = status.split("|")
                            if (len(parts) >= 2 and self.machine_name in parts[1]) or (len(parts) >= 3 and now - int(parts[2]) > LOCK_TIMEOUT_SEC):
                                target_row, target_data = loc_row, loc_data; break
                            else: other_running = True

                    if target_row != -1:
                        # --- LIVE SYNC BLACKLIST: Đọc lại Blacklist từ Sheet ngay trước khi cào ---
                        try:
                            # Đọc đúng dòng hiện tại của từ khóa để lấy Blacklist mới nhất
                            latest_kw_row = self._api_retry(self.ws_kw.row_values, kw_row)
                            raw_bl = latest_kw_row[1] if is_new_form else ""
                            current_blacklist = [w.strip().lower() for w in raw_bl.split(',') if w.strip()]
                        except: current_blacklist = []

                        time.sleep(random.uniform(1.0, 3.5))
                        lock_flag = f"Running... | {self.machine_name} | {now}"
                        self.update_cell(self.ws_loc, target_row, 4, lock_flag)
                        
                        time.sleep(random.uniform(2.0, 4.0)) 
                        try:
                            if self._api_retry(self.ws_loc.cell, target_row, 4).value != lock_flag: continue 
                        except: pass
                        
                        self.batch_data.clear() 
                        tinh, quan, phuong = target_data[0], target_data[1], target_data[2]
                        search_query = f"{kw} {phuong} {quan} {tinh}"
                        self.log(f"🎯 Cào: {search_query} (BL: {len(current_blacklist)} từ)")

                        try:
                            try: self.driver.get(f"https://www.google.com/maps/search/{urllib.parse.quote(search_query)}")
                            except: self.driver.execute_script("window.stop();")
                            
                            time.sleep(random.uniform(4, 6))
                            if "Không tìm thấy kết quả" in self.driver.page_source:
                                self.update_cell(self.ws_loc, target_row, 4, "No Result"); continue

                            all_urls = []
                            if safe_find_elements(self.driver, TITLE_SELECTOR) and "search" not in self.driver.current_url:
                                all_urls.append(self.driver.current_url.split('?')[0])
                            else:
                                scroll_pane = self.driver.execute_script("return document.querySelector('div[role=\"feed\"]') || document.querySelector('div.m6QErb[aria-label]');")
                                if scroll_pane:
                                    last_h = 0
                                    for _ in range(15):
                                        for el in safe_find_elements(self.driver, LINK_SELECTOR):
                                            h = el.get_attribute("href")
                                            if h:
                                                u = h.split('?')[0]
                                                if u not in all_urls: all_urls.append(u)
                                        if len(all_urls) >= limit_per_location: break
                                        self.driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight);", scroll_pane)
                                        time.sleep(3.5)
                                        new_h = self.driver.execute_script("return arguments[0].scrollHeight", scroll_pane); if new_h == last_h: break 
                                        last_h = new_h

                            for url in all_urls[:limit_per_location]:
                                if url in self.processed_urls: continue
                                try:
                                    self.driver.execute_script(f"window.location.href = '{url}';")
                                    self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, TITLE_SELECTOR)))
                                    try: self.driver.execute_script("window.stop();")
                                    except: pass
                                    self.processed_urls.add(url)
                                    name = safe_get_text(self.driver, TITLE_SELECTOR)
                                    cat = safe_get_text(self.driver, CATEGORY_SELECTOR)
                                    # Lọc rác real-time
                                    if any(w in cat.lower() or w in name.lower() for w in current_blacklist): continue

                                    addr = safe_get_text(self.driver, ADDRESS_SELECTOR, attr="aria-label").replace("Địa chỉ: ", "").strip() or f"{phuong}, {quan}, {tinh}"
                                    p_raw = safe_get_text(self.driver, PHONE_SELECTOR, attr="data-item-id").replace("phone:tel:", "")
                                    web = safe_get_text(self.driver, WEB_SELECTOR, attr="href").split("?")[0]
                                    rating = ""
                                    for r_css in RATING_SELECTORS:
                                        rt = safe_get_text(self.driver, r_css); if rt: rating = rt; break
                                    p_clean, p_type = classify_phone(p_raw)
                                    if p_clean and p_clean in self.processed_phones: continue
                                    if p_clean: self.processed_phones.add(p_clean)
                                    self.batch_data.append([kw, addr, name, cat, p_clean, p_type, web, "", rating, self.machine_name.split('_')[0]])
                                    self.flush_batch()
                                except: continue

                            self.update_cell(self.ws_loc, target_row, 4, "Done")
                            self.flush_batch(force=True)
                        except: self.update_cell(self.ws_loc, target_row, 4, "Error"); continue
                        
                        try:
                            if self._should_restart_browser(): self.setup_browser()
                        except: pass
                    elif other_running: time.sleep(30)
                    else: break

                self.update_cell(self.ws_kw, kw_row, (3 if is_new_form else 2), "Done")
            
            if not has_work: break 
            time.sleep(10)

if __name__ == "__main__":
    while True:
        try: bot = GSheetScraper(); bot.run(200); break 
        except Exception as e: print(f"Crash: {e}"); time.sleep(10)
