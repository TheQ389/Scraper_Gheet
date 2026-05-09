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
VERSION       = "6.4 - Dynamic Blacklist (Tùy biến theo Google Sheet)"
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
# HELPER FUNCTIONS (Stateless / Module-level)
# ==========================================

def notify_n8n(payload: dict, timeout: int = 8):
    """Gửi thông báo lên N8N Webhook — không block nếu lỗi."""
    try:
        requests.post(N8N_WEBHOOK_URL, json=payload, timeout=timeout, verify=False)
    except Exception:
        pass  

def classify_phone(phone: str):
    """Làm sạch & phân loại số điện thoại. Trả về (p_sheet, type_str)."""
    if not phone:
        return "", ""
    p = re.sub(r"\D", "", phone)
    if p.startswith("84") and len(p) >= 11:
        p = "0" + p[2:]
    p_sheet = f"'{p}"
    if len(p) == 10 and p.startswith(("03","05","07","08","09")):
        return p_sheet, "Di động"
    if len(p) in (10, 11) and p.startswith("02"):
        return p_sheet, "Máy bàn"
    if len(p) == 8 and p.startswith("1900"):
        return p_sheet, "Hotline"
    return p_sheet, "Khác"

def get_process_memory_mb() -> float:
    """Trả về RAM (MB) của process Python hiện tại."""
    try:
        return psutil.Process().memory_info().rss / 1024 / 1024
    except Exception:
        return 0.0

def safe_get_text(driver, css: str, attr: str = None) -> str:
    """Lấy text / attribute từ element, không raise exception."""
    try:
        el = driver.find_element(By.CSS_SELECTOR, css)
        return (el.get_attribute(attr) if attr else el.text).strip()
    except Exception:
        return ""

def safe_find_elements(driver, css: str):
    """find_elements không raise exception."""
    try:
        return driver.find_elements(By.CSS_SELECTOR, css)
    except Exception:
        return []

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
        
        # Tạo ID máy duy nhất để tránh trùng tên khi clone máy (vd: ServerVNP_xyz123)
        self.machine_name = f"{socket.gethostname()}_{random.randint(1000, 9999)}"
        
        self.start_time         = time.time()
        self.total_saved        = 0

        self._print_banner()
        self._load_dedup_from_sheet() 
        self.setup_browser()
        self.connect_sheet()

    # ─── Banner & Logging ───────────────────────────────────────────
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

    # ─── API RETRY WRAPPER (CHỐNG SẬP KHI 10 MÁY CÙNG CHỌC API) ───
    def _api_retry(self, func, *args, **kwargs):
        """Bọc các hàm Google API, tự động lùi thời gian nếu bị lỗi Quá tải (429)"""
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                err_msg = str(e).lower()
                if "429" in err_msg or "quota" in err_msg or "too many requests" in err_msg:
                    sleep_time = (2 ** attempt) + random.uniform(1, 3) # Backoff: 2s, 4s, 8s...
                    self.log(f"⚠️ Quá tải API Google. Đang lùi bước chờ {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
                else:
                    if attempt == max_attempts - 1:
                        raise e
                    time.sleep(2)
        return None

    # ─── Nạp dữ liệu chống trùng ─────────────────────────────────
    def _load_dedup_from_sheet(self):
        self.log("⏳ Đang nạp dữ liệu chống trùng từ Google Sheet...")
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
            client = gspread.authorize(creds)
            sheet  = self._api_retry(client.open_by_key, SHEET_ID)
            
            for ws in self._api_retry(sheet.worksheets):
                if not ws.title.startswith("KetQua"): continue
                
                phones   = self._api_retry(ws.col_values, 5)[1:]  
                websites = self._api_retry(ws.col_values, 7)[1:]
                
                for p in phones:
                    if p: self.processed_phones.add(p.strip())
                for w in websites:
                    if w: self.processed_urls.add(w.strip().split("?")[0])
                    
            self.log(f"✅ Đã nạp {len(self.processed_phones)} SĐT & {len(self.processed_urls)} URL đã có.")
        except Exception as e:
            self.log(f"⚠️ Không nạp được dedup data: {e}")

    # ─── Browser ──────────────────────────────────────────────────
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
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        })
        self.driver.set_page_load_timeout(25)
        self.wait = WebDriverWait(self.driver, 15)
        self.search_count = 0

    def _should_restart_browser(self) -> bool:
        return get_process_memory_mb() > MEMORY_LIMIT_MB

    # ─── Google Sheet Setup ───────────────────────────────────────
    def connect_sheet(self):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        self.client = gspread.authorize(creds)
        self.sheet  = self._api_retry(self.client.open_by_key, SHEET_ID)
        self.ws_kw  = self._api_retry(self.sheet.worksheet, "TuKhoa")
        self.ws_loc = self._api_retry(self.sheet.worksheet, "DiaDiem")
        self._find_or_create_output_sheet()

    def _find_or_create_output_sheet(self):
        max_num = 0
        latest_ws = None
        for ws in self._api_retry(self.sheet.worksheets):
            t = ws.title
            if t == "KetQua": n = 1
            elif t.startswith("KetQua ") and t[7:].isdigit(): n = int(t[7:])
            else: continue
            
            if n >= max_num:
                max_num = n
                latest_ws = ws

        if latest_ws is None:
            latest_ws = self._create_result_sheet(1)
            max_num   = 1

        self.ws_out = latest_ws
        self.current_sheet_num = max_num
        try: self.current_row_count = len(self._api_retry(self.ws_out.col_values, 1))
        except: self.current_row_count = 0

    def _create_result_sheet(self, num: int):
        title = "KetQua" if num == 1 else f"KetQua {num}"
        ws = self._api_retry(self.sheet.add_worksheet, title=title, rows=1000, cols=10)
        self._api_retry(ws.append_row, [
            "Từ Khóa","Địa Điểm","Tên Quán","Ngành Nghề",
            "Số Điện Thoại","Loại SĐT","Website","Email",
            "Đánh Giá","Máy Cào"
        ])
        return ws

    def _check_and_rotate_sheet(self):
        if self.current_row_count >= MAX_ROWS_PER_TAB:
            self.current_sheet_num += 1
            self.log(f"📄 Tạo tab mới: KetQua {self.current_sheet_num}")
            try:
                self.ws_out = self._create_result_sheet(self.current_sheet_num)
                self.current_row_count = 1
            except Exception as e:
                self.log(f"❌ Lỗi tạo tab: {e}")

    # ─── Ghi dữ liệu & Update Status ─────────────────────────────
    def flush_batch(self, force: bool = False):
        if not (len(self.batch_data) >= BATCH_SIZE or (force and self.batch_data)):
            return
            
        try:
            self._check_and_rotate_sheet()
            self._api_retry(self.ws_out.append_rows, self.batch_data, value_input_option='USER_ENTERED', table_range='A1')
            self.current_row_count += len(self.batch_data)
            self.total_saved += len(self.batch_data)
            self.log(f"✅ Đã lưu {len(self.batch_data)} dòng. (Tổng session: {self.total_saved})")
            self.batch_data.clear()
        except Exception as e:
            self.log(f"❌ Ghi Sheet thất bại. Dữ liệu vẫn giữ trong RAM chờ đợt sau. Chi tiết: {e}")
            try: self.connect_sheet() 
            except: pass

    def update_cell(self, worksheet, row, col, value):
        try: 
            self._api_retry(worksheet.update_cell, row, col, value)
        except Exception: 
            pass

    # ─── MAIN RUN ─────────────────────────────────────────────
    def run(self, limit_per_location=120):
        try: 
            keywords = self._api_retry(self.ws_kw.get_all_values)[1:] 
        except Exception as e: 
            self.log(f"❌ Lỗi đọc Sheet Từ Khóa: {e}")
            return

        for kw_index, kw_data in enumerate(keywords):
            kw_row = kw_index + 2
            kw = kw_data[0]
            
            # --- XỬ LÝ NHẬN DIỆN CẤU TRÚC SHEET ĐỘNG ---
            val_col_b = str(kw_data[1]).strip() if len(kw_data) > 1 else ""
            val_col_c = str(kw_data[2]).strip() if len(kw_data) > 2 else ""
            
            # Nếu user chưa thêm cột "Từ Khóa Loại Trừ" (Form cũ)
            if val_col_b.lower() in ["done", "running...", "error", ""]:
                kw_status = val_col_b
                raw_blacklist = ""
            else:
                # Nếu đã thêm cột "Từ Khóa Loại Trừ" ở Cột B, Trạng thái ở Cột C (Form mới)
                raw_blacklist = val_col_b
                kw_status = val_col_c

            if kw_status.lower() == "done": continue 
            
            # Tách các từ khóa loại trừ thành mảng
            current_blacklist = [w.strip().lower() for w in raw_blacklist.split(',') if w.strip()]

            self.log(f"🔥 BẮT ĐẦU TỪ KHÓA MỚI: {kw}")
            if current_blacklist:
                self.log(f"   -> 🛡️ Bộ lọc rác kích hoạt: {current_blacklist}")

            while True:
                self.log("🔄 Đang tải danh sách Địa điểm...")
                try: 
                    locations = self._api_retry(self.ws_loc.get_all_values)[1:]
                except: 
                    time.sleep(5); continue

                target_row = -1
                target_data = None
                other_running = False
                now = int(time.time())

                for loc_row, loc_data in enumerate(locations, start=2):
                    status = str(loc_data[3]).strip() if len(loc_data) > 3 else ""
                    if status in ["Done", "No Result", "Skip", "Error"]: continue
                    
                    if status == "":
                        target_row, target_data = loc_row, loc_data
                        break
                    elif status.startswith("Running..."):
                        parts = status.split("|")
                        if (len(parts) >= 2 and self.machine_name in parts[1]) or (len(parts) >= 3 and now - int(parts[2]) > LOCK_TIMEOUT_SEC):
                            target_row, target_data = loc_row, loc_data
                            break
                        else: other_running = True

                if target_row != -1:
                    time.sleep(random.uniform(1.0, 3.5))
                    
                    lock_flag = f"Running... | {self.machine_name} | {now}"
                    self.update_cell(self.ws_loc, target_row, 4, lock_flag)
                    
                    time.sleep(random.uniform(2.0, 4.0)) 
                    try:
                        verify_status = self._api_retry(self.ws_loc.cell, target_row, 4).value
                        if verify_status != lock_flag:
                            self.log(f"⚠️ Tranh chấp! Máy khác đã nhanh tay giành dòng {target_row}. Rút lui...")
                            continue 
                    except Exception:
                        pass 
                    
                    self.batch_data.clear() 
                    tinh, quan, phuong = target_data[0], target_data[1], target_data[2]
                    
                    search_query = f"{kw} {phuong} {quan} {tinh}"
                    self.log(f"🎯 Nhận độc quyền cào: {search_query}")

                    try:
                        try: self.driver.get(f"https://www.google.com/maps/search/{urllib.parse.quote(search_query)}")
                        except: self.driver.execute_script("window.stop();")
                        
                        self.random_sleep(4, 6)
                        if "Không tìm thấy kết quả" in self.driver.page_source:
                            self.update_cell(self.ws_loc, target_row, 4, "No Result")
                            continue

                        all_urls = []
                        if safe_find_elements(self.driver, TITLE_SELECTOR) and "search" not in self.driver.current_url:
                            all_urls.append(self.driver.current_url.split('?')[0])
                        else:
                            try:
                                scroll_pane = self.driver.execute_script("return document.querySelector('div[role=\"feed\"]') || document.querySelector('div.m6QErb[aria-label]');")
                                if scroll_pane:
                                    last_h = 0
                                    for _ in range(15):
                                        for el in safe_find_elements(self.driver, LINK_SELECTOR):
                                            h = el.get_attribute("href")
                                            if h:
                                                clean_url = h.split('?')[0]
                                                if clean_url not in all_urls: 
                                                    all_urls.append(clean_url)
                                                    
                                        if len(all_urls) >= limit_per_location: break
                                        
                                        self.driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight);", scroll_pane)
                                        time.sleep(3.5)
                                        
                                        new_h = self.driver.execute_script("return arguments[0].scrollHeight", scroll_pane)
                                        if new_h == last_h: break 
                                        last_h = new_h
                            except: pass

                        all_urls = all_urls[:limit_per_location]
                        self.log(f"📋 Tìm thấy {len(all_urls)} quán. Đang trích xuất...")

                        for i, url in enumerate(all_urls):
                            if url in self.processed_urls:
                                continue
                                
                            try:
                                self.driver.execute_script(f"window.location.href = '{url}';")
                                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, TITLE_SELECTOR)))
                                try: self.driver.execute_script("window.stop();")
                                except: pass

                                self.processed_urls.add(url)

                                name = safe_get_text(self.driver, TITLE_SELECTOR)
                                cat = safe_get_text(self.driver, CATEGORY_SELECTOR)

                                # KIỂM TRA BỘ LỌC ĐỘNG (Từ Khóa Loại Trừ từ Google Sheet)
                                is_spam = False
                                if current_blacklist:
                                    for word in current_blacklist:
                                        if word in cat.lower() or word in name.lower():
                                            is_spam = True
                                            break
                                
                                if is_spam:
                                    self.log(f"  -> 🚫 Lọc bỏ do chứa từ rác: {name} ({cat})")
                                    continue

                                real_address = f"{phuong}, {quan}, {tinh}"
                                addr_text = safe_get_text(self.driver, ADDRESS_SELECTOR, attr="aria-label")
                                if addr_text:
                                    real_address = addr_text.replace("Địa chỉ: ", "").strip()

                                p_raw = safe_get_text(self.driver, PHONE_SELECTOR, attr="data-item-id").replace("phone:tel:", "")
                                web = safe_get_text(self.driver, WEB_SELECTOR, attr="href").split("?")[0]
                                
                                rating = ""
                                for r_css in RATING_SELECTORS:
                                    r_text = safe_get_text(self.driver, r_css)
                                    if r_text:
                                        rating = r_text
                                        break
                                if not rating:
                                    for s in safe_find_elements(self.driver, 'span[role="img"]'):
                                        aria = s.get_attribute("aria-label")
                                        if aria and ("sao" in aria.lower() or "star" in aria.lower()): 
                                            rating = aria.split(" ")[0]
                                            break

                                p_clean, p_type = classify_phone(p_raw)
                                
                                if p_clean and p_clean in self.processed_phones: continue
                                if p_clean: self.processed_phones.add(p_clean)

                                self.batch_data.append([kw, real_address, name, cat, p_clean, p_type, web, "", rating, self.machine_name.split('_')[0]])
                                self.flush_batch()
                            except Exception as e: 
                                continue

                        self.update_cell(self.ws_loc, target_row, 4, "Done")
                        self.flush_batch(force=True)
                        
                    except Exception as e:
                        self.update_cell(self.ws_loc, target_row, 4, "Error")
                        if any(x in str(e).lower() for x in ["reachable", "disconnected", "timeout"]): 
                            try: self.setup_browser()
                            except: pass
                        continue
                        
                    try:
                        self.search_count += 1
                        if self._should_restart_browser() or self.search_count >= RESTART_BROWSER_AFTER * 2:
                            self.log("♻️ Giải phóng RAM do vượt ngưỡng...")
                            self.setup_browser()
                    except Exception as e:
                        self.log(f"⚠️ Lỗi khi khởi động lại Chrome: {e}")
                        
                elif other_running: 
                    self.log("⏳ Hết dòng trống. Đang chờ các máy khác hoàn thành...")
                    time.sleep(30)
                else: break

            # Nếu dùng form mới (Trạng thái ở cột C - index 3), nếu dùng form cũ (Trạng thái ở cột B - index 2)
            status_col_index = 3 if val_col_b.lower() not in ["done", "running...", "error", ""] else 2
            self.update_cell(self.ws_kw, kw_row, status_col_index, "Done")
            
        self.log(f"🎉 HOÀN TẤT CHIẾN DỊCH! TỔNG LƯU: {self.total_saved} dòng.")

    def random_sleep(self, min_sec=1.0, max_sec=3.0):
        time.sleep(random.uniform(min_sec, max_sec))

if __name__ == "__main__":
    while True:
        try:
            bot = GSheetScraper()
            bot.run(limit_per_location=200)
            break 
        except Exception as e:
            print(f"\n❌ CHƯƠNG TRÌNH DỪNG ĐỘT NGỘT: {e}")
            time.sleep(10)
