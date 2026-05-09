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
import json
import socket
import psutil
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
socket.setdefaulttimeout(60)

# ==========================================
# CẤU HÌNH DỰ ÁN
# ==========================================
PROJECT_NAME  = "ScrapTrangVang"
VERSION       = "1.0 - Distributed Scraper for 10 PCs"
CREDENTIALS_FILE = "credentials.json"
SHEET_ID      = "DÁN_ID_SHEET_MỚI_CỦA_BẠN_VÀO_ĐÂY"
BATCH_SIZE    = 15
LOCK_TIMEOUT_SEC = 600
UPDATE_URL = "https://raw.githubusercontent.com/TheQ389/Scraper_Gheet/refs/heads/main/main.py"

class TrangVangScraper:
    def __init__(self):
        self.processed_phones = set()
        self.batch_data = []
        self.machine_name = f"{socket.gethostname()}_{random.randint(1000, 9999)}"
        self.start_time = time.time()
        self.total_saved = 0
        
        self._print_banner()
        self.connect_sheet()
        self._load_dedup_data()

    def _print_banner(self):
        print("=" * 60)
        print(f"🚀 {PROJECT_NAME} v{VERSION}")
        print(f"💻 Machine ID: {self.machine_name}")
        print("=" * 60)

    def log(self, msg):
        ram = psutil.Process().memory_info().rss / 1024 / 1024
        print(f"[{datetime.now():%H:%M:%S}] [RAM:{ram:.0f}MB] {msg}")

    def connect_sheet(self):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        self.client = gspread.authorize(creds)
        self.ss = self.client.open_by_key(SHEET_ID)
        self.ws_kw = self.ss.worksheet("TuKhoa")
        self.ws_out = self.ss.worksheet("KetQua")
        try: self.ws_config = self.ss.worksheet("Config")
        except: self.ws_config = self.ss.add_worksheet("Config", 10, 5)

    def _load_dedup_data(self):
        self.log("⏳ Đang nạp dữ liệu chống trùng từ cột SĐT...")
        try:
            # Lấy cột E (index 5) của tab KetQua
            phones = self.ws_out.col_values(5)[1:]
            for p in phones:
                if p: self.processed_phones.add(p.strip().replace("'", ""))
            self.log(f"✅ Đã nạp {len(self.processed_phones)} SĐT.")
        except: pass

    def _api_retry(self, func, *args, **kwargs):
        for i in range(5):
            try: return func(*args, **kwargs)
            except Exception as e:
                if "429" in str(e): 
                    time.sleep(2**i + random.uniform(1, 3))
                else: raise e

    def get_html(self, url):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        try:
            res = requests.get(url, headers=headers, timeout=20, verify=False)
            res.encoding = 'utf-8'
            return res.text
        except: return None

    def flush_batch(self):
        if self.batch_data:
            self._api_retry(self.ws_out.append_rows, self.batch_data, value_input_option='USER_ENTERED')
            self.total_saved += len(self.batch_data)
            self.log(f"✅ Đã lưu {len(self.batch_data)} doanh nghiệp mới.")
            self.batch_data.clear()

    def run(self):
        kws = self._api_retry(self.ws_kw.get_all_values)[1:]
        
        for idx, row in enumerate(kws):
            row_idx = idx + 2
            nganh_nghe = row[0]
            tinh_thanh = row[1]
            blacklist_raw = row[2] if len(row) > 2 else ""
            status = row[3] if len(row) > 3 else ""
            
            if status.lower() == "done": continue
            
            # Xí chỗ (Lock)
            lock_flag = f"Running | {self.machine_name} | {int(time.time())}"
            self._api_retry(self.ws_kw.update_cell, row_idx, 4, lock_flag)
            time.sleep(2)
            if self._api_retry(self.ws_kw.cell, row_idx, 4).value != lock_flag: continue

            blacklist = [b.strip().lower() for b in blacklist_raw.split(",") if b.strip()]
            self.log(f"🎯 Đang quét: {nganh_nghe} tại {tinh_thanh}")

            # Cấu trúc URL Trang Vàng
            query = urllib.parse.quote(f"{nganh_nghe} {tinh_thanh}")
            base_url = f"https://trangvangvietnam.com/search.asp?kwd={query}"
            
            for page in range(1, 11): # Quét tối đa 10 trang mỗi từ khóa
                url = f"{base_url}&page={page}"
                html = self.get_html(url)
                if not html: break
                
                soup = BeautifulSoup(html, 'html.parser')
                listings = soup.select('div.listing_block')
                if not listings: break
                
                found_on_page = 0
                for box in listings:
                    try:
                        name = box.select_one('h2.title_company a').text.strip()
                        # Kiểm tra blacklist
                        if any(b in name.lower() for b in blacklist): continue
                        
                        addr = box.select_one('div.address_con').text.strip()
                        
                        # Trang vàng hay giấu SĐT, cần bóc tách từ text hoặc các thẻ span
                        phone_box = box.select_one('div.phone_con')
                        phone_raw = phone_box.text.strip() if phone_box else ""
                        
                        # Làm sạch số điện thoại
                        phones = re.findall(r'(?:02\d{8,9}|0[35789]\d{8}|1[89]00\d{4,6})', phone_raw.replace(".", "").replace(" ", ""))
                        
                        email = ""
                        email_tag = box.select_one('a[href^="mailto:"]')
                        if email_tag: email = email_tag.get('href').replace("mailto:", "").split('?')[0]
                        
                        web = ""
                        web_tag = box.select_one('a[target="_blank"]')
                        if web_tag and "trangvang" not in web_tag.get('href'): web = web_tag.get('href')

                        # Lưu những số chưa trùng
                        for p in phones:
                            if p not in self.processed_phones:
                                self.processed_phones.add(p)
                                self.batch_data.append([nganh_nghe, tinh_thanh, name, addr, f"'{p}", email, web, "", self.machine_name.split('_')[0]])
                                found_on_page += 1
                                break # Chỉ lấy 1 số đại diện

                        if len(self.batch_data) >= BATCH_SIZE: self.flush_batch()
                    except: continue
                
                self.log(f"   -> Trang {page}: Lấy được {found_on_page} doanh nghiệp.")
                time.sleep(random.uniform(2, 4))
                if found_on_page == 0: break # Hết dữ liệu thật

            self.flush_batch()
            self._api_retry(self.ws_kw.update_cell, row_idx, 4, "Done")

if __name__ == "__main__":
    while True:
        try:
            bot = TrangVangScraper()
            bot.run()
            break
        except Exception as e:
            print(f"Lỗi hệ thống: {e}")
            time.sleep(30)
