import os
import json
import time
import random
import threading
import instaloader
import re

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from threading import Lock
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build

scheduler = BackgroundScheduler()
current_job_req = None

# ─── FASTAPI APP ─────────────────────────────────────────────
app = FastAPI(title="IG View Worker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://analisis-data-instagram-fe.vercel.app"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── CONSTANTS ───────────────────────────────────────────────
IG_USERNAME       = "Ace.Shuttle"
HF_REPO_ID        = "adityaUHU/job-driver"
IMPORTANT_COOKIES = ["sessionid", "csrftoken", "ds_user_id", "ig_did", "mid"]

# List User-Agent modern untuk rotasi agar tidak terbaca sebagai satu bot statis
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
]

# ─── GLOBAL STATE ────────────────────────────────────────────
_loader_instance: Optional[instaloader.Instaloader] = None
_loader_lock = threading.Lock()

job_status = {
    "running":   False,
    "progress":  0,
    "total":     0,
    "processed": 0,
    "log":       [],
    "last_run":  None,
    "error":     None,
}
job_lock = threading.Lock()


# ─── REQUEST MODELS ──────────────────────────────────────────
class SpreadsheetRequest(BaseModel):
    spreadsheet_id:     str
    sheet_name:         str
    batch_size:         Optional[int] = 100
    google_credentials: Optional[dict] = None


# ─── INSTAGRAM SESSION ───────────────────────────────────────

def load_instagram_cookies() -> dict:
    cookies_env = os.environ.get("INSTAGRAM_COOKIES")
    if cookies_env:
        try:
            cookies   = json.loads(cookies_env)
            cookie_map = {c["name"]: c["value"] for c in cookies}
            if cookie_map.get("sessionid"):
                print(f"✅ Cookies dari env var | sessionid: {cookie_map['sessionid'][:10]}...")
                return cookie_map
        except Exception as e:
            print(f"⚠️ Gagal parse env var: {e}")

    try:
        with open("storage/cookies.json", "r", encoding="utf-8") as f:
            cookies    = json.load(f)
        cookie_map = {c["name"]: c["value"] for c in cookies}
        if cookie_map.get("sessionid"):
            print("✅ Cookies dari file lokal")
            return cookie_map
    except FileNotFoundError:
        pass

    print("❌ Tidak ada cookies tersedia!")
    return {}


def reset_loader():
    global _loader_instance
    with _loader_lock:
        _loader_instance = None
    print("[LOADER] Instance direset")


def get_loader() -> instaloader.Instaloader:
    global _loader_instance
    with _loader_lock:
        if _loader_instance is not None:
            _loader_instance.context._session.headers.update({"user-agent": random.choice(USER_AGENTS)})
            return _loader_instance

        ig_cookies = load_instagram_cookies()
        if not ig_cookies.get("sessionid"):
            print("⚠️ sessionid tidak ditemukan!")

        L = instaloader.Instaloader(
            quiet=True,
            request_timeout=30,
            download_pictures=False,
            download_videos=False,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            post_metadata_txt_pattern="",
            compress_json=False,
        )
        L.context.max_connection_attempts = 1
        L.context._session.cookies.update({
            "sessionid":  ig_cookies.get("sessionid", ""),
            "csrftoken":  ig_cookies.get("csrftoken", ""),
            "ds_user_id": ig_cookies.get("ds_user_id", ""),
            "ig_did":     ig_cookies.get("ig_did", ""),
            "mid":        ig_cookies.get("mid", ""),
        })
        L.context._session.headers.update({
            "x-csrftoken":      ig_cookies.get("csrftoken", ""),
            "x-ig-app-id":      "936619743392459",
            "x-requested-with": "XMLHttpRequest",
            "referer":          "https://www.instagram.com/",
            "user-agent":       random.choice(USER_AGENTS),
            "accept":           "*/*",
            "accept-language":  "en-US,en;q=0.9,id;q=0.8",
            "origin":           "https://www.instagram.com",
        })
        L.context.username = IG_USERNAME

        # try:
        #     L.context.graphql_query("d6f4427fbe92d846298cf93df0b937d3", {})
        #     print(f"✅ Session aktif — login sebagai: {IG_USERNAME}")
        # except Exception as e:
        #     print(f"⚠️ Verifikasi session gagal ({e}). Melanjutkan...")

        _loader_instance = L
        return L


# ─── INSTAGRAM FETCH HELPERS ─────────────────────────────────

def extract_shortcode(url: str) -> str | None:
    url   = url.strip().rstrip("/")
    match = re.search(r"/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)", url)
    return match.group(1) if match else None


def fetch_fresh_post(shortcode: str, loader: instaloader.Instaloader):
    endpoints = [
        f"https://www.instagram.com/p/{shortcode}/?__a=1&__d=dis",
        f"https://www.instagram.com/reel/{shortcode}/?__a=1&__d=dis",
    ]
    random.shuffle(endpoints)

    for url in endpoints:
        try:
            loader.context._session.headers.update({"referer": f"https://www.instagram.com/p/{shortcode}/"})
            response = loader.context._session.get(url, timeout=10)
            if response.status_code == 200:
                items = response.json().get("items", [])
                if items:
                    item = items[0]
                    class MockPost:
                        is_video       = item.get("media_type", 1) in (1, 2)
                        _full_metadata = item
                        likes          = item.get("like_count", 0)
                        comments       = item.get("comment_count", 0)
                    return MockPost()
        except Exception:
            pass

    # Fallback GraphQL
    try:
        post = instaloader.Post.from_shortcode(loader.context, shortcode)
        post._full_metadata_dict = None
        return post
    except Exception as e:
        print(f"  [FAIL] {shortcode}: {type(e).__name__}")

    class EmptyPost:
        is_video       = True
        _full_metadata = {}
        likes          = 0
        comments       = 0
    return EmptyPost()


def get_views_from_post(post) -> tuple[int, int, bool]:
    raw       = getattr(post, "_full_metadata", {}) or {}
    shortcode = raw.get("code", "unknown")
    boost_flags = ["is_ad", "is_boosted_post", "is_commercial", "is_paid_partnership"]

    print(f"\n🔍 [DEBUG FLAGS] Memeriksa URL Shortcode: {shortcode}")
    for flag in boost_flags:
        # Menggunakan raw.get(flag, "Tidak Ditemukan") untuk tahu apakah key-nya ada atau tidak
        status_flag = raw.get(flag, "Tidak Ditemukan")
        print(f"   └── {flag}: {status_flag} (Tipe: {type(status_flag).__name__})")

    if not raw.get("is_video", True) and raw.get("__typename") != "GraphVideo":
        likes_count = (
            raw.get("edge_media_preview_like", {}).get("count", 0)
            or getattr(post, "likes", 0)
            or 0
        )
        return likes_count, likes_count, False

    total_views = raw.get("video_play_count", 0)
    if total_views == 0:
        for key in ("play_count", "ig_play_count"):
            if isinstance(raw.get(key), (int, float)) and raw.get(key) > 0:
                total_views = int(raw.get(key))
                break
    if total_views == 0:
        total_views = raw.get("edge_media_to_media_video_view", {}).get("count", 0)
    if total_views == 0:
        return 0, 0, False

    views_organik = raw.get("video_view_count", 0)

    is_boosted  = False
    
    for flag in boost_flags:
        val = raw.get(flag)
        if val is True:
            is_boosted = True

    print(f"   [INFO] Tipe Media: VIDEO/REEL")
    print(f"   [INFO] Hasil Deteksi Akhir -> Is Boosted: {is_boosted}")
    print(f"   [INFO] Total Views: {total_views} | Views Organik (Raw): {views_organik}\n")

    if is_boosted:
        if not (0 < views_organik < total_views):
            views_organik = int(total_views * 0.40)
    else:
        views_organik = total_views

    if views_organik > total_views:
        views_organik = total_views
    if views_organik <= 0:
        views_organik = total_views

    return total_views, views_organik, is_boosted


def fetch_single_sequential(url: str, loader: instaloader.Instaloader) -> tuple:
    """Menggantikan sistem async. Memproses 1 URL dengan pola yang dinamis."""
    shortcode = extract_shortcode(url)
    if not shortcode:
        return 0, 0, "invalid_url", False

    # Rotasi User-Agent dinamis per-request
    loader.context._session.headers.update({"user-agent": random.choice(USER_AGENTS)})

    for attempt in range(2):
        if not job_status["running"]:
            return 0, 0, "stopped", False
        try:
            post = fetch_fresh_post(shortcode, loader)
            total_views, views_organik, is_boosted = get_views_from_post(post)

            if total_views > 0:
                return total_views, views_organik, "ok", is_boosted

            if attempt == 0:
                retry_delay = random.uniform(10.0, 15.0)
                print(f"  [RETRY] {shortcode} bernilai 0, menunggu {retry_delay:.1f}s...")
                time.sleep(retry_delay)

        except Exception as e:
            print(f"  ✗ {shortcode} Error attempt {attempt + 1}: {type(e).__name__}")
            if attempt == 0:
                time.sleep(15)

    return 0, 0, "error", False


# ─── GOOGLE SHEETS ───────────────────────────────────────────

def get_sheets_service(creds_override: dict = None):
    creds_json = creds_override or json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
    scopes     = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = service_account.Credentials.from_service_account_info(creds_json, scopes=scopes)
    return build("sheets", "v4", credentials=creds)


def get_sheet_values(service, spreadsheet_id: str, range_name: str):
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return result.get("values", [])


def update_sheet_values(service, spreadsheet_id: str, data_updates: list):
    if not data_updates:
        return
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"valueInputOption": "USER_ENTERED", "data": data_updates},
    ).execute()


# ─── COLUMN HELPERS ──────────────────────────────────────────

def col_letter(i: int) -> str:
    r, idx = "", i + 1
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        r = chr(65 + rem) + r
    return r


def _safe_int_val(val) -> int:
    try:
        if val is None:
            return 0
        val_str = str(val).strip()
        if val_str in ("", "nan"):
            return 0
        return int(float(val_str.replace(",", "")))
    except (ValueError, TypeError):
        return 0


def log(msg: str):
    print(msg)
    job_status["log"].append(msg)
    if len(job_status["log"]) > 200:
        job_status["log"] = job_status["log"][-200:]


# ─── CORE JOB (MODIFIKASI ANTI-BOT & ONE-TIME WRITE) ──────────

def run_job(req: SpreadsheetRequest):
    with job_lock:
        if job_status["running"]:
            log("[JOB] Job dilewati: Sesi analisis sebelumnya masih aktif berjalan.")
            return
        job_status.update({
            "running":   True,
            "progress":  0,
            "total":     0,
            "processed": 0,
            "log":       [],
            "error":     None,
        })

    try:
        service = get_sheets_service(req.google_credentials)
        loader  = get_loader() 

        raw_rows = get_sheet_values(service, req.spreadsheet_id, f"'{req.sheet_name}'!A1:ZZ")
        if not raw_rows:
            log("[JOB] Sheet kosong, skip.")
            return

        header    = raw_rows[0]
        data_rows = raw_rows[1:]

        # ── Deteksi kolom ──
        col_link       = next((i for i, c in enumerate(header) if "link post" in c.lower()), None)
        col_imp        = next((i for i, c in enumerate(header) if "total imp by job" in c.lower()), None)
        col_imp_ori    = next((i for i, c in enumerate(header) if "total imp organik" in c.lower()), None)
        col_status_idx = next((i for i, c in enumerate(header) if "status(job)" in c.lower()), None)
        col_boost      = next((i for i, c in enumerate(header) if "boost" in c.lower()), None)

        if col_link is None:
            log("[JOB] Kolom 'link post' tidak ditemukan!")
            return

        # ── Tambah kolom baru jika belum ada ──
        updates_header = []
        if col_imp_ori is None:
            col_imp_ori = len(header)
            header.append("TOTAL IMP ORGANIK(VIEW COUNT)")
            updates_header.append({
                "range":  f"'{req.sheet_name}'!{col_letter(col_imp_ori)}1",
                "values": [["TOTAL IMP ORGANIK(VIEW COUNT)"]],
            })
        if col_status_idx is None:
            col_status_idx = len(header)
            header.append("STATUS(JOB)")
            updates_header.append({
                "range":  f"'{req.sheet_name}'!{col_letter(col_status_idx)}1",
                "values": [["STATUS(JOB)"]],
            })
        if updates_header:
            update_sheet_values(service, req.spreadsheet_id, updates_header)

        # ── LOGIKA EVALUASI WAKTU KEDALUWARSA (24 JAM) ──
        waktu_sekarang = datetime.now()
        batas_kedaluwarsa = timedelta(hours=24) 

        url_index_pairs = []
        for i, row in enumerate(data_rows):
            if col_link >= len(row) or "instagram.com" not in str(row[col_link]):
                continue
            
            perlu_proses = True
            
            if col_status_idx is not None and col_status_idx < len(row):
                status_raw = str(row[col_status_idx]).strip()
                
                # Cek apakah kolom berisi label [ORGANIC] atau [BOOSTED]
                if "[ORGANIC]" in status_raw or "[BOOSTED]" in status_raw:
                    # Coba ekstrak timestamp dari teks (Format: [STATUS] YYYY-MM-DD HH:MM:SS)
                    match = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", status_raw)
                    if match:
                        try:
                            waktu_job_lalu = datetime.strptime(match.group(0), "%Y-%m-%d %H:%M:%S")
                            # Jika selisih waktu sekarang dengan waktu lalu BELUM melewati 2 jam, maka SKIP
                            if waktu_sekarang - waktu_job_lalu < batas_kedaluwarsa:
                                perlu_proses = False
                        except ValueError:
                            pass # Format tanggal korup/salah, paksa proses ulang

            if perlu_proses:
                url_index_pairs.append((i, row[col_link].strip()))

        total_antrean = len(url_index_pairs)
        job_status["total"] = total_antrean

        if not url_index_pairs:
            log("[JOB] Selesai. Semua baris data masih segar (belum melewati batas 24 jam).")
            return

        # Ambil sesuai ukuran batch agar Instagram tidak memblokir IP/Akun Anda
        limit = req.batch_size if req.batch_size else 50
        processing_pool = url_index_pairs[:limit]
        
        # Diacak agar pola hit ke Instagram tidak berurutan konstan (pola bot)
        random.shuffle(processing_pool)
        
        log(f"[JOB] Ditemukan {total_antrean} data kedaluwarsa/baru. Memproses batch ini sebanyak {len(processing_pool)} URL.")

        bulk_updates = []
        processed_count = 0

        # Loop pemrosesan data (Sekuensial)
        for idx, url in processing_pool:
            if not job_status["running"]:
                log("[JOB] Dihentikan paksa oleh user.")
                break

            # Jeda fluktuatif anti-bot (10-18 detik)
            time.sleep(random.uniform(10.0, 18.0))

            total_views, views_organik, status, is_boosted_api = fetch_single_sequential(url, loader)
            if status == "stopped":
                break

            # Override data jika ada status manual dari spreadsheet
            is_boosted = is_boosted_api
            if col_boost is not None and col_boost < len(data_rows[idx]):
                val = str(data_rows[idx][col_boost]).strip().lower()
                if val in ["yes", "y", "true", "boosting", "1"]:
                    is_boosted    = True
                    views_organik = int(total_views * 0.4) if total_views else 0

            # ── FORMAT BARU: Menyisipkan waktu pengerjaan ──
            timestamp_sekarang = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status_label = f"[BOOSTED] {timestamp_sekarang}" if is_boosted else f"[ORGANIC] {timestamp_sekarang}"
            gs_row       = idx + 2 

            if col_imp is not None:
                bulk_updates.append({
                    "range":  f"'{req.sheet_name}'!{col_letter(col_imp)}{gs_row}",
                    "values": [[_safe_int_val(total_views)]],
                })
            bulk_updates.append({
                "range":  f"'{req.sheet_name}'!{col_letter(col_imp_ori)}{gs_row}",
                "values": [[_safe_int_val(views_organik)]],
            })
            bulk_updates.append({
                "range":  f"'{req.sheet_name}'!{col_letter(col_status_idx)}{gs_row}",
                "values": [[status_label]],
            })

            processed_count += 1
            job_status["processed"] = processed_count
            job_status["progress"]  = round(processed_count / len(processing_pool) * 100)
            log(f"  ✓ [{processed_count}/{len(processing_pool)}] Sukses analisa baris {gs_row} -> {status_label}")

            # Jeda istirahat kopi (Coffee break) jika memproses banyak data
            if processed_count % random.randint(8, 14) == 0 and processed_count < len(processing_pool):
                sleep_break = random.uniform(30, 60)
                log(f"☕ [ANTI-BAN] Mengambil istirahat sejenak selama {sleep_break:.1f} detik...")
                time.sleep(sleep_break)

        # ── SIMPAN PER BATCH ──
        if bulk_updates:
            log(f"[WRITE] Menyimpan pembaruan batch ke Google Sheets ({len(bulk_updates)} cells)...")
            update_sheet_values(service, req.spreadsheet_id, bulk_updates)
            log("[WRITE] Sinkronisasi data baru berhasil disimpan.")
        else:
            log("[WRITE] Tidak ada perubahan data.")

    except Exception as e:
        job_status["error"] = str(e)
        log(f"[ERROR] {e}")
    finally:
        job_status["running"]  = False
        job_status["last_run"] = time.strftime("%Y-%m-%d %H:%M:%S")


def trigger_automatic_job():
    """Fungsi yang dipanggil otomatis oleh scheduler secara berkala"""
    global current_job_req
    
    # PERBAIKAN: Validasi ketat agar tidak menjalankan job jika spreadsheet_id kosong
    if current_job_req is None or not getattr(current_job_req, "spreadsheet_id", "").strip():
        print("[CRON] Job otomatis dilewati: Parameter spreadsheet_id kosong atau belum tersimpan.")
        return

    print(f"[CRON] Memulai job otomatis terjadwal pada {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not job_status["running"]:
        # Pastikan data req dioper dengan benar dan utuh
        run_job(current_job_req)
    else:
        print("[CRON] Job otomatis dilewati karena job sebelumnya masih berjalan.")


# ─── ENDPOINTS ───────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "message": "IG View Worker is running"}


@app.post("/api/instagram/save-session")
async def save_instagram_session(payload: dict):
    try:
        cookies  = payload.get("cookies", [])
        filtered = [c for c in cookies if c.get("name") in IMPORTANT_COOKIES]

        if not filtered:
            raise HTTPException(status_code=400, detail="Tidak ada cookies valid")

        cookies_json = json.dumps(filtered)
        os.environ["INSTAGRAM_COOKIES"] = cookies_json

        hf_token = os.environ.get("HF_TOKEN")
        if hf_token:
            try:
                from huggingface_hub import HfApi
                api = HfApi(token=hf_token)
                api.add_space_secret(
                    repo_id=HF_REPO_ID,
                    key="INSTAGRAM_COOKIES",
                    value=cookies_json,
                )
                print("[HF SECRET] Tersimpan permanen!")
            except Exception as e:
                print(f"[HF SECRET] Gagal: {e}")

        reset_loader()

        return {
            "success": True,
            "message": "Session saved",
            "total":   len(filtered),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/spreadSheet")
async def spreadsheet_job(req: SpreadsheetRequest, background_tasks: BackgroundTasks):
    global current_job_req
    
    cookies_raw = os.environ.get("INSTAGRAM_COOKIES", "")
    if not cookies_raw:
        raise HTTPException(
            status_code=401,
            detail="Session Instagram belum tersedia. Silakan login dulu via extension.",
        )
    try:
        parsed = json.loads(cookies_raw)
        cookie_map = {c["name"]: c["value"] for c in parsed}
        if not cookie_map.get("sessionid"):
            raise ValueError("sessionid kosong")
    except Exception:
        raise HTTPException(
            status_code=401,
            detail="Session tidak valid. Silakan login ulang via extension.",
        )

    if job_status["running"]:
        raise HTTPException(
            status_code=409,
            detail="Job sedang berjalan. Tunggu hingga selesai.",
        )

    current_job_req = req
    background_tasks.add_task(run_job, req)

    if not scheduler.get_job('automatic_ig_job'):
        if not scheduler.running:
            scheduler.start()
            
        scheduler.add_job(
            trigger_automatic_job, 
            'interval', 
            hours=24, 
            id='automatic_ig_job'
        )
        print(f"🔥 [SCHEDULER] Berhasil diaktifkan! Berjalan otomatis per 24 Jam.")
    else:
        print(f"ℹ️ [SCHEDULER] Menggunakan parameter spreadsheet terbaru.")

    return {
        "message":        "Job dimulai.",
        "spreadsheet_id": req.spreadsheet_id,
        "sheet_name":      req.sheet_name,
        "status":          "started",
    }


@app.get("/status")
def get_status():
    session_active = bool(os.environ.get("INSTAGRAM_COOKIES"))
    return {
        "running":        job_status["running"],
        "progress":       job_status["progress"],
        "processed":      job_status["processed"],
        "total":          job_status["total"],
        "last_run":       job_status["last_run"],
        "error":          job_status["error"],
        "log":            job_status["log"][-50:],
        "session_active": session_active,
    }


@app.post("/stop")
def stop_job():
    job_status["running"] = False
    return {"message": "Stop signal dikirim. Skrip akan berhenti setelah item ini selesai tanpa menulis data kotor."}


@app.get("/api/instagram/session-status")
def session_status():
    raw = os.environ.get("INSTAGRAM_COOKIES", "")
    if not raw:
        return {"active": False, "cookie_names": []}
    try:
        cookies = json.loads(raw)
        return {
            "active":       bool(cookies),
            "cookie_names": [c["name"] for c in cookies],
        }
    except Exception:
        return {"active": False, "cookie_names": []}


@app.on_event("startup")
async def startup_event():
    if os.environ.get("INSTAGRAM_COOKIES"):
        print("[STARTUP] INSTAGRAM_COOKIES ditemukan di env, loader siap dipakai.")
        reset_loader()
    else:
        print("[STARTUP] INSTAGRAM_COOKIES belum ada. Tunggu save-session dari extension.")
        

@app.post("/api/job/restart")
async def restart_job(req: SpreadsheetRequest, background_tasks: BackgroundTasks):
    global current_job_req  # Tambahkan global agar mereferensikan variabel utama
    
    cookies_raw = os.environ.get("INSTAGRAM_COOKIES", "")
    if not cookies_raw:
        raise HTTPException(
            status_code=401,
            detail="Session Instagram belum tersedia. Silakan login terlebih dahulu.",
        )
        
    # Validasi tambahan untuk memastikan ID tidak kosong dari request baru
    if not req.spreadsheet_id or not req.spreadsheet_id.strip():
        raise HTTPException(
            status_code=400,
            detail="Spreadsheet ID tidak boleh kosong.",
        )

    with job_lock:
        job_status["running"] = False
        time.sleep(1.0) 

    # PERBAIKAN: Simpan request terbaru ke state global agar dikenali oleh scheduler/cron job
    current_job_req = req
    
    background_tasks.add_task(run_job, req)

    return {
        "message":        "Job berhasil di-restart dari awal.",
        "spreadsheet_id": req.spreadsheet_id,
        "sheet_name":      req.sheet_name,
        "status":          "restarted",
    }


@app.on_event("shutdown")
def shutdown_event():
    if scheduler.running:
        scheduler.shutdown()
        print("[SHUTDOWN] Scheduler dimatikan dengan bersih.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("worker:app", host="0.0.0.0", port=7860, reload=False)