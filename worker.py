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
from typing import Optional
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.oauth2 import service_account
from googleapiclient.discovery import build

scheduler = BackgroundScheduler()
current_job_req = None

app = FastAPI(title="IG View Worker")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://analisis-data-instagram-fe.vercel.app"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── CONSTANTS ───────────────────────────────────────────────
IG_USERNAME       = "cat_streat"
HF_REPO_ID        = "adityaUHU/job-driver2"
IMPORTANT_COOKIES = ["sessionid", "csrftoken", "ds_user_id", "ig_did", "mid"]

MAX_FAIL_COUNT    = 3    
MAX_RETRY_ROUNDS  = 2    
RETRY_BATCH_LIMIT = 50   

# Jeda antar-request normal (Sangat Manusiawi)
DELAY_FAST_MIN  = 6.0
DELAY_FAST_MAX  = 10.0
DELAY_SLOW_MIN  = 12.0
DELAY_SLOW_MAX  = 18.0

# Jeda saat rate-limit
RATE_LIMIT_WAIT_MIN   = 45
RATE_LIMIT_WAIT_MAX   = 75
RATE_LIMIT_EXTRA_WAIT = 60   

RATE_LIMIT_RESET_AT = 4      
CONN_ERROR_RESET_AT = 3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9,id;q=0.8",
    "en-GB,en;q=0.9",
    "id-ID,id;q=0.9,en;q=0.8",
    "en-US,en;q=0.8",
]

BOOST_FLAGS = ["is_ad", "is_boosted_post", "is_commercial", "is_paid_partnership"]

# ─── GLOBAL STATE ────────────────────────────────────────────
_loader_instance: Optional[instaloader.Instaloader] = None
_loader_lock = threading.Lock()

_failed_shortcodes: dict[str, int] = {}

_consec_rate_limits = 0
_consec_conn_errors  = 0

job_status = {
    "running": False, "progress": 0, "total": 0,
    "processed": 0, "log": [], "last_run": None, "error": None,
    "retry_round": 0, "retry_total": 0, "retry_processed": 0,
}
job_lock = threading.Lock()


class SpreadsheetRequest(BaseModel):
    spreadsheet_id:     str
    sheet_name:         str
    batch_size:         Optional[int] = 300
    google_credentials: Optional[dict] = None


# ─── UTILS ───────────────────────────────────────────────────

def log(msg: str):
    print(msg)
    job_status["log"].append(msg)
    if len(job_status["log"]) > 300:
        job_status["log"] = job_status["log"][-300:]


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
        s = str(val).strip().replace(",", "")
        return int(float(s)) if s not in ("", "nan") else 0
    except (ValueError, TypeError):
        return 0


def _wib_now() -> datetime:
    return datetime.now(ZoneInfo("Asia/Jakarta")).replace(tzinfo=None)


def _wib_now_str() -> str:
    return datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S")


# ─── INSTAGRAM SESSION ───────────────────────────────────────

def load_instagram_cookies() -> dict:
    for source, getter in [
        ("env",  lambda: os.environ.get("INSTAGRAM_COOKIES")),
        ("file", lambda: open("storage/cookies.json").read()
                         if os.path.exists("storage/cookies.json") else None),
    ]:
        raw = getter()
        if not raw:
            continue
        try:
            cookies    = json.loads(raw)
            cookie_map = {c["name"]: c["value"] for c in cookies}
            if cookie_map.get("sessionid"):
                print(f"✅ Cookies dari {source}")
                return cookie_map
        except Exception as e:
            print(f"⚠️ Gagal parse cookies dari {source}: {e}")
    print("❌ Tidak ada cookies tersedia!")
    return {}


def _build_random_headers(csrftoken: str = "") -> dict:
    return {
        "x-csrftoken":      csrftoken,
        "x-ig-app-id":      "936619743392459",
        "x-requested-with": "XMLHttpRequest",
        "referer":          "https://www.instagram.com/",
        "user-agent":       random.choice(USER_AGENTS),
        "accept":           "*/*",
        "accept-language":  random.choice(ACCEPT_LANGUAGES),
        "origin":           "https://www.instagram.com",
        "viewport-width":   str(random.choice([1280, 1366, 1440, 1920])),
    }


def reset_loader():
    global _loader_instance
    with _loader_lock:
        _loader_instance = None
    print("[LOADER] Instance direset")


def get_loader() -> instaloader.Instaloader:
    global _loader_instance
    with _loader_lock:
        if _loader_instance is not None:
            _loader_instance.context._session.headers.update({
                "user-agent":      random.choice(USER_AGENTS),
                "accept-language": random.choice(ACCEPT_LANGUAGES),
            })
            return _loader_instance

        ig_cookies = load_instagram_cookies()
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
        L.context._session.cookies.update(
            {k: ig_cookies.get(k, "") for k in IMPORTANT_COOKIES}
        )
        L.context._session.headers.update(
            _build_random_headers(ig_cookies.get("csrftoken", ""))
        )
        L.context.username = IG_USERNAME
        _loader_instance = L
        return L


# ─── INSTAGRAM FETCH ─────────────────────────────────────────

_SHORTCODE_RE = re.compile(r"/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)")
_TS_RE         = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")


def extract_shortcode(url: str) -> Optional[str]:
    m = _SHORTCODE_RE.search(url.strip().rstrip("/"))
    return m.group(1) if m else None


def fetch_fresh_post(shortcode: str, loader: instaloader.Instaloader):
    """
    Menggabungkan Kecepatan Baru + Akurasi Kode Lama (Fallback Native Instaloader).
    Jika endpoint JSON mengembalikan data kosong atau tidak valid, 
    KODE AKAN DIPAKSA menggunakan Instaloader Native agar tidak menghasilkan 0 views.
    """
    session = loader.context._session
    session.headers.update({
        "user-agent":      random.choice(USER_AGENTS),
        "accept-language": random.choice(ACCEPT_LANGUAGES),
        "referer":          f"https://www.instagram.com/p/{shortcode}/",
    })

    endpoints = [
        f"https://www.instagram.com/p/{shortcode}/?__a=1&__d=dis",
        f"https://www.instagram.com/reel/{shortcode}/?__a=1&__d=dis",
    ]
    random.shuffle(endpoints)

    got_rate_limit = False

    # ── Layer 1: JSON Endpoint Tersembunyi ──
    for url in endpoints:
        try:
            resp = session.get(url, timeout=(5, 10))

            if resp.status_code == 200:
                try:
                    items = resp.json().get("items", [])
                except ValueError:
                    continue
                
                if items:
                    item = items[0]
                    
                    # AKURASI: Validasi apakah JSON dari IG ini memiliki data riil atau kosongan
                    has_views_data = any(k in item for k in ("video_play_count", "play_count", "ig_play_count", "edge_media_to_media_video_view"))
                    has_likes_data = "like_count" in item or "edge_media_preview_like" in item
                    
                    # Jika data krusial ditemukan, baru bungkus ke MockPost
                    if has_views_data or has_likes_data:
                        class MockPost:
                            # deteksi tipe media secara akurat
                            is_video       = item.get("media_type", 1) == 2 or item.get("__typename") == "GraphVideo"
                            _full_metadata = item
                            likes          = item.get("like_count", 0) or item.get("edge_media_preview_like", {}).get("count", 0)
                            comments       = item.get("comment_count", 0)
                        return MockPost(), "ok"
                
                # Jika status_code 200 tapi isinya kosong/terpotong oleh IG, 
                # JANGAN return ok dulu. Biarkan loop berlanjut ke Fallback Native Instaloader.
                continue

            if resp.status_code == 429:
                got_rate_limit = True
                break   

            if resp.status_code in (401, 403):
                got_rate_limit = True
                break

            if resp.status_code in (404, 410):
                # return None, "not_found"
                continue

            continue

        except Exception as e:
            name = type(e).__name__
            if any(k in name for k in ("ConnectionError", "ConnectTimeout", "ReadTimeout")):
                return None, "conn_error"
            continue

    # ── Layer 2: Fallback Instaloader Native (Kunci Akurasi Kode Lama Anda) ──
    if not got_rate_limit:
        try:
            post = instaloader.Post.from_shortcode(loader.context, shortcode)
            if post and (getattr(post, "shortcode", "") == shortcode):
                return post, "ok"
        except instaloader.exceptions.PostChangedException:
            return None, "not_found"
        except instaloader.exceptions.ConnectionException as e:
            if "404" in str(e):
                # Jika Instaloader Native juga bilang 404, baru ini FIX dihapus/not_found
                return None, "not_found"
            if "TooManyRequests" in type(e).__name__ or "429" in str(e):
                return None, "rate_limit"
            return None, "conn_error"
        except Exception as e:
            return None, "error"

    return None, "rate_limit"


def get_views_from_post(post) -> tuple[int, int, bool]:
    raw = getattr(post, "_full_metadata", {}) or {}

    # Jika ini adalah objek asli dari Instaloader Native, raw bisa kosong, kita baca dari atribut objeknya
    is_video_post = getattr(post, "is_video", False)
    if not raw and hasattr(post, "_node"):
        raw = post._node
        is_video_post = raw.get("is_video", is_video_post)

    # ── Foto / Non-video ────────────────────────────────────
    if not is_video_post and raw.get("__typename") != "GraphVideo":
        likes_count = (
            raw.get("edge_media_preview_like", {}).get("count", 0)
            or raw.get("like_count", 0)
            or getattr(post, "likes", 0)
            or 0
        )
        return likes_count, likes_count, False

    # ── Video / Reel ─────────────────────────────────────────
    total_views = 0
    # Gabungan pembacaan key JSON + Atribut Native Instaloader (Akurasi Maksimal)
    for key in ("video_play_count", "play_count", "ig_play_count", "play_count"):
        val = raw.get(key)
        if isinstance(val, (int, float)) and val > 0:
            total_views = int(val)
            break
            
    if total_views == 0:
        val = raw.get("edge_media_to_media_video_view", {})
        if isinstance(val, dict):
            total_views = val.get("count", 0)

    # Fallback ke atribut bawaan instaloader jika via dictionary di atas masih 0
    if total_views == 0 and hasattr(post, "video_view_count"):
        total_views = post.video_view_count or 0

    if total_views == 0:
        return 0, 0, False

    views_organik = raw.get("video_view_count", 0) or getattr(post, "video_view_count", 0) or 0
    is_boosted    = any(raw.get(f) is True for f in BOOST_FLAGS)

    if is_boosted:
        if not (0 < views_organik < total_views):
            views_organik = int(total_views * 0.40)
    else:
        views_organik = total_views

    views_organik = max(1, min(views_organik, total_views))
    return total_views, views_organik, is_boosted


def fetch_single(
    url: str,
    loader: instaloader.Instaloader,
    is_retry: bool = False,
) -> tuple[int, int, str, bool]:
    global _consec_rate_limits, _consec_conn_errors

    shortcode = extract_shortcode(url)
    if not shortcode:
        return 0, 0, "invalid_url", False

    if _failed_shortcodes.get(shortcode, 0) >= MAX_FAIL_COUNT:
        return 0, 0, "skipped", False

    attempts   = 3 if is_retry else 2
    loader_ref = [loader]

    for attempt in range(attempts):
        if not job_status["running"]:
            return 0, 0, "stopped", False

        post, status = fetch_fresh_post(shortcode, loader_ref[0])

        if status == "not_found":
            _consec_rate_limits = 0
            _consec_conn_errors  = 0
            return 0, 0, "not_found", False

        if status == "rate_limit":
            _consec_rate_limits += 1
            _consec_conn_errors  = 0
            wait = random.uniform(RATE_LIMIT_WAIT_MIN, RATE_LIMIT_WAIT_MAX)
            log(
                f"   ⏳ rate_limit [{shortcode}] attempt {attempt+1}/{attempts} "
                f"— tunggu {wait:.0f}s (berturut: {_consec_rate_limits})"
            )
            time.sleep(wait)

            if _consec_rate_limits >= RATE_LIMIT_RESET_AT:
                log(f"   🔄 {_consec_rate_limits}× rate_limit — reset loader + jeda {RATE_LIMIT_EXTRA_WAIT}s...")
                reset_loader()
                time.sleep(RATE_LIMIT_EXTRA_WAIT)
                loader_ref[0]       = get_loader()
                _consec_rate_limits = 0
            continue

        if status == "conn_error":
            _consec_conn_errors  += 1
            _consec_rate_limits   = 0
            wait = random.uniform(12, 24)
            log(
                f"   ⏳ conn_error [{shortcode}] attempt {attempt+1}/{attempts} "
                f"— tunggu {wait:.0f}s (berturut: {_consec_conn_errors})"
            )
            time.sleep(wait)

            if _consec_conn_errors >= CONN_ERROR_RESET_AT:
                log(f"   🔄 {_consec_conn_errors}× conn_error — reset loader + jeda 30s...")
                reset_loader()
                time.sleep(30)
                loader_ref[0]      = get_loader()
                _consec_conn_errors = 0
            continue

        if status == "error" or post is None:
            if attempt < attempts - 1:
                time.sleep(random.uniform(4.0, 8.0))
            continue

        _consec_rate_limits = 0
        _consec_conn_errors  = 0

        total_views, views_organik, is_boosted = get_views_from_post(post)

        if total_views > 0:
            _failed_shortcodes.pop(shortcode, None)
            return total_views, views_organik, "ok", is_boosted

        if attempt < attempts - 1:
            wait = random.uniform(6.0, 10.0)
            log(f"   🔁 views terdeteksi 0 [{shortcode}] — mencoba ulang dalam {wait:.1f}s...")
            time.sleep(wait)

    _failed_shortcodes[shortcode] = _failed_shortcodes.get(shortcode, 0) + 1
    return 0, 0, "error", False


# ─── GOOGLE SHEETS ───────────────────────────────────────────

def get_sheets_service(creds_override: dict = None):
    creds_json = creds_override or json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = service_account.Credentials.from_service_account_info(creds_json, scopes=scopes)
    return build("sheets", "v4", credentials=creds)


def get_sheet_values(service, spreadsheet_id: str, range_name: str):
    result = (
        service.spreadsheets().values()
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


def _build_row_updates(
    sheet_name: str,
    gs_row: int,
    total_views: int,
    views_organik: int,
    is_boosted: bool,
    col_imp: Optional[int],
    col_imp_ori: int,
    col_status_idx: int,
) -> list[dict]:
    ts    = _wib_now_str()
    label = f"[BOOSTED] {ts}" if is_boosted else f"[ORGANIC] {ts}"
    updates = []
    if col_imp is not None:
        updates.append({
            "range":  f"'{sheet_name}'!{col_letter(col_imp)}{gs_row}",
            "values": [[_safe_int_val(total_views)]],
        })
    updates.append({
        "range":  f"'{sheet_name}'!{col_letter(col_imp_ori)}{gs_row}",
        "values": [[_safe_int_val(views_organik)]],
    })
    updates.append({
        "range":  f"'{sheet_name}'!{col_letter(col_status_idx)}{gs_row}",
        "values": [[label]],
    })
    return updates


# ─── RETRY ROUND ─────────────────────────────────────────────

def run_retry_round(
    round_num:      int,
    retry_queue:    list,
    data_rows:      list,
    service,
    req:            SpreadsheetRequest,
    col_imp:        Optional[int],
    col_imp_ori:    int,
    col_status_idx: int,
    col_boost:      Optional[int],
) -> list:
    if not retry_queue:
        return []

    batch     = retry_queue[:RETRY_BATCH_LIMIT]
    remaining = retry_queue[RETRY_BATCH_LIMIT:]
    total     = len(batch)

    job_status["retry_round"]     = round_num
    job_status["retry_total"]     = total
    job_status["retry_processed"] = 0

    log(f"\n{'='*55}")
    log(f"🔁 RETRY ROUND {round_num} — {total} URL")
    log(f"{'='*55}")

    log(f"⏳ Cooling down 45s sebelum retry round {round_num}...")
    time.sleep(45)
    reset_loader()
    loader = get_loader()

    bulk_updates  = []
    still_failed  = []
    consec_ok     = 0
    CHECKPOINT_AT = 10

    for order, (idx, gs_row, url) in enumerate(batch, start=1):
        if not job_status["running"]:
            log("[RETRY] Dihentikan paksa.")
            still_failed.extend(batch[order - 1:])
            break

        delay = (
            random.uniform(12.0, 18.0) if consec_ok >= 3
            else random.uniform(22.0, 36.0)
        )
        time.sleep(delay)

        total_views, views_organik, status, is_boosted_api = fetch_single(
            url, loader, is_retry=True
        )

        if status == "stopped":
            still_failed.extend(batch[order - 1:])
            break

        job_status["retry_processed"] = order

        if status == "ok" and total_views > 0:
            consec_ok += 1

            is_boosted = is_boosted_api
            if col_boost is not None and col_boost < len(data_rows[idx]):
                val = str(data_rows[idx][col_boost]).strip().lower()
                if val in {"yes", "y", "true", "boosting", "1"}:
                    is_boosted    = True
                    views_organik = int(total_views * 0.4) if total_views else 0

            bulk_updates.extend(
                _build_row_updates(
                    req.sheet_name, gs_row,
                    total_views, views_organik, is_boosted,
                    col_imp, col_imp_ori, col_status_idx,
                )
            )
            log(
                f"  ✅ [R{round_num}] [{order}/{total}] baris {gs_row} "
                f"→ views={total_views:,} | streak={consec_ok} | jeda={delay:.1f}s"
            )
        else:
            consec_ok = 0
            still_failed.append((idx, gs_row, url))
            log(
                f"  ❌ [R{round_num}] [{order}/{total}] baris {gs_row} "
                f"→ {status} | jeda={delay:.1f}s"
            )

        if order % CHECKPOINT_AT == 0 and bulk_updates:
            log(f"[R{round_num} CKPT] Menyimpan {len(bulk_updates)} cells...")
            update_sheet_values(service, req.spreadsheet_id, bulk_updates)
            bulk_updates = []

        if order % random.randint(6, 10) == 0 and order < total:
            pause = random.uniform(40, 60)
            log(f"☕ [R{round_num}] Anti-ban {pause:.1f}s...")
            time.sleep(pause)

    if bulk_updates:
        update_sheet_values(service, req.spreadsheet_id, bulk_updates)

    all_failed = still_failed + remaining
    log(
        f"✔ R{round_num} selesai — "
        f"OK: {total - len(still_failed)}/{total} | gagal: {len(all_failed)}"
    )
    return all_failed


# ─── CORE JOB ────────────────────────────────────────────────

def run_job(req: SpreadsheetRequest):
    global _consec_rate_limits, _consec_conn_errors

    with job_lock:
        if job_status["running"]:
            log("[JOB] Dilewati: job sebelumnya masih berjalan.")
            return
        job_status.update({
            "running": True, "progress": 0, "total": 0,
            "processed": 0, "log": [], "error": None,
            "retry_round": 0, "retry_total": 0, "retry_processed": 0,
        })

    _consec_rate_limits = 0
    _consec_conn_errors  = 0

    try:
        service = get_sheets_service(req.google_credentials)
        loader  = get_loader()

        raw_rows = get_sheet_values(service, req.spreadsheet_id, f"'{req.sheet_name}'!A1:ZZ")
        if not raw_rows:
            log("[JOB] Sheet kosong, skip.")
            return

        header    = raw_rows[0]
        data_rows = raw_rows[1:]

        col_map: dict[str, int] = {}
        for i, c in enumerate(header):
            cl = c.lower()
            if "link post" in cl:           col_map["link"]    = i
            elif "total imp by job" in cl:  col_map["imp"]     = i
            elif "total imp organik" in cl: col_map["imp_ori"] = i
            elif "status(job)" in cl:       col_map["status"]  = i
            elif "boost" in cl:             col_map["boost"]   = i

        if "link" not in col_map:
            log("[JOB] Kolom 'link post' tidak ditemukan!")
            return

        col_link       = col_map["link"]
        col_imp        = col_map.get("imp")
        col_imp_ori    = col_map.get("imp_ori")
        col_status_idx = col_map.get("status")
        col_boost      = col_map.get("boost")

        updates_header = []
        if col_imp_ori is None:
            col_imp_ori = len(header); header.append("TOTAL IMP ORGANIK(VIEW COUNT)")
            updates_header.append({
                "range": f"'{req.sheet_name}'!{col_letter(col_imp_ori)}1",
                "values": [["TOTAL IMP ORGANIK(VIEW COUNT)"]],
            })
        if col_imp is None:
            col_imp = len(header); header.append("TOTAL IMP BY JOB(PLAY COUNT)")
            updates_header.append({
                "range": f"'{req.sheet_name}'!{col_letter(col_imp)}1",
                "values": [["TOTAL IMP BY JOB(PLAY COUNT)"]],
            })
        if col_status_idx is None:
            col_status_idx = len(header); header.append("STATUS(JOB)")
            updates_header.append({
                "range": f"'{req.sheet_name}'!{col_letter(col_status_idx)}1",
                "values": [["STATUS(JOB)"]],
            })
        if updates_header:
            update_sheet_values(service, req.spreadsheet_id, updates_header)

        now    = _wib_now()
        expiry = timedelta(hours=24)

        url_index_pairs = []
        for i, row in enumerate(data_rows):
            if col_link >= len(row) or "instagram.com" not in str(row[col_link]):
                continue
            need_process = True
            if col_status_idx is not None and col_status_idx < len(row):
                s = str(row[col_status_idx]).strip()
                if "[ORGANIC]" in s or "[BOOSTED]" in s:
                    m = _TS_RE.search(s)
                    if m:
                        try:
                            if now - datetime.strptime(m.group(0), "%Y-%m-%d %H:%M:%S") < expiry:
                                need_process = False
                        except ValueError:
                            pass
            if need_process:
                url_index_pairs.append((i, i + 2, row[col_link].strip()))

        total_antrean       = len(url_index_pairs)
        job_status["total"] = total_antrean

        if not url_index_pairs:
            log("[JOB] Selesai. Semua data masih segar (<24 jam).")
            return

        sampled = random.sample(
            url_index_pairs,
            min(req.batch_size or 300, total_antrean),
        )
        valid_pool = []
        for idx, gs_line, url in sampled:
            if extract_shortcode(url):
                valid_pool.append((idx, gs_line, url))
            else:
                log(f"  ⚠️ Skip URL tidak valid: {url}")

        limit = len(valid_pool)
        if not limit:
            log("[JOB] Tidak ada URL valid.")
            return

        log(f"[JOB] {total_antrean} kedaluwarsa — memproses {limit} URL batch ini.")

        # ════════════════════════════════════════════════════
        # PASS UTAMA
        # ════════════════════════════════════════════════════
        bulk_updates  = []
        failed_queue  = []          
        processed     = 0
        consec_ok     = 0
        CHECKPOINT_AT = 15

        for idx, gs_row, url in valid_pool:
            if not job_status["running"]:
                log("[JOB] Dihentikan paksa.")
                break

            delay = (
                random.uniform(DELAY_FAST_MIN, DELAY_FAST_MAX) if consec_ok >= 3
                else random.uniform(DELAY_SLOW_MIN, DELAY_SLOW_MAX)
            )
            time.sleep(delay)

            total_views, views_organik, status, is_boosted_api = fetch_single(
                url, loader, is_retry=False
            )

            if status == "stopped":
                break

            processed += 1
            job_status["processed"] = processed
            job_status["progress"]  = round(processed / limit * 100)

            if status != "ok" or total_views == 0:
                consec_ok = 0
                failed_queue.append((idx, gs_row, url))
                bulk_updates.append({
                    "range":  f"'{req.sheet_name}'!{col_letter(col_status_idx)}{gs_row}",
                    "values": [["[PENDING_RETRY]"]],
                })
                log(
                    f"  ⚠️ [{processed}/{limit}] baris {gs_row} "
                    f"→ {status} (antri retry) | jeda={delay:.1f}s"
                )
            else:
                consec_ok += 1

                is_boosted = is_boosted_api
                if col_boost is not None and col_boost < len(data_rows[idx]):
                    val = str(data_rows[idx][col_boost]).strip().lower()
                    if val in {"yes", "y", "true", "boosting", "1"}:
                        is_boosted    = True
                        views_organik = int(total_views * 0.4) if total_views else 0

                bulk_updates.extend(
                    _build_row_updates(
                        req.sheet_name, gs_row,
                        total_views, views_organik, is_boosted,
                        col_imp, col_imp_ori, col_status_idx,
                    )
                )
                log(
                    f"  ✓ [{processed}/{limit}] baris {gs_row} "
                    f"→ views={total_views:,} | streak={consec_ok} | jeda={delay:.1f}s"
                )

            if processed % CHECKPOINT_AT == 0 and bulk_updates:
                log(f"[CKPT] Menyimpan {len(bulk_updates)} cells...")
                update_sheet_values(service, req.spreadsheet_id, bulk_updates)
                bulk_updates = []

            if processed % random.randint(8, 14) == 0 and processed < limit:
                pause = random.uniform(20, 35) if limit <= 30 else random.uniform(30, 50)
                log(f"☕ [ANTI-BAN] Istirahat {pause:.1f}s...")
                time.sleep(pause)

        if bulk_updates:
            log(f"[WRITE] Menyimpan {len(bulk_updates)} cells...")
            update_sheet_values(service, req.spreadsheet_id, bulk_updates)

        log(
            f"\n[JOB] Pass utama selesai — "
            f"OK: {processed - len(failed_queue)}/{processed} | "
            f"retry: {len(failed_queue)}"
        )

        # ════════════════════════════════════════════════════
        # RETRY ROUNDS
        # ════════════════════════════════════════════════════
        retry_queue = failed_queue[:]

        for round_num in range(1, MAX_RETRY_ROUNDS + 1):
            if not retry_queue:
                log("[RETRY] Semua berhasil, tidak ada yang tersisa.")
                break
            if not job_status["running"]:
                log("[RETRY] Job dihentikan.")
                break

            retry_queue = run_retry_round(
                round_num=round_num,
                retry_queue=retry_queue,
                data_rows=data_rows,
                service=service,
                req=req,
                col_imp=col_imp,
                col_imp_ori=col_imp_ori,
                col_status_idx=col_status_idx,
                col_boost=col_boost,
            )

        if retry_queue:
            log(f"\n[FINAL] {len(retry_queue)} URL tetap gagal setelah semua retry.")
            final_upd = []
            for _, gs_row, url in retry_queue:
                sc = extract_shortcode(url) or url
                final_upd.append({
                    "range":  f"'{req.sheet_name}'!{col_letter(col_status_idx)}{gs_row}",
                    "values": [[f"[FAILED] {_wib_now_str()} | {sc}"]],
                })
            if final_upd:
                update_sheet_values(service, req.spreadsheet_id, final_upd)
                log(f"[FINAL] {len(final_upd)} baris ditandai [FAILED].")

    except Exception as e:
        job_status["error"] = str(e)
        log(f"[ERROR] {e}")
    finally:
        job_status["running"]  = False
        job_status["last_run"] = _wib_now_str()
        log(f"\n[JOB] Selesai total pada {job_status['last_run']}")


def trigger_automatic_job():
    global current_job_req
    if current_job_req is None or not getattr(current_job_req, "spreadsheet_id", "").strip():
        return
    if not job_status["running"]:
        run_job(current_job_req)

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
                HfApi(token=hf_token).add_space_secret(repo_id=HF_REPO_ID, key="INSTAGRAM_COOKIES", value=cookies_json)
            except Exception:
                pass

        reset_loader()
        return {"success": True, "message": "Session saved", "total": len(filtered)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/api/instagram/save-session2")
async def save_instagram_session(payload: dict):
    try:
        cookies  = payload.get("cookies", [])
        filtered = [c for c in cookies if c.get("name") in IMPORTANT_COOKIES]
        if not filtered:
            raise HTTPException(status_code=400, detail="Tidak ada cookies valid")

        cookies_json = json.dumps(filtered)
        os.environ["INSTAGRAM_COOKIES"] = cookies_json

        hf_token = os.environ.get("HF_TOKEN2")
        if hf_token:
            try:
                from huggingface_hub import HfApi
                HfApi(token=hf_token).add_space_secret(repo_id=HF_REPO_ID, key="INSTAGRAM_COOKIES", value=cookies_json)
            except Exception:
                pass

        reset_loader()
        return {"success": True, "message": "Session saved", "total": len(filtered)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/spreadSheet")
async def spreadsheet_job(req: SpreadsheetRequest, background_tasks: BackgroundTasks):
    global current_job_req
    _validate_session()
    if job_status["running"]:
        raise HTTPException(status_code=409, detail="Job sedang berjalan.")

    current_job_req = req
    background_tasks.add_task(run_job, req)
    _ensure_scheduler()

    return {
        "message":        "Job dimulai.",
        "spreadsheet_id": req.spreadsheet_id,
        "sheet_name":     req.sheet_name,
        "status":         "started",
    }


@app.get("/status")
def get_status():
    return {
        "running":        job_status["running"],
        "progress":       job_status["progress"],
        "processed":      job_status["processed"],
        "total":          job_status["total"],
        "last_run":       job_status["last_run"],
        "error":          job_status["error"],
        "log":            job_status["log"][-50:],
        "session_active": bool(os.environ.get("INSTAGRAM_COOKIES")),
    }


@app.post("/stop")
def stop_job():
    job_status["running"] = False
    return {"message": "Stop signal dikirim."}


@app.get("/api/instagram/session-status")
def session_status():
    raw = os.environ.get("INSTAGRAM_COOKIES", "")
    if not raw:
        return {"active": False, "cookie_names": []}
    try:
        cookies = json.loads(raw)
        return {"active": bool(cookies), "cookie_names": [c["name"] for c in cookies]}
    except Exception:
        return {"active": False, "cookie_names": []}


@app.post("/api/job/restart")
async def restart_job(req: SpreadsheetRequest, background_tasks: BackgroundTasks):
    global current_job_req
    _validate_session()
    if not req.spreadsheet_id.strip():
        raise HTTPException(status_code=400, detail="Spreadsheet ID tidak boleh kosong.")

    with job_lock:
        job_status["running"] = False
        time.sleep(1.0)

    current_job_req = req
    background_tasks.add_task(run_job, req)
    return {
        "message":        "Job di-restart.",
        "spreadsheet_id": req.spreadsheet_id,
        "sheet_name":     req.sheet_name,
        "status":         "restarted",
    }


def _validate_session():
    raw = os.environ.get("INSTAGRAM_COOKIES", "")
    if not raw:
        raise HTTPException(status_code=401, detail="Session belum tersedia.")
    try:
        parsed = json.loads(raw)
        if not {c["name"]: c["value"] for c in parsed}.get("sessionid"):
            raise ValueError
    except Exception:
        raise HTTPException(status_code=401, detail="Session tidak valid.")


def _ensure_scheduler():
    if not scheduler.get_job("automatic_ig_job"):
        if not scheduler.running:
            scheduler.start()
        scheduler.add_job(trigger_automatic_job, "interval", hours=24, id="automatic_ig_job")


@app.on_event("startup")
async def startup_event():
    if os.environ.get("INSTAGRAM_COOKIES"):
        reset_loader()


@app.on_event("shutdown")
async def shutdown_event():
    if scheduler.running:
        scheduler.shutdown()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("worker:app", host="0.0.0.0", port=7860, reload=False)