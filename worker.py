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

from google.oauth2 import service_account
from googleapiclient.discovery import build

scheduler = BackgroundScheduler()
current_job_req = None
_consecutive_conn_errors = 0

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
MAX_FAIL_COUNT    = 5          # dinaikkan agar retry lebih agresif
MAX_RETRY_ROUNDS  = 2          # jumlah putaran retry setelah batch utama selesai
RETRY_BATCH_LIMIT = 50         # max URL per putaran retry

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

# Menyimpan (idx, gs_row, url) yang gagal / hasilnya 0 untuk di-retry
_failed_shortcodes: dict[str, int] = {}
_zero_result_queue: list[tuple[int, int, str]] = []   # antrean retry khusus nilai 0

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
        ("file", lambda: open("storage/cookies.json").read() if os.path.exists("storage/cookies.json") else None),
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
        L.context._session.cookies.update({k: ig_cookies.get(k, "") for k in IMPORTANT_COOKIES})
        L.context._session.headers.update(_build_random_headers(ig_cookies.get("csrftoken", "")))
        L.context.username = IG_USERNAME

        _loader_instance = L
        return L


# ─── INSTAGRAM FETCH HELPERS ─────────────────────────────────

_SHORTCODE_RE = re.compile(r"/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)")
_TS_RE        = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")


def extract_shortcode(url: str) -> Optional[str]:
    m = _SHORTCODE_RE.search(url.strip().rstrip("/"))
    return m.group(1) if m else None


def fetch_fresh_post(shortcode: str, loader: instaloader.Instaloader, is_retry: bool = False):
    """
    Fetch post data dari Instagram.
    - is_retry=True  → jeda lebih panjang + 3 endpoint dicoba
    - is_retry=False → jeda normal + 2 endpoint dicoba
    """
    session = loader.context._session
    session.headers.update({
        "user-agent":      random.choice(USER_AGENTS),
        "accept-language": random.choice(ACCEPT_LANGUAGES),
        "referer":         f"https://www.instagram.com/p/{shortcode}/",
    })

    endpoints = [
        f"https://www.instagram.com/p/{shortcode}/?__a=1&__d=dis",
        f"https://www.instagram.com/reel/{shortcode}/?__a=1&__d=dis",
    ]
    if is_retry:
        # Tambah endpoint alternatif saat retry
        endpoints.append(f"https://www.instagram.com/tv/{shortcode}/?__a=1&__d=dis")
    random.shuffle(endpoints)

    last_status = None
    for url in endpoints:
        try:
            resp = session.get(url, timeout=(5, 15) if is_retry else (5, 10))
            last_status = resp.status_code

            if resp.status_code == 200:
                items = resp.json().get("items", [])
                if items:
                    item = items[0]
                    class MockPost:
                        is_video       = item.get("media_type", 1) in (1, 2)
                        _full_metadata = item
                        likes          = item.get("like_count", 0)
                        comments       = item.get("comment_count", 0)
                    return MockPost(), "ok"

            if resp.status_code == 429:
                log(f"  ⚡ Rate limit (429) pada {shortcode}, berhenti coba endpoint lain.")
                return None, "rate_limit"

            if resp.status_code in (404, 410):
                return None, "not_found"

        except Exception as e:
            err_name = type(e).__name__
            if "ConnectionException" in err_name or "ConnectionError" in err_name:
                return None, "connection_error"
            # Timeout / generic — lanjut coba endpoint berikutnya
            continue

    # Fallback ke instaloader langsung
    try:
        post = instaloader.Post.from_shortcode(loader.context, shortcode)
        return post, "ok"
    except Exception as e:
        err_name = type(e).__name__
        if "ConnectionException" in err_name or "TooManyRequestsException" in err_name:
            return None, "rate_limit"
        return None, "error"


def get_views_from_post(post) -> tuple[int, int, bool]:
    raw = getattr(post, "_full_metadata", {}) or {}

    # ── Konten Foto / Non-video ──────────────────────────────
    if not raw.get("is_video", True) and raw.get("__typename") != "GraphVideo":
        likes_count = (
            raw.get("edge_media_preview_like", {}).get("count", 0)
            or raw.get("like_count", 0)
            or getattr(post, "likes", 0)
            or 0
        )
        return likes_count, likes_count, False

    # ── Video / Reel ─────────────────────────────────────────
    total_views = 0
    for key in ("video_play_count", "play_count", "ig_play_count",
                "edge_media_to_media_video_view"):
        val = raw.get(key)
        if isinstance(val, dict):
            val = val.get("count", 0)
        if isinstance(val, (int, float)) and val > 0:
            total_views = int(val)
            break

    if total_views == 0:
        return 0, 0, False

    views_organik = raw.get("video_view_count", 0) or 0
    is_boosted    = any(raw.get(f) is True for f in BOOST_FLAGS)

    if is_boosted:
        if not (0 < views_organik < total_views):
            views_organik = int(total_views * 0.40)
    else:
        views_organik = total_views

    views_organik = max(1, min(views_organik, total_views))
    return total_views, views_organik, is_boosted


def fetch_single_sequential(
    url: str,
    loader: instaloader.Instaloader,
    is_retry: bool = False,
) -> tuple[int, int, str, bool]:
    """
    Mengambil views dari satu URL.
    Return: (total_views, views_organik, status, is_boosted)
    status: "ok" | "error" | "rate_limit" | "not_found" | "invalid_url" | "skipped" | "stopped"
    """
    global _consecutive_conn_errors

    shortcode = extract_shortcode(url)
    if not shortcode:
        return 0, 0, "invalid_url", False

    fail_count = _failed_shortcodes.get(shortcode, 0)
    if fail_count >= MAX_FAIL_COUNT:
        return 0, 0, "skipped", False

    attempts = 3 if is_retry else 2

    for attempt in range(attempts):
        if not job_status["running"]:
            return 0, 0, "stopped", False

        post, fetch_status = fetch_fresh_post(shortcode, loader, is_retry=is_retry)

        # ── Tangani error fetch ──────────────────────────────
        if fetch_status == "not_found":
            return 0, 0, "not_found", False

        if fetch_status in ("rate_limit", "connection_error"):
            _consecutive_conn_errors += 1
            wait = random.uniform(20, 40) if fetch_status == "rate_limit" else random.uniform(10, 20)
            log(f"  ⏳ {fetch_status} pada {shortcode} (attempt {attempt+1}/{attempts}), tunggu {wait:.0f}s...")
            time.sleep(wait)

            if _consecutive_conn_errors >= 3:
                log(f"  🔄 {_consecutive_conn_errors} error berturut, reset loader...")
                time.sleep(30)
                reset_loader()
                loader = get_loader()
                _consecutive_conn_errors = 0
            continue

        if post is None:
            if attempt < attempts - 1:
                time.sleep(random.uniform(5.0, 10.0))
            continue

        # ── Parse views ─────────────────────────────────────
        total_views, views_organik, is_boosted = get_views_from_post(post)

        if total_views > 0:
            _failed_shortcodes.pop(shortcode, None)
            _consecutive_conn_errors = 0
            return total_views, views_organik, "ok", is_boosted

        # Views = 0 tapi post berhasil diambil → kemungkinan foto tanpa views
        # Coba sekali lagi dengan jeda lebih panjang
        if attempt < attempts - 1:
            log(f"  🔁 Views=0 pada {shortcode}, coba ulang (attempt {attempt+1}/{attempts})...")
            time.sleep(random.uniform(8.0, 15.0) if is_retry else random.uniform(5.0, 8.0))

    # Semua attempt habis — catat sebagai gagal
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


# ─── SHARED WRITE HELPER ─────────────────────────────────────

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
    """Buat payload batchUpdate untuk satu baris."""
    ts    = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S")
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
    round_num: int,
    retry_queue: list[tuple[int, int, str]],
    data_rows: list,
    service,
    loader: instaloader.Instaloader,
    req: SpreadsheetRequest,
    col_imp: Optional[int],
    col_imp_ori: int,
    col_status_idx: int,
    col_boost: Optional[int],
) -> list[tuple[int, int, str]]:
    """
    Jalankan satu putaran retry untuk URL yang masih bernilai 0 / error.
    Mengembalikan sisa antrian yang masih gagal.
    """
    if not retry_queue:
        return []

    # Batasi jumlah per putaran
    batch = retry_queue[:RETRY_BATCH_LIMIT]
    remaining = retry_queue[RETRY_BATCH_LIMIT:]

    total = len(batch)
    job_status["retry_round"]     = round_num
    job_status["retry_total"]     = total
    job_status["retry_processed"] = 0

    log(f"\n{'='*55}")
    log(f"🔁 RETRY ROUND {round_num} — {total} URL akan dicoba ulang")
    log(f"{'='*55}")

    # Jeda sebelum mulai retry agar server Instagram tidak curiga
    log(f"⏳ Menunggu 30 detik sebelum mulai retry round {round_num}...")
    time.sleep(30)

    # Reset loader untuk sesi baru
    reset_loader()
    loader = get_loader()

    bulk_updates   = []
    still_failed   = []
    consecutive_ok = 0
    CHECKPOINT_EVERY = 10

    for order, (idx, gs_row, url) in enumerate(batch, start=1):
        if not job_status["running"]:
            log("[RETRY] Dihentikan paksa.")
            still_failed.extend(batch[order - 1:])
            break

        # Jeda adaptif — retry lebih sabar
        if consecutive_ok >= 3:
            delay = random.uniform(8.0, 14.0)
        else:
            delay = random.uniform(18.0, 28.0)
        time.sleep(delay)

        total_views, views_organik, status, is_boosted_api = fetch_single_sequential(
            url, loader, is_retry=True
        )

        if status == "stopped":
            still_failed.extend(batch[order - 1:])
            break

        if status == "ok" and total_views > 0:
            consecutive_ok += 1

            # Override boost dari kolom manual
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
            job_status["retry_processed"] = order
            log(
                f"  ✅ [RETRY {round_num}] [{order}/{total}] baris {gs_row} "
                f"→ views={total_views:,} | jeda={delay:.1f}s"
            )
        else:
            consecutive_ok = 0
            still_failed.append((idx, gs_row, url))
            log(
                f"  ❌ [RETRY {round_num}] [{order}/{total}] baris {gs_row} "
                f"→ status={status}, views={total_views} | jeda={delay:.1f}s"
            )

        # Checkpoint
        if order % CHECKPOINT_EVERY == 0 and bulk_updates:
            log(f"[RETRY CHECKPOINT] Menyimpan {len(bulk_updates)} cells...")
            update_sheet_values(service, req.spreadsheet_id, bulk_updates)
            bulk_updates = []

        # Anti-ban pause
        if order % random.randint(6, 10) == 0 and order < total:
            pause = random.uniform(30, 50)
            log(f"☕ [RETRY ANTI-BAN] Istirahat {pause:.1f}s...")
            time.sleep(pause)

    # Simpan sisa update
    if bulk_updates:
        log(f"[RETRY WRITE] Menyimpan {len(bulk_updates)} cells tersisa...")
        update_sheet_values(service, req.spreadsheet_id, bulk_updates)

    # Gabungkan kembali dengan sisa yang belum masuk batch ini
    all_still_failed = still_failed + remaining
    log(
        f"✔ RETRY ROUND {round_num} selesai. "
        f"Berhasil: {total - len(still_failed)}/{total} | "
        f"Masih gagal: {len(all_still_failed)}"
    )
    return all_still_failed


# ─── CORE JOB ────────────────────────────────────────────────

def run_job(req: SpreadsheetRequest):
    global _zero_result_queue

    with job_lock:
        if job_status["running"]:
            log("[JOB] Dilewati: job sebelumnya masih berjalan.")
            return
        job_status.update({
            "running": True, "progress": 0, "total": 0,
            "processed": 0, "log": [], "error": None,
            "retry_round": 0, "retry_total": 0, "retry_processed": 0,
        })

    _zero_result_queue = []

    try:
        service = get_sheets_service(req.google_credentials)
        loader  = get_loader()

        raw_rows = get_sheet_values(service, req.spreadsheet_id, f"'{req.sheet_name}'!A1:ZZ")
        if not raw_rows:
            log("[JOB] Sheet kosong, skip.")
            return

        header    = raw_rows[0]
        data_rows = raw_rows[1:]

        # ── Deteksi kolom ────────────────────────────────────
        col_map = {}
        for i, c in enumerate(header):
            cl = c.lower()
            if "link post" in cl:          col_map["link"]    = i
            elif "total imp by job" in cl: col_map["imp"]     = i
            elif "total imp organik" in cl: col_map["imp_ori"] = i
            elif "status(job)" in cl:      col_map["status"]  = i
            elif "boost" in cl:            col_map["boost"]   = i

        if "link" not in col_map:
            log("[JOB] Kolom 'link post' tidak ditemukan!")
            return

        col_link       = col_map["link"]
        col_imp        = col_map.get("imp")
        col_imp_ori    = col_map.get("imp_ori")
        col_status_idx = col_map.get("status")
        col_boost      = col_map.get("boost")

        # ── Buat kolom baru jika belum ada ───────────────────
        updates_header = []
        if col_imp_ori is None:
            col_imp_ori = len(header)
            header.append("TOTAL IMP ORGANIK(VIEW COUNT)")
            updates_header.append({
                "range":  f"'{req.sheet_name}'!{col_letter(col_imp_ori)}1",
                "values": [["TOTAL IMP ORGANIK(VIEW COUNT)"]],
            })

        if col_imp is None:
            col_imp = len(header)
            header.append("TOTAL IMP BY JOB(PLAY COUNT)")
            updates_header.append({
                "range":  f"'{req.sheet_name}'!{col_letter(col_imp)}1",
                "values": [["TOTAL IMP BY JOB(PLAY COUNT)"]],
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

        # ── Filter baris yang perlu diproses ─────────────────
        now    = _wib_now()
        expiry = timedelta(hours=24)

        url_index_pairs: list[tuple[int, int, str]] = []
        for i, row in enumerate(data_rows):
            if col_link >= len(row) or "instagram.com" not in str(row[col_link]):
                continue

            need_process = True
            if col_status_idx is not None and col_status_idx < len(row):
                status_raw = str(row[col_status_idx]).strip()
                if "[ORGANIC]" in status_raw or "[BOOSTED]" in status_raw:
                    m = _TS_RE.search(status_raw)
                    if m:
                        try:
                            last_run = datetime.strptime(m.group(0), "%Y-%m-%d %H:%M:%S")
                            if now - last_run < expiry:
                                need_process = False
                        except ValueError:
                            pass

            if need_process:
                url_index_pairs.append((i, i + 2, row[col_link].strip()))

        total_antrean = len(url_index_pairs)
        job_status["total"] = total_antrean

        if not url_index_pairs:
            log("[JOB] Selesai. Semua data masih segar (<24 jam).")
            return

        # ── Ambil Sampel Acak & validasi shortcode ───────────
        sampled_pairs = random.sample(
            url_index_pairs,
            min(req.batch_size or 300, total_antrean),
        )

        valid_pool: list[tuple[int, int, str]] = []
        skipped = 0
        for idx, gs_line, url in sampled_pairs:
            if extract_shortcode(url):
                valid_pool.append((idx, gs_line, url))
            else:
                skipped += 1
                log(f"  ⚠️ Skip URL tidak valid: {url}")

        if skipped:
            log(f"[JOB] {skipped} URL dilewati (format tidak valid).")

        limit = len(valid_pool)
        if not limit:
            log("[JOB] Tidak ada URL valid untuk diproses.")
            return

        log(f"[JOB] {total_antrean} kedaluwarsa. Memproses {limit} URL valid batch ini.")

        # ═══════════════════════════════════════════════════════
        # ── PASS UTAMA ─────────────────────────────────────────
        # ═══════════════════════════════════════════════════════
        bulk_updates:    list[dict]             = []
        failed_queue:    list[tuple[int,int,str]] = []   # URL hasil 0/error → akan di-retry
        processed_count  = 0
        consecutive_ok   = 0
        CHECKPOINT_EVERY = 15

        for idx, gs_row, url in valid_pool:
            if not job_status["running"]:
                log("[JOB] Dihentikan paksa.")
                break

            # Jeda adaptif
            delay = random.uniform(5.0, 9.0) if consecutive_ok >= 3 else random.uniform(10.0, 16.0)
            time.sleep(delay)

            total_views, views_organik, status, is_boosted_api = fetch_single_sequential(
                url, loader, is_retry=False
            )

            if status == "stopped":
                break

            processed_count += 1
            job_status["processed"] = processed_count
            job_status["progress"]  = round(processed_count / limit * 100)

            # ── Nilai 0 atau gagal → masukkan antrian retry ──
            if status != "ok" or total_views == 0:
                consecutive_ok = 0
                failed_queue.append((idx, gs_row, url))
                log(
                    f"  ⚠️ [{processed_count}/{limit}] baris {gs_row} "
                    f"→ status={status}, views=0 (antri retry) | jeda={delay:.1f}s"
                )
                # Update status PENDING di sheet agar tidak dianggap "segar"
                bulk_updates.append({
                    "range":  f"'{req.sheet_name}'!{col_letter(col_status_idx)}{gs_row}",
                    "values": [["[PENDING_RETRY]"]],
                })
            else:
                consecutive_ok += 1

                # Override boost dari kolom manual
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
                    f"  ✓ [{processed_count}/{limit}] baris {gs_row} "
                    f"→ views={total_views:,} | streak={consecutive_ok} | jeda={delay:.1f}s"
                )

            # Checkpoint write
            if processed_count % CHECKPOINT_EVERY == 0 and bulk_updates:
                log(f"[CHECKPOINT] Menyimpan {len(bulk_updates)} cells...")
                update_sheet_values(service, req.spreadsheet_id, bulk_updates)
                bulk_updates = []

            # Anti-ban coffee break
            if processed_count % random.randint(8, 14) == 0 and processed_count < limit:
                pause = random.uniform(15, 30) if limit <= 30 else random.uniform(25, 45)
                log(f"☕ [ANTI-BAN] Istirahat {pause:.1f}s...")
                time.sleep(pause)

        # ── Simpan sisa update pass utama ────────────────────
        if bulk_updates:
            log(f"[WRITE] Menyimpan {len(bulk_updates)} cells tersisa...")
            update_sheet_values(service, req.spreadsheet_id, bulk_updates)

        log(
            f"\n[JOB] Pass utama selesai. "
            f"Berhasil: {processed_count - len(failed_queue)}/{processed_count} | "
            f"Masuk retry: {len(failed_queue)}"
        )

        # ═══════════════════════════════════════════════════════
        # ── RETRY ROUNDS (setelah semua data batch utama ditulis)
        # ═══════════════════════════════════════════════════════
        retry_queue = failed_queue[:]

        for round_num in range(1, MAX_RETRY_ROUNDS + 1):
            if not retry_queue:
                log("[RETRY] Tidak ada URL tersisa untuk di-retry. Selesai.")
                break
            if not job_status["running"]:
                log("[RETRY] Job dihentikan, skip retry.")
                break

            retry_queue = run_retry_round(
                round_num=round_num,
                retry_queue=retry_queue,
                data_rows=data_rows,
                service=service,
                loader=loader,
                req=req,
                col_imp=col_imp,
                col_imp_ori=col_imp_ori,
                col_status_idx=col_status_idx,
                col_boost=col_boost,
            )

        # ── Tandai URL yang benar-benar tidak bisa diambil ───
        if retry_queue:
            log(f"\n[FINAL] {len(retry_queue)} URL tetap gagal setelah semua retry.")
            final_updates = []
            for _, gs_row, url in retry_queue:
                sc = extract_shortcode(url) or url
                final_updates.append({
                    "range":  f"'{req.sheet_name}'!{col_letter(col_status_idx)}{gs_row}",
                    "values": [[f"[FAILED] {_wib_now_str()} | {sc}"]],
                })
            if final_updates:
                update_sheet_values(service, req.spreadsheet_id, final_updates)
                log(f"[FINAL] Status [FAILED] ditulis untuk {len(final_updates)} baris.")

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
                HfApi(token=hf_token).add_space_secret(
                    repo_id=HF_REPO_ID, key="INSTAGRAM_COOKIES", value=cookies_json
                )
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
        "running":         job_status["running"],
        "progress":        job_status["progress"],
        "processed":       job_status["processed"],
        "total":           job_status["total"],
        "last_run":        job_status["last_run"],
        "error":           job_status["error"],
        "log":             job_status["log"][-50:],
        "session_active":  bool(os.environ.get("INSTAGRAM_COOKIES")),
        "retry_round":     job_status["retry_round"],
        "retry_total":     job_status["retry_total"],
        "retry_processed": job_status["retry_processed"],
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
        scheduler.add_job(
            trigger_automatic_job,
            "interval",
            hours=24,
            id="automatic_ig_job",
        )


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
