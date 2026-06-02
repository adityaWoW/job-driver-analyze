import os
import json
import time
import random
import asyncio
import threading
import instaloader
import re

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from threading import Lock

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ─── FASTAPI APP ─────────────────────────────────────────────
app = FastAPI(title="IG View Worker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── CONSTANTS ───────────────────────────────────────────────
IG_USERNAME       = "Ace.Shuttle"
HF_REPO_ID        = "adityaUHU/job-driver"
IMPORTANT_COOKIES = ["sessionid", "csrftoken", "ds_user_id", "ig_did", "mid"]

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
    """Dikirim dari form FE — langsung trigger job."""
    spreadsheet_id:     str
    sheet_name:         str
    batch_size:         Optional[int] = 100
    google_credentials: Optional[dict] = None   # override env jika perlu


# ─── INSTAGRAM SESSION ───────────────────────────────────────

def load_instagram_cookies() -> dict:
    """
    Urutan prioritas:
    1. Env var INSTAGRAM_COOKIES (di-set saat runtime / dari HF Secret)
    2. File lokal storage/cookies.json (fallback dev)
    """
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
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "accept":          "*/*",
            "accept-language": "en-US,en;q=0.9,id;q=0.8",
            "origin":          "https://www.instagram.com",
        })
        L.context.username = IG_USERNAME

        try:
            L.context.graphql_query("d6f4427fbe92d846298cf93df0b937d3", {})
            print(f"✅ Session aktif — login sebagai: {IG_USERNAME}")
        except Exception as e:
            print(f"⚠️ Verifikasi session gagal ({e}). Melanjutkan...")

        _loader_instance = L
        return L


# ─── INSTAGRAM FETCH HELPERS (dari analyzer.py) ──────────────

def extract_shortcode(url: str) -> str | None:
    url   = url.strip().rstrip("/")
    match = re.search(r"/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)", url)
    return match.group(1) if match else None


def fetch_fresh_post(shortcode: str, loader: instaloader.Instaloader):
    """Coba endpoint JSON dulu, fallback ke GraphQL instaloader."""
    for url in [
        f"https://www.instagram.com/p/{shortcode}/?__a=1&__d=dis",
        f"https://www.instagram.com/reel/{shortcode}/?__a=1&__d=dis",
    ]:
        try:
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
    """Extract total views, organic views, dan is_boosted dari post object."""
    raw       = getattr(post, "_full_metadata", {}) or {}
    shortcode = raw.get("code", "unknown")

    # Jika bukan video, pakai likes sebagai proxy
    if not raw.get("is_video", True) and raw.get("__typename") != "GraphVideo":
        likes_count = (
            raw.get("edge_media_preview_like", {}).get("count", 0)
            or getattr(post, "likes", 0)
            or 0
        )
        return likes_count, likes_count, False

    # Ambil total views dari berbagai key yang mungkin ada
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

    # Deteksi boost dari berbagai flag
    is_boosted  = False
    boost_flags = ["is_ad", "is_boosted_post", "is_commercial", "is_paid_partnership"]
    debug_logs  = []
    for flag in boost_flags:
        val = raw.get(flag)
        debug_logs.append(f"{flag}: {val} ({type(val).__name__})")
        if val is True:
            is_boosted = True
    print(f"    [DEBUG-FLAGS] {shortcode} -> {' | '.join(debug_logs)}")

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


def fetch_single(args) -> tuple:
    """Fetch satu URL Instagram secara sinkron (dengan retry 2x)."""
    index, url, loader, print_lock, delay_range = args

    shortcode = extract_shortcode(url)
    if not shortcode:
        return index, 0, 0, "invalid_url", False

    time.sleep(random.uniform(*delay_range))

    for attempt in range(2):
        try:
            post = fetch_fresh_post(shortcode, loader)
            total_views, views_organik, is_boosted = get_views_from_post(post)

            if total_views > 0:
                with print_lock:
                    boost_status = "[BOOSTED]" if is_boosted else "[ORGANIC]"
                    print(f"  ✓ {shortcode}: Total:{total_views:,} | Ori:{views_organik:,} {boost_status}")
                return index, total_views, views_organik, "ok", is_boosted

            if attempt == 0:
                with print_lock:
                    print(f"  [RETRY] {shortcode} dapat 0, tunggu 15s lalu retry...")
                time.sleep(15)

        except Exception as e:
            with print_lock:
                print(f"  ✗ {shortcode} Error attempt {attempt + 1}: {type(e).__name__}")
            if attempt == 0:
                time.sleep(15)

    with print_lock:
        print(f"  ✗ {shortcode}: gagal setelah 2 percobaan")
    return index, 0, 0, "error", False


async def _worker_async(args, semaphore: asyncio.Semaphore):
    async with semaphore:
        return await asyncio.to_thread(fetch_single, args)


async def bulk_fetch_views_async(
    url_index_pairs: list,
    loader: instaloader.Instaloader,
    max_concurrent_tasks: int = 3,
    delay_range: tuple = (5.0, 10.0),
) -> dict:
    results    = {}
    print_lock = Lock()
    semaphore  = asyncio.Semaphore(max_concurrent_tasks)
    args_list  = [
        (idx, url, loader, print_lock, delay_range)
        for idx, url in url_index_pairs
    ]
    total = len(args_list)
    print(f"\n[ASYNC] Bulk fetch: {total} URL | {max_concurrent_tasks} slot | delay {delay_range[0]}–{delay_range[1]}s")
    print("─" * 55)

    tasks = [_worker_async(args, semaphore) for args in args_list]
    done  = 0
    for future in asyncio.as_completed(tasks):
        try:
            index, total_views, views_organik, status, is_boosted = await future
            results[index] = (total_views, views_organik, is_boosted, status)
        except Exception as e:
            print(f"  ✗ Task crash: {e}")
        done += 1
        print(f"  [{done}/{total}] selesai diproses")

    print("─" * 55)
    return results


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


# ─── CORE JOB ────────────────────────────────────────────────

def run_job(req: SpreadsheetRequest):
    with job_lock:
        if job_status["running"]:
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
        loader  = get_loader()   # pakai session yang sudah tersimpan

        raw_rows = get_sheet_values(service, req.spreadsheet_id, f"'{req.sheet_name}'!A1:ZZ")
        if not raw_rows:
            log("[JOB] Sheet kosong, skip.")
            return

        header    = raw_rows[0]
        data_rows = raw_rows[1:]

        # ── Deteksi kolom ──
        col_link       = next((i for i, c in enumerate(header) if "link post"                    in c.lower()), None)
        col_imp        = next((i for i, c in enumerate(header) if "total imp by job"             in c.lower()), None)
        col_imp_ori    = next((i for i, c in enumerate(header) if "total imp organik"            in c.lower()), None)
        col_status_idx = next((i for i, c in enumerate(header) if "status(job)"                 in c.lower()), None)
        col_boost      = next((i for i, c in enumerate(header) if "boost"                        in c.lower()), None)

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

        # ── Kumpulkan URL valid ──
        url_index_pairs = [
            (i, row[col_link].strip())
            for i, row in enumerate(data_rows)
            if col_link < len(row) and "instagram.com" in str(row[col_link])
        ]

        total = len(url_index_pairs)
        job_status["total"] = total
        log(f"[JOB] {total} URL ditemukan, mulai fetch async...")

        if not url_index_pairs:
            log("[JOB] Tidak ada URL Instagram valid.")
            return

        # ── Jalankan bulk fetch async ──
        results = asyncio.run(bulk_fetch_views_async(
            url_index_pairs,
            loader,
            max_concurrent_tasks=req.batch_size or 3,
            delay_range=(4.0, 7.0),
        ))

        # ── Bangun bulk write ke Sheets ──
        bulk_updates = []
        for idx, (total_views, views_organik, is_boosted_api, status) in results.items():
            # Cek override boost dari kolom spreadsheet
            is_boosted = is_boosted_api
            if col_boost is not None and col_boost < len(data_rows[idx]):
                val = str(data_rows[idx][col_boost]).strip().lower()
                if val in ["yes", "y", "true", "boosting", "1"]:
                    is_boosted    = True
                    views_organik = int(total_views * 0.4) if total_views else 0

            status_label = "[BOOSTED]" if is_boosted else "[ORGANIC]"
            gs_row       = idx + 2   # +1 header, +1 base-1

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

            log(f"  ✓ row {gs_row}: {total_views:,} | {views_organik:,} {status_label}")
            job_status["processed"] += 1
            job_status["progress"]   = round(job_status["processed"] / total * 100)

        # ── Execute bulk write ──
        if bulk_updates:
            log(f"[WRITE] Mengirimkan {len(bulk_updates)} cell ke Sheets...")
            update_sheet_values(service, req.spreadsheet_id, bulk_updates)
            log(f"[WRITE] Sukses! {total} baris diperbarui.")
        else:
            log("[WRITE] Tidak ada data untuk ditulis.")

        log(f"[JOB] Selesai! {total} URL diproses.")

    except Exception as e:
        job_status["error"] = str(e)
        log(f"[ERROR] {e}")
    finally:
        job_status["running"]  = False
        job_status["last_run"] = time.strftime("%Y-%m-%d %H:%M:%S")


# ─── ENDPOINTS ───────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "message": "IG View Worker is running"}

@app.post("/api/instagram/save-session")
async def save_instagram_session(payload: dict):
    """
    Dipanggil oleh browser extension setelah user login Instagram.
    Payload: { "cookies": [{name, value, ...}, ...] }
    """
    try:
        cookies  = payload.get("cookies", [])
        filtered = [c for c in cookies if c.get("name") in IMPORTANT_COOKIES]

        if not filtered:
            raise HTTPException(status_code=400, detail="Tidak ada cookies valid")

        cookies_json = json.dumps(filtered)

        # Simpan ke env var runtime
        os.environ["INSTAGRAM_COOKIES"] = cookies_json

        # Persist permanen ke HuggingFace Secret
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

        # Reset loader agar pakai cookies baru
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


# ── 2. Terima spreadsheet & jalankan job otomatis ─────────────
@app.post("/spreadSheet")
async def spreadsheet_job(req: SpreadsheetRequest, background_tasks: BackgroundTasks):
    # Validasi session tersedia
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

    background_tasks.add_task(run_job, req)

    return {
        "message":        "Job dimulai.",
        "spreadsheet_id": req.spreadsheet_id,
        "sheet_name":     req.sheet_name,
        "status":         "started",
    }


# ── 3. Polling status ─────────────────────────────────────────
@app.get("/status")
def get_status():
    # Cek session dari env (diisi dari HF Secret saat startup)
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


# ── 4. Stop job ───────────────────────────────────────────────
@app.post("/stop")
def stop_job():
    job_status["running"] = False
    return {"message": "Stop signal dikirim. Job berhenti setelah batch saat ini selesai."}


# ── 5. Cek session saja ───────────────────────────────────────
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


# ─── STARTUP: load cookies dari HF Secret / env ──────────────
@app.on_event("startup")
async def startup_event():
    """
    HuggingFace otomatis inject secret INSTAGRAM_COOKIES ke env var.
    Kita reset loader agar langsung terbaca saat pertama kali dipakai.
    """
    if os.environ.get("INSTAGRAM_COOKIES"):
        print("[STARTUP] INSTAGRAM_COOKIES ditemukan di env, loader siap dipakai.")
        reset_loader()
    else:
        print("[STARTUP] INSTAGRAM_COOKIES belum ada. Tunggu save-session dari extension.")


# ─── ENTRYPOINT ──────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("worker:app", host="0.0.0.0", port=7860, reload=False)