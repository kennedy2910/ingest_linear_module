# -*- coding: utf-8 -*-

import os
import re
import sqlite3
import secrets
from typing import List, Optional

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.youtube_duration import fetch_youtube_metadata


DATA_DIR = os.getenv("DATA_DIR", "/data")
DB_PATH = os.path.join(DATA_DIR, "central_nex.db")
os.makedirs(DATA_DIR, exist_ok=True)

app = FastAPI(title=os.getenv("CENTRAL_TITLE","Central-Nex"))

BASE_DIR = os.path.dirname(__file__)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

def youtube_to_embed(url: str | None):
    if not url:
        return None

    yt_patterns = [
        r"youtube\.com/watch\?v=([a-zA-Z0-9_-]+)",
        r"youtube\.com/live/([a-zA-Z0-9_-]+)",
        r"youtu\.be/([a-zA-Z0-9_-]+)",
        r"youtube\.com/embed/([a-zA-Z0-9_-]+)"
    ]

    for pat in yt_patterns:
        match = re.search(pat, url)
        if match:
            video_id = match.group(1)
            return f"https://www.youtube.com/embed/{video_id}"

    return None


# -------------------------
# Helpers (URL typing)
# -------------------------

YOUTUBE_HOST_SNIPPETS = (
    "youtube.com",
    "youtu.be",
    "youtube-nocookie.com",
)


def is_youtube_url(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    return any(h in u for h in YOUTUBE_HOST_SNIPPETS)


def to_youtube_embed(url: str) -> str:
    """Best-effort conversion to an embeddable URL.
    If conversion fails, return the original URL.
    """
    if not url:
        return url
    if "youtube.com/embed/" in url or "youtube-nocookie.com/embed/" in url:
        return url

    # youtu.be/<id>
    m = re.search(r"youtu\.be/([A-Za-z0-9_-]{6,})", url)
    if m:
        vid = m.group(1)
        return f"https://www.youtube.com/embed/{vid}"

    # youtube.com/watch?v=<id>
    m = re.search(r"[?&]v=([A-Za-z0-9_-]{6,})", url)
    if m:
        vid = m.group(1)
        return f"https://www.youtube.com/embed/{vid}"

    # youtube.com/live/<id>
    m = re.search(r"youtube\.com/live/([A-Za-z0-9_-]{6,})", url)
    if m:
        vid = m.group(1)
        return f"https://www.youtube.com/embed/{vid}"

    return url

def youtube_metadata(url: str) -> dict:
    """Fetch YouTube metadata using lightweight HTML scraping (no APIs, no yt-dlp)."""
    meta = fetch_youtube_metadata(url)
    return {
        "title": meta.title,
        "duration": meta.duration_seconds,
        "thumbnail": meta.thumbnail,
        "webpage_url": meta.url,
    }



def db() -> sqlite3.Connection:
    # timeout avoids immediate 'database is locked' on concurrent access
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    # Ensure FK constraints + cascades work as expected in SQLite.
    conn.execute("PRAGMA foreign_keys = ON")
    # Improve read/write concurrency in SQLite
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_db() -> None:
    conn = db()
    cur = conn.cursor()

    # -------------------------
    # Providers
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS providers (
        provider_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT DEFAULT ''
    )
    """)

    # -------------------------
    # Edges
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS edges (
        edge_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        api_key TEXT NOT NULL UNIQUE,
        hls_base_url TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS edge_providers (
        edge_id TEXT NOT NULL,
        provider_id TEXT NOT NULL,
        PRIMARY KEY(edge_id, provider_id),
        FOREIGN KEY(edge_id) REFERENCES edges(edge_id) ON DELETE CASCADE,
        FOREIGN KEY(provider_id) REFERENCES providers(provider_id) ON DELETE CASCADE
    )
    """)

    # -------------------------
    # Channels (v2 schema)
    # -------------------------
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='channels'"
    )
    has_channels = cur.fetchone() is not None

    if has_channels:
        cols = [
            r["name"]
            for r in cur.execute("PRAGMA table_info(channels)").fetchall()
        ]
        is_v2 = "channel_number" in cols and "id" in cols

        if not is_v2:
            # Rename old table
            cur.execute("ALTER TABLE channels RENAME TO channels_old")

            # Create new schema
            cur.execute("""
            CREATE TABLE channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_number TEXT NOT NULL,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                source_url TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'auto',
                schedule_start TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 100,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY(provider_id) REFERENCES providers(provider_id) ON DELETE CASCADE,
                UNIQUE(provider_id, channel_number)
            )
            """)

            # Migrate data
            cur.execute("""
            INSERT INTO channels (
                channel_number,
                name,
                category,
                provider_id,
                source_url,
                is_active,
                sort_order,
                created_at
            )
            SELECT
                c.channel_id,
                c.name,
                COALESCE(p.name, 'Geral') AS category,
                c.provider_id,
                c.source_url,
                c.is_active,
                c.sort_order,
                c.created_at
            FROM channels_old c
            LEFT JOIN providers p ON p.provider_id = c.provider_id
            """)

            cur.execute("DROP TABLE channels_old")

    else:
        # Fresh install
        cur.execute("""
        CREATE TABLE channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_number TEXT NOT NULL,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            provider_id TEXT NOT NULL,
            source_url TEXT NOT NULL,
            kind TEXT NOT NULL DEFAULT 'auto',
            schedule_start TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 100,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(provider_id) REFERENCES providers(provider_id) ON DELETE CASCADE,
            UNIQUE(provider_id, channel_number)
        )
        """)

    # Ensure new columns for scheduling exist (safe for upgrades)
    cols = [
        r["name"]
        for r in cur.execute("PRAGMA table_info(channels)").fetchall()
    ]
    if "kind" not in cols:
        cur.execute("ALTER TABLE channels ADD COLUMN kind TEXT NOT NULL DEFAULT 'auto'")
    if "schedule_start" not in cols:
        cur.execute("ALTER TABLE channels ADD COLUMN schedule_start TEXT")

    # -------------------------
    # Channel Items (programming)
    # -------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS channel_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id INTEGER NOT NULL,
        position INTEGER NOT NULL,
        type TEXT NOT NULL,
        url TEXT,
        title TEXT,
        thumbnail TEXT,
        duration INTEGER NOT NULL,
        FOREIGN KEY(channel_id) REFERENCES channels(id) ON DELETE CASCADE
    )
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_channel_items_channel_pos
    ON channel_items(channel_id, position)
    """)

    # Safe DB upgrade: add missing columns if the table already existed.
    ci_cols = [r["name"] for r in cur.execute("PRAGMA table_info(channel_items)").fetchall()]
    if "title" not in ci_cols:
        cur.execute("ALTER TABLE channel_items ADD COLUMN title TEXT")
    if "thumbnail" not in ci_cols:
        cur.execute("ALTER TABLE channel_items ADD COLUMN thumbnail TEXT")

    # -------------------------
    # Seed default data
    # -------------------------
    cur.execute("SELECT COUNT(*) AS c FROM providers")
    if cur.fetchone()["c"] == 0:
        cur.execute(
            "INSERT INTO providers(provider_id,name,description) VALUES (?,?,?)",
            ("prov-nex", "Nex (Default)", "Pacote padr\u00e3o do MVP")
        )

    cur.execute("SELECT COUNT(*) AS c FROM edges")
    if cur.fetchone()["c"] == 0:
        key = "edge_" + secrets.token_hex(8)
        cur.execute(
            "INSERT INTO edges(edge_id,name,api_key,hls_base_url,is_active) VALUES (?,?,?,?,1)",
            ("edge-001", "Edge Default", key, "http://EDGE_IP:8080/hls")
        )
        cur.execute(
            "INSERT OR IGNORE INTO edge_providers(edge_id,provider_id) VALUES (?,?)",
            ("edge-001", "prov-nex")
        )

    conn.commit()
    conn.close()



@app.on_event("startup")
def _startup():
    init_db()


def fetch_all(sql: str, args=()) -> List[sqlite3.Row]:
    conn = db()
    rows = conn.execute(sql, args).fetchall()
    conn.close()
    return rows


def fetch_one(sql: str, args=()):
    conn = db()
    row = conn.execute(sql, args).fetchone()
    conn.close()
    return row


def execute(sql: str, args=()) -> None:
    conn = db()
    try:
        conn.execute(sql, args)
        conn.commit()
    except sqlite3.IntegrityError as e:
        # Avoid returning a generic 500 for duplicate keys / FK violations.
        msg = str(e)
        # Surface duplicates as 409 Conflict (more semantically correct).
        if "UNIQUE constraint failed" in msg:
            raise HTTPException(status_code=409, detail=f"db unique violation: {msg}")
        raise HTTPException(status_code=400, detail=f"db integrity error: {msg}")
    except sqlite3.OperationalError as e:
        # Most common in SQLite under load: database is locked.
        msg = str(e)
        if "database is locked" in msg:
            raise HTTPException(status_code=503, detail="database is busy (locked)")
        raise HTTPException(status_code=500, detail=f"db operational error: {msg}")
    finally:
        conn.close()


def must_auth_edge(request: Request) -> sqlite3.Row:
    api_key = request.headers.get("X-API-KEY") or request.query_params.get("api_key")
    if not api_key:
        raise HTTPException(status_code=401, detail="missing X-API-KEY")
    edge = fetch_one("SELECT * FROM edges WHERE api_key=? AND is_active=1", (api_key,))
    if not edge:
        raise HTTPException(status_code=403, detail="invalid api key")
    return edge


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    providers = fetch_all("SELECT * FROM providers ORDER BY provider_id")
    edges = fetch_all("SELECT * FROM edges ORDER BY edge_id")
    channels = fetch_all("""SELECT c.id, c.channel_number, c.name, c.category, c.provider_id, c.source_url, c.kind, c.schedule_start, c.is_active, c.sort_order, c.created_at,
                            p.name AS provider_name
                            FROM channels c JOIN providers p ON p.provider_id=c.provider_id
                            ORDER BY p.provider_id, c.sort_order, c.channel_number""")
    channel_items = fetch_all("""SELECT ci.id, ci.channel_id, ci.position, ci.type, ci.url, ci.title, ci.thumbnail, ci.duration,
                                 c.channel_number, c.name AS channel_name
                                 FROM channel_items ci
                                 JOIN channels c ON c.id=ci.channel_id
                                 ORDER BY c.channel_number, ci.position""")
    return templates.TemplateResponse("home.html", {
        "request": request,
        "providers": providers,
        "edges": edges,
        "channels": channels,
        "channel_items": channel_items
    })


@app.post("/admin/providers/create")
def create_provider(provider_id: str = Form(...), name: str = Form(...), description: str = Form("")):
    execute("INSERT INTO providers(provider_id,name,description) VALUES (?,?,?)",
            (provider_id.strip(), name.strip(), description.strip()))
    return RedirectResponse("/", status_code=303)


@app.post("/admin/providers/delete")
def delete_provider(provider_id: str = Form(...)):
    execute("DELETE FROM providers WHERE provider_id=?", (provider_id,))
    return RedirectResponse("/", status_code=303)


@app.post("/admin/channels/create")
def create_channel(
    channel_number: str = Form(...),
    name: str = Form(...),
    category: str = Form(...),
    provider_id: str = Form(...),
    source_url: str = Form(...),
    kind: str = Form("auto"),
    schedule_start: str = Form(""),
    sort_order: int = Form(100),
    is_active: int = Form(1),
):
    # Some browsers may submit empty strings; convert defensively.
    try:
        sort_order_i = int(sort_order)
        is_active_i = int(is_active)
    except Exception:
        raise HTTPException(status_code=400, detail="sort_order/is_active must be numeric")

    try:
        kind_clean = (kind or "auto").strip().lower()
        if kind_clean not in ("auto", "hls", "youtube", "youtube_linear"):
            kind_clean = "auto"
        schedule_clean = schedule_start.strip() or None
        execute("""INSERT INTO channels(channel_number,name,category,provider_id,source_url,kind,schedule_start,sort_order,is_active)
               VALUES (?,?,?,?,?,?,?,?,?)""",
                (channel_number.strip(), name.strip(), category.strip(), provider_id, source_url.strip(),
                 kind_clean, schedule_clean, sort_order_i, is_active_i))
    except HTTPException as e:
        # Re-map common duplicate to a friendlier message for the admin UI.
        if e.status_code == 400 and "UNIQUE constraint failed" in str(e.detail) and "channels.provider_id" in str(e.detail):
            raise HTTPException(status_code=409, detail="NÃƒÂºmero de canal jÃƒÂ¡ existe neste Provider. Apague o canal antigo ou use outro nÃƒÂºmero.")
        raise
    return RedirectResponse("/", status_code=303)


@app.post("/admin/channels/delete")
def delete_channel(id: int = Form(...)):
    execute("DELETE FROM channels WHERE id=?", (id,))
    return RedirectResponse("/", status_code=303)


@app.post("/admin/channel-items/create")
def create_channel_item(
    channel_id: int = Form(...),
    position: int = Form(...),
    type: str = Form(...),
    url: str = Form(""),
    duration: str = Form(""),
):
    # Note: duration comes from HTML form; empty string must be accepted.
    try:
        position_i = int(position)
    except Exception:
        raise HTTPException(status_code=400, detail="position must be numeric")

    if position_i < 1:
        raise HTTPException(status_code=400, detail="position must be >= 1")

    item_type = (type or "").strip().lower()
    if item_type not in ("video", "ad"):
        raise HTTPException(status_code=400, detail="type must be video or ad")

    url_clean = (url or "").strip()
    if item_type == "video" and not url_clean:
        raise HTTPException(status_code=400, detail="video items require url")

    # Duration is manual for both video and ad.

    title_db = None
    thumb_db = None

    dur_raw = (duration or "").strip()

    if item_type == "ad":
        url_db = url_clean or None
        if dur_raw:
            try:
                duration_i = int(dur_raw)
            except Exception:
                raise HTTPException(status_code=400, detail="duration must be a valid integer")
        else:
            duration_i = 30
    else:
        # video
        url_db = url_clean
        if not is_youtube_url(url_db):
            raise HTTPException(status_code=400, detail="video items currently support only YouTube URLs")
        if not dur_raw:
            raise HTTPException(status_code=400, detail="video items require manual duration")
        try:
            duration_i = int(dur_raw)
        except Exception:
            raise HTTPException(status_code=400, detail="duration must be a valid integer")

        # Manual ingest path: keep optional metadata empty.
        title_db = None
        thumb_db = None

    if duration_i <= 0:
        raise HTTPException(status_code=400, detail="duration must be > 0")

    execute(
        """INSERT INTO channel_items(channel_id,position,type,url,title,thumbnail,duration)
           VALUES (?,?,?,?,?,?,?)""",
        (channel_id, position_i, item_type, url_db, title_db, thumb_db, int(duration_i)),
    )
    return RedirectResponse("/", status_code=303)


@app.post("/admin/channel-items/delete")
def delete_channel_item(id: int = Form(...)):
    execute("DELETE FROM channel_items WHERE id=?", (id,))
    return RedirectResponse("/", status_code=303)


def gen_api_key() -> str:
    return "edge_" + secrets.token_hex(12)


@app.post("/admin/edges/create")
def create_edge(edge_id: str = Form(...), name: str = Form(...), hls_base_url: str = Form(...)):
    api_key = gen_api_key()
    execute("""INSERT INTO edges(edge_id,name,api_key,hls_base_url,is_active)
               VALUES (?,?,?,?,1)""",
            (edge_id.strip(), name.strip(), api_key, hls_base_url.strip()))
    return RedirectResponse("/", status_code=303)


@app.post("/admin/edges/rotate_key")
def rotate_key(edge_id: str = Form(...)):
    api_key = gen_api_key()
    execute("UPDATE edges SET api_key=? WHERE edge_id=?", (api_key, edge_id))
    return RedirectResponse("/", status_code=303)


@app.post("/admin/edges/delete")
def delete_edge(edge_id: str = Form(...)):
    execute("DELETE FROM edges WHERE edge_id=?", (edge_id,))
    return RedirectResponse("/", status_code=303)


@app.get("/admin/edges/{edge_id}/providers", response_class=HTMLResponse)
def edge_providers_page(edge_id: str, request: Request):
    edge = fetch_one("SELECT * FROM edges WHERE edge_id=?", (edge_id,))
    if not edge:
        return RedirectResponse("/", status_code=303)

    providers = fetch_all("SELECT * FROM providers ORDER BY provider_id")
    current = set([r["provider_id"] for r in fetch_all("SELECT provider_id FROM edge_providers WHERE edge_id=?", (edge_id,))])

    return templates.TemplateResponse("edge_providers.html", {
        "request": request,
        "edge": edge,
        "providers": providers,
        "current": current
    })


@app.post("/admin/edges/{edge_id}/providers/save")
def edge_providers_save(edge_id: str, provider_ids: Optional[List[str]] = Form(None)):
    execute("DELETE FROM edge_providers WHERE edge_id=?", (edge_id,))
    if provider_ids:
        conn = db()
        for pid in provider_ids:
            conn.execute("INSERT OR IGNORE INTO edge_providers(edge_id,provider_id) VALUES (?,?)", (edge_id, pid))
        conn.commit()
        conn.close()
    return RedirectResponse(f"/admin/edges/{edge_id}/providers", status_code=303)


@app.get("/api/edge/channels")
def api_edge_channels(request: Request):
    edge = must_auth_edge(request)
    p_rows = fetch_all("SELECT provider_id FROM edge_providers WHERE edge_id=?", (edge["edge_id"],))
    provider_ids = [r["provider_id"] for r in p_rows]
    if not provider_ids:
        return {"edge_id": edge["edge_id"], "providers": []}

    placeholders = ",".join(["?"] * len(provider_ids))
    providers = fetch_all(f"SELECT * FROM providers WHERE provider_id IN ({placeholders}) ORDER BY provider_id", tuple(provider_ids))

    result = []
    for p in providers:
        ch = fetch_all("""SELECT id, channel_number, name, category, provider_id, source_url, kind, schedule_start, is_active, sort_order
                          FROM channels
                          WHERE provider_id=? AND is_active=1
                          ORDER BY sort_order, channel_number""", (p["provider_id"],))

        # Enrich channels with computed playback information.
        # - For regular HLS sources, the Edge will cache/transcode and serve from its own hls_base_url.
        # - For YouTube sources, we DO NOT cache/proxy; we pass through the original URL so the App can
        #   use an embed/native YouTube player.
        hls_base = edge["hls_base_url"].rstrip("/")
        enriched = []
        for r in ch:
            item = dict(r)
            # Backward-compat: keep old field name used by older clients
            item["channel_id"] = item.get("channel_number")
            src = item.get("source_url") or ""
            kind_raw = (item.get("kind") or "auto").strip().lower()
            if kind_raw not in ("auto", "hls", "youtube", "youtube_linear"):
                kind_raw = "auto"

            if kind_raw == "auto":
                kind = "youtube" if is_youtube_url(src) else "hls"
            else:
                kind = kind_raw

            item["kind"] = kind

            if kind == "youtube":
                item["playback_url"] = src
                item["embed_url"] = youtube_to_embed(src)
                item.pop("schedule_start", None)
            elif kind == "hls":
                item["playback_url"] = f"{hls_base}/{item['provider_id']}/{item['channel_number']}/index.m3u8"
                item["embed_url"] = None
                item.pop("schedule_start", None)
            else:
                # youtube_linear: schedule + items list only (no direct playback URL)
                item["playback_url"] = None
                item["embed_url"] = None
                item["schedule_start"] = item.get("schedule_start")
                items = fetch_all(
                    """SELECT position, type, url, title, thumbnail, duration
                       FROM channel_items
                       WHERE channel_id=?
                       ORDER BY position""",
                    (item["id"],)
                )
                item["items"] = [
                    {
                        "type": r["type"],
                        **({"url": r["url"]} if r["type"] == "video" and r["url"] else {}),
                        **({"title": r["title"]} if r["type"] == "video" and r["title"] else {}),
                        **({"thumbnail": r["thumbnail"]} if r["type"] == "video" and r["thumbnail"] else {}),
                        "duration": r["duration"]
                    }
                    for r in items
                ]
            enriched.append(item)

        result.append({
            "provider_id": p["provider_id"],
            "provider_name": p["name"],
            "channels": enriched,
        })

    return {"edge_id": edge["edge_id"], "hls_base_url": edge["hls_base_url"], "providers": result}


@app.get("/iptv/edge.m3u")
def playlist_for_edge(request: Request):
    edge = must_auth_edge(request)
    payload = api_edge_channels(request)
    hls_base = payload["hls_base_url"].rstrip("/")

    lines = ["#EXTM3U"]
    for p in payload["providers"]:
        grp = p["provider_name"]
        for ch in p["channels"]:
            if ch.get("kind") != "hls":
                continue
            name = ch["name"]
            cnum = ch.get("channel_number") or ""
            display = f"{cnum} - {name}".strip(" -")
            url = ch.get("playback_url") or f"{hls_base}/{ch['provider_id']}/{cnum}/index.m3u8"
            lines.append(f'#EXTINF:-1 group-title="{grp}",{display}')
            lines.append(url)

    return PlainTextResponse("\n".join(lines), media_type="audio/x-mpegurl")


@app.get("/health")
def health():
    return {"ok": True}
