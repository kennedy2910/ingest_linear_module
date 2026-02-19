
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# INGEST PRO — versão robusta
# - aceita CLI args
# - detecta youtube automaticamente
# - converte watch -> embed internamente
# - extrai duração via scraping leve
# - calcula próxima posição automaticamente
# - retry inteligente

import os
import sys
import time
import re
import requests
import json
import argparse
from urllib.request import Request, urlopen
from dotenv import load_dotenv

load_dotenv()

CENTRAL_URL = os.getenv("CENTRAL_URL")
API_KEY = os.getenv("API_KEY")

if not CENTRAL_URL:
    raise Exception("CENTRAL_URL missing in .env")

HEADERS = {
    "X-API-KEY": API_KEY or ""
}

_PLAYLIST_CACHE = {}

YOUTUBE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

def _normalize_channel_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip()).casefold()

def _norm_id(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    # Treat purely-numeric identifiers as ints so "003" matches "3".
    if re.fullmatch(r"\d+", s):
        try:
            return str(int(s))
        except Exception:
            return s
    return s

def extract_video_id(url: str) -> str:
    if "youtu.be/" in url:
        return url.split("youtu.be/")[1].split("?")[0]
    if "watch?v=" in url:
        return url.split("watch?v=")[1].split("&")[0]
    if "/embed/" in url:
        return url.split("/embed/")[1].split("?")[0]
    raise Exception("invalid youtube url")

def normalize_watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"

def _parse_iso8601_duration_to_seconds(value: str):
    # YouTube may expose duration in ISO-8601 (e.g. PT1H02M03S).
    m = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", (value or "").strip())
    if not m:
        return None
    h = int(m.group(1) or 0)
    mins = int(m.group(2) or 0)
    secs = int(m.group(3) or 0)
    total = (h * 3600) + (mins * 60) + secs
    return total if total > 0 else None

def _extract_json_object_after(html: str, anchor: str):
    i = html.find(anchor)
    if i < 0:
        return None
    i += len(anchor)
    while i < len(html) and html[i].isspace():
        i += 1
    if i >= len(html) or html[i] != "{":
        return None

    depth = 0
    in_str = False
    esc = False
    j = i
    while j < len(html):
        ch = html[j]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    j += 1
                    break
        j += 1

    if depth != 0:
        return None
    try:
        return json.loads(html[i:j])
    except Exception:
        return None

def _safe_int(v):
    try:
        return int(str(v).strip())
    except Exception:
        return None

def extract_duration(html: str, player_response: dict | None = None):
    # Common pattern in watch HTML payload.
    m = re.search(r'"lengthSeconds":"(\d+)"', html)
    if m:
        return int(m.group(1))

    # Fallback: numeric JSON form.
    m = re.search(r'"lengthSeconds":\s*(\d+)', html)
    if m:
        return int(m.group(1))

    # Fallback: millisecond duration field.
    m = re.search(r'"approxDurationMs":"(\d+)"', html)
    if m:
        ms = int(m.group(1))
        if ms > 0:
            return ms // 1000

    pr = player_response or {}
    video_details = (pr.get("videoDetails") or {}) if isinstance(pr, dict) else {}
    dur = _safe_int(video_details.get("lengthSeconds"))
    if dur and dur > 0:
        return dur

    # Additional fallback seen in microformat.
    iso_dur = (
        (pr.get("microformat") or {})
        .get("playerMicroformatRenderer", {})
        .get("lengthSeconds")
    ) if isinstance(pr, dict) else None
    dur = _safe_int(iso_dur)
    if dur and dur > 0:
        return dur

    # Last fallback from schema metadata.
    m = re.search(r'"duration":"(PT[^"]+)"', html)
    if m:
        dur = _parse_iso8601_duration_to_seconds(m.group(1))
        if dur:
            return dur

    return None

def fetch_youtube_meta(video_id):

    url = normalize_watch_url(video_id)

    req = Request(url, headers=YOUTUBE_HEADERS)

    html = urlopen(req, timeout=20).read().decode("utf-8", errors="ignore")

    player_response = _extract_json_object_after(html, "ytInitialPlayerResponse = ")
    duration = extract_duration(html, player_response=player_response)

    if not duration:
        playability = (player_response or {}).get("playabilityStatus", {})
        status = (playability.get("status") or "").strip()
        reason = (playability.get("reason") or "").strip()
        if status and status.upper() != "OK":
            detail = f"{status}: {reason}" if reason else status
            raise Exception(f"duration not found (video unavailable: {detail})")
        raise Exception("duration not found (unsupported YouTube page format)")

    title = (
        ((player_response or {}).get("videoDetails") or {}).get("title")
        or None
    )
    if not title:
        title_match = re.search(r'<meta\s+name="title"\s+content="([^"]+)"', html)
        if not title_match:
            title_match = re.search(r'<meta\s+property="og:title"\s+content="([^"]+)"', html)
        title = title_match.group(1) if title_match else "Unknown"

    thumbnail = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

    return {
        "duration": duration,
        "title": title,
        "thumbnail": thumbnail
    }

def fetch_edge_channels():
    url = f"{CENTRAL_URL}/api/edge/channels"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_default_playlist_id(channel_id):
    key = _norm_id(channel_id)
    if not key:
        return None
    if key in _PLAYLIST_CACHE:
        return _PLAYLIST_CACHE[key]

    url = f"{CENTRAL_URL}/admin/channels/{key}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if not r.ok:
            _PLAYLIST_CACHE[key] = None
            return None
        html = r.text or ""
        m_select = re.search(
            r'<select[^>]*name=["\']playlist_id["\'][^>]*>(.*?)</select>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not m_select:
            _PLAYLIST_CACHE[key] = None
            return None

        select_html = m_select.group(1)
        m_selected = re.search(
            r'<option[^>]*value=["\']?(\d+)["\']?[^>]*selected',
            select_html,
            flags=re.IGNORECASE,
        )
        if m_selected:
            value = _norm_id(m_selected.group(1))
            _PLAYLIST_CACHE[key] = value
            return value

        m_first = re.search(
            r'<option[^>]*value=["\']?(\d+)["\']?',
            select_html,
            flags=re.IGNORECASE,
        )
        if m_first:
            value = _norm_id(m_first.group(1))
            _PLAYLIST_CACHE[key] = value
            return value

    except Exception:
        pass

    _PLAYLIST_CACHE[key] = None
    return None

def flatten_edge_channels(edge_payload: dict):
    out = []
    for p in (edge_payload or {}).get("providers", []) or []:
        provider_id = p.get("provider_id")
        provider_name = p.get("provider_name")
        for ch in p.get("channels", []) or []:
            item = dict(ch)
            item["_provider_id"] = provider_id
            item["_provider_name"] = provider_name
            out.append(item)
    return out

def resolve_channel_by_name(channel_name: str, channels_flat):
    needle = _normalize_channel_name(channel_name)
    if not needle:
        raise Exception("empty channel name")

    matches = []
    for ch in channels_flat or []:
        if _normalize_channel_name(ch.get("name") or "") == needle:
            matches.append(ch)

    if not matches:
        sample = sorted({(c.get("name") or "").strip() for c in (channels_flat or []) if (c.get("name") or "").strip()})[:20]
        raise Exception(f"channel name not found: '{channel_name}'. Example available: {sample}")

    if len(matches) > 1:
        summary = [
            {
                "id": m.get("id"),
                "channel_number": m.get("channel_number"),
                "provider": m.get("_provider_name"),
            }
            for m in matches
        ]
        raise Exception(f"ambiguous channel name: '{channel_name}'. Matches: {summary}")

    return matches[0]

def get_channel_items(channel_id):
    # New Central exposes channel items via /api/edge/channels.
    target = _norm_id(channel_id)
    url = f"{CENTRAL_URL}/api/edge/channels"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if not r.ok:
            return []
        data = r.json()
        providers = data.get("providers", [])
        for provider in providers:
            for channel in provider.get("channels", []):
                # Be tolerant: some clients use internal channel DB id, others use channel_number.
                variants = {
                    _norm_id(channel.get("id")),
                    _norm_id(channel.get("channel_number")),
                    _norm_id(channel.get("channel_id")),
                }
                if target in variants:
                    return channel.get("items", []) or []
        return []
    except Exception:
        return []

def get_next_position(channel_id, items=None):
    if items is None:
        items = get_channel_items(channel_id)
    if not items:
        return 1
    positions = []
    for item in items:
        p = item.get("position")
        if p is None:
            continue
        try:
            positions.append(int(p))
        except Exception:
            continue
    if not positions:
        return len(items) + 1
    return max(positions) + 1

def channel_has_video(video_id, items):
    target_url = normalize_watch_url(video_id)
    for item in items:
        item_url = (item.get("url") or "").strip()
        if not item_url:
            continue
        try:
            if extract_video_id(item_url) == video_id:
                return True
        except Exception:
            if item_url == target_url:
                return True
    return False

def _read_item_duration(v):
    try:
        return int(str(v).strip())
    except Exception:
        return None

def get_stored_duration_for_inserted_video(channel_id, position, video_id):
    items = get_channel_items(channel_id)
    if not items:
        return None

    target_pos = _read_item_duration(position)
    # Prefer exact (position + video id) match.
    for item in items:
        item_pos = _read_item_duration(item.get("position"))
        if target_pos is not None and item_pos != target_pos:
            continue
        try:
            if extract_video_id(item.get("url") or "") == video_id:
                return _read_item_duration(item.get("duration"))
        except Exception:
            continue

    # Fallback: latest match by video id only.
    for item in reversed(items):
        try:
            if extract_video_id(item.get("url") or "") == video_id:
                return _read_item_duration(item.get("duration"))
        except Exception:
            continue
    return None

def retry(fn, attempts=3):
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            if i == attempts - 1:
                raise
            time.sleep(2)

def insert(channel_id, url, *, items_cache=None, next_position=None, dry_run=False, playlist_id=None):

    if "youtube" not in url and "youtu.be" not in url:
        raise Exception("Only youtube supported in PRO ingest for now")

    video_id = extract_video_id(url)

    items = items_cache if items_cache is not None else get_channel_items(channel_id)
    if channel_has_video(video_id, items):
        print(f"Skip duplicate: channel_id={channel_id} video_id={video_id}")
        return False

    meta = retry(lambda: fetch_youtube_meta(video_id))

    position = int(next_position) if next_position is not None else get_next_position(channel_id, items=items)

    payload = {
        "channel_id": str(channel_id),
        "return_channel_id": str(channel_id),
        "position": str(position),
        "type": "video",
        "item_type": "video",
        "url": normalize_watch_url(video_id),
        "duration": str(meta["duration"]),
    }
    if playlist_id is not None:
        payload["playlist_id"] = str(playlist_id)


    endpoint = f"{CENTRAL_URL}/admin/channel-items/create"

    print(f"POST {endpoint}")
    print("Payload:", payload)
    if dry_run:
        return True

    r = requests.post(
        endpoint,
        data=payload,
        headers=HEADERS,
        timeout=30
    )
    response_text = (r.text or "").encode("ascii", "ignore").decode()
    print("Response:", r.status_code, response_text[:500])
    r.raise_for_status()

    stored_duration = get_stored_duration_for_inserted_video(channel_id, position, video_id)
    expected_duration = _read_item_duration(meta.get("duration"))
    if (
        stored_duration is not None
        and expected_duration is not None
        and stored_duration != expected_duration
    ):
        print(
            "WARN: Central stored duration differs from YouTube metadata. "
            f"video_id={video_id} expected={expected_duration} stored={stored_duration}"
        )
    return True

def _strip_trailing_commas_json(s: str) -> str:
    # Allow "JSON with trailing commas" (common in hand-edited files).
    # Removes commas immediately before a closing ']' or '}', ignoring strings.
    out = []
    in_str = False
    esc = False
    i = 0
    while i < len(s):
        ch = s[i]
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            i += 1
            continue

        if ch == '"':
            in_str = True
            out.append(ch)
            i += 1
            continue

        if ch == ",":
            j = i + 1
            while j < len(s) and s[j] in " \t\r\n":
                j += 1
            if j < len(s) and s[j] in "]}":
                i += 1
                continue

        out.append(ch)
        i += 1
    return "".join(out)

def load_channel_list(path: str) -> dict:
    # utf-8-sig tolerates BOM (common on Windows).
    raw = open(path, "r", encoding="utf-8-sig").read()
    cleaned = _strip_trailing_commas_json(raw)
    return json.loads(cleaned)

def ingest_from_json(path: str, *, delay_seconds: float = 3.0, dry_run: bool = False, continue_on_error: bool = False, channel_id_mode: str = "id"):
    data = load_channel_list(path)
    channels_spec = (data or {}).get("channels")
    if not isinstance(channels_spec, list):
        raise Exception("JSON must contain {\"channels\": [...]} ")

    edge_payload = fetch_edge_channels()
    channels_flat = flatten_edge_channels(edge_payload)

    for idx, ch_spec in enumerate(channels_spec, start=1):
        name = (ch_spec or {}).get("name")
        urls = (ch_spec or {}).get("urls") or []
        if not name:
            raise Exception(f"channels[{idx}] missing 'name'")
        if not isinstance(urls, list):
            raise Exception(f"channels[{idx}].urls must be a list")

        ch = resolve_channel_by_name(name, channels_flat)
        internal_id = ch.get("id")
        channel_number = ch.get("channel_number") or ch.get("channel_id")
        if internal_id is None and not channel_number:
            raise Exception(f"resolved channel '{name}' but missing id in payload: {ch}")

        if (channel_id_mode or "").strip().lower() in ("number", "channel_number"):
            channel_id = _norm_id(channel_number)
        else:
            channel_id = _norm_id(internal_id)
        if not channel_id:
            raise Exception(f"cannot compute channel_id for '{name}' (mode={channel_id_mode}) from payload: {ch}")

        print(
            f"\n== Channel: '{name}' (use_channel_id={channel_id}, internal_id={internal_id}, number={channel_number}, provider={ch.get('_provider_name')}) =="
        )

        playlist_id = fetch_default_playlist_id(channel_id)
        if playlist_id is not None:
            print(f"Detected playlist_id={playlist_id} for channel_id={channel_id}")
        else:
            print(
                f"WARN: playlist_id not detected for channel_id={channel_id}; server may fallback duration to default"
            )

        # Fetch items once per channel (avoid hammering /api/edge/channels).
        items = ch.get("items")
        if not isinstance(items, list):
            items = get_channel_items(channel_id)
        pos = get_next_position(channel_id, items=items)

        for u_idx, url in enumerate(urls, start=1):
            did_insert = False
            try:
                print(f"\n[{u_idx}/{len(urls)}] {url}")
                did_insert = insert(
                    channel_id,
                    url,
                    items_cache=items,
                    next_position=pos,
                    dry_run=dry_run,
                    playlist_id=playlist_id,
                )
                if did_insert:
                    # Keep local state consistent (best-effort; Central is source of truth).
                    try:
                        vid = extract_video_id(url)
                        items.append({"url": normalize_watch_url(vid), "position": pos})
                    except Exception:
                        pass
                    pos += 1
            except Exception as e:
                print(f"ERROR: channel='{name}' url='{url}': {e}")
                if not continue_on_error:
                    raise

            if did_insert and delay_seconds and delay_seconds > 0:
                print(f"Sleep {delay_seconds}s...")
                time.sleep(delay_seconds)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog="ingest_linear.py")
    parser.add_argument("channel_id", nargs="?", help="ID do canal (modo single)")
    parser.add_argument("youtube_url", nargs="?", help="URL do YouTube (modo single)")
    parser.add_argument("--json", dest="json_path", help="Arquivo JSON: {channels:[{name,urls:[]}]} (modo batch)")
    parser.add_argument("--delay-seconds", type=float, default=3.0, help="Delay entre insercoes (batch). Default: 3")
    parser.add_argument("--dry-run", action="store_true", help="Nao faz POST na Central, apenas mostra payloads")
    parser.add_argument("--continue-on-error", action="store_true", help="No batch, continua mesmo se uma URL falhar")
    parser.add_argument("--channel-id-mode", choices=["id", "number"], default="id", help="No batch: usa 'id' (id interno) ou 'number' (channel_number) como channel_id no POST. Default: id")
    args = parser.parse_args()

    if args.json_path:
        ingest_from_json(
            args.json_path,
            delay_seconds=args.delay_seconds,
            dry_run=args.dry_run,
            continue_on_error=args.continue_on_error,
            channel_id_mode=args.channel_id_mode,
        )
        sys.exit(0)

    if not args.channel_id or not args.youtube_url:
        print("Usage:")
        print("  python ingest_linear.py <channel_id> <youtube_url>")
        print("  python ingest_linear.py --json lista.json [--delay-seconds 3] [--dry-run] [--continue-on-error]")
        sys.exit(1)

    single_playlist_id = fetch_default_playlist_id(args.channel_id)
    if single_playlist_id is not None:
        print(f"Detected playlist_id={single_playlist_id} for channel_id={args.channel_id}")
    else:
        print(
            f"WARN: playlist_id not detected for channel_id={args.channel_id}; server may fallback duration to default"
        )

    insert(
        args.channel_id,
        args.youtube_url,
        dry_run=args.dry_run,
        playlist_id=single_playlist_id,
    )
