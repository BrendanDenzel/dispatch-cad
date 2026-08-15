import os, time, threading, tempfile, json, subprocess, difflib
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urljoin
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from groq import Groq
from supabase import create_client
from collections import deque

clients = []

GROQ_API_KEY   = os.environ.get("GROQ_API_KEY")
SUPABASE_URL   = os.environ.get("SUPABASE_URL")
SUPABASE_KEY   = os.environ.get("SUPABASE_KEY")
STREAM_URL     = os.environ.get("STREAM_URL")   # .m3u8 playlist URL
CHUNK_SECONDS  = 120    # target seconds of actual audio per captured chunk
SEG_DURATION   = 4.032 # seconds per HLS segment, from the playlist's #EXTINF value
MAX_INCIDENTS  = 5000
AUDIO_BUCKET   = "audio-clips"

EASTERN     = ZoneInfo("America/New_York")
groq_client = Groq(api_key=GROQ_API_KEY)

def get_db():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

app = Flask(__name__)
CORS(app)

# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────

@app.route("/ping")
def ping():
    return "pong", 200

@app.route("/incidents")
def get_incidents():
    offset = request.args.get("offset", 0, type=int)
    res = (get_db().table("incidents")
           .select("*")
           .order("created_at", desc=True)
           .range(offset, offset + 49)
           .execute())
    return jsonify(res.data)

@app.route("/stream")
def stream():
    def event_stream():
        clients.append(queue := [])
        try:
            while True:
                if queue:
                    data = queue.pop(0)
                    yield f"data: {json.dumps(data)}\n\n"
                time.sleep(0.1)
        except GeneratorExit:
            clients.remove(queue)
    return Response(event_stream(), mimetype="text/event-stream")

@app.after_request
def add_headers(response):
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response

@app.route("/stats")
def get_stats():
    try:
        db = get_db()
        now_et = datetime.now(EASTERN)
        today_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start.astimezone(ZoneInfo("UTC")).isoformat()

        all_time = db.table("incidents").select("id", count="exact").execute().count or 0

        total = (db.table("incidents")
                 .select("id", count="exact")
                 .gte("created_at", today_start_utc)
                 .execute()).count or 0

        high = (db.table("incidents")
                .select("id", count="exact")
                .gte("created_at", today_start_utc)
                .eq("priority", "High")
                .execute()).count or 0

        rows = (db.table("incidents")
                .select("units, time_str, created_at, incident_type")
                .gte("created_at", today_start_utc)
                .order("created_at", desc=True)
                .execute()).data or []

        all_units = {u for r in rows for u in (r.get("units") or [])}
        last_call = rows[0]["time_str"] if rows else "—"

        rate = "0"
        if len(rows) > 1:
            newest = datetime.fromisoformat(rows[0]["created_at"])
            oldest = datetime.fromisoformat(rows[-1]["created_at"])
            hrs = max((newest - oldest).total_seconds() / 3600, 0.1)
            rate = f"{len(rows) / hrs:.1f}"

        types = {}
        for r in rows:
            t = r.get("incident_type") or "Unknown"
            types[t] = types.get(t, 0) + 1

        return jsonify({
            "total": total,
            "all_time": all_time,
            "high": high,
            "units": len(all_units),
            "last_call": last_call,
            "rate": rate,
            "breakdown": types
        })
    except Exception as e:
        print(f"Stats error: {e}", flush=True)
        return jsonify({"total": 0, "all_time": 0, "high": 0, "units": 0, "last_call": "—", "rate": "0", "breakdown": {}})


# ─────────────────────────────────────────────
# Capture — curl-subprocess (bypasses Broadcastify's TLS-fingerprint block
# on Python's requests/urllib3) + polling to build up a full-length chunk
# ─────────────────────────────────────────────

def curl_fetch(url: str) -> bytes:
    result = subprocess.run(
        ["curl", "-sS", "-f", "--max-time", "15", url],
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed ({result.returncode}) for {url}: "
                            f"{result.stderr.decode(errors='ignore')[:200]}")
    return result.stdout


def fetch_playlist() -> list[str]:
    text = curl_fetch(STREAM_URL).decode()
    return [
        urljoin(STREAM_URL, line.strip())
        for line in text.splitlines()
        if line.strip() and not line.startswith("#")
    ]


def download_segments(min_seconds: int = CHUNK_SECONDS) -> bytes:
    """
    Broadcastify's live playlist only ever lists a handful of segments at once
    (a rolling window, e.g. 6 segments ≈ 24s) — it does NOT grow to expose
    min_seconds worth of history just because we ask for more. To actually
    accumulate min_seconds of audio, we poll the playlist repeatedly over
    time, track which segment URLs we've already grabbed, and keep collecting
    new ones as they roll in until we hit the target duration.
    """
    seen = set()
    ts_bytes = b""
    collected_seconds = 0.0
    deadline = time.time() + min_seconds + 30  # safety timeout

    while collected_seconds < min_seconds and time.time() < deadline:
        try:
            segments = fetch_playlist()
        except Exception as e:
            print(f"[capture] playlist fetch failed: {e}", flush=True)
            time.sleep(SEG_DURATION)
            continue

        new_segments = [s for s in segments if s not in seen]
        for url in new_segments:
            try:
                ts_bytes += curl_fetch(url)
                seen.add(url)
                collected_seconds += SEG_DURATION
            except Exception as e:
                print(f"[capture] segment fetch failed: {e}", flush=True)

        if collected_seconds < min_seconds:
            time.sleep(SEG_DURATION)  # wait for the next segment(s) to roll in

    # NEW: log how many segments we actually grabbed and whether we hit the
    # target or bailed out early via the deadline timeout.
    hit_deadline = time.time() >= deadline
    print(f"[capture] download_segments done: {len(seen)} segments, "
          f"~{collected_seconds:.1f}s estimated (target {min_seconds}s), "
          f"{len(ts_bytes)} raw bytes, "
          f"{'HIT DEADLINE TIMEOUT' if hit_deadline else 'reached target normally'}",
          flush=True)

    return ts_bytes


def get_duration(audio_bytes: bytes) -> float:
    """NEW: measure actual duration (in seconds) of audio bytes via ffprobe."""
    if not audio_bytes:
        return 0.0
    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
        f.write(audio_bytes)
        path = f.name
    try:
        result = subprocess.run(
    ["ffprobe", "-v", "error", "-show_entries", "format=duration",
     "-of", "default=noprint_wrappers=1:nokey=1", path],
    capture_output=True, text=True, timeout=15
)
        return float(result.stdout.strip())
    except Exception as e:
        print(f"[capture] ffprobe duration check failed: {e}", flush=True)
        return -1.0
    finally:
        os.unlink(path)


def convert_to_mp3(ts_bytes: bytes) -> bytes | None:
    if not ts_bytes:
        return None

    in_path = tempfile.NamedTemporaryFile(suffix=".ts", delete=False).name
    out_path = in_path.replace(".ts", ".mp3")
    with open(in_path, "wb") as f:
        f.write(ts_bytes)

    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", in_path,
        "-vn", "-ac", "1", "-ar", "16000",
        "-acodec", "libmp3lame", "-b:a", "64k",
        out_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=60)
    finally:
        os.unlink(in_path)

    if result.returncode != 0 or not os.path.exists(out_path) or os.path.getsize(out_path) < 1000:
        err = result.stderr.decode(errors="ignore")[-300:] if result.stderr else ""
        print(f"[capture] ffmpeg convert failed: {err}", flush=True)
        if os.path.exists(out_path):
            os.unlink(out_path)
        return None

    with open(out_path, "rb") as f:
        data = f.read()
    os.unlink(out_path)
    return data


def capture_chunk() -> bytes | None:
    try:
        ts_bytes = download_segments()
        mp3 = convert_to_mp3(ts_bytes)
        if mp3:
            # NEW: log the real measured duration of the raw (pre-trim) mp3
            dur = get_duration(mp3)
            print(f"[capture] raw mp3 duration (pre-trim): {dur:.1f}s, "
                  f"{len(mp3)} bytes", flush=True)
        return mp3
    except Exception as e:
        print(f"[capture] error: {e}", flush=True)
        return None


def scanner_loop():
    if not STREAM_URL:
        print("[scanner] STREAM_URL not set — nothing to capture.", flush=True)
        return

    print("[scanner] Starting capture loop against:", STREAM_URL, flush=True)
    while True:
        try:
            audio = capture_chunk()
            if audio:
                print(f"[scanner] captured {len(audio)} bytes — processing", flush=True)
                process_audio_chunk(audio)
            else:
                print("[scanner] capture returned nothing, retrying in 5s", flush=True)
                time.sleep(5)
        except Exception as e:
            import traceback
            print(f"[scanner] error: {e}", flush=True)
            traceback.print_exc()
            time.sleep(10)


# ─────────────────────────────────────────────
# Processing pipeline
# ─────────────────────────────────────────────

def trim_silence(audio_bytes: bytes) -> bytes:
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as fin:
            fin.write(audio_bytes)
            in_path = fin.name
        out_path = in_path.replace(".mp3", "_trimmed.mp3")

        result = subprocess.run([
            "ffmpeg", "-y", "-i", in_path,
            "-af",
            "silenceremove=start_periods=1:start_silence=0.5:start_threshold=-40dB"
            ":stop_periods=-1:stop_silence=0.5:stop_threshold=-40dB",
            "-b:a", "64k",
            out_path
        ], capture_output=True, timeout=30)

        os.unlink(in_path)

        if result.returncode != 0 or not os.path.exists(out_path):
            print("ffmpeg failed, using original audio", flush=True)
            return audio_bytes

        with open(out_path, "rb") as f:
            trimmed = f.read()
        os.unlink(out_path)

        if len(trimmed) < 1000:
            print("Trim: pure silence detected, skipping", flush=True)
            return b""

        print(f"Trim: {len(audio_bytes)//1024}KB → {len(trimmed)//1024}KB", flush=True)
        return trimmed

    except Exception as e:
        print(f"Trim error: {e}", flush=True)
        return audio_bytes


def upload_audio(audio_bytes: bytes) -> str | None:
    try:
        db   = get_db()
        ts   = datetime.now(EASTERN).strftime("%Y%m%d_%H%M%S")
        path = f"clips/clip_{ts}.mp3"
        db.storage.from_(AUDIO_BUCKET).upload(
            path, audio_bytes, {"content-type": "audio/mpeg", "upsert": "false"}
        )
        return f"{SUPABASE_URL}/storage/v1/object/public/{AUDIO_BUCKET}/{path}"
    except Exception as e:
        print(f"Audio upload error: {e}", flush=True)
        return None


def delete_audio(audio_url: str):
    try:
        db     = get_db()
        marker = f"/public/{AUDIO_BUCKET}/"
        if marker in audio_url:
            db.storage.from_(AUDIO_BUCKET).remove([audio_url.split(marker, 1)[1]])
    except Exception as e:
        print(f"Audio delete error: {e}", flush=True)


def transcribe(audio_bytes: bytes) -> str:
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
            f.write(audio_bytes)
            tmp_path = f.name
        with open(tmp_path, "rb") as f:
            result = groq_client.audio.transcriptions.create(
                file=("audio.mp3", f, "audio/mpeg"),
                model="whisper-large-v3-turbo",
                response_format="text"
            )
        os.unlink(tmp_path)
        return result.strip() if result else ""
    except Exception as e:
        print(f"Transcription error: {e}", flush=True)
        return ""


PARSE_PROMPT = """You are a police dispatch parser for Erie County / Amherst NY.
Extract structured data from this radio transcript.

Transcript: {transcript}

Respond ONLY with a valid JSON object with these exact fields:
- incident_type: string (e.g. "MVA", "Domestic", "Theft", "Medical", "Noise Complaint", "Burglary", "Suspicious", "Unknown")
- location: string (address or intersection mentioned, or "Unknown")
- units: array of strings (unit numbers or call signs mentioned, empty array if none)
- priority: string, one of exactly: "High", "Medium", "Low", "Unknown"
- notes: string (any other relevant detail, max 1 sentence)

If the transcript is static, silence, or contains no real dispatch content return exactly: null

Return raw JSON only. No markdown, no explanation, no code blocks."""


def parse_transcript(transcript: str):
    try:
        resp = groq_client.chat.completions.create(
    model="openai/gpt-oss-120b",
    messages=[{"role": "user", "content": PARSE_PROMPT.format(transcript=transcript)}],
    max_tokens=300,
    temperature=0.1,
    reasoning_effort="low"  # keep it quick since this is a simple extraction task
)
        text = resp.choices[0].message.content.strip()
        if text.lower() == "null":
            return None
        return json.loads(text.replace("```json", "").replace("```", "").strip())
    except Exception as e:
        print(f"Parse error: {e}", flush=True)
        return None


def purge_old_incidents():
    try:
        db    = get_db()
        total = db.table("incidents").select("id", count="exact").execute().count or 0
        if total <= MAX_INCIDENTS:
            return
        oldest = (db.table("incidents")
                  .select("id, audio_url")
                  .order("created_at", desc=False)
                  .limit(total - MAX_INCIDENTS)
                  .execute()).data or []
        for row in oldest:
            if row.get("audio_url"):
                delete_audio(row["audio_url"])
            db.table("incidents").delete().eq("id", row["id"]).execute()
            print(f"Purged incident id={row['id']}", flush=True)
    except Exception as e:
        print(f"Purge error: {e}", flush=True)


def save_incident(parsed: dict, transcript: str, audio_url: str | None):
    try:
        db  = get_db()
        row = {
            "incident_type": parsed.get("incident_type", "Unknown"),
            "location":      parsed.get("location", "Unknown"),
            "units":         parsed.get("units", []),
            "priority":      parsed.get("priority", "Unknown"),
            "notes":         parsed.get("notes", ""),
            "transcript":    transcript,
            "time_str":      datetime.now(EASTERN).strftime("%I:%M %p"),
            "audio_url":     audio_url,
        }
        res   = db.table("incidents").insert(row).execute()
        saved = res.data[0] if res.data else row
        for q in clients:
            q.append(saved)
        print(f"Saved + broadcasted: {row['incident_type']}", flush=True)
        purge_old_incidents()
    except Exception as e:
        print(f"Save error: {e}", flush=True)


def process_audio_chunk(audio: bytes):
    try:
        print("Trimming silence...", flush=True)
        trimmed = trim_silence(audio)
        if not trimmed:
            print("Pure silence, skipping.", flush=True)
            return

        # NEW: log measured durations before vs after trim so we can see
        # exactly how much silenceremove is cutting.
        raw_dur = get_duration(audio)
        trimmed_dur = get_duration(trimmed)
        print(f"[capture] trim result: {raw_dur:.1f}s raw -> {trimmed_dur:.1f}s trimmed", flush=True)

        print("Uploading audio...", flush=True)
        audio_url = upload_audio(trimmed)

        print("Transcribing...", flush=True)
        transcript = transcribe(trimmed)
        print(f"Transcript ({len(transcript)} chars): {transcript[:100]!r}", flush=True)

        if len(transcript) < 15:
            print("Too short, skipping.", flush=True)
            if audio_url:
                delete_audio(audio_url)
            return

        print("Parsing...", flush=True)
        parsed = parse_transcript(transcript)

        if parsed:
            save_incident(parsed, transcript, audio_url)
        else:
            print("No incident detected, cleaning up.", flush=True)
            if audio_url:
                delete_audio(audio_url)

    except Exception as e:
        import traceback
        print(f"Processing error: {e}", flush=True)
        traceback.print_exc()


# ═════════════════════════════════════════════════════════════════════════
# FIRE / EMS SECTION — fully separate from everything above. Own env vars,
# own Supabase tables, own storage prefix, own SSE client list, own thread.
# Nothing above this line was changed to add this; nothing below reads or
# writes any police variable, table, or state.
# ═════════════════════════════════════════════════════════════════════════

fire_clients = []

GROQ_API_KEY_BACKUP = os.environ.get("GROQ_API_KEY_BACKUP")
STREAM_URL2         = os.environ.get("STREAM_URL2")   # fire/EMS .m3u8 playlist URL

FIRE_MAX_INCIDENTS = 5000
FIRE_MAX_LOG_ROWS  = 5000
FIRE_AUDIO_PREFIX  = "fire_clips"      # same AUDIO_BUCKET, separate folder from police "clips/"
FIRE_INCIDENT_TABLE = "fire_incidents"
FIRE_LOG_TABLE       = "fire_radio_log"
GEOCODE_CACHE_TABLE  = "geocode_cache"   # server-side cache of resolved map coordinates
FIRE_SEG_SILENCE_DB      = -40.0  # ffmpeg mean_volume threshold; segments quieter than this = silence
FIRE_PREROLL_SEGMENTS    = 1      # keep this many segments (~4s) buffered before a trigger, so we don't clip the start of a call
FIRE_HANGOVER_SEGMENTS   = 3      # end capture after this many consecutive silent segments (~4s of quiet)
FIRE_MAX_CLIP_SEGMENTS   = 15     # safety cap (~60s) in case a call/noise never goes quiet


# Repeated-transmission de-dupe: fire/EMS dispatch conventionally reads a
# call out twice. We keep a short rolling memory of recent call text and
# skip creating a second incident card if something very similar just came
# through — the repeat still gets logged to the radio log, just not carded.
FIRE_DEDUP_WINDOW_SECONDS       = 300
FIRE_DEDUP_SIMILARITY_THRESHOLD = 0.55
_fire_recent_calls = []  # list of (timestamp, normalized_text)
_fire_recent_calls_lock = threading.Lock()

fire_groq_client = Groq(api_key=GROQ_API_KEY_BACKUP)

def fire_segment_mean_volume_db(seg_bytes: bytes) -> float:
    """Returns the mean volume (dB) of a single ~4s TS segment via ffmpeg's
    volumedetect filter. Used as the voice-activity trigger — cheap enough
    to run on every segment as it arrives."""
    if not seg_bytes:
        return -99.0
    with tempfile.NamedTemporaryFile(suffix=".ts", delete=False) as f:
        f.write(seg_bytes)
        path = f.name
    try:
        result = subprocess.run(
            ["ffmpeg", "-i", path, "-af", "volumedetect", "-f", "null", "-"],
            capture_output=True, text=True, timeout=15
        )
        for line in result.stderr.splitlines():
            if "mean_volume:" in line:
                return float(line.split("mean_volume:")[1].strip().split(" ")[0])
        return -99.0
    except Exception as e:
        print(f"[fire-vad] volume check failed: {e}", flush=True)
        return -99.0
    finally:
        os.unlink(path)


@app.route("/fire/incidents")
def get_fire_incidents():
    offset = request.args.get("offset", 0, type=int)
    res = (get_db().table(FIRE_INCIDENT_TABLE)
           .select("*")
           .order("created_at", desc=True)
           .range(offset, offset + 49)
           .execute())
    return jsonify(res.data)


@app.route("/fire/radio-log")
def get_fire_radio_log():
    """Flat, unstructured, everything-that-was-said transcript feed — not
    tied to incidents at all."""
    offset = request.args.get("offset", 0, type=int)
    res = (get_db().table(FIRE_LOG_TABLE)
           .select("*")
           .order("created_at", desc=True)
           .range(offset, offset + 49)
           .execute())
    return jsonify(res.data)


# ─────────────────────────────────────────────
# Geocode cache proxy — the frontend never talks to Supabase directly for
# this. It hits these two routes; the Supabase URL/key stay server-side.
# ─────────────────────────────────────────────

def _geocode_key(loc: str) -> str:
    return " ".join(loc.strip().lower().split())


@app.route("/fire/geocode", methods=["GET"])
def get_geocode_cache():
    loc = request.args.get("location", "", type=str)
    key = _geocode_key(loc)
    if not key:
        return jsonify({"error": "location required"}), 400
    try:
        res = (get_db().table(GEOCODE_CACHE_TABLE)
               .select("lat,lng")
               .eq("location_key", key)
               .limit(1)
               .execute())
        if res.data:
            return jsonify(res.data[0])
        return jsonify(None), 404
    except Exception as e:
        print(f"[geocode-cache] get error: {e}", flush=True)
        return jsonify(None), 500


@app.route("/fire/geocode", methods=["POST"])
def save_geocode_cache():
    body = request.get_json(silent=True) or {}
    loc  = body.get("location", "")
    lat  = body.get("lat")
    lng  = body.get("lng")
    key  = _geocode_key(loc)
    if not key or lat is None or lng is None:
        return jsonify({"error": "location, lat, lng required"}), 400
    try:
        get_db().table(GEOCODE_CACHE_TABLE).upsert({
            "location_key":      key,
            "original_location": loc.strip(),
            "lat":               lat,
            "lng":               lng,
        }).execute()
        return jsonify({"ok": True})
    except Exception as e:
        print(f"[geocode-cache] save error: {e}", flush=True)
        return jsonify({"ok": False}), 500


@app.route("/fire/stream")
def fire_stream():
    def event_stream():
        fire_clients.append(queue := [])
        try:
            while True:
                if queue:
                    data = queue.pop(0)
                    yield f"data: {json.dumps(data)}\n\n"
                time.sleep(0.1)
        except GeneratorExit:
            fire_clients.remove(queue)
    return Response(event_stream(), mimetype="text/event-stream")


def fire_broadcast(kind: str, data: dict):
    payload = {"kind": kind, "data": data}
    for q in fire_clients:
        q.append(payload)


@app.route("/fire/stats")
def get_fire_stats():
    try:
        db = get_db()
        now_et = datetime.now(EASTERN)
        today_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start.astimezone(ZoneInfo("UTC")).isoformat()

        all_time = db.table(FIRE_INCIDENT_TABLE).select("id", count="exact").execute().count or 0

        total = (db.table(FIRE_INCIDENT_TABLE)
                 .select("id", count="exact")
                 .gte("created_at", today_start_utc)
                 .execute()).count or 0

        high = (db.table(FIRE_INCIDENT_TABLE)
                .select("id", count="exact")
                .gte("created_at", today_start_utc)
                .eq("priority", "High")
                .execute()).count or 0

        rows = (db.table(FIRE_INCIDENT_TABLE)
                .select("units, time_str, created_at, incident_type")
                .gte("created_at", today_start_utc)
                .order("created_at", desc=True)
                .execute()).data or []

        all_units = {u for r in rows for u in (r.get("units") or [])}
        last_call = rows[0]["time_str"] if rows else "—"

        rate = "0"
        if len(rows) > 1:
            newest = datetime.fromisoformat(rows[0]["created_at"])
            oldest = datetime.fromisoformat(rows[-1]["created_at"])
            hrs = max((newest - oldest).total_seconds() / 3600, 0.1)
            rate = f"{len(rows) / hrs:.1f}"

        types = {}
        for r in rows:
            t = r.get("incident_type") or "Unknown"
            types[t] = types.get(t, 0) + 1

        return jsonify({
            "total": total,
            "all_time": all_time,
            "high": high,
            "units": len(all_units),
            "last_call": last_call,
            "rate": rate,
            "breakdown": types
        })
    except Exception as e:
        print(f"[fire] Stats error: {e}", flush=True)
        return jsonify({"total": 0, "all_time": 0, "high": 0, "units": 0, "last_call": "—", "rate": "0", "breakdown": {}})


def fire_fetch_playlist() -> list[str]:
    text = curl_fetch(STREAM_URL2).decode()
    return [
        urljoin(STREAM_URL2, line.strip())
        for line in text.splitlines()
        if line.strip() and not line.startswith("#")
    ]


def fire_scanner_loop():
    """Voice-activated capture with pre-roll. Instead of grabbing a fixed
    60s window and hoping a call doesn't straddle the boundary, this watches
    each ~4s segment as it rolls in and only starts accumulating a clip once
    it actually hears audio — seeded with a bit of pre-roll so the start of
    the call isn't clipped. It keeps capturing until a run of silent
    segments (hangover) says the call is over, then hands the whole clip
    off to fire_process_audio_chunk untouched, same as before."""
    if not STREAM_URL2:
        print("[fire-scanner] STREAM_URL2 not set — nothing to capture.", flush=True)
        return

    print("[fire-scanner] Starting VAD capture loop against:", STREAM_URL2, flush=True)

    seen_urls = deque(maxlen=500)
    seen_set = set()
    preroll_buffer = deque(maxlen=FIRE_PREROLL_SEGMENTS)
    capture_buffer = []
    capturing = False
    silent_run = 0

    while True:
        try:
            segments = fire_fetch_playlist()
        except Exception as e:
            print(f"[fire-scanner] playlist fetch failed: {e}", flush=True)
            time.sleep(SEG_DURATION)
            continue

        new_segments = [s for s in segments if s not in seen_set]
        if not new_segments:
            time.sleep(SEG_DURATION)
            continue

        for url in new_segments:
            seen_urls.append(url)
            seen_set = set(seen_urls)

            try:
                seg_bytes = curl_fetch(url)
            except Exception as e:
                print(f"[fire-scanner] segment fetch failed: {e}", flush=True)
                continue

            vol_db = fire_segment_mean_volume_db(seg_bytes)
            has_audio = vol_db > FIRE_SEG_SILENCE_DB

            if not capturing:
                if has_audio:
                    print(f"[fire-scanner] audio detected ({vol_db:.1f}dB) — starting capture", flush=True)
                    capture_buffer = list(preroll_buffer) + [seg_bytes]  # true pre-roll + trigger segment
                    capturing = True
                    silent_run = 0
                else:
                    preroll_buffer.append(seg_bytes)
                continue

            # capturing
            capture_buffer.append(seg_bytes)
            silent_run = 0 if has_audio else silent_run + 1

            ended     = silent_run >= FIRE_HANGOVER_SEGMENTS
            too_long  = len(capture_buffer) >= FIRE_MAX_CLIP_SEGMENTS

            if ended or too_long:
                reason = "hit max length" if too_long else "went quiet"
                print(f"[fire-scanner] capture ended ({reason}), {len(capture_buffer)} segments — processing", flush=True)
                ts_bytes = b"".join(capture_buffer)
                try:
                    mp3 = convert_to_mp3(ts_bytes)
                    if mp3:
                        dur = get_duration(mp3)
                        print(f"[fire-scanner] clip duration: {dur:.1f}s, {len(mp3)} bytes", flush=True)
                        fire_process_audio_chunk(mp3)
                except Exception as e:
                    import traceback
                    print(f"[fire-scanner] error: {e}", flush=True)
                    traceback.print_exc()

                capture_buffer = []
                capturing = False
                silent_run = 0
                preroll_buffer.clear()


def fire_upload_audio(audio_bytes: bytes) -> str | None:
    try:
        db   = get_db()
        ts   = datetime.now(EASTERN).strftime("%Y%m%d_%H%M%S")
        path = f"{FIRE_AUDIO_PREFIX}/clip_{ts}.mp3"
        db.storage.from_(AUDIO_BUCKET).upload(
            path, audio_bytes, {"content-type": "audio/mpeg", "upsert": "false"}
        )
        return f"{SUPABASE_URL}/storage/v1/object/public/{AUDIO_BUCKET}/{path}"
    except Exception as e:
        print(f"[fire] Audio upload error: {e}", flush=True)
        return None


def fire_transcribe(audio_bytes: bytes) -> str:
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
            f.write(audio_bytes)
            tmp_path = f.name
        with open(tmp_path, "rb") as f:
            result = fire_groq_client.audio.transcriptions.create(
                file=("audio.mp3", f, "audio/mpeg"),
                model="whisper-large-v3-turbo",
                response_format="text"
            )
        os.unlink(tmp_path)
        return result.strip() if result else ""
    except Exception as e:
        print(f"[fire] Transcription error: {e}", flush=True)
        return ""


FIRE_PARSE_PROMPT = """You are a fire/EMS dispatch parser for Amherst NY (Erie County).
Extract structured data from this radio transcript, ONLY if it represents an
actual NEW dispatch / call being issued (a box alarm, EMS call, MVA, fire
call, alarm activation, rescue, hazmat, mutual aid, etc.).

Transcript: {transcript}

Rules — return exactly the word null (no quotes, no JSON) if ANY of these apply:
- The transcript is ONLY a unit acknowledging, responding to, or giving a
  status update on a call already in progress (e.g. "Engine 4 responding",
  "Ladder 12 en route", "Medic 3 available", "10-8", "show us on scene",
  "command established") with no new incident information.
- The transcript is static, silence, dead air, or otherwise not real
  dispatch content.

Otherwise, respond ONLY with a valid JSON object with these exact fields:
- incident_type: string (e.g. "Structure Fire", "Vehicle Fire", "Brush Fire", "EMS - Medical", "MVA", "Alarm Activation", "Rescue", "Hazmat", "Mutual Aid", "Unknown")
- location: string (address or intersection mentioned, or "Unknown")
- units: array of strings (unit numbers or call signs mentioned, empty array if none)
- priority: string, one of exactly: "High", "Medium", "Low", "Unknown"
- notes: string (any other relevant detail, max 1 sentence)

Return raw JSON only. No markdown, no explanation, no code blocks."""


def fire_parse_transcript(transcript: str):
    try:
        resp = fire_groq_client.chat.completions.create(
            model="openai/gpt-oss-120b",
            messages=[{"role": "user", "content": FIRE_PARSE_PROMPT.format(transcript=transcript)}],
            max_tokens=600,
            temperature=0.1,
            reasoning_effort="low"
        )
        text = (resp.choices[0].message.content or "").strip()
        if not text:
            print(f"[fire] Parse warning: empty response, finish_reason={resp.choices[0].finish_reason}", flush=True)
            return None
        if text.lower() == "null":
            return None
        return json.loads(text.replace("```json", "").replace("```", "").strip())
    except Exception as e:
        print(f"[fire] Parse error: {e}", flush=True)
        return None

def _fire_normalize(text: str) -> str:
    return " ".join(text.strip().lower().split())


def fire_is_duplicate_call(transcript: str) -> bool:
    """True if this transcript is a near-repeat of a call we heard in the
    last FIRE_DEDUP_WINDOW_SECONDS — i.e. the second time dispatch reads
    out the same call. Keeps us from double-carding one incident."""
    now = time.time()
    norm = _fire_normalize(transcript)
    with _fire_recent_calls_lock:
        global _fire_recent_calls
        _fire_recent_calls = [(t, txt) for (t, txt) in _fire_recent_calls if now - t < FIRE_DEDUP_WINDOW_SECONDS]
        for (_, txt) in _fire_recent_calls:
            if difflib.SequenceMatcher(None, norm, txt).ratio() >= FIRE_DEDUP_SIMILARITY_THRESHOLD:
                return True
    return False


def fire_remember_call(transcript: str):
    with _fire_recent_calls_lock:
        _fire_recent_calls.append((time.time(), _fire_normalize(transcript)))


def fire_purge_old_incidents():
    try:
        db    = get_db()
        total = db.table(FIRE_INCIDENT_TABLE).select("id", count="exact").execute().count or 0
        if total <= FIRE_MAX_INCIDENTS:
            return
        oldest = (db.table(FIRE_INCIDENT_TABLE)
                  .select("id, audio_url")
                  .order("created_at", desc=False)
                  .limit(total - FIRE_MAX_INCIDENTS)
                  .execute()).data or []
        for row in oldest:
            if row.get("audio_url"):
                delete_audio(row["audio_url"])
            db.table(FIRE_INCIDENT_TABLE).delete().eq("id", row["id"]).execute()
            print(f"[fire] Purged incident id={row['id']}", flush=True)
    except Exception as e:
        print(f"[fire] Purge error: {e}", flush=True)


def fire_purge_old_log_rows():
    try:
        db    = get_db()
        total = db.table(FIRE_LOG_TABLE).select("id", count="exact").execute().count or 0
        if total <= FIRE_MAX_LOG_ROWS:
            return
        oldest = (db.table(FIRE_LOG_TABLE)
                  .select("id")
                  .order("created_at", desc=False)
                  .limit(total - FIRE_MAX_LOG_ROWS)
                  .execute()).data or []
        for row in oldest:
            db.table(FIRE_LOG_TABLE).delete().eq("id", row["id"]).execute()
    except Exception as e:
        print(f"[fire] Log purge error: {e}", flush=True)


def fire_save_log_entry(transcript: str):
    """Append EVERYTHING that was said to the flat radio log — chatter,
    responding traffic, real calls, all of it — one row per audio chunk.
    Intentionally decoupled from incident detection."""
    try:
        db  = get_db()
        row = {
            "text":     transcript,
            "time_str": datetime.now(EASTERN).strftime("%I:%M %p"),
        }
        res   = db.table(FIRE_LOG_TABLE).insert(row).execute()
        saved = res.data[0] if res.data else row
        fire_broadcast("log", saved)
        fire_purge_old_log_rows()
    except Exception as e:
        print(f"[fire] Log save error: {e}", flush=True)


def fire_save_incident(parsed: dict, transcript: str, audio_url: str | None):
    try:
        db  = get_db()
        row = {
            "incident_type": parsed.get("incident_type", "Unknown"),
            "location":      parsed.get("location", "Unknown"),
            "units":         parsed.get("units", []),
            "priority":      parsed.get("priority", "Unknown"),
            "notes":         parsed.get("notes", ""),
            "transcript":    transcript,
            "time_str":      datetime.now(EASTERN).strftime("%I:%M %p"),
            "audio_url":     audio_url,
        }
        res   = db.table(FIRE_INCIDENT_TABLE).insert(row).execute()
        saved = res.data[0] if res.data else row
        fire_broadcast("incident", saved)
        print(f"[fire] Saved + broadcasted: {row['incident_type']}", flush=True)
        fire_purge_old_incidents()
    except Exception as e:
        print(f"[fire] Save error: {e}", flush=True)


def fire_process_audio_chunk(audio: bytes):
    try:
        print("[fire] Trimming silence...", flush=True)
        trimmed = trim_silence(audio)
        if not trimmed:
            print("[fire] Pure silence, skipping.", flush=True)
            return

        raw_dur = get_duration(audio)
        trimmed_dur = get_duration(trimmed)
        print(f"[fire] trim result: {raw_dur:.1f}s raw -> {trimmed_dur:.1f}s trimmed", flush=True)

        print("[fire] Transcribing...", flush=True)
        transcript = fire_transcribe(trimmed)
        print(f"[fire] Transcript ({len(transcript)} chars): {transcript[:100]!r}", flush=True)

        if len(transcript.strip()) < 10:
            print("[fire] Too short, skipping entirely (not even logged).", flush=True)
            return

        # Log EVERYTHING first, unconditionally — chatter, responding
        # traffic, real dispatch calls. Powers the flat radio log tab.
        fire_save_log_entry(transcript)

        # Fresh-calls-only gate #1: is this just a repeat of a call that
        # was already read out in the last few minutes?
        if fire_is_duplicate_call(transcript):
            print("[fire] repeat transmission of a recent call — logged, no new incident card", flush=True)
            return

        # Fresh-calls-only gate #2: LLM filters out pure "responding" /
        # acknowledgment chatter and anything that isn't a real new call.
        print("[fire] Parsing...", flush=True)
        parsed = fire_parse_transcript(transcript)
        if not parsed:
            print("[fire] Not a new dispatch call (chatter/responding) — logged only.", flush=True)
            return

        fire_remember_call(transcript)

        # Only now, once we know it's a genuine new call, keep the audio.
        print("[fire] Uploading audio for confirmed new incident...", flush=True)
        audio_url = fire_upload_audio(trimmed)
        fire_save_incident(parsed, transcript, audio_url)

    except Exception as e:
        import traceback
        print(f"[fire] Processing error: {e}", flush=True)
        traceback.print_exc()


# ─────────────────────────────────────────────
# Start both capture threads, then the server
# ─────────────────────────────────────────────

thread = threading.Thread(target=scanner_loop, daemon=True)
thread.start()

fire_thread = threading.Thread(target=fire_scanner_loop, daemon=True)
fire_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
