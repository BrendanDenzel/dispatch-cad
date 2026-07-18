import os, io, time, requests, threading, tempfile, json, subprocess
import queue as queue_mod
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from groq import Groq
from supabase import create_client

clients = []

GROQ_API_KEY  = os.environ.get("GROQ_API_KEY")
SUPABASE_URL  = os.environ.get("SUPABASE_URL")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY")
STREAM_URL    = os.environ.get("STREAM_URL")
CHUNK_SECONDS = 30
MAX_INCIDENTS = 5000
AUDIO_BUCKET  = "audio-clips"

# How far back we look for incidents a new transmission might belong to.
OPEN_WINDOW_MINUTES  = 20
MAX_OPEN_CANDIDATES  = 15

# Anything the correlator drops (chatter, too-short fragments) still gets
# written here so nothing said on the channel is silently gone — it's just
# not promoted to an incident. Flat file, no DB schema involved.
CHATTER_LOG_PATH = os.environ.get("CHATTER_LOG_PATH", "/tmp/chatter_log.txt")
_chatter_lock     = threading.Lock()

# Audio chunks flow from the capture thread to the processing thread through
# this queue. Capture never waits on processing, so recording never pauses.
audio_queue = queue_mod.Queue()

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

@app.route("/chatter-log")
def chatter_log():
    """Everything the correlator dropped (chatter / too-short fragments),
    so you can confirm nothing on the channel is actually being lost."""
    try:
        limit = request.args.get("limit", 200, type=int)
        if not os.path.exists(CHATTER_LOG_PATH):
            return jsonify([])
        with _chatter_lock:
            with open(CHATTER_LOG_PATH) as f:
                lines = f.readlines()[-limit:]
        return jsonify([l.rstrip("\n") for l in lines])
    except Exception as e:
        print(f"Chatter log read error: {e}", flush=True)
        return jsonify([])

# ─────────────────────────────────────────────
# Scanner capture / audio helpers
# ─────────────────────────────────────────────

def log_chatter(chunk_time: datetime, transcript: str, reason: str):
    try:
        line = f"{chunk_time.strftime('%Y-%m-%d %H:%M:%S')} [{reason}] {transcript}\n"
        with _chatter_lock:
            with open(CHATTER_LOG_PATH, "a") as f:
                f.write(line)
    except Exception as e:
        print(f"Chatter log write error: {e}", flush=True)


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
        ts   = datetime.now(EASTERN).strftime("%Y%m%d_%H%M%S_%f")
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


def download_audio_bytes(url: str) -> bytes | None:
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        print(f"Audio download error: {e}", flush=True)
        return None


def merge_audio(old_bytes: bytes, new_bytes: bytes) -> bytes:
    """Concatenate two mp3 clips into one continuous clip so the incident
    has a single combined recording instead of separate snippets."""
    try:
        with tempfile.NamedTemporaryFile(suffix="_old.mp3", delete=False) as f1:
            f1.write(old_bytes)
            old_path = f1.name
        with tempfile.NamedTemporaryFile(suffix="_new.mp3", delete=False) as f2:
            f2.write(new_bytes)
            new_path = f2.name
        out_path = old_path.replace("_old.mp3", "_merged.mp3")

        result = subprocess.run([
            "ffmpeg", "-y",
            "-i", old_path, "-i", new_path,
            "-filter_complex", "[0:a][1:a]concat=n=2:v=0:a=1[out]",
            "-map", "[out]", "-b:a", "64k",
            out_path
        ], capture_output=True, timeout=30)

        os.unlink(old_path)
        os.unlink(new_path)

        if result.returncode != 0 or not os.path.exists(out_path):
            print("ffmpeg merge failed, keeping old audio only", flush=True)
            return old_bytes

        with open(out_path, "rb") as f:
            merged = f.read()
        os.unlink(out_path)
        return merged

    except Exception as e:
        print(f"Audio merge error: {e}", flush=True)
        return old_bytes


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


# ─────────────────────────────────────────────
# Correlation: decide chatter / new incident / continuation of an existing one
# ─────────────────────────────────────────────

CORRELATE_PROMPT = """You are a police dispatch parser and incident correlator for Erie County / Amherst NY.

New radio transmission (format is "HH:MM:SS: transcript"):
{new_entry}

Currently open incidents from the last {window_minutes} minutes:
{open_incidents_block}

Decide ONE of:
1. "chatter" - this transmission has no real dispatch content (radio checks, static, unrelated banter, bare acknowledgements with no information)
2. "continuation" - this transmission is clearly about one of the open incidents listed above (same unit(s), same location/address, or an explicit follow-up on it)
3. "new" - this is a new, distinct incident not covered by the list above

Respond ONLY with a valid JSON object with these exact fields:
{{
  "action": "chatter" | "continuation" | "new",
  "incident_id": <id of the matched incident if action is "continuation", otherwise null>,
  "incident_type": string,
  "location": string,
  "units": array of strings mentioned in THIS transmission (empty array if none),
  "priority": one of exactly "High", "Medium", "Low", "Unknown",
  "notes": string, max 1 sentence, describing what is new in THIS transmission
}}

Rules:
- For "continuation": incident_type/location/priority should reflect the original incident's context; units/notes should reflect only what's new in this transmission.
- Only choose "continuation" if you are reasonably confident (matching unit numbers, matching address/location, or an explicit reference back to it). When unsure, prefer "new".
- If action is "chatter", other fields can be "Unknown" / empty — they will be ignored.
- Return raw JSON only. No markdown, no explanation, no code blocks."""


def fetch_open_incidents() -> list:
    try:
        db = get_db()
        cutoff = (datetime.now(ZoneInfo("UTC")) - timedelta(minutes=OPEN_WINDOW_MINUTES)).isoformat()
        rows = (db.table("incidents")
                .select("id, incident_type, location, units, transcript, notes, priority, created_at")
                .gte("created_at", cutoff)
                .order("created_at", desc=True)
                .limit(MAX_OPEN_CANDIDATES)
                .execute()).data or []
        return rows
    except Exception as e:
        print(f"Fetch open incidents error: {e}", flush=True)
        return []


def format_open_incidents(open_incidents: list) -> str:
    if not open_incidents:
        return f"(none — no open incidents in the last {OPEN_WINDOW_MINUTES} minutes)"
    blocks = []
    for inc in open_incidents:
        units = ", ".join(inc.get("units") or []) or "—"
        blocks.append(
            f"[ID {inc['id']}] Type: {inc.get('incident_type', 'Unknown')} | "
            f"Location: {inc.get('location', 'Unknown')} | Units: {units}\n"
            f"Transcript so far:\n{inc.get('transcript', '')}"
        )
    return "\n\n".join(blocks)


def correlate_transcript(timestamped_entry: str, open_incidents: list):
    try:
        prompt = CORRELATE_PROMPT.format(
            new_entry=timestamped_entry,
            window_minutes=OPEN_WINDOW_MINUTES,
            open_incidents_block=format_open_incidents(open_incidents)
        )
        resp = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0.1
        )
        text = resp.choices[0].message.content.strip()
        text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"Correlate error: {e}", flush=True)
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


def create_incident(parsed: dict, timestamped_entry: str, audio_url: str | None):
    try:
        db  = get_db()
        row = {
            "incident_type": parsed.get("incident_type", "Unknown"),
            "location":      parsed.get("location", "Unknown"),
            "units":         parsed.get("units", []),
            "priority":      parsed.get("priority", "Unknown"),
            "notes":         parsed.get("notes", ""),
            "transcript":    timestamped_entry,
            "time_str":      datetime.now(EASTERN).strftime("%I:%M %p"),
            "audio_url":     audio_url,
        }
        res   = db.table("incidents").insert(row).execute()
        saved = res.data[0] if res.data else row
        for q in clients:
            q.append({"event": "new", **saved})
        print(f"Created incident: {row['incident_type']}", flush=True)
        purge_old_incidents()
    except Exception as e:
        print(f"Create incident error: {e}", flush=True)


def update_incident_continuation(incident_id, parsed: dict, timestamped_entry: str, new_audio_bytes: bytes | None):
    try:
        db = get_db()
        existing = db.table("incidents").select("*").eq("id", incident_id).single().execute().data
        if not existing:
            print(f"Continuation target {incident_id} not found, creating new instead", flush=True)
            audio_url = upload_audio(new_audio_bytes) if new_audio_bytes else None
            create_incident(parsed, timestamped_entry, audio_url)
            return

        merged_transcript = (existing.get("transcript") or "").rstrip() + "\n" + timestamped_entry

        old_units    = set(existing.get("units") or [])
        new_units    = set(parsed.get("units") or [])
        merged_units = sorted(old_units | new_units)

        old_notes    = (existing.get("notes") or "").strip()
        new_note     = (parsed.get("notes") or "").strip()
        merged_notes = f"{old_notes} | {new_note}".strip(" |") if new_note else old_notes

        # Combine audio into one continuous clip instead of leaving separate snippets.
        new_audio_url = existing.get("audio_url")
        if new_audio_bytes:
            if existing.get("audio_url"):
                old_bytes = download_audio_bytes(existing["audio_url"])
                merged_bytes = merge_audio(old_bytes, new_audio_bytes) if old_bytes else new_audio_bytes
            else:
                merged_bytes = new_audio_bytes

            uploaded_url = upload_audio(merged_bytes)
            if uploaded_url:
                if existing.get("audio_url"):
                    delete_audio(existing["audio_url"])
                new_audio_url = uploaded_url

        update_row = {
            "transcript": merged_transcript,
            "units":      merged_units,
            "notes":      merged_notes,
            "audio_url":  new_audio_url,
        }

        # Let priority escalate (e.g. Medium -> High) but never silently downgrade it.
        priority_rank = {"High": 3, "Medium": 2, "Low": 1, "Unknown": 0}
        new_priority  = parsed.get("priority", "Unknown")
        if priority_rank.get(new_priority, 0) > priority_rank.get(existing.get("priority", "Unknown"), 0):
            update_row["priority"] = new_priority

        res   = db.table("incidents").update(update_row).eq("id", incident_id).execute()
        saved = res.data[0] if res.data else {**existing, **update_row}
        for q in clients:
            q.append({"event": "update", **saved})
        print(f"Updated incident {incident_id} (continuation)", flush=True)

    except Exception as e:
        print(f"Update continuation error: {e}", flush=True)


# ─────────────────────────────────────────────
# Capture thread — reads the live stream continuously and never pauses to
# wait on transcription/correlation/saving. It slices off a ~CHUNK_SECONDS
# chunk, hands it to audio_queue, and immediately keeps reading. This is
# what closes the old "dead air" gap where the recorder stopped listening
# while a chunk was being processed.
# ─────────────────────────────────────────────

def capture_loop():
    print("Capture loop started...", flush=True)
    target_bytes = 16000 * CHUNK_SECONDS
    while True:
        try:
            resp = requests.get(STREAM_URL, stream=True, timeout=(10, 60))
            buf = io.BytesIO()
            bytes_read = 0
            chunk_start = datetime.now(EASTERN)

            for chunk in resp.iter_content(chunk_size=4096):
                if not chunk:
                    continue
                buf.write(chunk)
                bytes_read += len(chunk)
                if bytes_read >= target_bytes:
                    audio_queue.put((chunk_start, buf.getvalue()))
                    buf = io.BytesIO()
                    bytes_read = 0
                    chunk_start = datetime.now(EASTERN)

            resp.close()
            print("Stream ended, reconnecting immediately...", flush=True)

        except Exception as e:
            print(f"Capture loop error: {e}, reconnecting...", flush=True)
            time.sleep(2)


# ─────────────────────────────────────────────
# Processing thread — does all the slow work (trim, transcribe, correlate,
# save) off the queue, at its own pace. If it falls behind, chunks just wait
# in the queue — audio itself is never dropped because of processing time.
# ─────────────────────────────────────────────

def processing_loop():
    print("Processing loop started...", flush=True)
    while True:
        try:
            chunk_time, audio = audio_queue.get()

            backlog = audio_queue.qsize()
            if backlog > 5:
                print(f"Warning: processing is behind by {backlog} queued chunk(s)", flush=True)

            trimmed = trim_silence(audio)
            if not trimmed:
                continue

            transcript = transcribe(trimmed)
            print(f"[{chunk_time.strftime('%H:%M:%S')}] Transcript ({len(transcript)} chars): {transcript[:100]!r}", flush=True)

            if len(transcript) < 15:
                if transcript:
                    log_chatter(chunk_time, transcript, reason="too_short")
                continue

            timestamped_entry = f"{chunk_time.strftime('%H:%M:%S')}: {transcript}"

            open_incidents = fetch_open_incidents()
            result = correlate_transcript(timestamped_entry, open_incidents)

            if not result or result.get("action") == "chatter":
                log_chatter(chunk_time, transcript, reason="chatter")
                continue

            action = result.get("action")

            if action == "continuation" and result.get("incident_id"):
                update_incident_continuation(result["incident_id"], result, timestamped_entry, trimmed)
            else:
                audio_url = upload_audio(trimmed)
                create_incident(result, timestamped_entry, audio_url)

        except Exception as e:
            import traceback
            print(f"Processing loop error: {e}", flush=True)
            traceback.print_exc()
            time.sleep(2)


capture_thread    = threading.Thread(target=capture_loop, daemon=True)
processing_thread = threading.Thread(target=processing_loop, daemon=True)
capture_thread.start()
processing_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
