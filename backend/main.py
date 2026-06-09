import os, io, time, requests, threading, tempfile, json, subprocess, re
from datetime import datetime, timezone, timedelta
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
MAX_INCIDENTS = 1000
AUDIO_BUCKET  = "audio-clips"
MERGE_WINDOW_MINUTES = 20   # how old an incident can be and still receive updates

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
# Scanner helpers
# ─────────────────────────────────────────────

def capture_chunk():
    try:
        resp = requests.get(STREAM_URL, stream=True, timeout=(10, 45))
        buf  = io.BytesIO()
        bytes_read = 0
        target = 16000 * CHUNK_SECONDS
        for chunk in resp.iter_content(chunk_size=4096):
            buf.write(chunk)
            bytes_read += len(chunk)
            if bytes_read >= target:
                break
        resp.close()
        return buf.getvalue()
    except Exception as e:
        print(f"Capture error: {e}", flush=True)
        return None


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


# ─────────────────────────────────────────────
# Multi-event parse prompt
# ─────────────────────────────────────────────

PARSE_PROMPT = """You are a police dispatch parser for Erie County / Amherst NY.

A single 30-second radio clip may contain MULTIPLE distinct transmissions. Your job is to
identify each one separately and classify it.

Transcript:
{transcript}

Rules:
1. Split into separate events if you hear distinct calls, unit responses, or status updates.
2. Filter out pure noise: routine status checks like "unit X 10-8" with no call attached,
   test transmissions, cross-talk with no incident content, or anything that is clearly not
   a dispatch event. Set is_dispatch_call=false for these.
3. For each real dispatch event extract a call_signature — a short normalized key that
   represents the SAME physical call across multiple transmissions. Format:
   TYPE|NORMALIZED_LOCATION  e.g. "MVA|TRANSIT_RD_MAPLE" or "DOMESTIC|123_MAIN_ST"
   - Normalize street abbreviations (Road→RD, Street→ST, Avenue→AVE, Drive→DR, etc.)
   - Drop apartment/unit numbers
   - Use uppercase, underscores, no punctuation
   - If location is truly unknown use TYPE|UNKNOWN
4. For is_update: set true if the transmission sounds like a unit arriving, a status
   update, additional units being assigned, or a follow-up rather than the initial dispatch.

Return ONLY a valid JSON array. Each element:
{{
  "is_dispatch_call": bool,
  "is_update": bool,
  "call_signature": "TYPE|LOCATION_KEY",
  "incident_type": "string (MVA / Domestic / Theft / Medical / Fire / Noise / Burglary / Suspicious / Assault / Unknown)",
  "location": "string (human readable address or intersection, or Unknown)",
  "units": ["array", "of", "unit", "strings"],
  "priority": "High | Medium | Low | Unknown",
  "notes": "string (max 1 sentence, any relevant detail)"
}}

If the ENTIRE transcript is static, silence, or contains zero dispatch content, return: null

Return raw JSON only. No markdown, no explanation, no code blocks."""


def parse_transcript(transcript: str):
    """Returns a list of event dicts, or None if nothing useful in audio."""
    try:
        resp = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": PARSE_PROMPT.format(transcript=transcript)}],
            max_tokens=800,
            temperature=0.1
        )
        text = resp.choices[0].message.content.strip()
        if text.lower() == "null":
            return None
        cleaned = text.replace("```json", "").replace("```", "").strip()
        result = json.loads(cleaned)
        # Ensure it's a list
        if isinstance(result, dict):
            result = [result]
        return result
    except Exception as e:
        print(f"Parse error: {e}", flush=True)
        return None


# ─────────────────────────────────────────────
# Call signature matching
# ─────────────────────────────────────────────

def normalize_sig(sig: str) -> str:
    """Extra normalization pass on the call_signature for comparison."""
    return re.sub(r'[^A-Z0-9_|]', '', sig.upper().strip())


def find_matching_incident(db, call_signature: str, units: list) -> dict | None:
    """
    Look for an open incident within MERGE_WINDOW_MINUTES that matches either:
    - Same normalized call_signature, OR
    - Any unit overlap (units are the most reliable anchor)
    Returns the full incident row or None.
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=MERGE_WINDOW_MINUTES)).isoformat()
        rows = (db.table("incidents")
                .select("*")
                .gte("created_at", cutoff)
                .order("created_at", desc=True)
                .execute()).data or []

        norm_sig = normalize_sig(call_signature) if call_signature else ""
        unit_set = set(u.upper().strip() for u in (units or []) if u)

        for row in rows:
            # Primary: call_signature exact match (after normalization)
            row_sig = normalize_sig(row.get("call_signature") or "")
            if norm_sig and row_sig and norm_sig == row_sig and "UNKNOWN" not in norm_sig:
                print(f"Signature match: {norm_sig} → incident {row['id']}", flush=True)
                return row

            # Secondary: unit overlap
            row_units = set(u.upper().strip() for u in (row.get("units") or []) if u)
            if unit_set and row_units and unit_set & row_units:
                print(f"Unit overlap match {unit_set & row_units} → incident {row['id']}", flush=True)
                return row

        return None
    except Exception as e:
        print(f"Match error: {e}", flush=True)
        return None


# ─────────────────────────────────────────────
# Save / update incidents
# ─────────────────────────────────────────────

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


def merge_units(existing: list, new: list) -> list:
    combined = list(existing or [])
    for u in (new or []):
        if u not in combined:
            combined.append(u)
    return combined


def append_transcript(existing: str, new: str) -> str:
    if not existing:
        return new
    if not new or new in existing:
        return existing
    return existing + "\n---\n" + new


def save_or_update_incident(parsed: dict, transcript: str, audio_url: str | None):
    """
    Decides whether to INSERT a new incident or UPDATE an existing one.
    Broadcasts the result to all SSE clients with _event_type = 'new' or 'update'.
    """
    try:
        db = get_db()
        call_sig = parsed.get("call_signature", "")
        units    = parsed.get("units", [])
        is_update_hint = parsed.get("is_update", False)

        existing = find_matching_incident(db, call_sig, units)

        now_str = datetime.now(EASTERN).strftime("%I:%M %p")

        if existing:
            # ── UPDATE path ──────────────────────────────────────────────
            inc_id = existing["id"]

            merged_units      = merge_units(existing.get("units"), units)
            merged_transcript = append_transcript(existing.get("transcript",""), transcript)

            # Only upgrade priority, never downgrade
            pri_order = {"High": 3, "Medium": 2, "Low": 1, "Unknown": 0}
            old_pri = existing.get("priority", "Unknown")
            new_pri = parsed.get("priority", "Unknown")
            final_pri = old_pri if pri_order.get(old_pri,0) >= pri_order.get(new_pri,0) else new_pri

            # Build update history entry
            history_entry = {
                "time": now_str,
                "notes": parsed.get("notes", ""),
                "units_added": [u for u in units if u not in (existing.get("units") or [])],
                "audio_url": audio_url,
            }
            history = existing.get("update_history") or []
            history.append(history_entry)

            updates = {
                "units":           merged_units,
                "transcript":      merged_transcript,
                "priority":        final_pri,
                "update_history":  history,
                "last_updated_str": now_str,
                # Keep original audio_url; only replace if this clip has audio and original didn't
                "audio_url": existing.get("audio_url") or audio_url,
            }
            # Only update notes if new ones are more informative
            if parsed.get("notes") and len(parsed["notes"]) > len(existing.get("notes") or ""):
                updates["notes"] = parsed["notes"]

            res  = db.table("incidents").update(updates).eq("id", inc_id).execute()
            saved = res.data[0] if res.data else {**existing, **updates}
            saved["_event_type"] = "update"
            print(f"Updated incident {inc_id}: {existing.get('incident_type')}", flush=True)

        else:
            # ── INSERT path ──────────────────────────────────────────────
            row = {
                "incident_type":    parsed.get("incident_type", "Unknown"),
                "location":         parsed.get("location", "Unknown"),
                "units":            units,
                "priority":         parsed.get("priority", "Unknown"),
                "notes":            parsed.get("notes", ""),
                "transcript":       transcript,
                "call_signature":   call_sig,
                "time_str":         now_str,
                "audio_url":        audio_url,
                "update_history":   [],
                "last_updated_str": now_str,
            }
            res   = db.table("incidents").insert(row).execute()
            saved = res.data[0] if res.data else row
            saved["_event_type"] = "new"
            print(f"New incident: {row['incident_type']} @ {row['location']}", flush=True)
            purge_old_incidents()

        # Broadcast to all SSE clients
        for q in clients:
            q.append(saved)

    except Exception as e:
        print(f"Save/update error: {e}", flush=True)


# ─────────────────────────────────────────────
# Main scanner loop
# ─────────────────────────────────────────────

def scanner_loop():
    print("Scanner loop started...", flush=True)
    while True:
        try:
            print("Capturing audio chunk...", flush=True)
            audio = capture_chunk()
            if not audio:
                time.sleep(5)
                continue

            print("Trimming silence...", flush=True)
            trimmed = trim_silence(audio)
            if not trimmed:
                print("Pure silence, skipping.", flush=True)
                continue

            print("Uploading audio...", flush=True)
            audio_url = upload_audio(trimmed)

            print("Transcribing...", flush=True)
            transcript = transcribe(trimmed)
            print(f"Transcript ({len(transcript)} chars): {transcript[:120]!r}", flush=True)

            if len(transcript) < 15:
                print("Too short, skipping.", flush=True)
                if audio_url:
                    delete_audio(audio_url)
                continue

            print("Parsing events...", flush=True)
            events = parse_transcript(transcript)

            if not events:
                print("No dispatch events detected, cleaning up.", flush=True)
                if audio_url:
                    delete_audio(audio_url)
                continue

            # Filter to only real dispatch calls
            dispatch_events = [e for e in events if e.get("is_dispatch_call", True)]
            non_dispatch    = len(events) - len(dispatch_events)

            if non_dispatch:
                print(f"Filtered {non_dispatch} non-dispatch transmission(s).", flush=True)

            if not dispatch_events:
                print("All events were non-dispatch, cleaning up.", flush=True)
                if audio_url:
                    delete_audio(audio_url)
                continue

            print(f"Processing {len(dispatch_events)} dispatch event(s)...", flush=True)

            # Only attach the audio clip to the first event in a multi-event clip
            # (the clip covers all of them but we can't split it here)
            for idx, event in enumerate(dispatch_events):
                clip_url = audio_url if idx == 0 else None
                save_or_update_incident(event, transcript, clip_url)

        except Exception as e:
            import traceback
            print(f"Loop error: {e}", flush=True)
            traceback.print_exc()
            time.sleep(10)


thread = threading.Thread(target=scanner_loop, daemon=True)
thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
