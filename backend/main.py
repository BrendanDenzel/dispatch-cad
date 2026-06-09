import os, io, time, requests, threading, tempfile, json, subprocess
from datetime import datetime
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
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": PARSE_PROMPT.format(transcript=transcript)}],
            max_tokens=300,
            temperature=0.1
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
            print(f"Transcript ({len(transcript)} chars): {transcript[:100]!r}", flush=True)

            if len(transcript) < 15:
                print("Too short, skipping.", flush=True)
                if audio_url:
                    delete_audio(audio_url)
                continue

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
            print(f"Loop error: {e}", flush=True)
            traceback.print_exc()
            time.sleep(10)


thread = threading.Thread(target=scanner_loop, daemon=True)
thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
