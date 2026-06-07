import os, io, time, requests, threading, tempfile, json, concurrent.futures
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from groq import Groq
from supabase import create_client
import sys
sys.stdout.reconfigure(line_buffering=True)

clients = []

GROQ_API_KEY  = os.environ.get("GROQ_API_KEY")
SUPABASE_URL  = os.environ.get("SUPABASE_URL")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY")
STREAM_URL    = os.environ.get("STREAM_URL")
CHUNK_SECONDS = 30
MAX_INCIDENTS = 500
AUDIO_BUCKET  = "audio-clips"

EASTERN = ZoneInfo("America/New_York")

groq_client = Groq(api_key=GROQ_API_KEY)
supabase    = create_client(SUPABASE_URL, SUPABASE_KEY)

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
    res = (supabase.table("incidents")
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

# ─────────────────────────────────────────────
# Scanner helpers
# ─────────────────────────────────────────────

def capture_chunk():
    try:
        resp = requests.get(STREAM_URL, stream=True, timeout=10)
        
        # Flush the backfill buffer first (~3MB gets you close to live)
        flushed = 0
        flush_target = 3 * 1024 * 1024  # 3MB
        for chunk in resp.iter_content(chunk_size=4096):
            flushed += len(chunk)
            if flushed >= flush_target:
                break

        # Now capture from closer to live position
        buf = io.BytesIO()
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
        print(f"Capture error: {e}")
        return None


@app.route("/stats")
def get_stats():
    try:
        # Get today's date range in Eastern Time
        now_et = datetime.now(EASTERN)
        today_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start.astimezone(ZoneInfo("UTC")).isoformat()

        # Count all today's incidents
        total_res = (supabase.table("incidents")
                     .select("id", count="exact")
                     .gte("created_at", today_start_utc)
                     .execute())
        total = total_res.count or 0

        # Count high priority
        high_res = (supabase.table("incidents")
                    .select("id", count="exact")
                    .gte("created_at", today_start_utc)
                    .eq("priority", "High")
                    .execute())
        high = high_res.count or 0

        # Get units and last call from today's incidents (lightweight, no transcript)
        detail_res = (supabase.table("incidents")
                      .select("units, time_str, created_at, incident_type")
                      .gte("created_at", today_start_utc)
                      .order("created_at", desc=True)
                      .execute())
        rows = detail_res.data or []

        all_units = set()
        for r in rows:
            for u in (r.get("units") or []):
                all_units.add(u)

        last_call = rows[0]["time_str"] if rows else "—"

        # Calls per hour
        rate = "0"
        if len(rows) > 1:
            newest = datetime.fromisoformat(rows[0]["created_at"])
            oldest = datetime.fromisoformat(rows[-1]["created_at"])
            hrs = max((newest - oldest).total_seconds() / 3600, 0.1)
            rate = f"{len(rows) / hrs:.1f}"

        # Breakdown by type
        types = {}
        for r in rows:
            t = r.get("incident_type") or "Unknown"
            types[t] = types.get(t, 0) + 1

        return jsonify({
            "total": total,
            "high": high,
            "units": len(all_units),
            "last_call": last_call,
            "rate": rate,
            "breakdown": types
        })
    except Exception as e:
        print(f"Stats error: {e}")
        return jsonify({"total":0,"high":0,"units":0,"last_call":"—","rate":"0","breakdown":{}})



def trim_silence(audio_bytes: bytes) -> bytes:
    """Use ffmpeg silenceremove filter to strip leading/trailing silence."""
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as fin:
            fin.write(audio_bytes)
            in_path = fin.name
        out_path = in_path.replace(".mp3", "_trimmed.mp3")

        import subprocess
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
            print("ffmpeg trim failed, using original")
            return audio_bytes

        with open(out_path, "rb") as f:
            trimmed = f.read()
        os.unlink(out_path)

        if len(trimmed) < 1000:
            print("Trim: result too small, likely pure silence")
            return b""

        original_kb = len(audio_bytes) // 1024
        trimmed_kb  = len(trimmed) // 1024
        print(f"Trim: {original_kb}KB → {trimmed_kb}KB")
        return trimmed

    except Exception as e:
        print(f"Trim error: {e}")
        return audio_bytes



def upload_audio(audio_bytes: bytes) -> str | None:
    try:
        ts       = datetime.now(EASTERN).strftime("%Y%m%d_%H%M%S")
        filename = f"clip_{ts}.mp3"
        path     = f"clips/{filename}"
        supabase.storage.from_(AUDIO_BUCKET).upload(
            path,
            audio_bytes,
            {"content-type": "audio/mpeg", "upsert": "false"},
        )
        return f"{SUPABASE_URL}/storage/v1/object/public/{AUDIO_BUCKET}/{path}"
    except Exception as e:
        print(f"Audio upload error: {e}")
        return None


def delete_audio(audio_url: str):
    """Helper to remove a clip from storage given its public URL."""
    try:
        marker = f"/public/{AUDIO_BUCKET}/"
        if marker in audio_url:
            clip_path = audio_url.split(marker, 1)[1]
            supabase.storage.from_(AUDIO_BUCKET).remove([clip_path])
    except Exception as e:
        print(f"Audio delete error: {e}")


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
        print(f"Transcription error: {e}")
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
        text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"Parse error: {e}")
        return None


def purge_old_incidents():
    try:
        count_res = (supabase.table("incidents")
                     .select("id", count="exact")
                     .execute())
        total = count_res.count or 0
        if total <= MAX_INCIDENTS:
            return
        excess = total - MAX_INCIDENTS
        oldest = (supabase.table("incidents")
                  .select("id, audio_url")
                  .order("created_at", desc=False)
                  .limit(excess)
                  .execute())
        for row in (oldest.data or []):
            if row.get("audio_url"):
                delete_audio(row["audio_url"])
            supabase.table("incidents").delete().eq("id", row["id"]).execute()
            print(f"Purged old incident id={row['id']}")
    except Exception as e:
        print(f"Purge error: {e}")


def save_incident(parsed: dict, transcript: str, audio_url: str | None):
    try:
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
        res   = supabase.table("incidents").insert(row).execute()
        saved = res.data[0] if res.data else row
        for q in clients:
            q.append(saved)
        print(f"Saved + broadcasted: {row['incident_type']}")
        purge_old_incidents()
    except Exception as e:
        print(f"Save error: {e}")


# ─────────────────────────────────────────────
# Main scanner loop
# ─────────────────────────────────────────────

def scanner_loop():
    print("Scanner loop started...")
    while True:
        try:
            print("Capturing audio chunk...", flush=True)
            audio = capture_chunk()
            if not audio:
                time.sleep(5)
                continue

            # Step 1: trim silence — if result is empty bytes it was pure silence
            print("Trimming silence...")
            trimmed = trim_silence(audio)
            if not trimmed:
                print("Pure silence, skipping.")
                continue

            # Step 2: transcribe and upload IN PARALLEL (upload uses trimmed audio)
            print("Transcribing + uploading in parallel...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                transcribe_future = ex.submit(transcribe, trimmed)
                upload_future     = ex.submit(upload_audio, trimmed)
                transcript = transcribe_future.result()
                audio_url  = upload_future.result()

            print(f"Transcript: {transcript[:100] if transcript else 'empty'}")

            # Step 3: if transcript too short, clean up and skip
            if len(transcript) < 15:
                print("Too short, skipping.")
                if audio_url:
                    delete_audio(audio_url)
                continue

            # Step 4: parse
            print("Parsing...")
            parsed = parse_transcript(transcript)

            if parsed:
                save_incident(parsed, transcript, audio_url)
            else:
                print("No incident detected.")
                if audio_url:
                    delete_audio(audio_url)

        except Exception as e:
            print(f"Loop error: {e}")
            time.sleep(10)


thread = threading.Thread(target=scanner_loop, daemon=True)
thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
