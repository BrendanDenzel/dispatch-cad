import os, io, time, requests, threading, tempfile, json
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify
from flask_cors import CORS
from groq import Groq
from supabase import create_client
from flask import Response

clients = []

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
STREAM_URL   = os.environ.get("STREAM_URL")
CHUNK_SECONDS = 30

EASTERN = ZoneInfo("America/New_York")

groq_client = Groq(api_key=GROQ_API_KEY)
supabase    = create_client(SUPABASE_URL, SUPABASE_KEY)

app = Flask(__name__)
CORS(app)

@app.route("/ping")
def ping():
    return "pong", 200

@app.route("/incidents")
def get_incidents():
    res = supabase.table("incidents").select("*").order("created_at", desc=True).limit(100).execute()
    return jsonify(res.data)

def capture_chunk():
    try:
        resp = requests.get(STREAM_URL, stream=True, timeout=10)
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
        print(f"Capture error: {e}")
        return None

def transcribe(audio_bytes):
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

def parse_transcript(transcript):
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

def save_incident(parsed, transcript):
    try:
        row = {
            "incident_type": parsed.get("incident_type", "Unknown"),
            "location":      parsed.get("location", "Unknown"),
            "units":         parsed.get("units", []),
            "priority":      parsed.get("priority", "Unknown"),
            "notes":         parsed.get("notes", ""),
            "transcript":    transcript,
            "time_str":      datetime.now(EASTERN).strftime("%I:%M %p")
        }
        supabase.table("incidents").insert(row).execute()
        print(f"Saved: {row['incident_type']} @ {row['location']}")
    except Exception as e:
        print(f"Save error: {e}")

def scanner_loop():
    print("Scanner loop started...")
    while True:
        try:
            print("Capturing audio chunk...")
            audio = capture_chunk()
            if not audio:
                time.sleep(5)
                continue
            print("Transcribing...")
            transcript = transcribe(audio)
            print(f"Transcript: {transcript[:100] if transcript else 'empty'}")
            if len(transcript) < 15:
                print("Too short, skipping.")
                continue
            print("Parsing...")
            parsed = parse_transcript(transcript)
            if parsed:
                save_incident(parsed, transcript)
            else:
                print("No incident detected.")
        except Exception as e:
            print(f"Loop error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    thread = threading.Thread(target=scanner_loop, daemon=True)
    thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
@app.route("/stream")
def stream():
    def event_stream():
        last_id = None

        while True:
            try:
                # fetch latest incident only
                res = supabase.table("incidents") \
                    .select("*") \
                    .order("created_at", desc=True) \
                    .limit(1) \
                    .execute()

                if res.data:
                    latest = res.data[0]

                    if latest["id"] != last_id:
                        last_id = latest["id"]
                        yield f"data: {json.dumps(latest)}\n\n"

                time.sleep(2)

            except Exception as e:
                print("SSE error:", e)
                time.sleep(5)

    return Response(event_stream(), mimetype="text/event-stream")

@app.after_request
def add_headers(response):
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response
