"""
mdc_detect.py: MDC-1200 burst detection for the fire scanner server.
Wraps Matthew Kaufman's decoder library (GPL-2.0) in decoder/.

    detect_mdc_bursts(audio_bytes)  -> list of bursts with timestamps
    build_speaker_lines(segs, bursts) -> speaker-labeled transcript lines

Test from the command line:  python mdc_detect.py clip.mp3
"""
import ctypes
import platform
import subprocess
import sys
import threading
from pathlib import Path

import numpy as np

DECODER_DIR = Path(__file__).resolve().parent / "decoder"
MDC_SR = 16000          # decoder's four-point method wants 16 kHz or higher
BLOCK = 160             # 10 ms blocks: burst times accurate to ~10 ms
DEDUP_SECONDS = 0.8     # ignore an identical burst repeated this quickly
PTT_OP = 0x01
TOL = 0.35              # seconds of slack when matching speech to a burst


def unit_label(unit_id: int) -> str:
    return f"ID {unit_id:04X}"


# ── Library loading (built once, thread-safe) ────────────────────────────
CALLBACK = ctypes.CFUNCTYPE(
    None, ctypes.c_int, ctypes.c_ubyte, ctypes.c_ubyte, ctypes.c_ushort,
    ctypes.c_ubyte, ctypes.c_ubyte, ctypes.c_ubyte, ctypes.c_ubyte, ctypes.c_void_p
)

_lib = None
_lib_failed = False
_lib_lock = threading.Lock()

try:
    _libc = ctypes.CDLL(None)
    _libc.free.argtypes = [ctypes.c_void_p]
    _libc.free.restype = None
except Exception:
    _libc = None


def _build_lib() -> Path:
    system = platform.system()
    lib = DECODER_DIR / ("libmdcdecoder.dylib" if system == "Darwin" else "libmdcdecoder.so")
    src = DECODER_DIR / "mdc_decode.c"
    if lib.exists() and lib.stat().st_mtime >= src.stat().st_mtime:
        return lib
    flag = "-dynamiclib" if system == "Darwin" else "-shared"
    subprocess.run(["cc", flag, "-fPIC", "-O2", "-o", str(lib), str(src)],
                   check=True, capture_output=True, text=True)
    return lib


def _load():
    global _lib, _lib_failed
    if _lib is not None or _lib_failed:
        return _lib
    with _lib_lock:
        if _lib is None and not _lib_failed:
            try:
                lib = ctypes.CDLL(str(_build_lib()))
                lib.mdc_decoder_new.argtypes = [ctypes.c_int]
                lib.mdc_decoder_new.restype = ctypes.c_void_p
                lib.mdc_decoder_set_callback.argtypes = [ctypes.c_void_p, CALLBACK, ctypes.c_void_p]
                lib.mdc_decoder_set_callback.restype = ctypes.c_int
                lib.mdc_decoder_process_samples.argtypes = [
                    ctypes.c_void_p, ctypes.POINTER(ctypes.c_short), ctypes.c_int]
                lib.mdc_decoder_process_samples.restype = ctypes.c_int
                _lib = lib
            except Exception as e:
                _lib_failed = True   # don't retry (or spam logs) on every clip
                err = getattr(e, "stderr", "") or e
                print(f"[mdc] decoder unavailable, MDC labeling disabled: {err}", flush=True)
    return _lib


def _decode_pcm16(audio_bytes: bytes):
    try:
        r = subprocess.run(
            ["ffmpeg", "-loglevel", "error", "-i", "pipe:0",
             "-ac", "1", "-ar", str(MDC_SR), "-f", "s16le", "-"],
            input=audio_bytes, capture_output=True, timeout=30)
        if r.returncode != 0 or not r.stdout:
            return None
        return np.frombuffer(r.stdout, dtype="<i2").copy()
    except Exception as e:
        print(f"[mdc] pcm decode failed: {e}", flush=True)
        return None


# ── Detection ────────────────────────────────────────────────────────────
def detect_mdc_bursts(audio_bytes: bytes) -> list[dict]:
    """Returns [{t_start, t_end, unit_id, op, arg, pre, frames}, ...] sorted by time.
    t_end is when the packet finished decoding (seconds into the clip);
    t_start is an estimate (single packet is ~0.27s on air). Never raises."""
    try:
        lib = _load()
        if lib is None:
            return []
        pcm = _decode_pcm16(audio_bytes)
        if pcm is None or len(pcm) < MDC_SR * 0.3:
            return []

        found = []
        progress = {"n": 0}   # samples fed so far, including the current block

        @CALLBACK
        def on_packet(frames, op, arg, unit_id, e0, e1, e2, e3, ctx):
            t_end = progress["n"] / MDC_SR
            dur = 0.27 if frames == 1 else 0.37
            found.append({
                "t_start": max(0.0, t_end - dur),
                "t_end": t_end,
                "unit_id": int(unit_id),
                "op": int(op),
                "arg": int(arg),
                "pre": bool(arg & 0x80),   # 0x80 = PTT ID sent at key-up, else at key-down
                "frames": int(frames),
            })

        dec = lib.mdc_decoder_new(MDC_SR)
        if not dec:
            return []
        try:
            lib.mdc_decoder_set_callback(dec, on_packet, None)
            n = len(pcm)
            for start in range(0, n, BLOCK):
                end = min(start + BLOCK, n)
                progress["n"] = end
                ptr = pcm[start:end].ctypes.data_as(ctypes.POINTER(ctypes.c_short))
                lib.mdc_decoder_process_samples(dec, ptr, end - start)
        finally:
            if _libc is not None:
                _libc.free(dec)   # the library has no free function; avoids a leak per clip

        found.sort(key=lambda b: b["t_end"])
        out = []
        for b in found:
            if out and (b["unit_id"], b["op"], b["arg"]) == \
                       (out[-1]["unit_id"], out[-1]["op"], out[-1]["arg"]) \
                   and b["t_end"] - out[-1]["t_end"] < DEDUP_SECONDS:
                continue
            out.append(b)
        return out
    except Exception as e:
        print(f"[mdc] detect error: {e}", flush=True)
        return []


# ── Merge MDC bursts with Whisper segments ───────────────────────────────
def build_speaker_lines(segments: list[dict], bursts: list[dict]) -> list[dict]:
    """segments: [{"start","end","text"}] from Whisper (clip-relative seconds).
    Returns [{"t","speaker","unit_id","mdc_t","text"}], or [] when there is no
    PTT ID to go on (callers then keep the normal sentence splitting)."""
    ptt = [b for b in bursts if b["op"] == PTT_OP]
    segs = sorted((s for s in segments if (s.get("text") or "").strip()),
                  key=lambda s: s["start"])
    if not ptt or not segs:
        return []

    pre = [b for b in ptt if b["pre"]]
    post = [b for b in ptt if not b["pre"]]

    def speaker_at(mid):
        # Pre-ID: it starts a transmission and labels the speech that follows.
        prev_pre = max((b for b in pre if b["t_end"] <= mid + TOL),
                       key=lambda b: b["t_end"], default=None)
        prev_post = max((b for b in post if b["t_end"] <= mid),
                        key=lambda b: b["t_end"], default=None)
        if prev_pre and not (prev_post and prev_post["t_end"] > prev_pre["t_end"]):
            return prev_pre
        # Post-ID: it ends a transmission and labels the speech before it.
        next_post = min((b for b in post if b["t_start"] >= mid - TOL),
                        key=lambda b: b["t_start"], default=None)
        next_pre = min((b for b in pre if b["t_start"] > mid),
                       key=lambda b: b["t_start"], default=None)
        if next_post and not (next_pre and next_pre["t_start"] < next_post["t_start"]):
            return next_post
        return None

    lines = []
    for s in segs:
        b = speaker_at((s["start"] + s["end"]) / 2)
        text = s["text"].strip()
        key = b["t_end"] if b else None
        # Segments belonging to the same burst are one transmission: join them.
        if key is not None and lines and lines[-1]["_key"] == key:
            lines[-1]["text"] += " " + text
            continue
        lines.append({
            "_key": key,
            "t": round(s["start"], 2),
            "speaker": unit_label(b["unit_id"]) if b else None,
            "unit_id": f"{b['unit_id']:04X}" if b else None,
            "mdc_t": round(b["t_end"], 2) if b else None,
            "text": text,
        })
    for l in lines:
        l.pop("_key")
    return lines


def format_labeled_transcript(lines: list[dict]) -> str:
    """Builds the saved text: '[[|id]] words|||[[|id]] words'.
    The name field is left empty on purpose; the frontend looks it up from the ID.
    Returns '' when no line has a speaker."""
    if not any(l.get("speaker") for l in lines):
        return ""
    parts = []
    for l in lines:
        text = l["text"].replace("|||", " ").replace("[[", "(").replace("]]", ")")
        if l.get("speaker"):
            parts.append(f"[[|{l['unit_id']}]] {text}")
        else:
            parts.append(text)
    return "|||".join(parts)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python mdc_detect.py clip.mp3|clip.wav")
        sys.exit(1)
    bursts = detect_mdc_bursts(Path(sys.argv[1]).read_bytes())
    if not bursts:
        print("No MDC-1200 packets found.")
    for b in bursts:
        print(f"{b['t_end']:7.2f}s  op=0x{b['op']:02X} arg=0x{b['arg']:02X}  "
              f"id={b['unit_id']:04X}  {'PRE ' if b['pre'] else 'POST'}  frames={b['frames']}"
              f"  ({unit_label(b['unit_id'])})")
