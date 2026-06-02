import base64
import logging
import os
import tempfile
import time
import wave
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger("aidemo.asr.qwen3")

try:
    import torch
    from qwen_asr import Qwen3ASRModel
except Exception as error:  # pragma: no cover
    logger.warning("failed to import qwen_asr runtime: %s", error)
    torch = None
    Qwen3ASRModel = None


MODEL_DIR = os.getenv("QWEN3_ASR_MODEL", os.getenv("ASR_MODEL", "D:/models/models/Qwen/Qwen3-ASR-1___7B"))
ASR_DEVICE = os.getenv("ASR_DEVICE", "cuda:0")
ASR_DTYPE = os.getenv("QWEN3_ASR_DTYPE", "bfloat16")
ASR_LANGUAGE = os.getenv("ASR_LANGUAGE", "Chinese")
MIN_FINAL_RMS = float(os.getenv("ASR_MIN_FINAL_RMS", "0.004"))
TRIM_RMS = float(os.getenv("ASR_TRIM_RMS", "0.004"))
TARGET_RMS = float(os.getenv("ASR_TARGET_RMS", "0.055"))
MAX_GAIN = float(os.getenv("ASR_MAX_GAIN", "8.0"))
DEBUG_DIR = os.getenv("ASR_DEBUG_DIR", "").strip()
MAX_NEW_TOKENS = int(os.getenv("QWEN3_ASR_MAX_NEW_TOKENS", "256"))
WARMUP_ENABLED = os.getenv("QWEN3_ASR_WARMUP", "1") != "0"


class PartialRequest(BaseModel):
    seq: int
    sampleRate: int = 16000
    audio: str
    sessionId: str | None = None


class FinalRequest(BaseModel):
    audio: str
    sessionId: str | None = None


@dataclass
class StreamState:
    chunks: List[np.ndarray] = field(default_factory=list)


app = FastAPI(title="AIDemo Qwen3 ASR")
states: Dict[str, StreamState] = {}
model = None


@app.on_event("startup")
def load_model() -> None:
    global model
    if Qwen3ASRModel is None or torch is None:
        logger.warning("qwen-asr is not installed; ASR service will return empty text")
        return
    device_map = ASR_DEVICE if torch.cuda.is_available() and ASR_DEVICE.startswith("cuda") else "cpu"
    dtype = torch.bfloat16 if ASR_DTYPE == "bfloat16" and device_map != "cpu" else torch.float32
    logger.info(
        "Loading Qwen3-ASR: model=%s, device=%s, dtype=%s, language=%s, minFinalRms=%.4f, trimRms=%.4f, targetRms=%.4f, maxGain=%.1f",
        MODEL_DIR,
        device_map,
        dtype,
        ASR_LANGUAGE,
        MIN_FINAL_RMS,
        TRIM_RMS,
        TARGET_RMS,
        MAX_GAIN,
    )
    model = Qwen3ASRModel.from_pretrained(
        MODEL_DIR,
        dtype=dtype,
        device_map=device_map,
        max_inference_batch_size=1,
        max_new_tokens=MAX_NEW_TOKENS,
    )
    logger.info("Qwen3-ASR model loaded")
    warmup_model()


@app.post("/asr/partial")
def partial(req: PartialRequest) -> dict:
    pcm = decode_pcm(req.audio)
    state = states.setdefault(req.sessionId or "default", StreamState())
    state.chunks.append(pcm)
    return {"text": ""}


@app.post("/asr/final")
def final(req: FinalRequest) -> dict:
    state = states.pop(req.sessionId or "default", None)
    if state and state.chunks:
        pcm = np.concatenate(state.chunks)
    else:
        pcm = decode_pcm(req.audio)

    rms = pcm_rms(pcm)
    peak = pcm_peak(pcm)
    seconds = len(pcm) / 16000
    logger.info(
        "ASR final request: session=%s, seconds=%.2f, rms=%.4f, peak=%.4f, model_loaded=%s",
        req.sessionId,
        seconds,
        rms,
        peak,
        model is not None,
    )
    if rms < MIN_FINAL_RMS:
        logger.info("ASR final ignored as silence: session=%s, rms=%.4f < %.4f", req.sessionId, rms, MIN_FINAL_RMS)
        return {"text": ""}
    if model is None:
        logger.warning("Qwen3-ASR model not loaded; returning empty text")
        return {"text": ""}

    prepared, prep = prepare_for_asr(pcm)
    logger.info(
        "ASR prepared audio: session=%s, seconds=%.2f, rms=%.4f, peak=%.4f, gain=%.2f, trim=%s",
        req.sessionId,
        len(prepared) / 16000,
        pcm_rms(prepared),
        pcm_peak(prepared),
        prep["gain"],
        prep["trimmed"],
    )

    started = time.perf_counter()
    text = transcribe_pcm(prepared, 16000)
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    logger.info("ASR final text: session=%s, elapsedMs=%s, text=%s", req.sessionId, elapsed_ms, text)
    if not text:
        save_debug_audio(req.sessionId, pcm, prepared)
    return {"text": text, "elapsedMs": elapsed_ms}


def decode_pcm(audio: str) -> np.ndarray:
    raw = base64.b64decode(audio)
    return np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0


def transcribe_pcm(pcm: np.ndarray, sample_rate: int) -> str:
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        write_pcm16_wav(tmp_path, pcm, sample_rate)
        language_value = None if ASR_LANGUAGE.lower() in ("", "auto", "none") else ASR_LANGUAGE
        result = model.transcribe(audio=str(tmp_path), language=language_value)
        return extract_text(result).strip()
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


def write_pcm16_wav(path: Path, pcm: np.ndarray, sample_rate: int) -> None:
    pcm16 = np.clip(pcm, -1.0, 1.0)
    raw = (pcm16 * 32767).astype(np.int16).tobytes()
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        wav.writeframes(raw)


def pcm_rms(pcm: np.ndarray) -> float:
    return float(np.sqrt(np.mean(np.square(pcm)))) if len(pcm) else 0.0


def pcm_peak(pcm: np.ndarray) -> float:
    return float(np.max(np.abs(pcm))) if len(pcm) else 0.0


def prepare_for_asr(pcm: np.ndarray) -> tuple[np.ndarray, dict]:
    prepared = trim_silence(pcm)
    trimmed = len(prepared) != len(pcm)
    if len(prepared) < int(16000 * 0.25):
        prepared = pcm
        trimmed = False

    rms = pcm_rms(prepared)
    peak = pcm_peak(prepared)
    gain = 1.0
    if rms > 0 and TARGET_RMS > 0:
        gain = min(MAX_GAIN, TARGET_RMS / rms)
        if peak * gain > 0.98:
            gain = 0.98 / peak if peak > 0 else 1.0
    if gain > 1.05:
        prepared = np.clip(prepared * gain, -1.0, 1.0).astype(np.float32)
    return prepared, {"gain": gain, "trimmed": trimmed}


def trim_silence(pcm: np.ndarray) -> np.ndarray:
    if len(pcm) == 0:
        return pcm
    frame_samples = int(16000 * 0.02)
    pad_samples = int(16000 * 0.20)
    if len(pcm) <= frame_samples:
        return pcm

    voiced = []
    for start in range(0, len(pcm), frame_samples):
        frame = pcm[start:start + frame_samples]
        if len(frame) == 0:
            continue
        if pcm_rms(frame) >= TRIM_RMS:
            voiced.append((start, min(start + frame_samples, len(pcm))))
    if not voiced:
        return pcm

    start = max(0, voiced[0][0] - pad_samples)
    end = min(len(pcm), voiced[-1][1] + pad_samples)
    return pcm[start:end]


def save_debug_audio(session_id: str | None, raw: np.ndarray, prepared: np.ndarray) -> None:
    if not DEBUG_DIR:
        return
    try:
        debug_dir = Path(DEBUG_DIR)
        debug_dir.mkdir(parents=True, exist_ok=True)
        safe_session = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in (session_id or "default"))
        stamp = int(time.time() * 1000)
        write_pcm16_wav(debug_dir / f"{safe_session}_{stamp}_raw.wav", raw, 16000)
        write_pcm16_wav(debug_dir / f"{safe_session}_{stamp}_prepared.wav", prepared, 16000)
        logger.info("ASR debug audio saved: session=%s, dir=%s", session_id, debug_dir)
    except Exception:
        logger.exception("Failed to save ASR debug audio")


def warmup_model() -> None:
    if not WARMUP_ENABLED or model is None:
        return
    started = time.perf_counter()
    samples = int(16000 * 0.8)
    t = np.arange(samples, dtype=np.float32) / 16000
    pcm = np.sin(2 * np.pi * 440 * t).astype(np.float32) * 0.08
    try:
        text = transcribe_pcm(pcm, 16000)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        logger.info("ASR warmup complete: elapsedMs=%s, text=%s", elapsed_ms, text)
    except Exception:
        logger.exception("ASR warmup failed; continuing anyway")


def extract_text(result) -> str:
    if isinstance(result, list) and result:
        item = result[0]
        return str(getattr(item, "text", "") or item.get("text", "") if isinstance(item, dict) else getattr(item, "text", ""))
    if isinstance(result, dict):
        return str(result.get("text", ""))
    return str(getattr(result, "text", "") or "")
