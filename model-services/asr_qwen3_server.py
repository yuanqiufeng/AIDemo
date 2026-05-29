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
MIN_FINAL_RMS = float(os.getenv("ASR_MIN_FINAL_RMS", "0.018"))
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
        "Loading Qwen3-ASR: model=%s, device=%s, dtype=%s, language=%s",
        MODEL_DIR,
        device_map,
        dtype,
        ASR_LANGUAGE,
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

    rms = float(np.sqrt(np.mean(np.square(pcm)))) if len(pcm) else 0.0
    seconds = len(pcm) / 16000
    logger.info(
        "ASR final request: session=%s, seconds=%.2f, rms=%.4f, model_loaded=%s",
        req.sessionId,
        seconds,
        rms,
        model is not None,
    )
    if rms < MIN_FINAL_RMS:
        logger.info("ASR final ignored as silence: session=%s, rms=%.4f < %.4f", req.sessionId, rms, MIN_FINAL_RMS)
        return {"text": ""}
    if model is None:
        logger.warning("Qwen3-ASR model not loaded; returning empty text")
        return {"text": ""}

    started = time.perf_counter()
    text = transcribe_pcm(pcm, 16000)
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    logger.info("ASR final text: session=%s, elapsedMs=%s, text=%s", req.sessionId, elapsed_ms, text)
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
