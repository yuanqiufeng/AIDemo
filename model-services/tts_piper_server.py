import asyncio
import base64
import json
import logging
import math
import os
import time
from typing import AsyncIterator

import numpy as np
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger("aidemo.tts.piper")

MODEL_DIR = os.getenv("PIPER_MODEL_DIR", "D:/models/models/Trelis/piper-zh-cn-huayan-medium")
MODEL_PATH = os.getenv("PIPER_MODEL_PATH", os.path.join(MODEL_DIR, "model.onnx"))
CONFIG_PATH = os.getenv("PIPER_CONFIG_PATH", os.path.join(MODEL_DIR, "model.onnx.json"))
SAMPLE_RATE = int(os.getenv("PIPER_SAMPLE_RATE", "22050"))
SYNTH_TIMEOUT_SECONDS = float(os.getenv("PIPER_SYNTH_TIMEOUT_SECONDS", "20"))
MAX_CHARS_PER_REQUEST = int(os.getenv("PIPER_MAX_CHARS", "80"))
OUTBOUND_CHUNK_MS = int(os.getenv("PIPER_OUTBOUND_CHUNK_MS", "360"))
WARMUP_ENABLED = os.getenv("PIPER_WARMUP", "1") != "0"
WARMUP_TEXT = os.getenv("PIPER_WARMUP_TEXT", "\u4f60\u597d\u3002")

app = FastAPI(title="AIDemo Piper Streaming TTS")
voice = None
tts_lock = asyncio.Lock()


class TtsRequest(BaseModel):
    sessionId: str | None = None
    text: str


@app.on_event("startup")
def load_model() -> None:
    global voice
    try:
        from piper import PiperVoice

        logger.info("Loading Piper voice: model=%s config=%s", MODEL_PATH, CONFIG_PATH)
        voice = PiperVoice.load(MODEL_PATH, config_path=CONFIG_PATH)
        logger.info("Piper voice loaded: sample_rate=%s", runtime_sample_rate())
        warmup_model()
    except Exception:
        logger.exception("Failed to load Piper voice; service will return fallback tones")
        voice = None


@app.get("/")
def health() -> dict:
    return {
        "status": "ok" if voice is not None else "fallback",
        "provider": "piper",
        "model": MODEL_PATH,
        "sampleRate": runtime_sample_rate(),
    }


@app.post("/tts/ndjson")
async def tts_ndjson(req: TtsRequest):
    logger.info("Piper TTS request: session=%s, text=%s, model_loaded=%s", req.sessionId, req.text, voice is not None)
    return StreamingResponse(generate_audio_ndjson(req.text), media_type="application/x-ndjson")


@app.post("/tts")
async def tts_once(req: TtsRequest) -> dict:
    chunks = []
    async for event in generate_audio_ndjson(req.text):
        chunks.append(json.loads(event.decode("utf-8")))
    return {"chunks": chunks, "sampleRate": str(runtime_sample_rate())}


async def generate_audio_ndjson(text: str) -> AsyncIterator[bytes]:
    async with tts_lock:
        started = time.perf_counter()
        chunks = 0
        for segment in split_text(text):
            async for item in synthesize_segment(segment):
                chunks += 1
                logger.info("Piper TTS chunk ready: chunks=%s, elapsed=%.2fs", chunks, time.perf_counter() - started)
                yield (json.dumps(item) + "\n").encode("utf-8")
        logger.info("Piper TTS complete: chunks=%s, elapsed=%.2fs", chunks, time.perf_counter() - started)


async def synthesize_segment(text: str) -> AsyncIterator[dict]:
    if voice is None:
        for pcm in fallback_tone(text):
            yield {"audio": encode_pcm(pcm), "sampleRate": str(SAMPLE_RATE)}
        return
    try:
        pcm = await asyncio.wait_for(asyncio.to_thread(synthesize_pcm, text), timeout=SYNTH_TIMEOUT_SECONDS)
        for chunk in split_pcm(pcm, runtime_sample_rate()):
            yield {"audio": encode_pcm(chunk), "sampleRate": str(runtime_sample_rate())}
    except TimeoutError:
        logger.exception("Piper synthesis timed out after %.1fs; returning fallback tone", SYNTH_TIMEOUT_SECONDS)
        for pcm in fallback_tone(text):
            yield {"audio": encode_pcm(pcm), "sampleRate": str(SAMPLE_RATE)}
    except Exception:
        logger.exception("Piper synthesis failed; returning fallback tone")
        for pcm in fallback_tone(text):
            yield {"audio": encode_pcm(pcm), "sampleRate": str(SAMPLE_RATE)}


def synthesize_pcm(text: str) -> bytes:
    chunks = []
    for audio_chunk in voice.synthesize(text):
        pcm = getattr(audio_chunk, "audio_int16_bytes", None)
        if pcm:
            chunks.append(bytes(pcm))
            continue
        audio = np.asarray(audio_chunk.audio_float_array, dtype=np.float32)
        audio = np.clip(audio, -1.0, 1.0)
        chunks.append((audio * 32767).astype(np.int16).tobytes())
    return b"".join(chunks)


def split_text(text: str) -> list[str]:
    cleaned = (text or "").strip()
    if not cleaned:
        return []
    segments: list[str] = []
    current: list[str] = []
    for char in cleaned:
        current.append(char)
        if is_break(char) or len(current) >= MAX_CHARS_PER_REQUEST:
            segment = "".join(current).strip()
            if segment:
                segments.append(segment)
            current.clear()
    tail = "".join(current).strip()
    if tail:
        segments.append(tail)
    return segments


def is_break(char: str) -> bool:
    return char in "\u3002\uff01\uff1f\uff1b;!?\n"


def split_pcm(pcm: bytes, sample_rate: int) -> list[bytes]:
    chunk_bytes = max(2, sample_rate * OUTBOUND_CHUNK_MS // 1000 * 2)
    return [pcm[start:start + chunk_bytes] for start in range(0, len(pcm), chunk_bytes) if pcm[start:start + chunk_bytes]]


def runtime_sample_rate() -> int:
    if voice is not None:
        config = getattr(voice, "config", None)
        sample_rate = getattr(config, "sample_rate", None)
        if sample_rate:
            return int(sample_rate)
    return SAMPLE_RATE


def encode_pcm(pcm: bytes) -> str:
    return base64.b64encode(pcm).decode("ascii")


def fallback_tone(text: str) -> list[bytes]:
    chunks = []
    duration_ms = max(120, min(720, len(text) * 60))
    frames = SAMPLE_RATE * duration_ms // 1000
    for start in range(0, frames, SAMPLE_RATE // 10):
        count = min(SAMPLE_RATE // 10, frames - start)
        samples = []
        for i in range(count):
            t = (start + i) / SAMPLE_RATE
            samples.append(math.sin(2 * math.pi * 520 * t) * 0.25)
        chunks.append((np.array(samples) * 32767).astype(np.int16).tobytes())
    return chunks


def warmup_model() -> None:
    if not WARMUP_ENABLED or voice is None:
        return
    started = time.perf_counter()
    try:
        pcm = synthesize_pcm(WARMUP_TEXT)
        logger.info("Piper warmup complete: bytes=%s, elapsed=%.2fs", len(pcm), time.perf_counter() - started)
    except Exception:
        logger.exception("Piper warmup failed; continuing anyway")
