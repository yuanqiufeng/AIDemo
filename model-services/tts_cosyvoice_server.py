import base64
import json
import logging
import math
import os
import sys
from typing import AsyncIterator

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

# Numba caching can fail in Windows uv-managed environments when librosa imports.
os.environ.setdefault("NUMBA_DISABLE_JIT", "1")

COSYVOICE_REPO = os.getenv("COSYVOICE_REPO", "D:/aidemo/AIDemo/model/CosyVoice-main")
for path in (COSYVOICE_REPO, os.path.join(COSYVOICE_REPO, "third_party", "Matcha-TTS")):
    if os.path.isdir(path) and path not in sys.path:
        sys.path.insert(0, path)

logger = logging.getLogger("aidemo.tts")

try:
    from cosyvoice.cli.cosyvoice import CosyVoice2
except Exception as error:  # pragma: no cover
    logger.warning("failed to import CosyVoice2: %s", error)
    CosyVoice2 = None


MODEL_DIR = os.getenv("COSYVOICE_MODEL_DIR", "D:/models/iic/CosyVoice2-0___5B")
SAMPLE_RATE = int(os.getenv("COSYVOICE_SAMPLE_RATE", "24000"))
SPEAKER_ID = os.getenv("COSYVOICE_SPEAKER_ID", "\u4e2d\u6587\u5973")

app = FastAPI(title="AIDemo CosyVoice Streaming TTS")
cosyvoice = None


class TtsRequest(BaseModel):
    sessionId: str | None = None
    text: str


@app.on_event("startup")
def load_model() -> None:
    global cosyvoice
    if CosyVoice2 is None:
        logger.warning("cosyvoice is not installed; TTS service will return fallback tones")
        return
    logger.info("Loading CosyVoice model: model_dir=%s, speaker=%s", MODEL_DIR, SPEAKER_ID)
    cosyvoice = CosyVoice2(MODEL_DIR, load_jit=False, load_trt=False, fp16=False)
    logger.info("CosyVoice model loaded")


@app.post("/tts/stream")
async def tts_stream(req: TtsRequest):
    logger.info("TTS request: session=%s, text=%s, model_loaded=%s", req.sessionId, req.text, cosyvoice is not None)
    return EventSourceResponse(generate_audio(req.text))


async def generate_audio(text: str) -> AsyncIterator[dict]:
    chunks = 0
    if cosyvoice is None:
        for chunk in fallback_tone(text):
            chunks += 1
            yield {"data": json.dumps({"audio": encode_pcm(chunk), "sampleRate": str(SAMPLE_RATE)})}
        logger.warning("TTS returned fallback tone chunks=%s", chunks)
        return

    for result in cosyvoice.inference_sft(text, SPEAKER_ID, stream=True):
        audio = result.get("tts_speech")
        if audio is None:
            continue
        pcm = tensor_to_pcm(audio)
        chunks += 1
        yield {"data": json.dumps({"audio": encode_pcm(pcm), "sampleRate": str(SAMPLE_RATE)})}
    logger.info("TTS returned audio chunks=%s", chunks)


def tensor_to_pcm(audio) -> bytes:
    array = audio.squeeze().detach().cpu().numpy()
    array = np.clip(array, -1.0, 1.0)
    return (array * 32767).astype(np.int16).tobytes()


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
