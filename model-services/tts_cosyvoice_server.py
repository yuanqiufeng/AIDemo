import base64
import json
import math
from typing import AsyncIterator

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

try:
    from cosyvoice.cli.cosyvoice import CosyVoice2
except Exception:  # pragma: no cover
    CosyVoice2 = None


MODEL_DIR = "iic/CosyVoice2-0.5B"
SAMPLE_RATE = 24000
app = FastAPI(title="AIDemo CosyVoice Streaming TTS")
cosyvoice = None


class TtsRequest(BaseModel):
    sessionId: str | None = None
    text: str


@app.on_event("startup")
def load_model() -> None:
    global cosyvoice
    if CosyVoice2 is not None:
        cosyvoice = CosyVoice2(MODEL_DIR, load_jit=False, load_trt=False, fp16=False)


@app.post("/tts/stream")
async def tts_stream(req: TtsRequest):
    return EventSourceResponse(generate_audio(req.text))


async def generate_audio(text: str) -> AsyncIterator[dict]:
    if cosyvoice is None:
        for chunk in fallback_tone(text):
            yield {"data": json.dumps({"audio": encode_pcm(chunk), "sampleRate": str(SAMPLE_RATE)})}
        return

    for result in cosyvoice.inference_sft(text, "中文女", stream=True):
        audio = result.get("tts_speech")
        if audio is None:
            continue
        pcm = tensor_to_pcm(audio)
        yield {"data": json.dumps({"audio": encode_pcm(pcm), "sampleRate": str(SAMPLE_RATE)})}


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
            samples.append(math.sin(2 * math.pi * 520 * t) * 0.08)
        chunks.append((np.array(samples) * 32767).astype(np.int16).tobytes())
    return chunks
