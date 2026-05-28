import base64
import asyncio
import json
import logging
import math
import os
import sys
import time
from typing import AsyncIterator

import numpy as np
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
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
TTS_MODE = os.getenv("COSYVOICE_TTS_MODE", "zero_shot")
PROMPT_TEXT = os.getenv("COSYVOICE_PROMPT_TEXT", "\u5e0c\u671b\u4f60\u4ee5\u540e\u80fd\u591f\u505a\u7684\u6bd4\u6211\u8fd8\u597d\u5466\u3002")
PROMPT_WAV = os.getenv("COSYVOICE_PROMPT_WAV", os.path.join(COSYVOICE_REPO, "asset", "zero_shot_prompt.wav"))
INSTRUCT_TEXT = os.getenv("COSYVOICE_INSTRUCT_TEXT", "\u7528\u666e\u901a\u8bdd\u81ea\u7136\u5730\u8bf4\u8fd9\u53e5\u8bdd<|endofprompt|>")
SYNTH_TIMEOUT_SECONDS = float(os.getenv("COSYVOICE_SYNTH_TIMEOUT_SECONDS", "45"))

app = FastAPI(title="AIDemo CosyVoice Streaming TTS")
cosyvoice = None
tts_lock = asyncio.Lock()


class TtsRequest(BaseModel):
    sessionId: str | None = None
    text: str


@app.on_event("startup")
def load_model() -> None:
    global cosyvoice
    if CosyVoice2 is None:
        logger.warning("cosyvoice is not installed; TTS service will return fallback tones")
        return
    logger.info("Loading CosyVoice model: model_dir=%s, mode=%s, prompt_wav=%s", MODEL_DIR, TTS_MODE, PROMPT_WAV)
    cosyvoice = CosyVoice2(MODEL_DIR, load_jit=False, load_trt=False, fp16=False)
    logger.info("CosyVoice model loaded: sample_rate=%s", getattr(cosyvoice, "sample_rate", SAMPLE_RATE))


@app.post("/tts/stream")
async def tts_stream(req: TtsRequest):
    logger.info("TTS request: session=%s, text=%s, model_loaded=%s", req.sessionId, req.text, cosyvoice is not None)
    return EventSourceResponse(generate_audio(req.text))


@app.post("/tts")
async def tts_once(req: TtsRequest) -> dict:
    logger.info("TTS JSON request: session=%s, text=%s, model_loaded=%s", req.sessionId, req.text, cosyvoice is not None)
    chunks = []
    async for event in generate_audio(req.text):
        payload = json.loads(event["data"])
        chunks.append(payload)
    return {"chunks": chunks, "sampleRate": str(runtime_sample_rate())}


@app.post("/tts/ndjson")
async def tts_ndjson(req: TtsRequest):
    logger.info("TTS NDJSON request: session=%s, text=%s, model_loaded=%s", req.sessionId, req.text, cosyvoice is not None)
    return StreamingResponse(generate_audio_ndjson(req.text), media_type="application/x-ndjson")


async def generate_audio(text: str) -> AsyncIterator[dict]:
    async with tts_lock:
        async for event in generate_audio_locked(text):
            yield event


async def generate_audio_ndjson(text: str) -> AsyncIterator[bytes]:
    async with tts_lock:
        async for event in generate_audio_streaming_locked(text):
            yield (event["data"] + "\n").encode("utf-8")


async def generate_audio_locked(text: str) -> AsyncIterator[dict]:
    chunks = 0
    started = time.perf_counter()
    if cosyvoice is None:
        for chunk in fallback_tone(text):
            chunks += 1
            yield {"data": json.dumps({"audio": encode_pcm(chunk), "sampleRate": str(SAMPLE_RATE)})}
        logger.warning("TTS returned fallback tone chunks=%s", chunks)
        return

    try:
        results = await asyncio.wait_for(
            asyncio.to_thread(lambda: list(inference_results(text))),
            timeout=SYNTH_TIMEOUT_SECONDS,
        )
        for result in results:
            audio = result.get("tts_speech")
            if audio is None:
                continue
            pcm = tensor_to_pcm(audio)
            chunks += 1
            yield {"data": json.dumps({"audio": encode_pcm(pcm), "sampleRate": str(runtime_sample_rate())})}
            logger.info("TTS chunk ready: chunks=%s, elapsed=%.2fs", chunks, time.perf_counter() - started)
    except TimeoutError:
        logger.exception("TTS synthesis timed out after %.1fs; returning fallback tone", SYNTH_TIMEOUT_SECONDS)
        for chunk in fallback_tone(text):
            chunks += 1
            yield {"data": json.dumps({"audio": encode_pcm(chunk), "sampleRate": str(SAMPLE_RATE)})}
    except Exception:
        logger.exception("TTS synthesis failed; returning fallback tone")
        for chunk in fallback_tone(text):
            chunks += 1
            yield {"data": json.dumps({"audio": encode_pcm(chunk), "sampleRate": str(SAMPLE_RATE)})}
    logger.info("TTS returned audio chunks=%s, elapsed=%.2fs", chunks, time.perf_counter() - started)


async def generate_audio_streaming_locked(text: str) -> AsyncIterator[dict]:
    chunks = 0
    started = time.perf_counter()
    if cosyvoice is None:
        for chunk in fallback_tone(text):
            chunks += 1
            yield {"data": json.dumps({"audio": encode_pcm(chunk), "sampleRate": str(SAMPLE_RATE)})}
        logger.warning("TTS returned fallback tone chunks=%s", chunks)
        return

    try:
        queue: asyncio.Queue[dict | None] = asyncio.Queue()

        def worker() -> None:
            try:
                for result in inference_results(text):
                    audio = result.get("tts_speech")
                    if audio is None:
                        continue
                    pcm = tensor_to_pcm(audio)
                    queue.put_nowait({"audio": encode_pcm(pcm), "sampleRate": str(runtime_sample_rate())})
            except Exception as error:
                logger.exception("TTS streaming synthesis failed")
                queue.put_nowait({"error": str(error)})
            finally:
                queue.put_nowait(None)

        task = asyncio.create_task(asyncio.to_thread(worker))
        while True:
            item = await asyncio.wait_for(queue.get(), timeout=SYNTH_TIMEOUT_SECONDS)
            if item is None:
                break
            if "error" in item:
                for chunk in fallback_tone(text):
                    chunks += 1
                    yield {"data": json.dumps({"audio": encode_pcm(chunk), "sampleRate": str(SAMPLE_RATE)})}
                break
            chunks += 1
            logger.info("TTS stream chunk ready: chunks=%s, elapsed=%.2fs", chunks, time.perf_counter() - started)
            yield {"data": json.dumps(item)}
        await task
    except TimeoutError:
        logger.exception("TTS streaming timed out after %.1fs; returning fallback tone", SYNTH_TIMEOUT_SECONDS)
        for chunk in fallback_tone(text):
            chunks += 1
            yield {"data": json.dumps({"audio": encode_pcm(chunk), "sampleRate": str(SAMPLE_RATE)})}
    logger.info("TTS streaming returned audio chunks=%s, elapsed=%.2fs", chunks, time.perf_counter() - started)


def inference_results(text: str):
    if TTS_MODE == "sft":
        return cosyvoice.inference_sft(text, SPEAKER_ID, stream=True)
    if TTS_MODE == "instruct2":
        return cosyvoice.inference_instruct2(text, INSTRUCT_TEXT, PROMPT_WAV, stream=True)
    return cosyvoice.inference_zero_shot(text, PROMPT_TEXT, PROMPT_WAV, stream=True)


def runtime_sample_rate() -> int:
    return int(getattr(cosyvoice, "sample_rate", SAMPLE_RATE) or SAMPLE_RATE)


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
