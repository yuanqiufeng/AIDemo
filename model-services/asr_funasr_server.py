import base64
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

try:
    from funasr import AutoModel
except Exception as error:  # pragma: no cover
    logger.warning("failed to import funasr AutoModel: %s", error)
    AutoModel = None


DEFAULT_ONLINE_MODEL = "D:/models/models/iic/speech_paraformer_asr_nat-zh-cn-16k-common-vocab8404-online"
ONLINE_MODEL = os.getenv("ASR_ONLINE_MODEL", os.getenv("ASR_MODEL", DEFAULT_ONLINE_MODEL))
FINAL_MODEL = os.getenv("ASR_FINAL_MODEL", os.getenv("ASR_MODEL", ONLINE_MODEL))
ASR_DEVICE = os.getenv("ASR_DEVICE", "cuda:0")
ASR_LANGUAGE = os.getenv("ASR_LANGUAGE", "zh")
MIN_FINAL_RMS = float(os.getenv("ASR_MIN_FINAL_RMS", "0.015"))

logger = logging.getLogger("aidemo.asr")


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
    cache: Dict = field(default_factory=dict)
    chunks: List[np.ndarray] = field(default_factory=list)
    pending: List[np.ndarray] = field(default_factory=list)
    pending_samples: int = 0


app = FastAPI(title="AIDemo FunASR Streaming ASR")
states: Dict[str, StreamState] = {}
online_model = None
final_model = None


@app.on_event("startup")
def load_models() -> None:
    global online_model, final_model
    if AutoModel is None:
        logger.warning("funasr AutoModel is unavailable; ASR service will return empty text")
        return
    logger.info("Loading ASR model: model=%s, online_model=%s, device=%s", FINAL_MODEL, ONLINE_MODEL or "-", ASR_DEVICE)
    if ONLINE_MODEL:
        online_model = AutoModel(model=ONLINE_MODEL, disable_update=True, device=ASR_DEVICE)
    final_model = AutoModel(
        model=FINAL_MODEL,
        disable_update=True,
        trust_remote_code=True,
        remote_code="./model.py",
        device=ASR_DEVICE,
    )
    logger.info("ASR model loaded")


@app.post("/asr/partial")
def partial(req: PartialRequest) -> dict:
    pcm = decode_pcm(req.audio)
    state = states.setdefault(req.sessionId or "default", StreamState())
    state.chunks.append(pcm)
    state.pending.append(pcm)
    state.pending_samples += len(pcm)

    if online_model is None:
        return {"text": ""}
    if state.pending_samples < 16000 * 0.48:
        return {"text": ""}

    streaming_chunk = np.concatenate(state.pending)
    state.pending.clear()
    state.pending_samples = 0
    result = online_model.generate(
        input=streaming_chunk,
        cache=state.cache,
        is_final=False,
        chunk_size=[0, 10, 5],
        encoder_chunk_look_back=4,
        decoder_chunk_look_back=1,
    )
    text = extract_text(result)
    if text:
        logger.info("ASR partial text: session=%s, text=%s", req.sessionId, text)
    return {"text": text}


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
        final_model is not None,
    )
    if rms < MIN_FINAL_RMS:
        logger.info("ASR final ignored as silence: session=%s, rms=%.4f < %.4f", req.sessionId, rms, MIN_FINAL_RMS)
        return {"text": ""}

    if final_model is None:
        logger.warning("ASR model not loaded; returning empty text instead of fake recognition")
        return {"text": ""}

    result = final_model.generate(input=pcm, cache={}, batch_size=1, language=ASR_LANGUAGE, itn=True)
    text = extract_text(result)
    logger.info("ASR final text: session=%s, text=%s", req.sessionId, text)
    return {"text": text}


def decode_pcm(audio: str) -> np.ndarray:
    raw = base64.b64decode(audio)
    return np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0


def extract_text(result) -> str:
    if isinstance(result, list) and result:
        item = result[0]
        if isinstance(item, dict):
            return item.get("text", "")
    if isinstance(result, dict):
        return result.get("text", "")
    return ""
