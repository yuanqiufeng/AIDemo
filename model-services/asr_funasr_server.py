import base64
import os
from dataclasses import dataclass, field
from typing import Dict, List

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel

try:
    from funasr import AutoModel
except Exception:  # pragma: no cover - keeps the server importable before deps are installed.
    AutoModel = None


ONLINE_MODEL = os.getenv("ASR_ONLINE_MODEL", "")
FINAL_MODEL = os.getenv("ASR_FINAL_MODEL", os.getenv("ASR_MODEL", "D:/models/FunAudioLLM/Fun-ASR-Nano-2512"))
ASR_DEVICE = os.getenv("ASR_DEVICE", "cuda:0")
ASR_LANGUAGE = os.getenv("ASR_LANGUAGE", "zh")


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
        return
    if ONLINE_MODEL:
        online_model = AutoModel(model=ONLINE_MODEL, disable_update=True, device=ASR_DEVICE)
    final_model = AutoModel(
        model=FINAL_MODEL,
        disable_update=True,
        trust_remote_code=True,
        remote_code="./model.py",
        device=ASR_DEVICE,
    )


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
    return {"text": extract_text(result)}


@app.post("/asr/final")
def final(req: FinalRequest) -> dict:
    state = states.pop(req.sessionId or "default", None)
    if state and state.chunks:
        pcm = np.concatenate(state.chunks)
    else:
        pcm = decode_pcm(req.audio)

    if final_model is None:
        seconds = max(1, int(len(pcm) / 16000))
        return {"text": f"收到一段约 {seconds} 秒的语音"}

    result = final_model.generate(input=pcm, cache={}, batch_size=1, language=ASR_LANGUAGE, itn=True)
    return {"text": extract_text(result)}


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
