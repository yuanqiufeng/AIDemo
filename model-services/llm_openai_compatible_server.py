import json
import logging
import os
from typing import AsyncIterator

import httpx
from fastapi import FastAPI
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse


OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "deepseek-v4-pro")
SYSTEM_PROMPT = os.getenv(
    "LLM_SYSTEM_PROMPT",
    "\u4f60\u662f\u4e00\u4e2a\u4f4e\u5ef6\u8fdf\u4e2d\u6587\u8bed\u97f3\u52a9\u624b\u3002"
    "\u56de\u7b54\u5fc5\u987b\u7b80\u77ed\u3001\u81ea\u7136\u3001\u9002\u5408\u76f4\u63a5\u6717\u8bfb\uff0c"
    "\u4e00\u822c\u4e0d\u8d85\u8fc750\u4e2a\u5b57\uff0c\u6700\u591a\u4e24\u53e5\u8bdd\u3002",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger("aidemo.llm")

app = FastAPI(title="AIDemo OpenAI Compatible LLM Stream")


class LlmRequest(BaseModel):
    sessionId: str
    model: str | None = None
    text: str


@app.post("/llm/stream")
async def llm_stream(req: LlmRequest):
    logger.info("LLM request: session=%s, model=%s, text=%s", req.sessionId, req.model or DEFAULT_MODEL, req.text)
    return EventSourceResponse(stream_openai(req))


async def stream_openai(req: LlmRequest) -> AsyncIterator[dict]:
    if not OPENAI_API_KEY:
        for token in ["\u6211\u542c\u5230\u4e86\uff1a", req.text, "\u3002"]:
            yield {"data": json.dumps({"delta": token}, ensure_ascii=False)}
        return

    payload = {
        "model": req.model or DEFAULT_MODEL,
        "stream": True,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": req.text},
        ],
    }
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    async with httpx.AsyncClient(timeout=None) as client:
        async with client.stream(
            "POST",
            f"{OPENAI_BASE_URL.rstrip('/')}/chat/completions",
            headers=headers,
            json=payload,
        ) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    continue
                data = line.removeprefix("data: ").strip()
                if data == "[DONE]":
                    break

                chunk = json.loads(data)
                delta = chunk["choices"][0].get("delta", {}).get("content")
                if delta:
                    yield {"data": json.dumps({"delta": delta}, ensure_ascii=False)}
