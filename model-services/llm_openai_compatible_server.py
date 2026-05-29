import json
import logging
import os
import time
from typing import AsyncIterator

import httpx
from fastapi import FastAPI
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse


OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "deepseek-v4-pro")
CONNECT_TIMEOUT_SECONDS = float(os.getenv("LLM_CONNECT_TIMEOUT_SECONDS", "8"))
READ_TIMEOUT_SECONDS = float(os.getenv("LLM_READ_TIMEOUT_SECONDS", "12"))
FALLBACK_ON_ERROR = os.getenv("LLM_FALLBACK_ON_ERROR", "1") != "0"
SYSTEM_PROMPT = os.getenv(
    "LLM_SYSTEM_PROMPT",
    "\u4f60\u662f\u4e00\u4e2a\u4f4e\u5ef6\u8fdf\u4e2d\u6587\u8bed\u97f3\u52a9\u624b\u3002"
    "\u56de\u7b54\u5fc5\u987b\u7b80\u77ed\u3001\u81ea\u7136\u3001\u9002\u5408\u76f4\u63a5\u6717\u8bfb\uff0c"
    "\u4e00\u822c\u4e0d\u8d85\u8fc730\u4e2a\u5b57\uff0c\u6700\u591a\u4e00\u53e5\u8bdd\u3002",
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
        logger.warning("OPENAI_API_KEY is empty; returning local fallback response")
        for token in ["\u6211\u542c\u5230\u4e86\uff1a", req.text, "\u3002"]:
            yield {"data": json.dumps({"delta": token}, ensure_ascii=False)}
        return

    started = time.perf_counter()
    first_delta_sent = False
    payload = {
        "model": req.model or DEFAULT_MODEL,
        "stream": True,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": req.text},
        ],
    }
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    timeout = httpx.Timeout(
        connect=CONNECT_TIMEOUT_SECONDS,
        read=READ_TIMEOUT_SECONDS,
        write=10.0,
        pool=5.0,
    )
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream(
                "POST",
                f"{OPENAI_BASE_URL.rstrip('/')}/chat/completions",
                headers=headers,
                json=payload,
            ) as resp:
                logger.info("DeepSeek response status: session=%s, status=%s", req.sessionId, resp.status_code)
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
                        if not first_delta_sent:
                            first_delta_sent = True
                            logger.info(
                                "LLM first delta: session=%s, elapsed=%.2fs, delta=%s",
                                req.sessionId,
                                time.perf_counter() - started,
                                delta,
                            )
                        yield {"data": json.dumps({"delta": delta}, ensure_ascii=False)}
        logger.info("LLM stream complete: session=%s, elapsed=%.2fs", req.sessionId, time.perf_counter() - started)
    except httpx.TimeoutException:
        logger.exception("LLM stream timed out: session=%s, elapsed=%.2fs", req.sessionId, time.perf_counter() - started)
        if FALLBACK_ON_ERROR:
            yield {"data": json.dumps({"delta": "\u6211\u8fd9\u8fb9\u54cd\u5e94\u6709\u70b9\u6162\uff0c\u8bf7\u518d\u8bf4\u4e00\u904d\u3002"}, ensure_ascii=False)}
    except httpx.HTTPStatusError as error:
        logger.exception(
            "LLM upstream HTTP error: session=%s, status=%s, body=%s",
            req.sessionId,
            error.response.status_code,
            error.response.text[:500],
        )
        if FALLBACK_ON_ERROR:
            yield {"data": json.dumps({"delta": "\u5927\u6a21\u578b\u63a5\u53e3\u8fd4\u56de\u9519\u8bef\uff0c\u8bf7\u7a0d\u540e\u518d\u8bd5\u3002"}, ensure_ascii=False)}
    except Exception:
        logger.exception("LLM stream failed: session=%s, elapsed=%.2fs", req.sessionId, time.perf_counter() - started)
        if FALLBACK_ON_ERROR:
            yield {"data": json.dumps({"delta": "\u6211\u8fd9\u8fb9\u5904\u7406\u5931\u8d25\u4e86\uff0c\u8bf7\u518d\u8bd5\u4e00\u6b21\u3002"}, ensure_ascii=False)}
