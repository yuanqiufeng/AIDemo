import json
import os
from typing import AsyncIterator

import httpx
from fastapi import FastAPI
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse


OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "deepseek-v4-pro")

app = FastAPI(title="AIDemo OpenAI Compatible LLM Stream")


class LlmRequest(BaseModel):
    sessionId: str
    model: str | None = None
    text: str


@app.post("/llm/stream")
async def llm_stream(req: LlmRequest):
    return EventSourceResponse(stream_openai(req))


async def stream_openai(req: LlmRequest) -> AsyncIterator[dict]:
    if not OPENAI_API_KEY:
        for token in ["我收到你说：", req.text, "。现在以模拟模式流式回复。"]:
            yield {"data": json.dumps({"delta": token}, ensure_ascii=False)}
        return

    payload = {
        "model": req.model or DEFAULT_MODEL,
        "stream": True,
        "messages": [
            {
                "role": "system",
                "content": "你是一个低延迟中文语音助手。回答要简短、自然，适合直接朗读。",
            },
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
