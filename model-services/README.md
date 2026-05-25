# Model Services

These small FastAPI services adapt ModelScope/FunASR/CosyVoice style models to the Spring realtime gateway.

Recommended first stack:

- Streaming ASR partial: `iic/speech_paraformer_asr_nat-zh-cn-16k-common-vocab8404-online`
- Final ASR correction: `iic/speech_paraformer-large_asr_nat-zh-cn-16k-common-vocab8404-pytorch`, or swap this adapter for Qwen3-ASR when your GPU service is ready
- Streaming TTS: `iic/CosyVoice2-0.5B`
- LLM: any OpenAI-compatible streaming endpoint, for example DashScope Qwen

Run examples:

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -r model-services\requirements.txt
.\.venv\Scripts\uvicorn model-services.asr_funasr_server:app --host 127.0.0.1 --port 10095
.\.venv\Scripts\uvicorn model-services.tts_cosyvoice_server:app --host 127.0.0.1 --port 8002
$env:OPENAI_API_KEY="your-key"
.\.venv\Scripts\uvicorn model-services.llm_openai_compatible_server:app --host 127.0.0.1 --port 8001
```

Then set Spring config:

```yaml
realtime:
  asr:
    mode: http
    base-url: http://127.0.0.1:10095
  llm:
    mode: http
    base-url: http://127.0.0.1:8001
  tts:
    mode: http
    base-url: http://127.0.0.1:8002
```
