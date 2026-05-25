# AIDemo Realtime Voice

This demo upgrades the voice path from "record one sentence, then process" to a full-duplex streaming architecture:

```text
Android continuous capture + AEC/NS/AGC
-> Spring Boot realtime session state machine
-> streaming ASR partial/final
-> streaming LLM deltas
-> streaming TTS PCM chunks
-> Android AudioTrack playback while the mic keeps listening
-> barge-in interrupt cancels current LLM/TTS
```

## What Is Included

- `server/`: Spring Boot WebFlux WebSocket gateway at `/ws/realtime`
- `android/`: Android client using `AudioRecord`, platform AEC/NS/AGC, WebSocket PCM, and `AudioTrack`
- `model-services/`: FastAPI adapters for ModelScope/FunASR ASR, OpenAI-compatible LLM streaming, and CosyVoice TTS streaming
- `docs/realtime-architecture.md`: event protocol and next WebRTC/LiveKit step

The current transport is WebSocket PCM so the end-to-end loop is easy to run. The event protocol is already shaped so audio transport can later move to LiveKit/WebRTC RTP Opus without rewriting the AI orchestrator.

## Run The Server

Mock ASR/LLM/TTS is enabled by default:

```powershell
mvn -f server/pom.xml spring-boot:run
```

Then connect the Android app to:

```text
ws://10.0.2.2:8080/ws/realtime
```

Use `10.0.2.2` from the Android emulator. Use your computer LAN IP from a real phone.

## Use Real Models

Recommended first ModelScope stack:

- Streaming ASR partial: `iic/speech_paraformer_asr_nat-zh-cn-16k-common-vocab8404-online`
- Final ASR correction: `iic/speech_paraformer-large_asr_nat-zh-cn-16k-common-vocab8404-pytorch`, or replace with Qwen3-ASR final correction
- Streaming TTS: `iic/CosyVoice2-0.5B`
- LLM: any OpenAI-compatible streaming endpoint

Start the adapters from `model-services/README.md`, then switch `server/src/main/resources/application.yml` modes from `mock` to `http`.

## Verify

Server tests:

```powershell
mvn -f server/pom.xml test
```

Android should be opened with Android Studio or a modern Gradle wrapper. The system Gradle on this machine is `4.10.2` and currently fails before project configuration with `Failed to load native library 'native-platform.dll'`.

## References

- [Android AcousticEchoCanceler](https://developer.android.com/reference/android/media/audiofx/AcousticEchoCanceler)
- [FunASR](https://github.com/modelscope/FunASR)
- [CosyVoice](https://github.com/FunAudioLLM/CosyVoice)
- [Spring WebFlux WebSocket](https://docs.spring.io/spring-framework/reference/web/webflux-websocket.html)
