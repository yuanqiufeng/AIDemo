package com.aidemo.realtime.protocol;

import com.fasterxml.jackson.annotation.JsonValue;

public enum EventType {
    SESSION_READY("session.ready"),
    AUDIO_START("audio.start"),
    AUDIO_CHUNK("audio.chunk"),
    AUDIO_END("audio.end"),
    VAD_SPEECH_START("vad.speech_start"),
    VAD_SPEECH_END("vad.speech_end"),
    ASR_PARTIAL("asr.partial"),
    ASR_FINAL("asr.final"),
    LLM_DELTA("llm.delta"),
    LLM_DONE("llm.done"),
    TTS_START("tts.start"),
    TTS_CHUNK("tts.chunk"),
    TTS_DONE("tts.done"),
    INTERRUPT("interrupt"),
    ERROR("error");

    private final String wireName;

    EventType(String wireName) {
        this.wireName = wireName;
    }

    @JsonValue
    public String wireName() {
        return wireName;
    }
}
