package com.aidemo.realtime.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ServerEvent(
        EventType type,
        String sessionId,
        Long seq,
        String text,
        Boolean isFinal,
        String audio,
        Integer sampleRate,
        String reason,
        Map<String, Object> meta
) {
    public static ServerEvent of(EventType type, String sessionId) {
        return new ServerEvent(type, sessionId, null, null, null, null, null, null, null);
    }

    public static ServerEvent text(EventType type, String sessionId, String text, boolean isFinal) {
        return new ServerEvent(type, sessionId, null, text, isFinal, null, null, null, null);
    }

    public static ServerEvent audio(EventType type, String sessionId, long seq, String audio, int sampleRate) {
        return new ServerEvent(type, sessionId, seq, null, null, audio, sampleRate, null, null);
    }

    public static ServerEvent error(String sessionId, String reason) {
        return new ServerEvent(EventType.ERROR, sessionId, null, null, null, null, null, reason, null);
    }
}
