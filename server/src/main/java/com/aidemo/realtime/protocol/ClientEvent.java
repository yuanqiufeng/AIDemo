package com.aidemo.realtime.protocol;

public record ClientEvent(
        String type,
        String sessionId,
        String audio,
        Long seq,
        String text,
        Boolean bargeIn
) {
}
