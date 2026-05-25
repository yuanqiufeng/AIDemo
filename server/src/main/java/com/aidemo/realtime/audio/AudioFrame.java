package com.aidemo.realtime.audio;

import java.time.Instant;

public record AudioFrame(
        long sequence,
        byte[] pcm16le,
        int sampleRate,
        int channels,
        Instant receivedAt
) {
}
