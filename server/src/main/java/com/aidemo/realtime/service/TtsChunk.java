package com.aidemo.realtime.service;

public record TtsChunk(
        long sequence,
        byte[] pcm16le,
        int sampleRate
) {
}
