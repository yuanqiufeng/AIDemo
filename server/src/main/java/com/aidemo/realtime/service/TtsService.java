package com.aidemo.realtime.service;

import reactor.core.publisher.Flux;

public interface TtsService {
    Flux<TtsChunk> streamAudio(String sessionId, Flux<String> textDeltas);
}
