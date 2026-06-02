package com.aidemo.realtime.service.mock;

import com.aidemo.realtime.service.TtsChunk;
import com.aidemo.realtime.service.TtsService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
@ConditionalOnProperty(prefix = "realtime.tts", name = "mode", havingValue = "none")
public class NoopTtsService implements TtsService {
    @Override
    public Flux<TtsChunk> streamAudio(String sessionId, Flux<String> textDeltas) {
        return textDeltas.thenMany(Flux.empty());
    }
}
