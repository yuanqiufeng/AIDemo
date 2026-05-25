package com.aidemo.realtime.service.mock;

import com.aidemo.realtime.audio.PcmAudio;
import com.aidemo.realtime.config.RealtimeProperties;
import com.aidemo.realtime.service.TtsChunk;
import com.aidemo.realtime.service.TtsService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@Service
@ConditionalOnProperty(prefix = "realtime.tts", name = "mode", havingValue = "mock", matchIfMissing = true)
public class MockTtsService implements TtsService {
    private final RealtimeProperties properties;

    public MockTtsService(RealtimeProperties properties) {
        this.properties = properties;
    }

    @Override
    public Flux<TtsChunk> streamAudio(String sessionId, Flux<String> textDeltas) {
        AtomicLong sequence = new AtomicLong();
        int sampleRate = properties.tts().sampleRate();
        return textDeltas.concatMap(delta -> Flux
                .range(0, Math.max(2, delta.length() / 3))
                .map(i -> {
                    double tone = 440.0 + (delta.charAt(0) % 10) * 28.0;
                    return new TtsChunk(sequence.incrementAndGet(), PcmAudio.sine16le(tone, sampleRate, 80, 0.12), sampleRate);
                })
                .delayElements(Duration.ofMillis(40)));
    }
}
