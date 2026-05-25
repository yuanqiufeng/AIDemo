package com.aidemo.realtime.service.mock;

import com.aidemo.realtime.audio.AudioFrame;
import com.aidemo.realtime.service.AsrService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@ConditionalOnProperty(prefix = "realtime.asr", name = "mode", havingValue = "mock", matchIfMissing = true)
public class MockAsrService implements AsrService {
    private final AtomicInteger partialCounter = new AtomicInteger();

    @Override
    public Flux<String> partial(String sessionId, AudioFrame frame) {
        int count = partialCounter.incrementAndGet();
        if (count % 8 != 0) {
            return Flux.empty();
        }
        return Flux.just("我正在听...");
    }

    @Override
    public Mono<String> finalText(String sessionId, List<AudioFrame> utteranceFrames) {
        int seconds = Math.max(1, utteranceFrames.size() / 50);
        return Mono.just("这是一段约 " + seconds + " 秒的语音");
    }
}
