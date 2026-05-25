package com.aidemo.realtime.service;

import com.aidemo.realtime.audio.AudioFrame;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public interface AsrService {
    Flux<String> partial(String sessionId, AudioFrame frame);

    Mono<String> finalText(String sessionId, List<AudioFrame> utteranceFrames);
}
