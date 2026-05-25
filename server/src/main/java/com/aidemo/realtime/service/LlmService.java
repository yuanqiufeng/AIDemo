package com.aidemo.realtime.service;

import reactor.core.publisher.Flux;

public interface LlmService {
    Flux<String> streamReply(String sessionId, String userText);
}
