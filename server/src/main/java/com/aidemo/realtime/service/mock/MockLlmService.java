package com.aidemo.realtime.service.mock;

import com.aidemo.realtime.service.LlmService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;

@Service
@ConditionalOnProperty(prefix = "realtime.llm", name = "mode", havingValue = "mock", matchIfMissing = true)
public class MockLlmService implements LlmService {
    @Override
    public Flux<String> streamReply(String sessionId, String userText) {
        List<String> chunks = List.of(
                "收到，",
                "我会按实时语音链路处理：",
                "先识别你的语音，",
                "再一边生成回答一边合成音频。"
        );
        return Flux.fromIterable(chunks).delayElements(Duration.ofMillis(180));
    }
}
