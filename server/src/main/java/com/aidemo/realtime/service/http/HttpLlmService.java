package com.aidemo.realtime.service.http;

import com.aidemo.realtime.config.RealtimeProperties;
import com.aidemo.realtime.service.LlmService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@ConditionalOnProperty(prefix = "realtime.llm", name = "mode", havingValue = "http")
public class HttpLlmService implements LlmService {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HttpLlmService.class);
    private static final String LLM_SLOW_FALLBACK = "\u6211\u8fd9\u8fb9\u54cd\u5e94\u6709\u70b9\u6162\uff0c\u8bf7\u518d\u8bf4\u4e00\u904d\u3002";
    private static final String LLM_ERROR_FALLBACK = "\u6211\u8fd9\u8fb9\u5904\u7406\u5931\u8d25\u4e86\uff0c\u8bf7\u518d\u8bd5\u4e00\u6b21\u3002";

    private final WebClient webClient;
    private final RealtimeProperties properties;
    private final ObjectMapper objectMapper;

    public HttpLlmService(WebClient.Builder builder, RealtimeProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.webClient = builder.baseUrl(properties.llm().baseUrl()).build();
    }

    @Override
    public Flux<String> streamReply(String sessionId, String userText) {
        Instant startedAt = Instant.now();
        AtomicBoolean firstDeltaSeen = new AtomicBoolean(false);
        AtomicInteger chars = new AtomicInteger();
        log.info("LLM stream start: session={}, baseUrl={}, model={}, text={}",
                sessionId, properties.llm().baseUrl(), properties.llm().model(), userText);
        return webClient.post()
                .uri("/llm/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(Map.of(
                        "sessionId", sessionId,
                        "model", properties.llm().model(),
                        "text", userText
                ))
                .retrieve()
                .bodyToFlux(String.class)
                .timeout(properties.llm().firstDeltaTimeout())
                .map(this::readEvent)
                .map(event -> event.getOrDefault("delta", ""))
                .filter(delta -> !delta.isBlank())
                .doOnNext(delta -> {
                    if (firstDeltaSeen.compareAndSet(false, true)) {
                        log.info("LLM stream first delta: session={}, elapsedMs={}, delta={}",
                                sessionId, Duration.between(startedAt, Instant.now()).toMillis(), delta);
                    }
                    chars.addAndGet(delta.length());
                })
                .doOnComplete(() -> log.info("LLM stream complete: session={}, elapsedMs={}, chars={}",
                        sessionId, Duration.between(startedAt, Instant.now()).toMillis(), chars.get()))
                .onErrorResume(error -> {
                    log.warn("LLM stream failed: session={}, elapsedMs={}, error={}",
                            sessionId, Duration.between(startedAt, Instant.now()).toMillis(), error.toString());
                    String fallback = error instanceof TimeoutException
                            ? LLM_SLOW_FALLBACK
                            : LLM_ERROR_FALLBACK;
                    return Flux.just(fallback);
                });
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> readEvent(String data) {
        try {
            return objectMapper.readValue(data, Map.class);
        } catch (Exception error) {
            throw new IllegalArgumentException("Bad LLM event", error);
        }
    }
}
