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
    private static final Duration FIRST_DELTA_TIMEOUT = Duration.ofSeconds(12);

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
                .timeout(FIRST_DELTA_TIMEOUT)
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
                            ? "我这边响应有点慢，请再说一遍。"
                            : "我这边处理失败了，请再试一次。";
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
