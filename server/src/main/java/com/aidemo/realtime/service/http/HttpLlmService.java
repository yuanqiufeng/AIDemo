package com.aidemo.realtime.service.http;

import com.aidemo.realtime.config.RealtimeProperties;
import com.aidemo.realtime.service.LlmService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.Map;

@Service
@ConditionalOnProperty(prefix = "realtime.llm", name = "mode", havingValue = "http")
public class HttpLlmService implements LlmService {
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
                .map(this::readEvent)
                .map(event -> event.getOrDefault("delta", ""))
                .filter(delta -> !delta.isBlank());
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
