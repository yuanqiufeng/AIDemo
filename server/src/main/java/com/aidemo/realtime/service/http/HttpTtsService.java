package com.aidemo.realtime.service.http;

import com.aidemo.realtime.config.RealtimeProperties;
import com.aidemo.realtime.service.TtsChunk;
import com.aidemo.realtime.service.TtsService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Service
@ConditionalOnProperty(prefix = "realtime.tts", name = "mode", havingValue = "http")
public class HttpTtsService implements TtsService {
    private static final Logger log = LoggerFactory.getLogger(HttpTtsService.class);
    private static final int MAX_TEXT_CHARS = 45;

    private final WebClient webClient;
    private final RealtimeProperties properties;
    private final ObjectMapper objectMapper;

    public HttpTtsService(WebClient.Builder builder, RealtimeProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.webClient = builder.baseUrl(properties.tts().baseUrl()).build();
    }

    @Override
    public Flux<TtsChunk> streamAudio(String sessionId, Flux<String> textDeltas) {
        AtomicLong sequence = new AtomicLong();
        return sentenceChunks(textDeltas)
                .doOnNext(text -> log.info("TTS synth start: session={}, text={}", sessionId, text))
                .concatMap(text -> synthesize(sessionId, text, sequence));
    }

    private Flux<TtsChunk> synthesize(String sessionId, String text, AtomicLong sequence) {
        return webClient.post()
                .uri("/tts")
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("sessionId", sessionId, "text", text))
                .retrieve()
                .bodyToMono(TtsResponse.class)
                .flatMapMany(response -> Flux.fromIterable(response.chunks() == null ? List.of() : response.chunks()))
                .map(event -> {
                    byte[] pcm = Base64.getDecoder().decode(event.getOrDefault("audio", ""));
                    int sampleRate = Integer.parseInt(event.getOrDefault("sampleRate", String.valueOf(properties.tts().sampleRate())));
                    return new TtsChunk(sequence.incrementAndGet(), pcm, sampleRate);
                })
                .doOnComplete(() -> log.info("TTS synth complete: session={}, textChars={}", sessionId, text.length()))
                .onErrorResume(error -> {
                    log.warn("TTS synth failed: session={}, text={}", sessionId, text, error);
                    return Flux.empty();
                });
    }

    private Flux<String> sentenceChunks(Flux<String> textDeltas) {
        return Flux.defer(() -> {
            StringBuilder current = new StringBuilder();
            return textDeltas.<String>handle((delta, sink) -> {
                        if (delta == null || delta.isBlank()) {
                            return;
                        }
                        current.append(delta);
                        if (shouldFlush(current)) {
                            sink.next(drain(current));
                        }
                    })
                    .concatWith(Mono.fromSupplier(() -> drain(current)).filter(text -> !text.isBlank()));
        });
    }

    private boolean shouldFlush(StringBuilder text) {
        if (text.length() >= MAX_TEXT_CHARS) {
            return true;
        }
        if (text.isEmpty()) {
            return false;
        }
        return isSentenceEnd(text.charAt(text.length() - 1));
    }

    private boolean isSentenceEnd(char value) {
        return value == '\u3002'
                || value == '\uff01'
                || value == '\uff1f'
                || value == '!'
                || value == '?'
                || value == '\uff1b'
                || value == ';'
                || value == '\n';
    }

    private String drain(StringBuilder text) {
        String chunk = text.toString().trim();
        text.setLength(0);
        return chunk;
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> readEvent(String data) {
        try {
            return objectMapper.readValue(data, Map.class);
        } catch (Exception error) {
            throw new IllegalArgumentException("Bad TTS event", error);
        }
    }

    private record TtsResponse(List<Map<String, String>> chunks, String sampleRate) {
    }
}
