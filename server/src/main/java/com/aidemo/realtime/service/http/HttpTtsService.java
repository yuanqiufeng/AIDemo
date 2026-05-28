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
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Service
@ConditionalOnProperty(prefix = "realtime.tts", name = "mode", havingValue = "http")
public class HttpTtsService implements TtsService {
    private static final Logger log = LoggerFactory.getLogger(HttpTtsService.class);
    private static final int MAX_TEXT_CHARS = 45;
    private static final int MAX_RESPONSE_BYTES = 12 * 1024 * 1024;
    private static final int OUTBOUND_PCM_CHUNK_BYTES = 32 * 1024;

    private final WebClient webClient;
    private final RealtimeProperties properties;
    private final ObjectMapper objectMapper;

    public HttpTtsService(WebClient.Builder builder, RealtimeProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(MAX_RESPONSE_BYTES))
                .build();
        this.webClient = builder
                .baseUrl(properties.tts().baseUrl())
                .exchangeStrategies(strategies)
                .build();
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
                .uri("/tts/ndjson")
                .accept(MediaType.APPLICATION_NDJSON)
                .bodyValue(Map.of("sessionId", sessionId, "text", text))
                .retrieve()
                .bodyToFlux(String.class)
                .map(this::readEvent)
                .flatMap(event -> {
                    byte[] pcm = Base64.getDecoder().decode(event.getOrDefault("audio", ""));
                    int sampleRate = Integer.parseInt(event.getOrDefault("sampleRate", String.valueOf(properties.tts().sampleRate())));
                    return splitChunk(sequence, pcm, sampleRate);
                })
                .doOnComplete(() -> log.info("TTS synth complete: session={}, textChars={}", sessionId, text.length()))
                .onErrorResume(error -> {
                    log.warn("TTS synth failed: session={}, text={}", sessionId, text, error);
                    return Flux.empty();
                });
    }

    private Flux<TtsChunk> splitChunk(AtomicLong sequence, byte[] pcm, int sampleRate) {
        if (pcm.length <= OUTBOUND_PCM_CHUNK_BYTES) {
            return Flux.just(new TtsChunk(sequence.incrementAndGet(), pcm, sampleRate));
        }
        return Flux.range(0, (pcm.length + OUTBOUND_PCM_CHUNK_BYTES - 1) / OUTBOUND_PCM_CHUNK_BYTES)
                .map(index -> {
                    int start = index * OUTBOUND_PCM_CHUNK_BYTES;
                    int end = Math.min(start + OUTBOUND_PCM_CHUNK_BYTES, pcm.length);
                    return new TtsChunk(sequence.incrementAndGet(), Arrays.copyOfRange(pcm, start, end), sampleRate);
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

}
