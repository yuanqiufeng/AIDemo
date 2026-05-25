package com.aidemo.realtime.service.http;

import com.aidemo.realtime.audio.AudioFrame;
import com.aidemo.realtime.config.RealtimeProperties;
import com.aidemo.realtime.service.AsrService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Base64;
import java.util.List;
import java.util.Map;

@Service
@ConditionalOnProperty(prefix = "realtime.asr", name = "mode", havingValue = "http")
public class HttpAsrService implements AsrService {
    private final WebClient webClient;

    public HttpAsrService(WebClient.Builder builder, RealtimeProperties properties) {
        this.webClient = builder.baseUrl(properties.asr().baseUrl()).build();
    }

    @Override
    public Flux<String> partial(String sessionId, AudioFrame frame) {
        String audio = Base64.getEncoder().encodeToString(frame.pcm16le());
        return webClient.post()
                .uri("/asr/partial")
                .bodyValue(Map.of(
                        "seq", frame.sequence(),
                        "sampleRate", frame.sampleRate(),
                        "sessionId", sessionId,
                        "audio", audio
                ))
                .retrieve()
                .bodyToMono(AsrText.class)
                .filter(result -> result.text() != null && !result.text().isBlank())
                .map(AsrText::text)
                .flux()
                .onErrorResume(error -> Flux.empty());
    }

    @Override
    public Mono<String> finalText(String sessionId, List<AudioFrame> utteranceFrames) {
        int totalLength = utteranceFrames.stream().mapToInt(frame -> frame.pcm16le().length).sum();
        byte[] pcm = new byte[totalLength];
        int offset = 0;
        for (AudioFrame frame : utteranceFrames) {
            System.arraycopy(frame.pcm16le(), 0, pcm, offset, frame.pcm16le().length);
            offset += frame.pcm16le().length;
        }
        return webClient.post()
                .uri("/asr/final")
                .bodyValue(Map.of("audio", Base64.getEncoder().encodeToString(pcm), "sessionId", sessionId))
                .retrieve()
                .bodyToMono(AsrText.class)
                .map(AsrText::text);
    }

    private record AsrText(String text) {
    }
}
