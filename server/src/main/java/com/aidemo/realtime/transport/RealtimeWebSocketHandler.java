package com.aidemo.realtime.transport;

import com.aidemo.realtime.orchestrator.RealtimeSession;
import com.aidemo.realtime.orchestrator.RealtimeSessionFactory;
import com.aidemo.realtime.protocol.ClientEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class RealtimeWebSocketHandler implements WebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(RealtimeWebSocketHandler.class);

    private final ObjectMapper objectMapper;
    private final RealtimeSessionFactory sessionFactory;

    public RealtimeWebSocketHandler(ObjectMapper objectMapper, RealtimeSessionFactory sessionFactory) {
        this.objectMapper = objectMapper;
        this.sessionFactory = sessionFactory;
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        RealtimeSession realtime = sessionFactory.create();
        AtomicLong audioFrames = new AtomicLong();
        log.info("WebSocket connected: wsSessionId={}, realtimeSessionId={}, uri={}",
                webSocketSession.getId(), realtime.id(), webSocketSession.getHandshakeInfo().getUri());
        Mono<Void> receive = webSocketSession.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(payload -> accept(realtime, payload, audioFrames))
                .doOnError(error -> log.warn("WebSocket receive failed: wsSessionId={}, realtimeSessionId={}, error={}",
                        webSocketSession.getId(), realtime.id(), error.toString()))
                .doFinally(signal -> {
                    log.info("WebSocket disconnected: wsSessionId={}, realtimeSessionId={}, signal={}",
                            webSocketSession.getId(), realtime.id(), signal);
                    realtime.close();
                })
                .then();

        Mono<Void> send = webSocketSession.send(realtime.outbound()
                .doOnNext(event -> log.debug("WebSocket send event: realtimeSessionId={}, type={}",
                        realtime.id(), event.type()))
                .map(event -> {
                    try {
                        return webSocketSession.textMessage(objectMapper.writeValueAsString(event));
                    } catch (Exception error) {
                        throw new IllegalStateException(error);
                    }
                }))
                .doOnError(error -> log.warn("WebSocket send failed: wsSessionId={}, realtimeSessionId={}, error={}",
                        webSocketSession.getId(), realtime.id(), error.toString()));

        realtime.start();
        return Mono.zip(receive, send).then();
    }

    private void accept(RealtimeSession realtime, String payload, AtomicLong audioFrames) {
        try {
            ClientEvent event = objectMapper.readValue(payload, ClientEvent.class);
            String type = event.type();
            if (type == null) {
                log.warn("Ignored client event without type: realtimeSessionId={}", realtime.id());
                return;
            }
            switch (type) {
                case "audio.chunk" -> {
                    byte[] audio = Base64.getDecoder().decode(event.audio());
                    long count = audioFrames.incrementAndGet();
                    if (count == 1 || count % 50 == 0) {
                        log.info("Audio stream: realtimeSessionId={}, frames={}, bytes={}, rms={}",
                                realtime.id(), count, audio.length, String.format("%.4f", com.aidemo.realtime.audio.PcmAudio.rms16le(audio)));
                    }
                    realtime.acceptAudio(audio);
                }
                case "audio.end" -> {
                    log.info("WebSocket receive audio.end: realtimeSessionId={}, hasText={}",
                            realtime.id(), event.text() != null && !event.text().isBlank());
                    if (event.text() != null && !event.text().isBlank()) {
                        realtime.forceRespond(event.text());
                    } else {
                        realtime.finishCurrentUtterance();
                    }
                }
                case "interrupt" -> {
                    log.info("WebSocket receive interrupt: realtimeSessionId={}", realtime.id());
                    realtime.interrupt("client");
                }
                default -> {
                    log.warn("Ignored unsupported client event: realtimeSessionId={}, type={}", realtime.id(), type);
                }
            }
        } catch (Exception error) {
            log.warn("Bad client event: realtimeSessionId={}, error={}", realtime.id(), error.toString());
            realtime.interrupt("bad-client-event");
        }
    }
}
