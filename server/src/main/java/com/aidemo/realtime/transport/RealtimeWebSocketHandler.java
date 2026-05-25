package com.aidemo.realtime.transport;

import com.aidemo.realtime.orchestrator.RealtimeSession;
import com.aidemo.realtime.orchestrator.RealtimeSessionFactory;
import com.aidemo.realtime.protocol.ClientEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.Base64;

@Component
public class RealtimeWebSocketHandler implements WebSocketHandler {
    private final ObjectMapper objectMapper;
    private final RealtimeSessionFactory sessionFactory;

    public RealtimeWebSocketHandler(ObjectMapper objectMapper, RealtimeSessionFactory sessionFactory) {
        this.objectMapper = objectMapper;
        this.sessionFactory = sessionFactory;
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        RealtimeSession realtime = sessionFactory.create();
        Mono<Void> receive = webSocketSession.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(payload -> accept(realtime, payload))
                .doFinally(signal -> realtime.close())
                .then();

        Mono<Void> send = webSocketSession.send(realtime.outbound()
                .map(event -> {
                    try {
                        return webSocketSession.textMessage(objectMapper.writeValueAsString(event));
                    } catch (Exception error) {
                        throw new IllegalStateException(error);
                    }
                }));

        realtime.start();
        return Mono.zip(receive, send).then();
    }

    private void accept(RealtimeSession realtime, String payload) {
        try {
            ClientEvent event = objectMapper.readValue(payload, ClientEvent.class);
            String type = event.type();
            if (type == null) {
                return;
            }
            switch (type) {
                case "audio.chunk" -> realtime.acceptAudio(Base64.getDecoder().decode(event.audio()));
                case "audio.end" -> {
                    if (event.text() != null && !event.text().isBlank()) {
                        realtime.forceRespond(event.text());
                    } else {
                        realtime.finishCurrentUtterance();
                    }
                }
                case "interrupt" -> realtime.interrupt("client");
                default -> {
                }
            }
        } catch (Exception error) {
            realtime.interrupt("bad-client-event");
        }
    }
}
