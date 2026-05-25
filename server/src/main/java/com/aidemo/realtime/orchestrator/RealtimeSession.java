package com.aidemo.realtime.orchestrator;

import com.aidemo.realtime.audio.AudioFrame;
import com.aidemo.realtime.audio.StreamingVad;
import com.aidemo.realtime.config.RealtimeProperties;
import com.aidemo.realtime.protocol.EventType;
import com.aidemo.realtime.protocol.ServerEvent;
import com.aidemo.realtime.service.AsrService;
import com.aidemo.realtime.service.LlmService;
import com.aidemo.realtime.service.TtsService;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RealtimeSession {
    private final String id = UUID.randomUUID().toString();
    private final RealtimeProperties properties;
    private final AsrService asrService;
    private final LlmService llmService;
    private final TtsService ttsService;
    private final StreamingVad vad;
    private final Sinks.Many<ServerEvent> outbound = Sinks.many().multicast().onBackpressureBuffer();
    private final List<AudioFrame> utteranceFrames = new CopyOnWriteArrayList<>();
    private final AtomicLong audioSeq = new AtomicLong();
    private final AtomicBoolean assistantSpeaking = new AtomicBoolean(false);
    private volatile Disposable responsePipeline;

    public RealtimeSession(
            RealtimeProperties properties,
            AsrService asrService,
            LlmService llmService,
            TtsService ttsService
    ) {
        this.properties = properties;
        this.asrService = asrService;
        this.llmService = llmService;
        this.ttsService = ttsService;
        this.vad = new StreamingVad(properties.audio());
    }

    public String id() {
        return id;
    }

    public Flux<ServerEvent> outbound() {
        return outbound.asFlux();
    }

    public void start() {
        emit(ServerEvent.of(EventType.SESSION_READY, id));
    }

    public void acceptAudio(byte[] pcm16le) {
        AudioFrame frame = new AudioFrame(
                audioSeq.incrementAndGet(),
                pcm16le,
                properties.audio().sampleRate(),
                1,
                Instant.now()
        );
        switch (vad.accept(frame, assistantSpeaking.get())) {
            case SPEECH_START -> {
                if (assistantSpeaking.get()) {
                    interrupt("speech-start");
                }
                utteranceFrames.clear();
                utteranceFrames.add(frame);
                emit(ServerEvent.of(EventType.VAD_SPEECH_START, id));
            }
            case SPEECH_END -> {
                emit(ServerEvent.of(EventType.VAD_SPEECH_END, id));
                List<AudioFrame> finished = new ArrayList<>(utteranceFrames);
                utteranceFrames.clear();
                recognizeAndRespond(finished);
            }
            case BARGE_IN -> {
                interrupt("barge-in");
                utteranceFrames.clear();
                utteranceFrames.add(frame);
                emit(ServerEvent.of(EventType.VAD_SPEECH_START, id));
            }
            case NONE -> {
                if (!utteranceFrames.isEmpty()) {
                    utteranceFrames.add(frame);
                    asrService.partial(id, frame)
                            .map(text -> ServerEvent.text(EventType.ASR_PARTIAL, id, text, false))
                            .subscribe(this::emit, this::emitError);
                }
            }
        }
    }

    public void forceRespond(String text) {
        if (text != null && !text.isBlank()) {
            recognizeAndRespond(List.of(), text);
        }
    }

    public void finishCurrentUtterance() {
        List<AudioFrame> finished = new ArrayList<>(utteranceFrames);
        utteranceFrames.clear();
        vad.reset();
        emit(ServerEvent.of(EventType.VAD_SPEECH_END, id));
        recognizeAndRespond(finished);
    }

    public void interrupt(String reason) {
        Disposable pipeline = responsePipeline;
        if (pipeline != null && !pipeline.isDisposed()) {
            pipeline.dispose();
        }
        assistantSpeaking.set(false);
        emit(new ServerEvent(EventType.INTERRUPT, id, null, null, null, null, null, reason, null));
    }

    public void close() {
        interrupt("session-closed");
        outbound.tryEmitComplete();
    }

    private void recognizeAndRespond(List<AudioFrame> finished) {
        asrService.finalText(id, finished)
                .defaultIfEmpty("")
                .subscribe(text -> recognizeAndRespond(finished, text), this::emitError);
    }

    private void recognizeAndRespond(List<AudioFrame> finished, String text) {
        if (text == null || text.isBlank()) {
            return;
        }
        interrupt("new-turn");
        emit(ServerEvent.text(EventType.ASR_FINAL, id, text, true));

        Flux<String> llm = llmService.streamReply(id, text)
                .doOnNext(delta -> emit(ServerEvent.text(EventType.LLM_DELTA, id, delta, false)))
                .doOnComplete(() -> emit(ServerEvent.of(EventType.LLM_DONE, id)))
                .share();

        assistantSpeaking.set(true);
        emit(ServerEvent.of(EventType.TTS_START, id));
        responsePipeline = ttsService.streamAudio(id, llm)
                .map(chunk -> ServerEvent.audio(
                        EventType.TTS_CHUNK,
                        id,
                        chunk.sequence(),
                        Base64.getEncoder().encodeToString(chunk.pcm16le()),
                        chunk.sampleRate()
                ))
                .doOnNext(this::emit)
                .doOnComplete(() -> {
                    assistantSpeaking.set(false);
                    emit(ServerEvent.of(EventType.TTS_DONE, id));
                })
                .subscribe(event -> {
                }, error -> {
                    assistantSpeaking.set(false);
                    emitError(error);
                });
    }

    private void emit(ServerEvent event) {
        outbound.tryEmitNext(event);
    }

    private void emitError(Throwable error) {
        emit(ServerEvent.error(id, error.getMessage()));
    }
}
