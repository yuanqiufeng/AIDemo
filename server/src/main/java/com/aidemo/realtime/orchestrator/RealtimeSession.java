package com.aidemo.realtime.orchestrator;

import com.aidemo.realtime.audio.AudioFrame;
import com.aidemo.realtime.audio.PcmAudio;
import com.aidemo.realtime.audio.StreamingVad;
import com.aidemo.realtime.config.RealtimeProperties;
import com.aidemo.realtime.protocol.EventType;
import com.aidemo.realtime.protocol.ServerEvent;
import com.aidemo.realtime.service.AsrService;
import com.aidemo.realtime.service.LlmService;
import com.aidemo.realtime.service.TtsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RealtimeSession {
    private static final Logger log = LoggerFactory.getLogger(RealtimeSession.class);

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
        log.info("ASR final start: session={}, frames={}, durationMs={}, rms={}",
                id,
                finished.size(),
                durationMs(finished),
                String.format("%.4f", rms(finished)));
        asrService.finalText(id, finished)
                .defaultIfEmpty("")
                .subscribe(text -> {
                    if (text == null || text.isBlank()) {
                        log.info("ASR final empty/ignored: session={}, frames={}, rms={}",
                                id, finished.size(), String.format("%.4f", rms(finished)));
                    } else {
                        log.info("ASR final text: session={}, text={}", id, text);
                    }
                    recognizeAndRespond(finished, text);
                }, this::emitError);
    }

    private void recognizeAndRespond(List<AudioFrame> finished, String text) {
        if (text == null || text.isBlank()) {
            return;
        }
        interrupt("new-turn");
        emit(ServerEvent.text(EventType.ASR_FINAL, id, text, true));

        AtomicInteger llmChars = new AtomicInteger();
        Flux<String> llm = llmService.streamReply(id, text)
                .doOnNext(delta -> {
                    llmChars.addAndGet(delta.length());
                    emit(ServerEvent.text(EventType.LLM_DELTA, id, delta, false));
                })
                .doOnComplete(() -> {
                    log.info("LLM done: session={}, responseChars={}", id, llmChars.get());
                    emit(ServerEvent.of(EventType.LLM_DONE, id));
                })
                .share();

        assistantSpeaking.set(true);
        emit(ServerEvent.of(EventType.TTS_START, id));
        AtomicInteger ttsChunks = new AtomicInteger();
        responsePipeline = ttsService.streamAudio(id, llm)
                .doOnNext(chunk -> {
                    int count = ttsChunks.incrementAndGet();
                    if (count == 1 || count % 10 == 0) {
                        log.info("TTS chunk: session={}, count={}, bytes={}, sampleRate={}",
                                id, count, chunk.pcm16le().length, chunk.sampleRate());
                    }
                })
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
                    log.info("TTS done: session={}, chunks={}", id, ttsChunks.get());
                    emit(ServerEvent.of(EventType.TTS_DONE, id));
                })
                .subscribe(event -> {
                }, error -> {
                    assistantSpeaking.set(false);
                    log.warn("Realtime response pipeline failed: session={}", id, error);
                    emitError(error);
                });
    }

    private int durationMs(List<AudioFrame> frames) {
        int bytes = frames.stream().mapToInt(frame -> frame.pcm16le().length).sum();
        return bytes / Math.max(1, properties.audio().sampleRate() * 2 / 1000);
    }

    private double rms(List<AudioFrame> frames) {
        int totalLength = frames.stream().mapToInt(frame -> frame.pcm16le().length).sum();
        if (totalLength == 0) {
            return 0.0;
        }
        byte[] pcm = new byte[totalLength];
        int offset = 0;
        for (AudioFrame frame : frames) {
            System.arraycopy(frame.pcm16le(), 0, pcm, offset, frame.pcm16le().length);
            offset += frame.pcm16le().length;
        }
        return PcmAudio.rms16le(pcm);
    }

    private void emit(ServerEvent event) {
        outbound.tryEmitNext(event);
    }

    private void emitError(Throwable error) {
        emit(ServerEvent.error(id, error.getMessage()));
    }
}
