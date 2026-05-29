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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RealtimeSession {
    private static final Logger log = LoggerFactory.getLogger(RealtimeSession.class);
    private static final Duration LLM_DELTA_TIMEOUT = Duration.ofSeconds(12);
    private static final Duration RESPONSE_PIPELINE_TIMEOUT = Duration.ofSeconds(45);

    private final String id = UUID.randomUUID().toString();
    private final RealtimeProperties properties;
    private final AsrService asrService;
    private final LlmService llmService;
    private final TtsService ttsService;
    private final StreamingVad vad;
    private final Sinks.Many<ServerEvent> outbound = Sinks.many().multicast().onBackpressureBuffer();
    private final List<AudioFrame> utteranceFrames = new CopyOnWriteArrayList<>();
    private final Deque<AudioFrame> preRollFrames = new ArrayDeque<>();
    private final AtomicLong audioSeq = new AtomicLong();
    private final AtomicBoolean assistantSpeaking = new AtomicBoolean(false);
    private volatile Disposable responsePipeline;
    private volatile Instant ignoreAudioUntil = Instant.EPOCH;

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
        if (shouldIgnoreForAssistantEcho()) {
            utteranceFrames.clear();
            vad.reset();
            return;
        }
        AudioFrame frame = new AudioFrame(
                audioSeq.incrementAndGet(),
                pcm16le,
                properties.audio().sampleRate(),
                1,
                Instant.now()
        );
        rememberPreRoll(frame);
        switch (vad.accept(frame, assistantSpeaking.get())) {
            case SPEECH_START -> {
                if (assistantSpeaking.get()) {
                    if (properties.audio().bargeInEnabled()) {
                        interrupt("speech-start");
                    } else {
                        utteranceFrames.clear();
                        vad.reset();
                        return;
                    }
                }
                utteranceFrames.clear();
                utteranceFrames.addAll(preRollFrames);
                emit(ServerEvent.of(EventType.VAD_SPEECH_START, id));
            }
            case SPEECH_END -> {
                emit(ServerEvent.of(EventType.VAD_SPEECH_END, id));
                List<AudioFrame> finished = new ArrayList<>(utteranceFrames);
                utteranceFrames.clear();
                recognizeAndRespond(finished);
            }
            case BARGE_IN -> {
                if (!properties.audio().bargeInEnabled()) {
                    utteranceFrames.clear();
                    vad.reset();
                    return;
                }
                interrupt("barge-in");
                utteranceFrames.clear();
                utteranceFrames.addAll(preRollFrames);
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
        holdAssistantEcho();
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
        Instant turnStartedAt = Instant.now();
        log.info("Turn response start: session={}, userText={}", id, text);

        AtomicInteger llmChars = new AtomicInteger();
        AtomicBoolean firstLlmDeltaSeen = new AtomicBoolean(false);
        Flux<String> llm = llmService.streamReply(id, text)
                .timeout(LLM_DELTA_TIMEOUT, Flux.just("我这边响应有点慢，请再说一遍。"))
                .onErrorResume(error -> {
                    log.warn("LLM stream failed before TTS: session={}, elapsedMs={}, error={}",
                            id, Duration.between(turnStartedAt, Instant.now()).toMillis(), error.toString());
                    return Flux.just("我这边处理失败了，请再试一次。");
                })
                .doOnNext(delta -> {
                    if (firstLlmDeltaSeen.compareAndSet(false, true)) {
                        log.info("LLM first delta: session={}, elapsedMs={}", id, Duration.between(turnStartedAt, Instant.now()).toMillis());
                    }
                    llmChars.addAndGet(delta.length());
                    emit(ServerEvent.text(EventType.LLM_DELTA, id, delta, false));
                })
                .doOnComplete(() -> {
                    log.info("LLM done: session={}, elapsedMs={}, responseChars={}",
                            id, Duration.between(turnStartedAt, Instant.now()).toMillis(), llmChars.get());
                    emit(ServerEvent.of(EventType.LLM_DONE, id));
                })
                .share();

        assistantSpeaking.set(true);
        ignoreAudioUntil = Instant.now().plus(Duration.ofHours(1));
        emit(ServerEvent.of(EventType.TTS_START, id));
        AtomicInteger ttsChunks = new AtomicInteger();
        AtomicLong ttsPlaybackMs = new AtomicLong();
        AtomicBoolean firstTtsChunkSeen = new AtomicBoolean(false);
        responsePipeline = ttsService.streamAudio(id, llm)
                .timeout(RESPONSE_PIPELINE_TIMEOUT)
                .doOnNext(chunk -> {
                    if (firstTtsChunkSeen.compareAndSet(false, true)) {
                        log.info("TTS first chunk: session={}, elapsedMs={}, bytes={}, sampleRate={}",
                                id, Duration.between(turnStartedAt, Instant.now()).toMillis(), chunk.pcm16le().length, chunk.sampleRate());
                    }
                    ttsPlaybackMs.addAndGet(chunkDurationMs(chunk.pcm16le().length, chunk.sampleRate()));
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
                    holdAssistantEcho(ttsPlaybackMs.get());
                    log.info("TTS done: session={}, elapsedMs={}, chunks={}, playbackMs={}",
                            id, Duration.between(turnStartedAt, Instant.now()).toMillis(), ttsChunks.get(), ttsPlaybackMs.get());
                    emit(ServerEvent.of(EventType.TTS_DONE, id));
                })
                .subscribe(event -> {
                }, error -> {
                    assistantSpeaking.set(false);
                    holdAssistantEcho(ttsPlaybackMs.get());
                    log.warn("Realtime response pipeline failed: session={}, elapsedMs={}",
                            id, Duration.between(turnStartedAt, Instant.now()).toMillis(), error);
                    emitError(error);
                });
    }

    private int durationMs(List<AudioFrame> frames) {
        int bytes = frames.stream().mapToInt(frame -> frame.pcm16le().length).sum();
        return bytes / Math.max(1, properties.audio().sampleRate() * 2 / 1000);
    }

    private void rememberPreRoll(AudioFrame frame) {
        preRollFrames.addLast(frame);
        int maxFrames = Math.max(1, 400 / Math.max(1, properties.audio().frameMs()));
        while (preRollFrames.size() > maxFrames) {
            preRollFrames.removeFirst();
        }
    }

    private boolean shouldIgnoreForAssistantEcho() {
        return assistantSpeaking.get() || Instant.now().isBefore(ignoreAudioUntil);
    }

    private void holdAssistantEcho() {
        holdAssistantEcho(0);
    }

    private void holdAssistantEcho(long playbackMs) {
        int holdMs = properties.audio().assistantEchoHoldMs();
        long totalHoldMs = Math.max(0, playbackMs) + Math.max(0, holdMs);
        if (totalHoldMs > 0) {
            ignoreAudioUntil = Instant.now().plus(Duration.ofMillis(totalHoldMs));
        }
        utteranceFrames.clear();
        vad.reset();
    }

    private long chunkDurationMs(int pcmBytes, int sampleRate) {
        if (sampleRate <= 0 || pcmBytes <= 0) {
            return 0;
        }
        return Math.round((pcmBytes / 2.0) * 1000.0 / sampleRate);
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
