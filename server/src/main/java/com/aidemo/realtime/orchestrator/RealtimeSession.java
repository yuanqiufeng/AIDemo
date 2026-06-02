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
    private static final String LLM_SLOW_FALLBACK = "\u6211\u8fd9\u8fb9\u54cd\u5e94\u6709\u70b9\u6162\uff0c\u8bf7\u518d\u8bf4\u4e00\u904d\u3002";
    private static final String LLM_ERROR_FALLBACK = "\u6211\u8fd9\u8fb9\u5904\u7406\u5931\u8d25\u4e86\uff0c\u8bf7\u518d\u8bd5\u4e00\u6b21\u3002";

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
    private volatile Instant assistantSpeechStartedAt = Instant.EPOCH;
    private volatile Instant lastAssistantTextAt = Instant.EPOCH;
    private volatile String lastAssistantText = "";
    private int bargeInSpeechMs;

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
        acceptAudio(pcm16le, false);
    }

    public void acceptAudio(byte[] pcm16le, boolean clientBargeIn) {
        AudioFrame frame = new AudioFrame(
                audioSeq.incrementAndGet(),
                pcm16le,
                properties.audio().sampleRate(),
                1,
                Instant.now()
        );
        boolean acceptedClientBargeIn = false;
        if (assistantSpeaking.get() && clientBargeIn && properties.audio().bargeInEnabled()) {
            log.info("Client barge-in: session={}, rms={}", id, String.format("%.4f", PcmAudio.rms16le(frame.pcm16le())));
            interruptForBargeIn("client-barge-in");
            utteranceFrames.clear();
            preRollFrames.clear();
            vad.reset();
            ignoreAudioUntil = Instant.EPOCH;
            acceptedClientBargeIn = true;
        }
        if (!clientBargeIn && !acceptedClientBargeIn && shouldIgnoreForAssistantEcho()) {
            rememberPreRoll(frame);
            if (properties.audio().bargeInEnabled() && PcmAudio.rms16le(frame.pcm16le()) >= properties.audio().interruptRms()) {
                log.info("Audio ignored during echo hold but above interrupt threshold: session={}, rms={}",
                        id, String.format("%.4f", PcmAudio.rms16le(frame.pcm16le())));
            }
            return;
        }
        rememberPreRoll(frame);
        switch (vad.accept(frame, assistantSpeaking.get())) {
            case SPEECH_START -> {
                if (assistantSpeaking.get()) {
                    if (properties.audio().bargeInEnabled() && clientBargeIn) {
                        interruptForBargeIn("speech-start");
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
                if (!properties.audio().bargeInEnabled() || !clientBargeIn) {
                    utteranceFrames.clear();
                    vad.reset();
                    return;
                }
                if (!shouldAcceptBargeIn(frame)) {
                    return;
                }
                interruptForBargeIn("barge-in");
                utteranceFrames.clear();
                preRollFrames.clear();
                vad.reset();
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
        bargeInSpeechMs = 0;
        holdAssistantEcho();
        emit(new ServerEvent(EventType.INTERRUPT, id, null, null, null, null, null, reason, null));
    }

    private void interruptForBargeIn(String reason) {
        Disposable pipeline = responsePipeline;
        if (pipeline != null && !pipeline.isDisposed()) {
            pipeline.dispose();
        }
        assistantSpeaking.set(false);
        bargeInSpeechMs = 0;
        ignoreAudioUntil = Instant.now().plus(Duration.ofMillis(properties.audio().bargeInRestartDelayMs()));
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
        if (isLikelyAssistantEcho(text)) {
            log.info("ASR final ignored as assistant echo: session={}, text={}, lastAssistantText={}", id, text, lastAssistantText);
            emit(ServerEvent.of(EventType.VAD_SPEECH_END, id));
            return;
        }
        interrupt("new-turn");
        emit(ServerEvent.text(EventType.ASR_FINAL, id, text, true));
        Instant turnStartedAt = Instant.now();
        log.info("Turn response start: session={}, userText={}", id, text);

        AtomicInteger llmChars = new AtomicInteger();
        AtomicBoolean firstLlmDeltaSeen = new AtomicBoolean(false);
        Flux<String> llm = llmService.streamReply(id, text)
                .timeout(properties.llm().firstDeltaTimeout(), Flux.just(LLM_SLOW_FALLBACK))
                .onErrorResume(error -> {
                    log.warn("LLM stream failed before TTS: session={}, elapsedMs={}, error={}",
                            id, Duration.between(turnStartedAt, Instant.now()).toMillis(), error.toString());
                    return Flux.just(LLM_ERROR_FALLBACK);
                })
                .doOnNext(delta -> {
                    if (firstLlmDeltaSeen.compareAndSet(false, true)) {
                        log.info("LLM first delta: session={}, elapsedMs={}", id, Duration.between(turnStartedAt, Instant.now()).toMillis());
                    }
                    llmChars.addAndGet(delta.length());
                    rememberAssistantText(delta);
                    emit(ServerEvent.text(EventType.LLM_DELTA, id, delta, false));
                })
                .doOnComplete(() -> {
                    log.info("LLM done: session={}, elapsedMs={}, responseChars={}",
                            id, Duration.between(turnStartedAt, Instant.now()).toMillis(), llmChars.get());
                    emit(ServerEvent.of(EventType.LLM_DONE, id));
                })
                .share();

        assistantSpeaking.set(true);
        assistantSpeechStartedAt = Instant.now();
        bargeInSpeechMs = 0;
        ignoreAudioUntil = Instant.EPOCH;
        AtomicInteger ttsChunks = new AtomicInteger();
        AtomicLong ttsPlaybackMs = new AtomicLong();
        AtomicBoolean firstTtsChunkSeen = new AtomicBoolean(false);
        responsePipeline = ttsService.streamAudio(id, llm)
                .timeout(properties.llm().responseTimeout())
                .doOnNext(chunk -> {
                    if (firstTtsChunkSeen.compareAndSet(false, true)) {
                        emit(ServerEvent.of(EventType.TTS_START, id));
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
                    bargeInSpeechMs = 0;
                    holdAssistantEcho(properties.audio().ttsTailIgnoreMs());
                    log.info("TTS done: session={}, elapsedMs={}, chunks={}, playbackMs={}",
                            id, Duration.between(turnStartedAt, Instant.now()).toMillis(), ttsChunks.get(), ttsPlaybackMs.get());
                    emit(ServerEvent.of(EventType.TTS_DONE, id));
                })
                .subscribe(event -> {
                }, error -> {
                    assistantSpeaking.set(false);
                    bargeInSpeechMs = 0;
                    holdAssistantEcho(properties.audio().ttsTailIgnoreMs());
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
        return !assistantSpeaking.get() && Instant.now().isBefore(ignoreAudioUntil);
    }

    private boolean shouldAcceptBargeIn(AudioFrame frame) {
        if (!assistantSpeaking.get()) {
            bargeInSpeechMs = 0;
            return true;
        }
        long sinceAssistantStartMs = Duration.between(assistantSpeechStartedAt, Instant.now()).toMillis();
        if (sinceAssistantStartMs < properties.audio().assistantBargeInDelayMs()) {
            bargeInSpeechMs = 0;
            return false;
        }
        double rms = PcmAudio.rms16le(frame.pcm16le());
        if (rms < properties.audio().interruptRms()) {
            bargeInSpeechMs = 0;
            return false;
        }
        bargeInSpeechMs += properties.audio().frameMs();
        if (bargeInSpeechMs < properties.audio().interruptMinSpeechMs()) {
            return false;
        }
        log.info("Accepted barge-in: session={}, rms={}, speechMs={}, sinceAssistantStartMs={}",
                id, String.format("%.4f", rms), bargeInSpeechMs, sinceAssistantStartMs);
        return true;
    }

    private void holdAssistantEcho() {
        holdAssistantEcho(properties.audio().assistantEchoHoldMs());
    }

    private void holdAssistantEcho(long holdMs) {
        if (holdMs > 0) {
            ignoreAudioUntil = Instant.now().plus(Duration.ofMillis(holdMs));
        }
        utteranceFrames.clear();
        vad.reset();
    }

    private void rememberAssistantText(String delta) {
        if (delta == null || delta.isBlank()) {
            return;
        }
        String combined = lastAssistantText + delta;
        if (combined.length() > 240) {
            combined = combined.substring(combined.length() - 240);
        }
        lastAssistantText = combined;
        lastAssistantTextAt = Instant.now();
    }

    private boolean isLikelyAssistantEcho(String text) {
        String normalizedText = normalizeForEchoSafe(text);
        String normalizedAssistant = normalizeForEchoSafe(lastAssistantText);
        if (normalizedText.length() < 2 || normalizedAssistant.length() < 2) {
            return false;
        }
        if (Duration.between(lastAssistantTextAt, Instant.now()).toSeconds() > 45) {
            return false;
        }
        return normalizedAssistant.contains(normalizedText)
                || normalizedText.contains(normalizedAssistant)
                || echoOverlapRatio(normalizedText, normalizedAssistant) >= 0.70
                || longestCommonSubstringRatio(normalizedText, normalizedAssistant) >= 0.60;
    }

    private double echoOverlapRatio(String text, String assistantText) {
        int matched = 0;
        for (int i = 0; i < text.length(); i++) {
            if (assistantText.indexOf(text.charAt(i)) >= 0) {
                matched++;
            }
        }
        return matched / (double) text.length();
    }

    private double longestCommonSubstringRatio(String text, String assistantText) {
        int best = 0;
        int[] previous = new int[assistantText.length() + 1];
        for (int i = 1; i <= text.length(); i++) {
            int[] current = new int[assistantText.length() + 1];
            for (int j = 1; j <= assistantText.length(); j++) {
                if (text.charAt(i - 1) == assistantText.charAt(j - 1)) {
                    current[j] = previous[j - 1] + 1;
                    best = Math.max(best, current[j]);
                }
            }
            previous = current;
        }
        return best / (double) text.length();
    }

    private String normalizeForEcho(String value) {
        if (value == null) {
            return "";
        }
        return value.replaceAll("[\\p{Punct}\\s，。！？；：、~～]+", "");
    }

    private long chunkDurationMs(int pcmBytes, int sampleRate) {
        if (sampleRate <= 0 || pcmBytes <= 0) {
            return 0;
        }
        return Math.round((pcmBytes / 2.0) * 1000.0 / sampleRate);
    }

    private String normalizeForEchoSafe(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder normalized = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (Character.isLetterOrDigit(current) || Character.UnicodeScript.of(current) == Character.UnicodeScript.HAN) {
                normalized.append(current);
            }
        }
        return normalized.toString();
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
