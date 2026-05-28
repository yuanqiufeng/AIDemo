package com.aidemo.realtime.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "realtime")
public record RealtimeProperties(
        Audio audio,
        Asr asr,
        Llm llm,
        Tts tts
) {
    public record Audio(
            int sampleRate,
            int frameMs,
            double speechStartRms,
            double speechEndRms,
            int minSpeechMs,
            int silenceEndpointMs,
            int maxUtteranceMs,
            int assistantEchoHoldMs,
            boolean bargeInEnabled,
            double interruptRms
    ) {
        public int bytesPerFrame() {
            return sampleRate * frameMs / 1000 * 2;
        }
    }

    public record Asr(
            String mode,
            String baseUrl,
            Duration connectTimeout,
            Duration responseTimeout
    ) {
    }

    public record Llm(
            String mode,
            String baseUrl,
            String model
    ) {
    }

    public record Tts(
            String mode,
            String baseUrl,
            int sampleRate
    ) {
    }
}
