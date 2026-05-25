package com.aidemo.realtime.audio;

import com.aidemo.realtime.config.RealtimeProperties;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class StreamingVadTest {
    @Test
    void emitsSpeechStartAndEnd() {
        RealtimeProperties.Audio config = new RealtimeProperties.Audio(16000, 20, 0.018, 0.010, 60, 100, 0.022);
        StreamingVad vad = new StreamingVad(config);

        assertThat(vad.accept(frame(PcmAudio.sine16le(440, 16000, 20, 0.2)), false)).isEqualTo(VadEvent.NONE);
        assertThat(vad.accept(frame(PcmAudio.sine16le(440, 16000, 20, 0.2)), false)).isEqualTo(VadEvent.NONE);
        assertThat(vad.accept(frame(PcmAudio.sine16le(440, 16000, 20, 0.2)), false)).isEqualTo(VadEvent.SPEECH_START);
        assertThat(vad.accept(frame(PcmAudio.silence16le(16000, 20)), false)).isEqualTo(VadEvent.NONE);
        assertThat(vad.accept(frame(PcmAudio.silence16le(16000, 20)), false)).isEqualTo(VadEvent.NONE);
        assertThat(vad.accept(frame(PcmAudio.silence16le(16000, 20)), false)).isEqualTo(VadEvent.NONE);
        assertThat(vad.accept(frame(PcmAudio.silence16le(16000, 20)), false)).isEqualTo(VadEvent.NONE);
        assertThat(vad.accept(frame(PcmAudio.silence16le(16000, 20)), false)).isEqualTo(VadEvent.SPEECH_END);
    }

    private AudioFrame frame(byte[] pcm) {
        return new AudioFrame(1, pcm, 16000, 1, Instant.now());
    }
}
