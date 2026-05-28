package com.aidemo.realtime.audio;

import com.aidemo.realtime.config.RealtimeProperties;

public class StreamingVad {
    private final RealtimeProperties.Audio config;
    private VadState state = VadState.IDLE;
    private int speechMs;
    private int silenceMs;

    public StreamingVad(RealtimeProperties.Audio config) {
        this.config = config;
    }

    public VadEvent accept(AudioFrame frame, boolean assistantSpeaking) {
        double rms = PcmAudio.rms16le(frame.pcm16le());
        int frameMs = config.frameMs();
        if (assistantSpeaking && rms >= config.interruptRms()) {
            if (state == VadState.IDLE) {
                speechMs += frameMs;
                if (speechMs < config.minSpeechMs()) {
                    return VadEvent.NONE;
                }
            }
            state = VadState.SPEECH;
            speechMs = frameMs;
            silenceMs = 0;
            return VadEvent.BARGE_IN;
        }

        if (state == VadState.IDLE) {
            if (rms >= config.speechStartRms()) {
                speechMs += frameMs;
                if (speechMs >= config.minSpeechMs()) {
                    silenceMs = 0;
                    state = VadState.SPEECH;
                    return VadEvent.SPEECH_START;
                }
            } else {
                speechMs = 0;
            }
            return VadEvent.NONE;
        }

        if (rms <= config.speechEndRms()) {
            silenceMs += frameMs;
            if (silenceMs >= config.silenceEndpointMs()) {
                reset();
                return VadEvent.SPEECH_END;
            }
        } else {
            speechMs += frameMs;
            silenceMs = 0;
            if (config.maxUtteranceMs() > 0 && speechMs >= config.maxUtteranceMs()) {
                reset();
                return VadEvent.SPEECH_END;
            }
        }
        return VadEvent.NONE;
    }

    public void reset() {
        state = VadState.IDLE;
        speechMs = 0;
        silenceMs = 0;
    }
}
