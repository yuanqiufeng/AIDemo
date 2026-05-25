package com.aidemo.realtime.orchestrator;

import com.aidemo.realtime.config.RealtimeProperties;
import com.aidemo.realtime.service.AsrService;
import com.aidemo.realtime.service.LlmService;
import com.aidemo.realtime.service.TtsService;
import org.springframework.stereotype.Component;

@Component
public class RealtimeSessionFactory {
    private final RealtimeProperties properties;
    private final AsrService asrService;
    private final LlmService llmService;
    private final TtsService ttsService;

    public RealtimeSessionFactory(
            RealtimeProperties properties,
            AsrService asrService,
            LlmService llmService,
            TtsService ttsService
    ) {
        this.properties = properties;
        this.asrService = asrService;
        this.llmService = llmService;
        this.ttsService = ttsService;
    }

    public RealtimeSession create() {
        return new RealtimeSession(properties, asrService, llmService, ttsService);
    }
}
