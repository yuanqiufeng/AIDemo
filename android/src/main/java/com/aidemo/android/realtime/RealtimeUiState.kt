package com.aidemo.android.realtime

data class RealtimeUiState(
    val connected: Boolean = false,
    val state: String = "idle",
    val asrText: String = "",
    val aiText: String = "",
    val events: List<String> = emptyList()
)
