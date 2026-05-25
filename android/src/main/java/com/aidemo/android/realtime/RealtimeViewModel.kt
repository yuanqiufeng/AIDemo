package com.aidemo.android.realtime

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.aidemo.android.realtime.audio.FullDuplexAudioEngine
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString.Companion.decodeBase64
import org.json.JSONObject
import java.util.concurrent.TimeUnit
import android.util.Base64

class RealtimeViewModel(application: Application) : AndroidViewModel(application) {
    private val client = OkHttpClient.Builder()
        .pingInterval(15, TimeUnit.SECONDS)
        .build()
    private val audioEngine = FullDuplexAudioEngine(application.applicationContext)
    private val _ui = MutableStateFlow(RealtimeUiState())
    val ui: StateFlow<RealtimeUiState> = _ui
    private var serverUrl = "ws://10.0.2.2:8080/ws/realtime"
    private var webSocket: WebSocket? = null
    private var audioJob: Job? = null

    fun updateServerUrl(url: String) {
        serverUrl = url
    }

    fun connect() {
        if (webSocket != null) return
        val request = Request.Builder().url(serverUrl).build()
        webSocket = client.newWebSocket(request, listener)
        _ui.update { it.copy(state = "connecting") }
    }

    fun interrupt() {
        webSocket?.send(JSONObject().put("type", "interrupt").toString())
        audioEngine.stopPlayback()
    }

    fun disconnect() {
        audioJob?.cancel()
        audioJob = null
        audioEngine.stop()
        webSocket?.close(1000, "client stop")
        webSocket = null
        _ui.update { it.copy(connected = false, state = "idle") }
    }

    private val listener = object : WebSocketListener() {
        override fun onOpen(webSocket: WebSocket, response: Response) {
            _ui.update { it.copy(connected = true, state = "listening").log("connected") }
            startAudioLoop(webSocket)
        }

        override fun onMessage(webSocket: WebSocket, text: String) {
            val event = ServerEvent.parse(text)
            handleEvent(event)
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            disconnect()
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            _ui.update { it.copy(connected = false, state = "error").log("error: ${t.message}") }
            this@RealtimeViewModel.webSocket = null
            audioEngine.stop()
        }
    }

    private fun startAudioLoop(socket: WebSocket) {
        audioJob?.cancel()
        audioJob = viewModelScope.launch(Dispatchers.IO) {
            audioEngine.startCapture { pcm ->
                val audio = Base64.encodeToString(pcm, Base64.NO_WRAP)
                val payload = JSONObject()
                    .put("type", "audio.chunk")
                    .put("audio", audio)
                    .toString()
                socket.send(payload)
            }
        }
    }

    private fun handleEvent(event: ServerEvent) {
        when (event.type) {
            "vad.speech_start" -> _ui.update { it.copy(state = "user speaking").log("vad speech_start") }
            "vad.speech_end" -> _ui.update { it.copy(state = "thinking").log("vad speech_end") }
            "asr.partial" -> _ui.update { it.copy(asrText = event.text.orEmpty()).log("asr.partial ${event.text.orEmpty()}") }
            "asr.final" -> _ui.update { it.copy(asrText = event.text.orEmpty(), aiText = "").log("asr.final ${event.text.orEmpty()}") }
            "llm.delta" -> _ui.update { it.copy(aiText = it.aiText + event.text.orEmpty()).log("llm.delta ${event.text.orEmpty()}") }
            "tts.start" -> _ui.update { it.copy(state = "assistant speaking").log("tts.start") }
            "tts.chunk" -> event.audio?.let { audio ->
                val bytes = audio.decodeBase64()?.toByteArray()
                if (bytes != null) {
                    audioEngine.play(bytes, event.sampleRate ?: 24000)
                }
            }
            "tts.done" -> _ui.update { it.copy(state = "listening").log("tts.done") }
            "interrupt" -> {
                audioEngine.stopPlayback()
                _ui.update { it.copy(state = "interrupted").log("interrupt ${event.reason.orEmpty()}") }
            }
            "error" -> _ui.update { it.copy(state = "error").log("server error ${event.reason.orEmpty()}") }
            else -> _ui.update { it.log(event.type) }
        }
    }

    private fun RealtimeUiState.log(line: String): RealtimeUiState =
        copy(events = events + line)
}
