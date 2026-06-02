package com.aidemo.android.realtime

import android.app.Application
import android.speech.tts.TextToSpeech
import android.util.Base64
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.aidemo.android.realtime.audio.FullDuplexAudioEngine
import java.util.Locale
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import kotlin.math.sqrt
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
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

class RealtimeViewModel(application: Application) : AndroidViewModel(application) {
    private val client = OkHttpClient.Builder()
        .pingInterval(15, TimeUnit.SECONDS)
        .build()
    private val audioEngine = FullDuplexAudioEngine(application.applicationContext)
    private val _ui = MutableStateFlow(RealtimeUiState())
    val ui: StateFlow<RealtimeUiState> = _ui
    private var serverUrl = DEFAULT_SERVER_URL
    private var webSocket: WebSocket? = null
    private var audioJob: Job? = null
    private var localTts: TextToSpeech? = null
    private var localTtsReady = false
    private var localTtsFallbackJob: Job? = null
    private var receivedServerTtsThisTurn = false
    private var sentAudioFrames = 0L
    private var suppressedPlaybackFrames = 0L
    private var receivedTtsChunks = 0L
    private val playbackPreRollFrames = ArrayDeque<ByteArray>()
    @Volatile
    private var suppressMicUntilMs = 0L

    init {
        var tts: TextToSpeech? = null
        tts = TextToSpeech(application.applicationContext) { status ->
            localTtsReady = status == TextToSpeech.SUCCESS
            if (localTtsReady) {
                tts?.language = Locale.CHINA
                tts?.setSpeechRate(1.08f)
            }
        }
        localTts = tts
    }

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
        stopLocalTtsFallback()
        audioEngine.stopPlayback()
    }

    fun sendText(text: String) {
        val trimmed = text.trim()
        if (trimmed.isBlank()) return
        val sent = webSocket?.send(
            JSONObject()
                .put("type", "audio.end")
                .put("text", trimmed)
                .toString()
        ) ?: false
        _ui.update {
            if (sent) {
                it.copy(state = "thinking").log("you: $trimmed")
            } else {
                it.copy(state = "error").log("text send failed: WebSocket is not connected")
            }
        }
    }

    fun disconnect() {
        audioJob?.cancel()
        audioJob = null
        stopLocalTtsFallback()
        audioEngine.stop()
        webSocket?.close(1000, "client stop")
        webSocket = null
        _ui.update { it.copy(connected = false, state = "idle") }
    }

    override fun onCleared() {
        stopLocalTtsFallback()
        localTts?.shutdown()
        localTts = null
        super.onCleared()
    }

    private val listener = object : WebSocketListener() {
        override fun onOpen(webSocket: WebSocket, response: Response) {
            receivedTtsChunks = 0
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
        sentAudioFrames = 0
        suppressedPlaybackFrames = 0
        suppressMicUntilMs = 0
        playbackPreRollFrames.clear()
        _ui.update { it.log("audio capture starting") }
        audioJob = viewModelScope.launch(Dispatchers.IO) {
            audioEngine.startCapture { frame ->
                if (frame.bargeIn) {
                    val preRoll = synchronized(playbackPreRollFrames) {
                        val frames = playbackPreRollFrames.toList()
                        playbackPreRollFrames.clear()
                        frames
                    }
                    socket.send(JSONObject().put("type", "interrupt").toString())
                    stopLocalTtsFallback()
                    audioEngine.stopPlayback()
                    suppressMicUntilMs = 0
                    _ui.update {
                        it.log(
                            "audio local bargeIn interrupt rms=${"%.4f".format(frame.rms)} " +
                                "echo=${"%.4f".format(frame.playbackEchoRms)} " +
                                "speechMs=${frame.bargeInSpeechMs} preRoll=${preRoll.size}"
                        )
                    }
                    preRoll.forEach { pcm -> sendAudioFrame(socket, pcm, true, frame.rms, true) }
                } else if (System.currentTimeMillis() < suppressMicUntilMs) {
                    return@startCapture
                } else if (frame.playbackActive) {
                    rememberPlaybackPreRoll(frame.pcm16le)
                    suppressedPlaybackFrames += 1
                    if (suppressedPlaybackFrames == 1L || suppressedPlaybackFrames % 25L == 0L) {
                        _ui.update {
                            it.log(
                                "audio suppressed playback rms=${"%.4f".format(frame.rms)} " +
                                    "echo=${"%.4f".format(frame.playbackEchoRms)} speechMs=${frame.bargeInSpeechMs}"
                            )
                        }
                    }
                    return@startCapture
                }
                sendAudioFrame(socket, frame.pcm16le, frame.bargeIn, frame.rms, frame.playbackActive)
            }
        }
    }

    private fun rememberPlaybackPreRoll(pcm16le: ByteArray) {
        synchronized(playbackPreRollFrames) {
            playbackPreRollFrames.addLast(pcm16le)
            while (playbackPreRollFrames.size > BARGE_IN_PREROLL_FRAMES) {
                playbackPreRollFrames.removeFirst()
            }
        }
    }

    private fun sendAudioFrame(
        socket: WebSocket,
        pcm16le: ByteArray,
        bargeIn: Boolean,
        rms: Double,
        playbackActive: Boolean
    ) {
        val audio = Base64.encodeToString(pcm16le, Base64.NO_WRAP)
        val payload = JSONObject()
            .put("type", "audio.chunk")
            .put("audio", audio)
            .put("bargeIn", bargeIn)
            .toString()
        if (socket.send(payload)) {
            sentAudioFrames += 1
            if (playbackActive && sentAudioFrames % 25L == 0L) {
                _ui.update { it.log("audio playbackActive rms=${"%.4f".format(rms)}") }
            } else if (sentAudioFrames == 1L || sentAudioFrames % 50L == 0L) {
                _ui.update { it.log("audio sent frames=$sentAudioFrames rms=${"%.4f".format(rms)}") }
            }
        } else {
            _ui.update { it.copy(state = "error").log("audio send failed") }
        }
    }

    private fun handleEvent(event: ServerEvent) {
        when (event.type) {
            "vad.speech_start" -> _ui.update { it.copy(state = "user speaking").log("vad speech_start") }
            "vad.speech_end" -> _ui.update { it.copy(state = "thinking").log("vad speech_end") }
            "asr.partial" -> _ui.update { it.copy(asrText = event.text.orEmpty()).log("asr.partial ${event.text.orEmpty()}") }
            "asr.final" -> {
                receivedServerTtsThisTurn = false
                localTtsFallbackJob?.cancel()
                _ui.update { it.copy(state = "thinking", asrText = event.text.orEmpty(), aiText = "").log("asr.final ${event.text.orEmpty()}") }
            }
            "llm.delta" -> {
                val delta = event.text.orEmpty()
                _ui.update { it.copy(aiText = it.aiText + delta).log("llm.delta $delta") }
            }
            "llm.done" -> {
                _ui.update { it.log("llm.done") }
            }
            "tts.start" -> _ui.update { it.copy(state = "assistant speaking").log("tts.start") }
            "tts.chunk" -> event.audio?.let { audio ->
                val bytes = audio.decodeBase64()?.toByteArray()
                if (bytes != null) {
                    receivedServerTtsThisTurn = true
                    stopLocalTtsFallback()
                    receivedTtsChunks += 1
                    if (receivedTtsChunks == 1L || receivedTtsChunks % 10L == 0L) {
                        _ui.update { it.log("tts.chunk count=$receivedTtsChunks bytes=${bytes.size} sampleRate=${event.sampleRate ?: 24000}") }
                    }
                    audioEngine.play(bytes, event.sampleRate ?: 24000)
                } else {
                    _ui.update { it.log("tts.chunk decode failed") }
                }
            }
            "tts.done" -> {
                suppressMicUntilMs = audioEngine.playbackSuppressUntil(TTS_TAIL_SUPPRESS_MS)
                val suppressForMs = (suppressMicUntilMs - System.currentTimeMillis()).coerceAtLeast(0L)
                _ui.update { it.copy(state = "listening").log("tts.done suppressMic=${suppressForMs}ms") }
            }
            "interrupt" -> {
                stopLocalTtsFallback()
                audioEngine.stopPlayback()
                _ui.update { it.copy(state = "interrupted").log("interrupt ${event.reason.orEmpty()}") }
            }
            "error" -> _ui.update { it.copy(state = "error").log("server error ${event.reason.orEmpty()}") }
            else -> _ui.update { it.log(event.type) }
        }
    }

    private fun RealtimeUiState.log(line: String): RealtimeUiState =
        copy(events = events + line)

    private fun scheduleLocalTtsFallback() {
        localTtsFallbackJob?.cancel()
        if (!ENABLE_LOCAL_TTS_FALLBACK) return
        val reply = _ui.value.aiText.trim()
        if (reply.isBlank()) return
        localTtsFallbackJob = viewModelScope.launch {
            delay(1500)
            if (!receivedServerTtsThisTurn && localTtsReady) {
                _ui.update { it.log("local tts fallback: no server audio after llm.done") }
                localTts?.speak(reply, TextToSpeech.QUEUE_FLUSH, null, "fallback-${System.nanoTime()}")
            }
        }
    }

    private fun stopLocalTtsFallback() {
        localTtsFallbackJob?.cancel()
        localTtsFallbackJob = null
        localTts?.stop()
    }

    companion object {
        const val DEFAULT_SERVER_URL = "ws://192.168.31.211:8080/ws/realtime"
        private const val ENABLE_LOCAL_TTS_FALLBACK = false
        private const val TTS_TAIL_SUPPRESS_MS = 500L
        private const val BARGE_IN_PREROLL_FRAMES = 16
    }
}

private fun ByteArray.rms16le(): Double {
    if (size < 2) return 0.0
    var sumSquares = 0.0
    var i = 0
    while (i + 1 < size) {
        val lo = this[i].toInt() and 0xff
        val hi = this[i + 1].toInt()
        val sample = ((hi shl 8) or lo).toShort().toInt()
        sumSquares += sample.toDouble() * sample
        i += 2
    }
    return sqrt(sumSquares / (size / 2)) / 32768.0
}
