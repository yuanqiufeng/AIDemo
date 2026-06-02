package com.aidemo.android.realtime.audio

import android.Manifest
import android.annotation.SuppressLint
import android.content.Context
import android.content.pm.PackageManager
import android.media.AudioAttributes
import android.media.AudioFormat
import android.media.AudioManager
import android.media.AudioRecord
import android.media.AudioTrack
import android.media.MediaRecorder
import android.media.audiofx.AcousticEchoCanceler
import android.media.audiofx.AutomaticGainControl
import android.media.audiofx.NoiseSuppressor
import androidx.core.content.ContextCompat
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import kotlin.math.sqrt

class FullDuplexAudioEngine(private val context: Context) {
    private val sampleRate = 16000
    private val channelConfig = AudioFormat.CHANNEL_IN_MONO
    private val audioFormat = AudioFormat.ENCODING_PCM_16BIT
    private val frameBytes = sampleRate / 50 * 2
    private val running = AtomicBoolean(false)
    private val playbackRunning = AtomicBoolean(false)
    private val playbackActive = AtomicBoolean(false)
    private val playbackQueue = LinkedBlockingQueue<ByteArray>()
    private var recorder: AudioRecord? = null
    private var audioTrack: AudioTrack? = null
    private var playbackThread: Thread? = null
    private var playbackRate = 24000
    private var echoCanceler: AcousticEchoCanceler? = null
    private var noiseSuppressor: NoiseSuppressor? = null
    private var gainControl: AutomaticGainControl? = null
    private var bargeInSpeechMs = 0
    private var lastBargeInAtMs = 0L
    @Volatile
    private var playbackActiveUntilMs = 0L
    @Volatile
    private var playbackStartedAtMs = 0L
    private var playbackEchoRms = 0.0

    data class CaptureFrame(
        val pcm16le: ByteArray,
        val rms: Double,
        val bargeIn: Boolean,
        val playbackActive: Boolean,
        val playbackEchoRms: Double,
        val bargeInSpeechMs: Int,
    )

    @SuppressLint("MissingPermission")
    fun startCapture(onFrame: (CaptureFrame) -> Unit) {
        if (ContextCompat.checkSelfPermission(context, Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED) {
            return
        }
        stop()
        val minBuffer = AudioRecord.getMinBufferSize(sampleRate, channelConfig, audioFormat)
        val bufferSize = maxOf(minBuffer, frameBytes * 8)
        val record = AudioRecord(
            MediaRecorder.AudioSource.VOICE_COMMUNICATION,
            sampleRate,
            channelConfig,
            audioFormat,
            bufferSize
        )
        recorder = record
        enableAudioProcessing(record.audioSessionId)
        val audioManager = context.getSystemService(Context.AUDIO_SERVICE) as AudioManager
        audioManager.mode = AudioManager.MODE_IN_COMMUNICATION
        running.set(true)
        startPlaybackIfNeeded(playbackRate)
        record.startRecording()
        val buffer = ByteArray(frameBytes)
        while (running.get()) {
            val read = record.read(buffer, 0, buffer.size)
            if (read > 0) {
                val pcm = buffer.copyOf(read)
                val rms = pcm.rms16le()
                val active = isPlaybackActive()
                onFrame(CaptureFrame(pcm, rms, detectBargeIn(rms, active), active, playbackEchoRms, bargeInSpeechMs))
            }
        }
    }

    fun play(pcm16le: ByteArray, sampleRate: Int) {
        if (pcm16le.isEmpty()) return
        if (sampleRate != playbackRate || audioTrack == null) {
            stopPlayback()
            playbackRate = sampleRate
            startPlaybackIfNeeded(sampleRate)
        }
        val now = System.currentTimeMillis()
        if (!playbackActive.get()) {
            playbackStartedAtMs = now
        }
        val durationMs = pcm16le.size / 2L * 1000L / sampleRate.coerceAtLeast(1)
        playbackActiveUntilMs = maxOf(now, playbackActiveUntilMs) + durationMs
        playbackActive.set(true)
        playbackQueue.offer(pcm16le)
    }

    fun stopPlayback() {
        playbackQueue.clear()
        playbackActive.set(false)
        playbackActiveUntilMs = 0L
        playbackStartedAtMs = 0L
        bargeInSpeechMs = 0
        playbackEchoRms = 0.0
        playbackRunning.set(false)
        playbackThread?.interrupt()
        playbackThread = null
        audioTrack?.pause()
        audioTrack?.flush()
        audioTrack?.stop()
        audioTrack?.release()
        audioTrack = null
    }

    fun playbackSuppressUntil(extraMs: Long): Long {
        val now = System.currentTimeMillis()
        return maxOf(now, playbackActiveUntilMs + PLAYBACK_TAIL_MS) + extraMs
    }

    fun stop() {
        running.set(false)
        recorder?.stop()
        recorder?.release()
        recorder = null
        releaseAudioProcessing()
        stopPlayback()
    }

    private fun enableAudioProcessing(audioSessionId: Int) {
        releaseAudioProcessing()
        if (AcousticEchoCanceler.isAvailable()) {
            echoCanceler = AcousticEchoCanceler.create(audioSessionId)?.also { it.enabled = true }
        }
        if (NoiseSuppressor.isAvailable()) {
            noiseSuppressor = NoiseSuppressor.create(audioSessionId)?.also { it.enabled = true }
        }
        if (AutomaticGainControl.isAvailable()) {
            gainControl = AutomaticGainControl.create(audioSessionId)?.also { it.enabled = true }
        }
    }

    private fun releaseAudioProcessing() {
        echoCanceler?.release()
        echoCanceler = null
        noiseSuppressor?.release()
        noiseSuppressor = null
        gainControl?.release()
        gainControl = null
    }

    private fun startPlaybackIfNeeded(sampleRate: Int) {
        if (playbackThread?.isAlive == true && audioTrack != null) return
        playbackRunning.set(true)
        val minBuffer = AudioTrack.getMinBufferSize(
            sampleRate,
            AudioFormat.CHANNEL_OUT_MONO,
            AudioFormat.ENCODING_PCM_16BIT
        )
        val track = AudioTrack.Builder()
            .setAudioAttributes(
                AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_VOICE_COMMUNICATION)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                    .build()
            )
            .setAudioFormat(
                AudioFormat.Builder()
                    .setSampleRate(sampleRate)
                    .setChannelMask(AudioFormat.CHANNEL_OUT_MONO)
                    .setEncoding(AudioFormat.ENCODING_PCM_16BIT)
                    .build()
            )
            .setTransferMode(AudioTrack.MODE_STREAM)
            .setBufferSizeInBytes(maxOf(minBuffer, sampleRate * 2 / 3))
            .build()
        audioTrack = track
        val audioManager = context.getSystemService(Context.AUDIO_SERVICE) as AudioManager
        audioManager.mode = AudioManager.MODE_IN_COMMUNICATION
        audioManager.isSpeakerphoneOn = true
        audioManager.setStreamVolume(
            AudioManager.STREAM_MUSIC,
            maxOf(audioManager.getStreamVolume(AudioManager.STREAM_MUSIC), audioManager.getStreamMaxVolume(AudioManager.STREAM_MUSIC) / 2),
            0
        )
        track.play()
        playbackRunning.set(true)
        playbackThread = thread(name = "realtime-playback", isDaemon = true) {
            while (playbackRunning.get()) {
                try {
                    val chunk = playbackQueue.take()
                    track.write(chunk, 0, chunk.size)
                } catch (_: InterruptedException) {
                    return@thread
                } catch (_: IllegalStateException) {
                    return@thread
                }
            }
        }
    }

    private fun isPlaybackActive(): Boolean {
        val active = System.currentTimeMillis() <= playbackActiveUntilMs + PLAYBACK_TAIL_MS
        if (!active) {
            playbackActive.set(false)
        }
        return active
    }

    private fun detectBargeIn(rms: Double, isPlaybackActive: Boolean): Boolean {
        if (!isPlaybackActive) {
            playbackActive.set(false)
            bargeInSpeechMs = 0
            playbackEchoRms = 0.0
            return false
        }
        val now = System.currentTimeMillis()
        if (now - playbackStartedAtMs < BARGE_IN_ARM_DELAY_MS) {
            bargeInSpeechMs = 0
            playbackEchoRms = smoothEcho(playbackEchoRms, rms)
            return false
        }
        val dynamicThreshold = maxOf(BARGE_IN_RMS, playbackEchoRms + BARGE_IN_ECHO_MARGIN)
        if (rms >= dynamicThreshold) {
            bargeInSpeechMs += FRAME_MS
        } else {
            bargeInSpeechMs = 0
            playbackEchoRms = smoothEcho(playbackEchoRms, rms)
        }
        if (bargeInSpeechMs < BARGE_IN_MIN_MS) {
            return false
        }
        if (now - lastBargeInAtMs < BARGE_IN_COOLDOWN_MS) {
            return false
        }
        lastBargeInAtMs = now
        playbackActive.set(false)
        playbackActiveUntilMs = 0L
        return true
    }

    private fun smoothEcho(current: Double, rms: Double): Double {
        return if (current <= 0.0) rms else current * 0.92 + rms * 0.08
    }

    companion object {
        private const val FRAME_MS = 20
        private const val BARGE_IN_RMS = 0.060
        private const val BARGE_IN_ECHO_MARGIN = 0.020
        private const val BARGE_IN_MIN_MS = 220
        private const val BARGE_IN_ARM_DELAY_MS = 450L
        private const val BARGE_IN_COOLDOWN_MS = 1200
        private const val PLAYBACK_TAIL_MS = 360L
    }
}

private fun ByteArray.rms16le(): Double {
    if (size < 2) return 0.0
    var sumSquares = 0.0
    var samples = 0
    var i = 0
    while (i + 1 < size) {
        val lo = this[i].toInt() and 0xff
        val hi = this[i + 1].toInt()
        val sample = ((hi shl 8) or lo).toShort().toInt()
        sumSquares += sample.toDouble() * sample
        samples += 1
        i += 2
    }
    return sqrt(sumSquares / samples) / 32768.0
}
