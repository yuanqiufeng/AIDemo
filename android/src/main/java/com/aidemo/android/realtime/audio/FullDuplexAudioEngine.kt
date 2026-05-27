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

class FullDuplexAudioEngine(private val context: Context) {
    private val sampleRate = 16000
    private val channelConfig = AudioFormat.CHANNEL_IN_MONO
    private val audioFormat = AudioFormat.ENCODING_PCM_16BIT
    private val frameBytes = sampleRate / 50 * 2
    private val running = AtomicBoolean(false)
    private val playbackRunning = AtomicBoolean(false)
    private val playbackQueue = LinkedBlockingQueue<ByteArray>()
    private var recorder: AudioRecord? = null
    private var audioTrack: AudioTrack? = null
    private var playbackThread: Thread? = null
    private var playbackRate = 24000

    @SuppressLint("MissingPermission")
    fun startCapture(onFrame: (ByteArray) -> Unit) {
        if (ContextCompat.checkSelfPermission(context, Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED) {
            return
        }
        stop()
        val minBuffer = AudioRecord.getMinBufferSize(sampleRate, channelConfig, audioFormat)
        val bufferSize = maxOf(minBuffer, frameBytes * 8)
        val record = AudioRecord(
            MediaRecorder.AudioSource.MIC,
            sampleRate,
            channelConfig,
            audioFormat,
            bufferSize
        )
        recorder = record
        val audioManager = context.getSystemService(Context.AUDIO_SERVICE) as AudioManager
        audioManager.mode = AudioManager.MODE_NORMAL
        running.set(true)
        startPlaybackIfNeeded(playbackRate)
        record.startRecording()
        val buffer = ByteArray(frameBytes)
        while (running.get()) {
            val read = record.read(buffer, 0, buffer.size)
            if (read > 0) {
                onFrame(buffer.copyOf(read))
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
        playbackQueue.offer(pcm16le)
    }

    fun stopPlayback() {
        playbackQueue.clear()
        playbackRunning.set(false)
        playbackThread?.interrupt()
        playbackThread = null
        audioTrack?.pause()
        audioTrack?.flush()
        audioTrack?.stop()
        audioTrack?.release()
        audioTrack = null
    }

    fun stop() {
        running.set(false)
        recorder?.stop()
        recorder?.release()
        recorder = null
        stopPlayback()
    }

    private fun enableAudioProcessing(audioSessionId: Int) {
        if (AcousticEchoCanceler.isAvailable()) {
            AcousticEchoCanceler.create(audioSessionId)?.enabled = true
        }
        if (NoiseSuppressor.isAvailable()) {
            NoiseSuppressor.create(audioSessionId)?.enabled = true
        }
        if (AutomaticGainControl.isAvailable()) {
            AutomaticGainControl.create(audioSessionId)?.enabled = true
        }
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
                    .setUsage(AudioAttributes.USAGE_MEDIA)
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
            .setBufferSizeInBytes(maxOf(minBuffer, sampleRate / 5 * 2))
            .build()
        audioTrack = track
        val audioManager = context.getSystemService(Context.AUDIO_SERVICE) as AudioManager
        audioManager.mode = AudioManager.MODE_NORMAL
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
}
