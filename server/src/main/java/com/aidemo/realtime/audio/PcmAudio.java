package com.aidemo.realtime.audio;

public final class PcmAudio {
    private PcmAudio() {
    }

    public static double rms16le(byte[] pcm) {
        if (pcm.length < 2) {
            return 0.0;
        }
        long sumSquares = 0;
        int samples = pcm.length / 2;
        for (int i = 0; i < samples; i++) {
            int lo = pcm[i * 2] & 0xff;
            int hi = pcm[i * 2 + 1];
            short sample = (short) ((hi << 8) | lo);
            sumSquares += (long) sample * sample;
        }
        double mean = sumSquares / (double) samples;
        return Math.sqrt(mean) / 32768.0;
    }

    public static byte[] sine16le(double frequencyHz, int sampleRate, int durationMs, double amplitude) {
        int samples = sampleRate * durationMs / 1000;
        byte[] out = new byte[samples * 2];
        for (int i = 0; i < samples; i++) {
            double phase = 2.0 * Math.PI * frequencyHz * i / sampleRate;
            short value = (short) Math.round(Math.sin(phase) * amplitude * Short.MAX_VALUE);
            out[i * 2] = (byte) (value & 0xff);
            out[i * 2 + 1] = (byte) ((value >> 8) & 0xff);
        }
        return out;
    }

    public static byte[] silence16le(int sampleRate, int durationMs) {
        int samples = sampleRate * durationMs / 1000;
        return new byte[samples * 2];
    }
}
