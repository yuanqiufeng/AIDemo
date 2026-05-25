package com.aidemo.android.realtime

import org.json.JSONObject

data class ServerEvent(
    val type: String,
    val sessionId: String?,
    val seq: Long?,
    val text: String?,
    val audio: String?,
    val sampleRate: Int?,
    val reason: String?
) {
    companion object {
        fun parse(json: String): ServerEvent {
            val obj = JSONObject(json)
            return ServerEvent(
                type = obj.optString("type"),
                sessionId = obj.optString("sessionId").takeIf { it.isNotBlank() },
                seq = obj.optLongOrNull("seq"),
                text = obj.optString("text").takeIf { it.isNotBlank() },
                audio = obj.optString("audio").takeIf { it.isNotBlank() },
                sampleRate = obj.optIntOrNull("sampleRate"),
                reason = obj.optString("reason").takeIf { it.isNotBlank() }
            )
        }
    }
}

private fun JSONObject.optLongOrNull(name: String): Long? =
    if (has(name) && !isNull(name)) optLong(name) else null

private fun JSONObject.optIntOrNull(name: String): Int? =
    if (has(name) && !isNull(name)) optInt(name) else null
