package com.aidemo.android

import android.Manifest
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.viewModels
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import com.aidemo.android.realtime.RealtimeViewModel
import com.aidemo.android.realtime.RealtimeViewModel.Companion.DEFAULT_SERVER_URL

class MainActivity : ComponentActivity() {
    private val viewModel: RealtimeViewModel by viewModels()
    private val permissionLauncher = registerForActivityResult(ActivityResultContracts.RequestPermission()) { granted ->
        if (granted) {
            viewModel.connect()
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent {
            RealtimeApp(viewModel, onStart = {
                permissionLauncher.launch(Manifest.permission.RECORD_AUDIO)
            })
        }
    }

    override fun onDestroy() {
        viewModel.disconnect()
        super.onDestroy()
    }
}

@Composable
private fun RealtimeApp(viewModel: RealtimeViewModel, onStart: () -> Unit) {
    val ui by viewModel.ui.collectAsState()
    var serverUrl by remember { mutableStateOf(DEFAULT_SERVER_URL) }
    var prompt by remember { mutableStateOf("你好，帮我介绍一下这个本地语音助手架构") }
    val logScroll = rememberScrollState()

    LaunchedEffect(ui.events.size) {
        logScroll.animateScrollTo(logScroll.maxValue)
    }

    MaterialTheme {
        Surface(modifier = Modifier.fillMaxSize()) {
            Column(
                modifier = Modifier
                    .fillMaxSize()
                    .padding(16.dp),
                verticalArrangement = Arrangement.spacedBy(10.dp)
            ) {
                Text(
                    "AI 实时语音通话 Demo",
                    modifier = Modifier.fillMaxWidth(),
                    style = MaterialTheme.typography.titleLarge,
                    fontWeight = FontWeight.SemiBold,
                    textAlign = TextAlign.Center
                )
                Text(
                    connectionText(ui.state),
                    modifier = Modifier.fillMaxWidth(),
                    style = MaterialTheme.typography.titleMedium,
                    textAlign = TextAlign.Center,
                    color = stateColor(ui.state, ui.connected)
                )
                OutlinedTextField(
                    value = serverUrl,
                    onValueChange = {
                        serverUrl = it
                        viewModel.updateServerUrl(it)
                    },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                    label = { Text("Server WebSocket") }
                )
                OutlinedTextField(
                    value = prompt,
                    onValueChange = { prompt = it },
                    modifier = Modifier.fillMaxWidth(),
                    minLines = 2,
                    label = { Text("手动文本测试") }
                )
                Text(
                    if (ui.connected) "请说话，后端会判断你是否说完。" else "启动后请确认 adb reverse 或真机局域网地址正确。",
                    style = MaterialTheme.typography.bodyMedium,
                    color = Color(0xFF555555)
                )
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    Button(
                        onClick = onStart,
                        enabled = !ui.connected,
                        modifier = Modifier.weight(1f)
                    ) {
                        Text("开始通话")
                    }
                    Button(
                        onClick = { viewModel.disconnect() },
                        enabled = ui.connected,
                        modifier = Modifier.weight(1f),
                        colors = ButtonDefaults.buttonColors(containerColor = Color(0xFF666666))
                    ) {
                        Text("挂断")
                    }
                }
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    Button(
                        onClick = { viewModel.sendText(prompt) },
                        enabled = ui.connected,
                        modifier = Modifier.weight(1f)
                    ) {
                        Text("文本发送")
                    }
                    TextButton(
                        onClick = { viewModel.interrupt() },
                        enabled = ui.connected,
                        modifier = Modifier.weight(1f)
                    ) {
                        Text("打断")
                    }
                }
                Text("识别：${ui.asrText.ifBlank { "等待语音输入..." }}")
                Text("回复：${ui.aiText.ifBlank { "等待 AI 回复..." }}")
                Box(
                    modifier = Modifier
                        .weight(1f)
                        .fillMaxWidth()
                        .verticalScroll(logScroll)
                ) {
                    Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
                        Text(
                            "后端启动后点“开始通话”。你说完一句，后端 VAD 会触发 utterance_end，然后返回 AI 回复并播放。\n",
                            style = MaterialTheme.typography.bodySmall,
                            color = Color(0xFF666666)
                        )
                        ui.events.takeLast(120).forEach {
                            Text("[${it}]", style = MaterialTheme.typography.bodySmall)
                        }
                    }
                }
            }
        }
    }
}

private fun connectionText(state: String): String = when (state) {
    "idle" -> "未通话"
    "connecting" -> "WebSocket 连接中"
    "listening" -> "正在监听"
    "user speaking" -> "检测到你开始说话"
    "thinking" -> "AI 正在处理"
    "assistant speaking" -> "AI 正在说话，可打断"
    "interrupted" -> "已打断，恢复监听"
    "error" -> "WebSocket 错误"
    else -> state
}

private fun stateColor(state: String, connected: Boolean): Color = when {
    state == "error" -> Color(0xFFC62828)
    connected -> Color(0xFF2E7D32)
    else -> Color(0xFF555555)
}
