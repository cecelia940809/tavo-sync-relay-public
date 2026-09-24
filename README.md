# Tavo Sync Relay

这是 `跨设备实时同步 v0.2.1` 的 WebSocket 中继服务。

## 本地运行

```bash
pip install -r requirements.txt
python server.py
```

默认监听 `0.0.0.0:8787`。

## 生产部署

建议部署到支持 WebSocket 的 HTTPS 平台，并获得 `wss://...` 地址。容器启动命令已经写在 Dockerfile 中。

环境变量：
- `PORT`：端口，默认 8787
- `MAX_ROOM_PEERS`：每个验证码房间最大设备数，默认 2
- `MAX_MESSAGE_BYTES`：单条 WebSocket 消息上限，默认 512 KiB
- `ROOM_IDLE_SECONDS`：空房间元数据清理时间，默认 3600 秒

部署完成后，把平台提供的 HTTPS 域名改成 `wss://`，例如：

`https://sync.example.com` -> `wss://sync.example.com`

然后在电脑和手机插件的「高级 · 同步服务地址」里填写同一个地址。

## 协议

客户端先发送：
```json
{"op":"join","room":"123456","deviceId":"device-a"}
```

随后发送事件：
```json
{"op":"event","room":"123456","deviceId":"device-a","kind":"message:add","payload":{}}
```

服务端只负责同房间转发，不读取或保存聊天历史。


## v0.2.1
- Health endpoint now supports Render GET/HEAD checks without WebSocket handshake errors.
- WebSocket relay remains available at the service root (`wss://your-service.onrender.com/`).
