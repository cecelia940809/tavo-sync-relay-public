import asyncio
import json
import os
import time
from collections import defaultdict
from aiohttp import web, WSMsgType

HOST = os.environ.get('HOST', '0.0.0.0')
PORT = int(os.environ.get('PORT', '8787'))
MAX_ROOM_PEERS = int(os.environ.get('MAX_ROOM_PEERS', '2'))
MAX_MESSAGE_BYTES = int(os.environ.get('MAX_MESSAGE_BYTES', str(512 * 1024)))
ROOM_IDLE_SECONDS = int(os.environ.get('ROOM_IDLE_SECONDS', '3600'))

rooms = defaultdict(dict)
last_seen = {}
lock = asyncio.Lock()

async def send_json(ws, payload):
    if ws.closed:
        return
    try:
        await ws.send_str(json.dumps(payload, ensure_ascii=False, separators=(',', ':')))
    except Exception:
        pass

async def broadcast_count(room):
    async with lock:
        peers = list(rooms.get(room, {}).values())
        count = len(peers)
    await asyncio.gather(*(send_json(ws, {'op':'peer-count','room':room,'peers':count}) for ws in peers), return_exceptions=True)

async def unregister(room, device_id, ws):
    async with lock:
        if room and rooms.get(room, {}).get(device_id) is ws:
            rooms[room].pop(device_id, None)
            last_seen[room] = time.time()
            if not rooms[room]:
                rooms.pop(room, None)
    if room:
        await broadcast_count(room)

async def health(request):
    # aiohttp automatically serves HEAD for this GET route as well.
    return web.json_response({'ok': True, 'service': 'tavo-sync-relay', 'version': '0.2.1'})

async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=MAX_MESSAGE_BYTES)
    await ws.prepare(request)
    room = None
    device_id = None
    try:
        async for item in ws:
            if item.type == WSMsgType.BINARY:
                if len(item.data) > MAX_MESSAGE_BYTES:
                    await send_json(ws, {'op':'error','message':'message too large'})
                    continue
                raw = item.data.decode('utf-8', 'replace')
            elif item.type == WSMsgType.TEXT:
                raw = item.data
                if len(raw.encode('utf-8')) > MAX_MESSAGE_BYTES:
                    await send_json(ws, {'op':'error','message':'message too large'})
                    continue
            else:
                continue

            try:
                msg = json.loads(raw)
            except Exception:
                await send_json(ws, {'op':'error','message':'invalid json'})
                continue

            op = msg.get('op')
            if op == 'join':
                candidate = str(msg.get('room', ''))
                did = str(msg.get('deviceId', ''))[:128]
                if len(candidate) != 6 or not candidate.isdigit() or not did:
                    await send_json(ws, {'op':'error','message':'invalid room or device'})
                    continue
                async with lock:
                    bucket = rooms[candidate]
                    existing = bucket.get(did)
                    if existing and existing is not ws:
                        try:
                            await existing.close(code=4001, message=b'replaced by reconnect')
                        except Exception:
                            pass
                        bucket.pop(did, None)
                    if did not in bucket and len(bucket) >= MAX_ROOM_PEERS:
                        await send_json(ws, {'op':'error','message':'room is full'})
                        continue
                    bucket[did] = ws
                    room, device_id = candidate, did
                    count = len(bucket)
                    last_seen[room] = time.time()
                await send_json(ws, {'op':'joined','room':room,'peers':count})
                await broadcast_count(room)
                continue

            if op == 'ping':
                await send_json(ws, {'op':'pong','at':int(time.time()*1000)})
                continue

            if op == 'event':
                if not room or msg.get('room') != room or msg.get('deviceId') != device_id:
                    await send_json(ws, {'op':'error','message':'join room first'})
                    continue
                target = msg.get('target')
                async with lock:
                    bucket = dict(rooms.get(room, {}))
                    last_seen[room] = time.time()
                recipients = [peer for did, peer in bucket.items() if did != device_id and (not target or did == target)]
                if recipients:
                    await asyncio.gather(*(peer.send_str(raw) for peer in recipients if not peer.closed), return_exceptions=True)
                continue

            await send_json(ws, {'op':'error','message':'unsupported op'})
    finally:
        await unregister(room, device_id, ws)
    return ws

async def cleanup_task(app):
    try:
        while True:
            await asyncio.sleep(60)
            cutoff = time.time() - ROOM_IDLE_SECONDS
            async with lock:
                for room in list(last_seen):
                    if room not in rooms and last_seen[room] < cutoff:
                        last_seen.pop(room, None)
    except asyncio.CancelledError:
        pass

async def on_startup(app):
    app['cleanup_task'] = asyncio.create_task(cleanup_task(app))

async def on_cleanup(app):
    task = app.get('cleanup_task')
    if task:
        task.cancel()
        await task

app = web.Application(client_max_size=MAX_MESSAGE_BYTES)
app.router.add_get('/health', health)
app.router.add_get('/', websocket_handler)
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)

if __name__ == '__main__':
    print(f'Tavo Sync Relay v0.2.1 listening on {HOST}:{PORT}')
    web.run_app(app, host=HOST, port=PORT, print=None)
