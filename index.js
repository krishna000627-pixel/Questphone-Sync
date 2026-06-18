const express = require('express');
const fs = require('fs');
const path = require('path');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
app.use(express.json({ limit: '10mb' }));

const PORT = process.env.PORT || 10000;
const SYNC_TOKEN = process.env.SYNC_TOKEN || 'changeme';
const DATA_DIR = path.join(__dirname, 'data');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

function auth(req, res, next) {
    const token = req.headers['x-sync-token'];
    if (token !== SYNC_TOKEN) return res.status(401).json({ error: 'Unauthorized' });
    next();
}

function filePath(deviceId) {
    const safe = deviceId.replace(/[^a-zA-Z0-9_-]/g, '_');
    return path.join(DATA_DIR, `${safe}.json`);
}

function readData(deviceId) {
    const fp = filePath(deviceId);
    if (!fs.existsSync(fp)) return null;
    try { return JSON.parse(fs.readFileSync(fp, 'utf8')); } catch { return null; }
}

function writeData(deviceId, data) {
    fs.writeFileSync(filePath(deviceId), JSON.stringify(data), 'utf8');
}

// ── Live connection registry ───────────────────────────────────────
// Multiple devices can share a deviceId (linked devices), so each id
// maps to a Set of open sockets, not a single socket.
const liveClients = new Map(); // deviceId -> Set<WebSocket>

function registerClient(deviceId, ws) {
    if (!liveClients.has(deviceId)) liveClients.set(deviceId, new Set());
    liveClients.get(deviceId).add(ws);
}

function unregisterClient(deviceId, ws) {
    const set = liveClients.get(deviceId);
    if (!set) return;
    set.delete(ws);
    if (set.size === 0) liveClients.delete(deviceId);
}

// Tell every other live connection for this deviceId that fresh data exists.
// excludeWs lets the pushing device's own socket skip notifying itself.
function notifyUpdate(deviceId, serverUpdatedAt, excludeWs = null) {
    const set = liveClients.get(deviceId);
    if (!set) return;
    const payload = JSON.stringify({ type: 'update', serverUpdatedAt });
    for (const client of set) {
        if (client === excludeWs) continue;
        if (client.readyState === client.OPEN) client.send(payload);
    }
}

// ── Health check ─────────────────────────────────────────────────
app.get('/', (req, res) => {
    res.json({ status: 'ok', service: 'QuestPhone Sync', version: 1 });
});

// ── Sync routes ───────────────────────────────────────────────────
app.post('/sync/push', auth, (req, res) => {
    const deviceId = req.headers['x-device-id'];
    if (!deviceId) return res.status(400).json({ error: 'x-device-id header required' });
    const body = req.body;
    if (!body || !body.userInfo || !Array.isArray(body.quests))
        return res.status(400).json({ error: 'Invalid backup format' });
    body.serverUpdatedAt = Date.now();
    writeData(deviceId, body);
    notifyUpdate(deviceId, body.serverUpdatedAt);
    res.json({ success: true, questCount: body.quests.length, serverUpdatedAt: body.serverUpdatedAt });
});

app.get('/sync/pull', auth, (req, res) => {
    const deviceId = req.headers['x-device-id'];
    if (!deviceId) return res.status(400).json({ error: 'x-device-id header required' });
    const data = readData(deviceId);
    if (!data) return res.status(404).json({ error: 'No backup found for this device' });
    res.json(data);
});

app.get('/sync/check', auth, (req, res) => {
    const deviceId = req.headers['x-device-id'];
    if (!deviceId) return res.status(400).json({ error: 'x-device-id header required' });
    const clientUpdatedAt = parseInt(req.query.clientUpdatedAt || '0');
    const data = readData(deviceId);
    if (!data) return res.json({ hasUpdate: false });
    const serverUpdatedAt = data.serverUpdatedAt || 0;
    res.json({ hasUpdate: serverUpdatedAt > clientUpdatedAt, serverUpdatedAt });
});

// ── Update routes ─────────────────────────────────────────────────
const VERSION_FILE = path.join(DATA_DIR, 'latest_version.json');

// Called by GitHub Actions after a successful release
app.post('/notify-update', auth, (req, res) => {
    const { versionCode, versionName, downloadUrl, changelog } = req.body;
    if (!versionCode || !downloadUrl)
        return res.status(400).json({ error: 'versionCode and downloadUrl required' });
    const info = { versionCode, versionName, downloadUrl, changelog: changelog || '', publishedAt: Date.now() };
    fs.writeFileSync(VERSION_FILE, JSON.stringify(info), 'utf8');
    console.log(`New version published: v${versionName} (${versionCode})`);
    res.json({ ok: true });
});

// Polled by app on launch
app.get('/latest-version', auth, (req, res) => {
    if (!fs.existsSync(VERSION_FILE))
        return res.status(404).json({ error: 'No version info yet' });
    res.json(JSON.parse(fs.readFileSync(VERSION_FILE, 'utf8')));
});

// ── Live WebSocket endpoint ─────────────────────────────────────────
// Path: wss://<host>/sync/live?token=<SYNC_TOKEN>&deviceId=<deviceId>
// (query params, not headers — not all WS handshakes reliably carry
// custom headers, especially through proxies/load balancers like Render's)
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
    let url;
    try { url = new URL(req.url, 'http://placeholder'); } catch { socket.destroy(); return; }
    if (url.pathname !== '/sync/live') { socket.destroy(); return; }

    const token = url.searchParams.get('token');
    const deviceId = url.searchParams.get('deviceId');
    if (token !== SYNC_TOKEN || !deviceId) {
        socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
        socket.destroy();
        return;
    }

    wss.handleUpgrade(req, socket, head, (ws) => {
        ws.deviceId = deviceId;
        registerClient(deviceId, ws);

        // Heartbeat so dead connections (phone lost signal, etc.) get
        // cleaned up instead of sitting in the registry forever.
        ws.isAlive = true;
        ws.on('pong', () => { ws.isAlive = true; });

        ws.on('message', (raw) => {
            // Currently the only inbound message we care about is a push
            // notification trigger; clients still use the REST /sync/push
            // for the actual data upload. This lets a client optimistically
            // notify its other sessions without waiting on a REST round trip
            // if it ever wants to (not required for current Android client).
            try {
                const msg = JSON.parse(raw.toString());
                if (msg.type === 'ping') ws.send(JSON.stringify({ type: 'pong' }));
            } catch { /* ignore malformed messages */ }
        });

        ws.on('close', () => unregisterClient(deviceId, ws));
        ws.on('error', () => unregisterClient(deviceId, ws));
    });
});

// Ping all sockets periodically; drop ones that didn't pong back.
const HEARTBEAT_INTERVAL_MS = 30000;
setInterval(() => {
    for (const set of liveClients.values()) {
        for (const ws of set) {
            if (!ws.isAlive) { ws.terminate(); continue; }
            ws.isAlive = false;
            ws.ping();
        }
    }
}, HEARTBEAT_INTERVAL_MS);

server.listen(PORT, () => console.log(`QuestPhone Sync running on port ${PORT}`));
