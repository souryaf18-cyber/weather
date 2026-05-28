/**
 * SkyWatch Pro — WebSocket Relay Server v2.0
 * -------------------------------------------
 * Key change from v1: relay serves the dashboard HTML directly
 * from dashboard.html — no round-trip to ESP32 for static content.
 * Only API calls (/data /save /history /download /api/*) are
 * forwarded through the WebSocket tunnel to the ESP32.
 *
 * Deploy on Render / Railway / Fly.io (free tier)
 *
 * Environment variables:
 *   TUNNEL_SECRET   shared secret (must match ws_tunnel.h)
 *   PORT            set automatically by host
 */

const http   = require('http');
const fs     = require('fs');
const path   = require('path');
const WebSocket = require('ws');
const crypto = require('crypto');

// ── Config ────────────────────────────────────────────────────────
const PORT          = process.env.PORT || 3000;
const TUNNEL_SECRET = process.env.TUNNEL_SECRET || '123456';
const REQ_TIMEOUT   = 12000;   // ms — wait for ESP32 response
const WS_PING_MS    = 20000;   // ms — keepalive ping interval
const MAX_QUEUE     = 50;

// ── Load dashboard HTML once at startup ──────────────────────────
const DASHBOARD_PATH = path.join(__dirname, 'dashboard.html');
let   dashboardHtml  = '';
try {
  dashboardHtml = fs.readFileSync(DASHBOARD_PATH, 'utf8');
  console.log(`[RELAY] Dashboard HTML loaded — ${dashboardHtml.length} bytes`);
} catch(e) {
  console.error('[RELAY] WARNING: dashboard.html not found — / will return 404');
}

// ── API paths that must be forwarded to ESP32 ─────────────────────
// Everything else is either served locally or rejected
const ESP32_PATHS = new Set([
  '/data', '/save', '/history', '/download',
  '/api/set', '/api/cal_mq', '/api/scan'
]);

// ── State ─────────────────────────────────────────────────────────
let espSocket    = null;
let espConnected = false;
let espLastSeen  = 0;
let pendingReqs  = new Map();

// ── HTTP server ───────────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  const reqUrl = new URL(req.url, 'http://localhost');
  const path_  = reqUrl.pathname;

  // CORS
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // ── Relay health endpoint (no ESP32 needed) ───────────────────
  if (path_ === '/relay/status') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      relay:         'SkyWatch Relay v2.0',
      esp_connected: espConnected,
      esp_last_seen: espConnected
                     ? 'now'
                     : (espLastSeen ? `${Math.floor((Date.now()-espLastSeen)/1000)}s ago` : 'never'),
      pending_reqs:  pendingReqs.size,
      uptime_s:      Math.floor(process.uptime()),
      secret_set:    TUNNEL_SECRET !== '123456'
    }));
    return;
  }

  // ── Dashboard HTML — served directly, no ESP32 needed ─────────
  if (path_ === '/' || path_ === '/index.html') {
    if (!dashboardHtml) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('dashboard.html not found on relay server.');
      return;
    }
    res.writeHead(200, {
      'Content-Type':  'text/html; charset=utf-8',
      'Cache-Control': 'no-cache'
    });
    res.end(dashboardHtml);
    return;
  }

  // ── API calls — must be forwarded to ESP32 ────────────────────
  if (!ESP32_PATHS.has(path_)) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found: ' + path_);
    return;
  }

  // ESP32 offline?
  if (!espConnected || !espSocket) {
    res.writeHead(503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      error:     'ESP32 offline',
      message:   'SkyWatch node is not connected. Check device power and WiFi.',
      last_seen: espLastSeen
                 ? `${Math.floor((Date.now()-espLastSeen)/1000)}s ago`
                 : 'never'
    }));
    return;
  }

  if (pendingReqs.size >= MAX_QUEUE) {
    res.writeHead(429, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Too many pending requests' }));
    return;
  }

  // Collect POST body
  let body = '';
  req.on('data', chunk => body += chunk.toString());
  req.on('end', () => {
    const reqId = crypto.randomBytes(8).toString('hex');

    const timer = setTimeout(() => {
      if (pendingReqs.has(reqId)) {
        pendingReqs.delete(reqId);
        if (!res.headersSent) {
          res.writeHead(504, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'ESP32 response timeout' }));
        }
      }
    }, REQ_TIMEOUT);

    pendingReqs.set(reqId, { res, timer });

    try {
      espSocket.send(JSON.stringify({
        type:    'http_request',
        id:      reqId,
        method:  req.method,
        path:    path_,
        query:   Object.fromEntries(reqUrl.searchParams),
        headers: req.headers,
        body:    body
      }));
    } catch(e) {
      clearTimeout(timer);
      pendingReqs.delete(reqId);
      if (!res.headersSent) {
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Failed to forward to ESP32', detail: e.message }));
      }
    }
  });
});

// ── WebSocket server ──────────────────────────────────────────────
const wss = new WebSocket.Server({ server: httpServer });

wss.on('connection', (ws, req) => {
  const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;

  // Authenticate
  const secret = req.headers['x-tunnel-secret'];
  if (secret !== TUNNEL_SECRET) {
    console.log(`[WS] Rejected from ${clientIp} — wrong secret (got: "${secret}")`);
    ws.close(4001, 'Unauthorized');
    return;
  }

  // Replace old connection
  if (espSocket && espSocket.readyState === WebSocket.OPEN) {
    console.log('[WS] Replacing old ESP32 connection.');
    espSocket.close(1000, 'Replaced');
  }

  espSocket    = ws;
  espConnected = true;
  espLastSeen  = Date.now();
  console.log(`[WS] ESP32 connected from ${clientIp}`);

  // Keepalive ping
  const pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.ping();
    else clearInterval(pingInterval);
  }, WS_PING_MS);

  ws.on('pong', () => { espLastSeen = Date.now(); });

  // Messages from ESP32
  ws.on('message', (data) => {
    espLastSeen = Date.now();
    let msg;
    try { msg = JSON.parse(data.toString()); }
    catch(e) { console.error('[WS] Bad JSON:', e.message); return; }

    if (msg.type === 'http_response') {
      const pending = pendingReqs.get(msg.id);
      if (!pending) return;
      clearTimeout(pending.timer);
      pendingReqs.delete(msg.id);
      const { res } = pending;
      if (res.headersSent) return;
      res.writeHead(msg.status || 200, {
        'Content-Type':                msg.ctype || 'application/json',
        'Access-Control-Allow-Origin': '*'
      });
      res.end(msg.body || '');

    } else if (msg.type === 'hello') {
      console.log(`[WS] ESP32 hello — node: ${msg.node}, fw: ${msg.fw}`);

    } else if (msg.type === 'pong') {
      espLastSeen = Date.now();
    }
  });

  ws.on('close', (code) => {
    clearInterval(pingInterval);
    if (espSocket === ws) {
      espSocket = null; espConnected = false; espLastSeen = Date.now();
      console.log(`[WS] ESP32 disconnected — code ${code}`);
      pendingReqs.forEach(({ res, timer }) => {
        clearTimeout(timer);
        if (!res.headersSent) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'ESP32 disconnected' }));
        }
      });
      pendingReqs.clear();
    }
  });

  ws.on('error', (err) => {
    console.error('[WS] Socket error:', err.message);
  });
});

// ── Start ─────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`[RELAY] SkyWatch relay v2.0 listening on port ${PORT}`);
  console.log(`[RELAY] Secret: ${TUNNEL_SECRET !== '123456' ? 'CUSTOM SET' : 'WARNING — using default 123456'}`);
  console.log(`[RELAY] Dashboard: ${dashboardHtml.length > 0 ? 'LOADED' : 'MISSING — push dashboard.html'}`);
});

process.on('SIGTERM', () => {
  if (espSocket) espSocket.close(1001, 'Server shutting down');
  httpServer.close(() => process.exit(0));
});
