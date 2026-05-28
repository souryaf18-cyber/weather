/**
 * SkyWatch Pro — WebSocket Relay Server
 * --------------------------------------
 * Bridges remote browsers to the ESP32 through a persistent
 * outbound WebSocket connection from the ESP32.
 *
 * Deploy free on: Render / Railway / Fly.io
 *
 * Environment variables to set on your hosting platform:
 *   TUNNEL_SECRET   — shared secret between relay and ESP32 (any strong string)
 *   PORT            — set automatically by host, fallback 3000
 */

const http      = require('http');
const WebSocket = require('ws');
const crypto    = require('crypto');

// ── Config ───────────────────────────────────────────────────────
const PORT          = process.env.PORT || 3000;
const TUNNEL_SECRET = process.env.TUNNEL_SECRET || 'changeme_set_in_env';
const REQ_TIMEOUT   = 12000;   // ms — how long to wait for ESP32 response
const WS_PING_MS    = 20000;   // ms — keepalive ping to ESP32
const MAX_QUEUE     = 50;      // max pending requests before rejecting

// ── State ────────────────────────────────────────────────────────
let espSocket     = null;   // the single ESP32 WebSocket connection
let espConnected  = false;
let pendingReqs   = new Map(); // reqId → { res, timer }
let espLastSeen   = 0;

// ── HTTP server ──────────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  const reqUrl  = new URL(req.url, 'http://localhost');
  const path    = reqUrl.pathname;

  // ── CORS headers (needed for browser fetch) ──────────────────
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // ── Health / status endpoint (relay itself, no ESP32 needed) ──
  if (path === '/relay/status') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      relay:        'SkyWatch Relay v1.0',
      esp_connected: espConnected,
      esp_last_seen: espConnected ? 'now' : `${Math.floor((Date.now()-espLastSeen)/1000)}s ago`,
      pending_reqs:  pendingReqs.size,
      uptime_s:      Math.floor(process.uptime())
    }));
    return;
  }

  // ── If ESP32 not connected, return 503 immediately ────────────
  if (!espConnected || !espSocket) {
    res.writeHead(503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      error:   'ESP32 offline',
      message: 'SkyWatch node is not connected. Check device power and WiFi.',
      last_seen: espLastSeen ? `${Math.floor((Date.now()-espLastSeen)/1000)}s ago` : 'never'
    }));
    return;
  }

  // ── Too many queued requests ──────────────────────────────────
  if (pendingReqs.size >= MAX_QUEUE) {
    res.writeHead(429, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Too many pending requests' }));
    return;
  }

  // ── Collect POST body if any ──────────────────────────────────
  let body = '';
  req.on('data', chunk => body += chunk.toString());
  req.on('end', () => {

    // Build a unique request ID
    const reqId = crypto.randomBytes(8).toString('hex');

    // Package the request as a JSON message for ESP32
    const msg = JSON.stringify({
      type:    'http_request',
      id:      reqId,
      method:  req.method,
      path:    path,
      query:   Object.fromEntries(reqUrl.searchParams),
      headers: req.headers,
      body:    body
    });

    // Timeout — if ESP32 doesn't respond in time
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

    // Forward to ESP32
    try {
      espSocket.send(msg);
    } catch(e) {
      clearTimeout(timer);
      pendingReqs.delete(reqId);
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Failed to forward to ESP32', detail: e.message }));
    }
  });
});

// ── WebSocket server ─────────────────────────────────────────────
const wss = new WebSocket.Server({ server: httpServer });

wss.on('connection', (ws, req) => {
  const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;

  // ── Authenticate ESP32 via secret in header ──────────────────
  const secret = req.headers['x-tunnel-secret'];
  if (secret !== TUNNEL_SECRET) {
    console.log(`[WS] Rejected connection from ${clientIp} — wrong secret`);
    ws.close(4001, 'Unauthorized');
    return;
  }

  // ── Only allow one ESP32 connection at a time ────────────────
  if (espSocket && espSocket.readyState === WebSocket.OPEN) {
    console.log(`[WS] Closing old ESP32 connection, new one from ${clientIp}`);
    espSocket.close(1000, 'Replaced by new connection');
  }

  espSocket    = ws;
  espConnected = true;
  espLastSeen  = Date.now();
  console.log(`[WS] ESP32 connected from ${clientIp}`);

  // ── Keepalive ping ────────────────────────────────────────────
  const pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    } else {
      clearInterval(pingInterval);
    }
  }, WS_PING_MS);

  ws.on('pong', () => {
    espLastSeen = Date.now();
  });

  // ── Handle messages from ESP32 ────────────────────────────────
  ws.on('message', (data) => {
    espLastSeen = Date.now();

    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch(e) {
      console.error('[WS] Bad JSON from ESP32:', e.message);
      return;
    }

    if (msg.type === 'http_response') {
      const pending = pendingReqs.get(msg.id);
      if (!pending) return; // already timed out

      clearTimeout(pending.timer);
      pendingReqs.delete(msg.id);

      const { res } = pending;
      if (res.headersSent) return;

      // Write response back to browser
      const status  = msg.status  || 200;
      const ctype   = msg.ctype   || 'application/json';
      const body    = msg.body    || '';

      res.writeHead(status, {
        'Content-Type':                ctype,
        'Access-Control-Allow-Origin': '*',
        'X-ESP32-Latency':            `${Date.now() - espLastSeen}ms`
      });
      res.end(body);

    } else if (msg.type === 'ping') {
      // ESP32 sending its own keepalive — just update timestamp
      espLastSeen = Date.now();

    } else {
      console.log('[WS] Unknown message type:', msg.type);
    }
  });

  ws.on('close', (code, reason) => {
    clearInterval(pingInterval);
    if (espSocket === ws) {
      espSocket    = null;
      espConnected = false;
      espLastSeen  = Date.now();
      console.log(`[WS] ESP32 disconnected — code ${code}`);
      // Fail all pending requests
      pendingReqs.forEach(({ res, timer }, id) => {
        clearTimeout(timer);
        if (!res.headersSent) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'ESP32 disconnected mid-request' }));
        }
      });
      pendingReqs.clear();
    }
  });

  ws.on('error', (err) => {
    console.error('[WS] ESP32 socket error:', err.message);
  });
});

// ── Start ────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`[RELAY] SkyWatch relay listening on port ${PORT}`);
  console.log(`[RELAY] Secret configured: ${TUNNEL_SECRET !== 'changeme_set_in_env' ? 'YES' : 'WARNING — using default, set TUNNEL_SECRET env var'}`);
});

// ── Graceful shutdown ────────────────────────────────────────────
process.on('SIGTERM', () => {
  console.log('[RELAY] SIGTERM received, shutting down...');
  if (espSocket) espSocket.close(1001, 'Server shutting down');
  httpServer.close(() => process.exit(0));
});
