require('dotenv').config();
const express   = require('express');
const cors      = require('cors');
const http      = require('http');
const WebSocket = require('ws');

const app    = express();
const server = http.createServer(app);

const {
  VK_CLIENT_ID     = '',
  VK_CLIENT_SECRET = '',
  FRONTEND_URL     = 'http://localhost:5500',
  PORT             = 3000,
  VK_CHANNEL       = 'fasolka',
} = process.env;

const VK_API       = 'https://api.live.vkvideo.ru/v1';
const VK_AUTH_URL  = 'https://auth.live.vkvideo.ru/app/oauth2/authorize';
const VK_TOKEN_URL = 'https://auth.live.vkvideo.ru/app/oauth2/token';

// Всегда https — Railway работает за прокси и req.protocol может быть http
function getRedirectUri(req) {
  return `https://${req.get('host')}/auth/callback`;
}

/* ── CORS ── */
app.use(cors({
  origin: (origin, cb) => {
    const allowed = [FRONTEND_URL, 'http://localhost:5500', 'http://127.0.0.1:5500'];
    if (!origin || allowed.includes(origin)) return cb(null, true);
    cb(new Error('Not allowed by CORS'));
  },
  credentials: true,
}));
app.use(express.json());

/* ── СОСТОЯНИЕ ── */
const clients  = new Set();
const sessions = new Map();
let vkWs         = null;
let vkWsUrl      = null;
let vkRetry      = 0;
let vkRetryTimer = null;

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

function broadcast(payload) {
  const data = JSON.stringify(payload);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  }
}

/* ── VK WebSocket (Centrifugo) ── */
async function getVkStreamData(token) {
  const headers = { 'User-Agent': 'Mozilla/5.0' };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const r = await fetch(`${VK_API}/blog/${VK_CHANNEL}/public_video_stream`, { headers });
  if (!r.ok) throw new Error(`VK API ${r.status}`);
  const data = await r.json();

  // Из debug: wsChatChannel = "channel-chat:2960797"
  const chatChannel = data?.wsChatChannel;
  if (!chatChannel) throw new Error('wsChatChannel не найден');

  // Получаем JWT токен для Centrifugo
  // VK отдаёт его через /v1/ws/connect (POST)
  let wsToken = null;
  try {
    const wsHeaders = { 'Content-Type': 'application/json' };
    if (token) wsHeaders['Authorization'] = `Bearer ${token}`;

    // Пробуем несколько эндпоинтов
    const tokenEndpoints = [
      `${VK_API}/ws/connect`,
      `${VK_API}/centrifugo/token`,
      `${VK_API}/user/public_websocket_token`,
    ];

    for (const ep of tokenEndpoints) {
      try {
        const res = await fetch(ep, {
          method: 'POST',
          headers: wsHeaders,
          body: JSON.stringify({ channels: [chatChannel] }),
        });
        log('Token endpoint', ep, 'status:', res.status);
        if (res.ok) {
          const d = await res.json();
          wsToken = d?.token || d?.data?.token || d?.wsToken;
          if (wsToken) { log('Got WS token from', ep); break; }
        }
      } catch(e) { log('Token ep error:', ep, e.message); }
    }
  } catch(e) { log('WS token error:', e.message); }

  return {
    chatChannel,
    wsToken,
    isOnline: !!data?.isOnline,
    viewers: data?.count?.viewers,
  };
}

async function connectVkWs(token) {
  if (vkRetryTimer) { clearTimeout(vkRetryTimer); vkRetryTimer = null; }
  if (vkWs) { try { vkWs.terminate(); } catch(e) {} vkWs = null; }

  let sd;
  try { sd = await getVkStreamData(token); }
  catch(e) {
    log('getVkStreamData error:', e.message);
    broadcast({ type: 'STATUS', status: 'offline', message: e.message });
    vkRetryTimer = setTimeout(() => connectVkWs(token), 30000);
    return;
  }

  if (!sd.isOnline) {
    log('Stream offline');
    broadcast({ type: 'STATUS', status: 'offline', message: 'Стрим офлайн · чат будет доступен во время трансляции' });
    vkRetryTimer = setTimeout(() => connectVkWs(token), 30000);
    return;
  }

  log('Connecting to Centrifugo, channel:', sd.chatChannel, 'wsToken:', !!sd.wsToken);
  broadcast({ type: 'STATUS', status: 'connecting' });

  // Передаём cf_protocol_version=v2 как делает браузер VK
  vkWs = new WebSocket(
    'wss://centrifugo.live.vkvideo.ru/connection/websocket?cf_protocol_version=v2',
    { headers: { 'Origin': 'https://live.vkvideo.ru' } }
  );

  let msgId = 1;
  const chatChannel = sd.chatChannel;

  vkWs.on('open', () => {
    log('Centrifugo WS open');
    vkRetry = 0;
    // Отправляем connect с токеном (если есть) или без
    const connectCmd = { connect: {}, id: msgId++ };
    if (sd.wsToken) connectCmd.connect.token = sd.wsToken;
    vkWs.send(JSON.stringify(connectCmd));
  });

  vkWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      log('Centrifugo msg:', JSON.stringify(msg).slice(0, 300));

      // Ответ на connect (id=1) — подписываемся на чат
      if (msg.id === 1 && (msg.connect !== undefined || msg.result !== undefined)) {
        log('Connected to Centrifugo, subscribing to', chatChannel);
        vkWs.send(JSON.stringify({ subscribe: { channel: chatChannel }, id: msgId++ }));
        broadcast({ type: 'STATUS', status: 'connected' });
        return;
      }

      // Ответ на subscribe — история
      if (msg.subscribe !== undefined || (msg.result && msg.result.channel)) {
        log('Subscribed to', chatChannel);
        const pubs = msg.subscribe?.publications || msg.result?.publications || [];
        const messages = pubs.map(p => parseMsg(p?.data)).filter(Boolean);
        if (messages.length) broadcast({ type: 'HISTORY', messages });
        return;
      }

      // Push — новое сообщение
      // Формат v2: {"push":{"channel":"channel-chat:...","pub":{"data":{...}}}}
      if (msg.push) {
        const pub = msg.push?.pub || msg.push?.message;
        if (pub?.data) {
          const parsed = parseMsg(pub.data);
          if (parsed) broadcast({ type: 'MESSAGE', message: parsed });
        }
        return;
      }

      // Fallback
      handleVkMessage(msg);
    } catch(e) { log('WS parse error:', e.message); }
  });

  vkWs.on('close', (code, reason) => {
    log('Centrifugo closed:', code, reason.toString().slice(0, 100));
    broadcast({ type: 'STATUS', status: 'reconnecting' });
    scheduleReconnect(token);
  });

  vkWs.on('error', (e) => log('Centrifugo error:', e.message));
}

function scheduleReconnect(token) {
  const delay = Math.min(1000 * Math.pow(2, vkRetry++), 30000);
  vkRetryTimer = setTimeout(() => connectVkWs(token), delay);
}

function handleVkMessage(msg) {
  const type = msg.type || msg.event;
  const data = msg.data || msg;

  if (type === 'history' && Array.isArray(data)) {
    const messages = data.map(parseMsg).filter(Boolean);
    if (messages.length) broadcast({ type: 'HISTORY', messages });
    return;
  }
  if (type === 'message' || type === 'CHAT_MESSAGE' || data?.author) {
    const parsed = parseMsg(data);
    if (parsed) broadcast({ type: 'MESSAGE', message: parsed });
    return;
  }
  if (Array.isArray(msg)) {
    const messages = msg.map(parseMsg).filter(Boolean);
    if (messages.length) broadcast({ type: 'HISTORY', messages });
    return;
  }
  if (Array.isArray(msg?.messages)) {
    const messages = msg.messages.map(parseMsg).filter(Boolean);
    if (messages.length) broadcast({ type: 'HISTORY', messages });
  }
}

function parseMsg(msg) {
  if (!msg) return null;
  const author  = msg.author || msg.user || {};
  const name    = author.displayName || author.nick || author.name || 'Аноним';
  const avatar  = author.avatarUrl   || author.avatar || null;
  const isOwner = !!(author.isBroadcaster || author.roles?.includes('STREAMER'));
  const id      = msg.id || `${name}:${Date.now()}:${Math.random()}`;

  const parts = msg.data || msg.parts || [];
  let text = '';
  const smiles = [];

  if (Array.isArray(parts) && parts.length) {
    parts.forEach(p => {
      if (p.type === 'text')       text += (p.content?.[0] ?? p.text ?? '');
      else if (p.type === 'smile') { text += `:${p.name}:`; smiles.push({ name: p.name, url: p.smallUrl || p.url }); }
      else if (p.type === 'mention') text += `@${p.displayName || p.nick || ''}`;
      else if (p.type === 'link')    text += p.url;
    });
  } else {
    text = String(msg.text || msg.content || msg.message || '');
  }

  if (!text.trim()) return null;
  return { id, name, avatar, text: text.trim(), smiles, isOwner };
}

/* ── OAUTH ── */
app.get('/auth/login', (req, res) => {
  if (!VK_CLIENT_ID) return res.status(500).send('VK_CLIENT_ID не задан');

  const redirectUri = getRedirectUri(req);
  const params = new URLSearchParams({
    response_type: 'code',
    client_id:     VK_CLIENT_ID.trim(),
    redirect_uri:  redirectUri,
    scope:         'chat',
    state:         Math.random().toString(36).slice(2),
  });

  log('OAuth login, redirect_uri:', redirectUri);
  res.redirect(`${VK_AUTH_URL}?${params}`);
});

app.get('/auth/callback', async (req, res) => {
  const { code, error } = req.query;
  if (error || !code) {
    log('OAuth error from VK:', error);
    return res.redirect(`${FRONTEND_URL}?auth=error`);
  }

  try {
    const redirectUri = getRedirectUri(req);
    log('Token exchange, redirect_uri:', redirectUri);

    const tokenRes = await fetch(VK_TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type:    'authorization_code',
        code,
        redirect_uri:  redirectUri,
        client_id:     VK_CLIENT_ID.trim(),
        client_secret: VK_CLIENT_SECRET.trim(),
      }),
    });

    const tokenData = await tokenRes.json();
    log('Token response:', tokenRes.status, JSON.stringify(tokenData).slice(0, 200));

    if (!tokenRes.ok || !tokenData.access_token) {
      throw new Error(tokenData.error_description || tokenData.error || 'No access_token');
    }

    const accessToken = tokenData.access_token;

    const profileRes = await fetch(`${VK_API}/user/public`, {
      headers: { 'Authorization': `Bearer ${accessToken}` },
    });
    const profile = profileRes.ok ? await profileRes.json() : {};

    const sessionId = Math.random().toString(36).slice(2) + Date.now().toString(36);
    sessions.set(sessionId, {
      accessToken,
      displayName: profile.displayName || profile.nick || 'Пользователь',
      avatarUrl:   profile.avatarUrl || null,
      expiresAt:   Date.now() + (tokenData.expires_in || 3600) * 1000,
    });

    log('User logged in:', sessions.get(sessionId).displayName);
    connectVkWs(accessToken);
    res.redirect(`${FRONTEND_URL}?session=${sessionId}`);

  } catch(e) {
    log('OAuth callback error:', e.message);
    res.redirect(`${FRONTEND_URL}?auth=error&message=${encodeURIComponent(e.message)}`);
  }
});

app.get('/auth/me', (req, res) => {
  const sessionId = req.headers['x-session-id'] || req.query.session;
  const session   = sessions.get(sessionId);
  if (!session || session.expiresAt < Date.now()) {
    sessions.delete(sessionId);
    return res.status(401).json({ error: 'Сессия истекла' });
  }
  res.json({ displayName: session.displayName, avatarUrl: session.avatarUrl });
});

app.post('/auth/logout', (req, res) => {
  sessions.delete(req.headers['x-session-id']);
  res.json({ ok: true });
});

/* ── ОТПРАВКА ── */
app.post('/chat/send', async (req, res) => {
  const sessionId = req.headers['x-session-id'];
  const session   = sessions.get(sessionId);
  if (!session || session.expiresAt < Date.now()) {
    sessions.delete(sessionId);
    return res.status(401).json({ error: 'Не авторизован' });
  }
  const { text } = req.body;
  if (!text?.trim()) return res.status(400).json({ error: 'Пустое сообщение' });

  try {
    const r = await fetch(`${VK_API}/blog/${VK_CHANNEL}/chat/message`, {
      method:  'POST',
      headers: { 'Authorization': `Bearer ${session.accessToken}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ data: [{ type: 'text', content: [text.trim()] }] }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      if (r.status === 401) { sessions.delete(sessionId); return res.status(401).json({ error: 'Токен истёк' }); }
      return res.status(r.status).json({ error: err.message || 'Ошибка VK API' });
    }
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/* ── WEBSOCKET ── */
const wss = new WebSocket.Server({ server, path: '/ws' });
wss.on('connection', (ws, req) => {
  log('Client connected');
  clients.add(ws);
  ws.send(JSON.stringify({
    type:   'STATUS',
    status: vkWs?.readyState === WebSocket.OPEN ? 'connected' : 'connecting',
  }));
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

/* ── DEBUG — показывает сырой ответ VK API ── */
app.get('/debug/vkapi', async (req, res) => {
  try {
    const r = await fetch(`${VK_API}/blog/${VK_CHANNEL}/public_video_stream`);
    const data = await r.json();
    res.json({ status: r.status, data });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/* ── HEALTHCHECK ── */
app.get('/health', (req, res) => {
  res.json({
    ok:        true,
    channel:   VK_CHANNEL,
    clients:   clients.size,
    vkWs:      vkWs?.readyState === WebSocket.OPEN ? 'connected' : 'disconnected',
    clientId:  VK_CLIENT_ID ? '✓ set' : '✗ NOT SET',
    secret:    VK_CLIENT_SECRET ? '✓ set' : '✗ NOT SET',
  });
});

/* ── ЗАПУСК ── */
server.listen(PORT, () => {
  log(`Server on port ${PORT}`);
  log(`Channel: ${VK_CHANNEL}`);
  log(`Frontend: ${FRONTEND_URL}`);
  log(`Client ID: ${VK_CLIENT_ID ? '✓' : '✗ NOT SET'}`);
  log(`Client Secret: ${VK_CLIENT_SECRET ? '✓' : '✗ NOT SET'}`);
  connectVkWs(null);
});
