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

/* ── VK WebSocket ── */
async function getVkWsUrl(token) {
  const headers = token ? { 'Authorization': `Bearer ${token}` } : {};

  // Пробуем несколько эндпоинтов — VK меняет структуру ответа
  const endpoints = [
    `${VK_API}/blog/${VK_CHANNEL}/public_video_stream`,
    `${VK_API}/blog/${VK_CHANNEL}/channel`,
  ];

  for (const endpoint of endpoints) {
    try {
      const r = await fetch(endpoint, { headers });
      if (!r.ok) continue;
      const data = await r.json();
      log('VK API response from', endpoint, ':', JSON.stringify(data).slice(0, 400));

      // Ищем WS URL во всех известных местах структуры
      const candidates = [data, data?.data?.[0], data?.data, data?.channel, data?.stream]
        .filter(Boolean);

      for (const d of candidates) {
        const url = d?.chatSettings?.webSocketChannels?.chat
                 || d?.webSocketChannels?.chat
                 || d?.chatSettings?.chatUrl
                 || d?.wsUrl
                 || d?.websocketUrl
                 || null;
        if (url) return url;
      }
    } catch(e) {
      log('Endpoint error:', endpoint, e.message);
    }
  }

  // Если стрим офлайн — VK не даёт WS URL.
  // Возвращаем null вместо ошибки — будем повторять попытки тихо.
  log('WS URL not found — stream may be offline, will retry');
  return null;
}

async function connectVkWs(token) {
  if (vkRetryTimer) { clearTimeout(vkRetryTimer); vkRetryTimer = null; }
  if (vkWs) { try { vkWs.terminate(); } catch(e) {} vkWs = null; }

  const url = await getVkWsUrl(token).catch(e => { log('getVkWsUrl error:', e.message); return null; });

  if (!url) {
    // Стрим офлайн — повторяем через 30 сек, не показываем ошибку
    broadcast({ type: 'STATUS', status: 'offline', message: 'Стрим офлайн — чат будет доступен во время трансляции' });
    vkRetryTimer = setTimeout(() => connectVkWs(token), 30000);
    return;
  }

  vkWsUrl = url;
  if (token) wsUrl += (wsUrl.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(token);

  log('VK WS connecting:', wsUrl.split('?')[0]);
  broadcast({ type: 'STATUS', status: 'connecting' });

  vkWs = new WebSocket(wsUrl);

  vkWs.on('open', () => {
    log('VK WS connected');
    vkRetry = 0;
    broadcast({ type: 'STATUS', status: 'connected' });
  });

  vkWs.on('message', (raw) => {
    try { handleVkMessage(JSON.parse(raw.toString())); } catch(e) {}
  });

  vkWs.on('close', (code) => {
    log(`VK WS closed: ${code}`);
    broadcast({ type: 'STATUS', status: 'reconnecting' });
    scheduleReconnect(token);
  });

  vkWs.on('error', (e) => log('VK WS error:', e.message));
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
