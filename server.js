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

/* ── VK Stream Data ── 
   Centrifugo недоступен с Railway (гео/DNS блокировка).
   Решение: сервер только получает chatChannel из VK API
   и отдаёт его браузеру. Браузер сам подключается к Centrifugo.
*/
async function getVkStreamData(token) {
  const headers = { 'User-Agent': 'Mozilla/5.0' };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const r = await fetch(`${VK_API}/blog/${VK_CHANNEL}/public_video_stream`, { headers });
  if (!r.ok) throw new Error(`VK API ${r.status}`);
  const data = await r.json();

  const chatChannel = data?.wsChatSlotChannel || data?.wsChatChannel;
  if (!chatChannel) throw new Error('wsChatChannel не найден');

  return {
    chatChannel,
    isOnline: !!data?.isOnline,
    viewers:  data?.count?.viewers || 0,
  };
}

// Храним последние данные стрима
let lastStreamData = null;

async function refreshStreamData(token) {
  try {
    lastStreamData = await getVkStreamData(token);
    log('Stream data:', lastStreamData.isOnline ? 'ONLINE' : 'OFFLINE', 
        'channel:', lastStreamData.chatChannel,
        'viewers:', lastStreamData.viewers);
    
    // Отправляем channel клиентам чтобы они могли подключиться сами
    broadcast({ 
      type: 'STREAM_DATA', 
      chatChannel: lastStreamData.chatChannel,
      isOnline: lastStreamData.isOnline,
      viewers: lastStreamData.viewers,
    });

    if (lastStreamData.isOnline) {
      broadcast({ type: 'STATUS', status: 'connected' });
    } else {
      broadcast({ type: 'STATUS', status: 'offline', message: 'Стрим офлайн' });
    }
  } catch(e) {
    log('refreshStreamData error:', e.message);
    broadcast({ type: 'STATUS', status: 'error', message: e.message });
  }
  // Обновляем каждые 30 сек
  vkRetryTimer = setTimeout(() => refreshStreamData(token), 30000);
}

// Stub для обратной совместимости
async function connectVkWs(token) {
  if (vkRetryTimer) { clearTimeout(vkRetryTimer); vkRetryTimer = null; }
  await refreshStreamData(token);
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

/* ── Токен для Centrifugo (браузер запрашивает перед подключением) ── */
app.get('/centrifugo/token', async (req, res) => {
  const sessionId = req.headers['x-session-id'];
  const session   = sessions.get(sessionId);
  const token     = session?.accessToken || null;

  try {
    const headers = { 'User-Agent': 'Mozilla/5.0' };
    if (token) headers['Authorization'] = `Bearer ${token}`;

    const r = await fetch(`${VK_API}/blog/${VK_CHANNEL}/public_video_stream`, { headers });
    if (!r.ok) throw new Error('VK API ' + r.status);
    const data = await r.json();
    const channel = data?.wsChatSlotChannel || data?.wsChatChannel;

    // Пробуем получить WS JWT токен
    let wsToken = null;
    if (token) {
      try {
        const tr = await fetch(`${VK_API}/ws/connect`, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ channels: [channel] }),
        });
        log('ws/connect status:', tr.status);
        if (tr.ok) {
          const td = await tr.json();
          log('ws/connect response:', JSON.stringify(td).slice(0, 300));
          wsToken = td?.token || td?.data?.token || td?.wsToken || td?.accessToken;
        }
      } catch(e) { log('ws/connect error:', e.message); }
    }

    res.json({ token: wsToken, channel, isOnline: !!data?.isOnline });
  } catch(e) {
    log('centrifugo/token error:', e.message);
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
