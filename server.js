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

const VK_API      = 'https://api.live.vkvideo.ru/v1';
const VK_AUTH_URL = 'https://auth.live.vkvideo.ru/app/oauth2/authorize';

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
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
let vkRetry      = 0;
let vkRetryTimer = null;

function broadcast(payload) {
  const data = JSON.stringify(payload);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  }
}

/* ── VK Stream Data ── */
async function getVkStreamData(token) {
  const headers = { 'User-Agent': 'Mozilla/5.0' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const r = await fetch(`${VK_API}/blog/${VK_CHANNEL}/public_video_stream`, { headers });
  if (!r.ok) throw new Error(`VK API ${r.status}`);
  const data = await r.json();
  const chatChannel = data?.wsChatSlotChannel || data?.wsChatChannel;
  if (!chatChannel) throw new Error('wsChatChannel не найден');
  return { chatChannel, isOnline: !!data?.isOnline, viewers: data?.count?.viewers || 0 };
}

let lastStreamData = null;

async function refreshStreamData(token) {
  try {
    lastStreamData = await getVkStreamData(token);
    log('Stream:', lastStreamData.isOnline ? 'ONLINE' : 'OFFLINE',
        'channel:', lastStreamData.chatChannel, 'viewers:', lastStreamData.viewers);

    broadcast({
      type: 'STREAM_DATA',
      chatChannel: lastStreamData.chatChannel,
      isOnline:    lastStreamData.isOnline,
      viewers:     lastStreamData.viewers,
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
  vkRetryTimer = setTimeout(() => refreshStreamData(token), 30000);
}

async function connectVkWs(token) {
  if (vkRetryTimer) { clearTimeout(vkRetryTimer); vkRetryTimer = null; }
  await refreshStreamData(token);
}

/* ── Centrifugo токен ── */
app.get('/centrifugo/token', async (req, res) => {
  const sessionId = req.headers['x-session-id'];
  const session   = sessions.get(sessionId);
  const token     = session?.accessToken || null;

  try {
    const sd = await getVkStreamData(token);

    let wsToken = null;
    if (token) {
      // Пробуем GET /ws/connect
      try {
        const tr = await fetch(`${VK_API}/ws/connect`, {
          method: 'GET',
          headers: { 'Authorization': `Bearer ${token}` },
        });
        log('ws/connect status:', tr.status);
        if (tr.ok) {
          const td = await tr.json();
          log('ws/connect response:', JSON.stringify(td).slice(0, 200));
          wsToken = td?.token || td?.data?.token || td?.wsToken;
        }
      } catch(e) { log('ws/connect error:', e.message); }
    }

    res.json({ token: wsToken, channel: sd.chatChannel, isOnline: sd.isOnline });
  } catch(e) {
    log('centrifugo/token error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

/* ── OAUTH — Implicit Flow (response_type=token) ── */
app.get('/auth/login', (req, res) => {
  if (!VK_CLIENT_ID) return res.status(500).send('VK_CLIENT_ID не задан');

  const redirectUri = `https://${req.get('host')}/auth/callback`;
  const params = new URLSearchParams({
    response_type: 'token',          // Implicit flow — токен сразу в URL
    client_id:     VK_CLIENT_ID.trim(),
    redirect_uri:  redirectUri,
    state:         Math.random().toString(36).slice(2),
  });

  log('OAuth login, redirect_uri:', redirectUri);
  res.redirect(`${VK_AUTH_URL}?${params}`);
});

// Callback — читает токен из fragment через JS
app.get('/auth/callback', (req, res) => {
  const { error } = req.query;
  if (error) {
    log('OAuth error from VK:', error);
    return res.redirect(`${FRONTEND_URL}?auth=error`);
  }

  // Токен во фрагменте (#access_token=...) — браузер не передаёт его серверу
  // Отдаём HTML страницу которая читает fragment и делает запрос на /auth/token
  res.send(`<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body><script>
  var hash = location.hash.substring(1);
  var params = new URLSearchParams(hash);
  var token = params.get('access_token');
  var err = params.get('error');
  if (token) {
    location.href = '/auth/token?access_token=' + encodeURIComponent(token);
  } else {
    location.href = '${FRONTEND_URL}?auth=error&message=' + encodeURIComponent(err || 'no_token');
  }
</script></body></html>`);
});

// Принимаем токен и создаём сессию
app.get('/auth/token', async (req, res) => {
  const { access_token } = req.query;
  if (!access_token) return res.redirect(`${FRONTEND_URL}?auth=error`);

  log('Got access token (first 30):', access_token.slice(0, 30));

  try {
    // Получаем профиль
    const profileRes = await fetch(`${VK_API}/user/public`, {
      headers: { 'Authorization': `Bearer ${access_token}` },
    });
    log('Profile status:', profileRes.status);
    const profile = profileRes.ok ? await profileRes.json() : {};
    log('Profile:', JSON.stringify(profile).slice(0, 200));

    const sessionId = Math.random().toString(36).slice(2) + Date.now().toString(36);
    sessions.set(sessionId, {
      accessToken: access_token,
      displayName: profile.displayName || profile.nick || profile.name || 'Пользователь',
      avatarUrl:   profile.avatarUrl || null,
      expiresAt:   Date.now() + 3600 * 1000,
    });

    log('User logged in:', sessions.get(sessionId).displayName);
    connectVkWs(access_token);
    res.redirect(`${FRONTEND_URL}?session=${sessionId}`);
  } catch(e) {
    log('Token handler error:', e.message);
    res.redirect(`${FRONTEND_URL}?auth=error`);
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

/* ── DEBUG ── */
app.get('/debug/vkapi', async (req, res) => {
  try {
    const r = await fetch(`${VK_API}/blog/${VK_CHANNEL}/public_video_stream`);
    const data = await r.json();
    res.json({ status: r.status, data });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ── WEBSOCKET ── */
const wss = new WebSocket.Server({ server, path: '/ws' });
wss.on('connection', (ws, req) => {
  log('Client connected');
  clients.add(ws);
  ws.send(JSON.stringify({ type: 'STATUS', status: 'connecting' }));
  // Отправляем данные стрима если есть
  if (lastStreamData) {
    ws.send(JSON.stringify({
      type: 'STREAM_DATA',
      chatChannel: lastStreamData.chatChannel,
      isOnline:    lastStreamData.isOnline,
      viewers:     lastStreamData.viewers,
    }));
  }
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

/* ── HEALTHCHECK ── */
app.get('/health', (req, res) => {
  res.json({
    ok:       true,
    channel:  VK_CHANNEL,
    clients:  clients.size,
    clientId: VK_CLIENT_ID ? '✓' : '✗',
    secret:   VK_CLIENT_SECRET ? '✓' : '✗',
    stream:   lastStreamData?.isOnline ? 'online' : 'offline',
  });
});

/* ── ЗАПУСК ── */
server.listen(PORT, () => {
  log(`Server on port ${PORT}`);
  log(`Channel: ${VK_CHANNEL}`);
  log(`Frontend: ${FRONTEND_URL}`);
  connectVkWs(null);
});
